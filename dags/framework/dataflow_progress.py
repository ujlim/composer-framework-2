# Author: 임의진 (LIM UI JIN)
# Created: 2026-10-30
# Updated: 2026-10-30
"""Bounded, fail-open Dataflow observations, never a source-row-count estimator."""
from __future__ import annotations

import json
import math
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote

from framework.dataflow_diagnostics import redact

MAX_SECONDS = 12.0
MAX_RESPONSE_BYTES = 524288
MAX_STAGES = 50
MAX_METRICS = 40
MAX_WORK_ITEMS = 10


def validate_progress_interval(value):
    if type(value) is not int or value < 60:
        raise ValueError('options.progress_log_interval_seconds must be an integer >= 60')
    return value


def _time(value):
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def _elapsed(start, end, state, now):
    start = _time(start)
    # API endTime may be a prediction for unfinished stages.
    end = _time(end) if state in {'EXECUTION_STATE_COMPLETE', 'EXECUTION_STATE_FAILED'} else now
    if start is None or end is None or start.tzinfo is None or end.tzinfo is None:
        return None
    return max(0.0, (end - start).total_seconds())


def _metrics(values, source, stage_id=None):
    """Keep identities and tentative variants separate; never sum across boundaries."""
    result, seen = [], set()
    candidates = [m for m in (values or []) if isinstance(m, dict)]
    candidates.sort(key=lambda m: 'post_jdbc_output_rows' not in str(m.get('name', {})))
    for metric in candidates:
        if not isinstance(metric, dict):
            continue
        name = metric.get('name') or {}
        if not isinstance(name, dict):
            continue
        label = str(name.get('name', ''))
        if not re.search(r'element.?count|byte|post_jdbc|rows_emitted', label, re.I):
            continue
        context = name.get('context') or {}
        if not isinstance(context, dict):
            context = {}
        identity = json.dumps([name.get('origin'), label, context], sort_keys=True)
        if identity in seen:
            continue
        seen.add(identity)
        scalar = metric.get('scalar')
        if type(scalar) not in (int, float) or not math.isfinite(scalar):
            scalar = None
        result.append(dict(source=source, stage_id=stage_id, name=label,
                           origin=name.get('origin'), context=context,
                           tentative=context.get('tentative') == 'true',
                           tentative_field=context.get('tentative'),
                           kind=metric.get('kind'), cumulative=metric.get('cumulative'),
                           scalar=scalar, unit='unknown', update_time=metric.get('updateTime')))
        if len(result) >= MAX_METRICS:
            break
    return result


class ProgressReporter:
    """Independent read-only observer using the existing launch ADC credentials."""
    def __init__(self, *, logger, credentials, project_id, region, job_id,
                 dag_id, run_id, task_id, interval=60, secrets=(),
                 session_factory=None, monotonic=time.monotonic, now=None, details=False):
        if type(details) is not bool:
            raise ValueError("options.progress_log_details must be a boolean")
        self.details = details
        self.last_job_state = None
        self.logger = logger
        self.credentials = credentials
        self.project_id, self.region, self.job_id = project_id, region, job_id
        self.identity = dict(dag_id=dag_id, run_id=run_id, task_id=task_id,
                             job_id=job_id, observer_pid=os.getpid(), observer='composer')
        self.interval = validate_progress_interval(interval)
        self.secrets = secrets
        self.session_factory = session_factory
        self.clock = monotonic
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.last_poll = None
        self.last_snapshot = None
        self.previous_states = {}
        self.first_seen = {}
        self.stage_names = {}
        self.description_checked = False

    def job_state(self, state):
        """Emit state transitions only; status polling frequency is unchanged."""
        state = state or 'unknown'
        if state != self.last_job_state:
            previous = self.last_job_state
            self.last_job_state = state
            self.emit('job_state_changed', job_state=state, previous_state=previous)

    def emit(self, event, **fields):
        try:
            summary = self._summary(event, fields)
            safe = redact(summary, self.secrets)
            # Bound UTF-8 after redaction, including known-secret expansion.
            safe = safe.encode('utf-8')[:2400].decode('utf-8', 'ignore')
            method = self.logger.warning if event in {'observer_unavailable', 'job_state_unavailable'} else self.logger.info
            method('Dataflow progress %s', safe)
        except Exception:
            pass
        if self.details:
            self._emit_detail(event, **fields)

    def _summary(self, event, fields):
        def short(value, limit=80):
            return re.sub(r'[\x00-\x1f\x7f]+', ' ', str(value))[:limit]
        state_names = {
            'JOB_STATE_RUNNING':'실행 중', 'JOB_STATE_DONE':'완료', 'JOB_STATE_FAILED':'실패',
            'JOB_STATE_PENDING':'시작 대기', 'JOB_STATE_QUEUED':'대기열',
            'JOB_STATE_CANCELLED':'취소', 'JOB_STATE_DRAINED':'드레인 종료',
            'JOB_STATE_UPDATED':'다른 Job으로 교체', 'unknown':'미확인',
            'EXECUTION_STATE_RUNNING':'실행 중', 'EXECUTION_STATE_COMPLETE':'완료',
            'EXECUTION_STATE_FAILED':'실패', 'EXECUTION_STATE_NOT_STARTED':'시작 전',
        }
        snapshot = fields.get('snapshot') or {}
        state = fields.get('job_state') or fields.get('state') or snapshot.get('job_state') or 'unknown'
        labels = {'job_started':'시작', 'snapshot':'진행', 'final_snapshot':'최종 관측',
                  'job_state_changed':'상태 변경', 'observer_unavailable':'관측 오류',
                  'job_state_unavailable':'상태 조회 오류'}
        parts = [f"[{labels.get(event, '안내')}] Job={short(self.job_id,128)} 작업={short(self.identity['task_id'],60)}",
                 '상태=' + state_names.get(state, short(state,60))]
        if event == 'job_started':
            parts += ['머신=' + short(fields.get('machine_type_configured', '서비스 기본값')).replace('<service-default>', '서비스 기본값'),
                      '적재방식=' + short(fields.get('write_method', '미확인')),
                      '원천읽기/BQ확정적재=미확인(후단 0도 미읽음을 뜻하지 않음)']
        elif event in {'observer_unavailable', 'job_state_unavailable'}:
            parts += ['조회 실패=' + short(fields.get('error_type', '미확인')),
                      '실제 작업 실패 여부는 별도 확인; 자동 재시도로 단정하지 않음']
        elif event == 'job_state_changed':
            previous = fields.get('previous_state')
            if previous is not None:
                parts += ['이전=' + state_names.get(previous, short(previous,60))]
        elif event in {'snapshot', 'final_snapshot'}:
            parts += ['원천읽기/BQ확정적재=미확인']
            stages = snapshot.get('stages') or []
            if stages:
                counts = {key:sum(s.get('state') == key for s in stages) for key in
                          ['EXECUTION_STATE_RUNNING','EXECUTION_STATE_COMPLETE','EXECUTION_STATE_FAILED']}
                count_text = ', '.join(label + ' ' + str(counts[key]) for key, label in
                    [('EXECUTION_STATE_RUNNING','실행'),('EXECUTION_STATE_COMPLETE','완료'),('EXECUTION_STATE_FAILED','실패')] if counts[key])
                others = len(stages) - sum(counts.values())
                if others:
                    count_text += (', ' if count_text else '') + f'기타/미확인 {others}'
                parts += [f"관측 단계 {len(stages)}개(전체 보장 아님): {count_text}"]
                running = [s for s in stages if s.get('state') == 'EXECUTION_STATE_RUNNING']
                if running:
                    def active(s):
                        elapsed = s.get('elapsed_seconds')
                        duration = f'{int(elapsed)}초' if type(elapsed) in (int,float) and math.isfinite(elapsed) else '미확인'
                        return short(s.get('stage_id'),24) + '(경과 ' + duration + ')'
                    parts += ['실행 단계=' + ', '.join(active(s) for s in running[:3]) + (' 외 생략' if len(running)>3 else '')]
                changes = [s for s in stages if s.get('state_changed')]
                if changes:
                    parts += ['단계 변경=' + ', '.join(short(s.get('stage_id'),24) + ':' + state_names.get(s.get('state'), short(s.get('state','unknown').replace('EXECUTION_STATE_',''),18)) for s in changes[:3]) + (' 외 생략' if len(changes)>3 else '')]
            else:
                parts += ['단계=미확인']
            # One job-level counter identity only. Never sum tentative/committed or transforms.
            metrics = [m for m in (snapshot.get('metrics') or []) if m.get('name') == 'post_jdbc_output_rows' and m.get('scalar') is not None]
            identities = {json.dumps([m.get('origin'), {k:v for k,v in (m.get('context') or {}).items() if k != 'tentative'}],sort_keys=True) for m in metrics}
            if len(identities) == 1:
                selected = next((m for m in metrics if not m.get('tentative')), metrics[0])
                mark = 'tentative' if selected.get('tentative') else 'tentative 아님'
                parts += [f"JDBC 후단 관측={selected['scalar']} ({mark}; BQ 적재건수 아님)"]
            elif len(identities) > 1:
                parts += ['JDBC 후단 관측=복수 시리즈(합산 안 함)']
            else:
                parts += ['JDBC 후단 관측=미확인']
            missing = snapshot.get('unavailable') or []
            if missing or snapshot.get('partial'):
                parts += [f'일부 관측 누락(API {len(missing)}개); 상세는 진단옵션']
            if fields.get('cached'):
                age = fields.get('snapshot_age_seconds')
                parts += ['캐시 관측(현재 최종값 아님), 경과=' + (str(int(age))+'초' if type(age) in (int,float) and math.isfinite(age) else '미확인')]
            if snapshot is None or not snapshot:
                parts += ['관측자료 없음']
        return ' | '.join(parts)

    def _emit_detail(self, event, **fields):
        try:
            payload = dict(event=event, **self.identity, **fields)
            if len(json.dumps(payload, default=str)) > 48000:
                snapshot = dict(payload.get('snapshot') or {})
                snapshot['metrics'] = []
                snapshot['stages'] = [dict(s, metrics=[]) for s in snapshot.get('stages', [])[:20]]
                snapshot['worker_sample'] = snapshot.get('worker_sample', [])[:5]
                snapshot['partial'] = True
                snapshot['log_truncated'] = True
                payload['snapshot'] = snapshot
            if len(json.dumps(payload, default=str)) > 48000:
                payload = dict(event=event, **self.identity, log_truncated=True,
                               observation='unknown; snapshot exceeded log budget')
            # Redact before formatting into logs. Do not emit raw API errors/bodies.
            rendered = redact(json.dumps(payload, default=str, ensure_ascii=False), self.secrets)
            if len(rendered.encode('utf-8')) > 48000:
                rendered = json.dumps(dict(
                    event=str(event)[:64], job_id=str(self.job_id)[:256],
                    log_truncated=True, observation='unknown; snapshot exceeded byte budget'),
                    ensure_ascii=False)
                rendered = redact(rendered, self.secrets)
            self.logger.info('Dataflow progress detail %s', rendered)
        except Exception:
            pass

    def poll(self, job_state=None, final=False):
        try:
            return self._poll(job_state, final)
        except Exception as exc:
            self.emit('observer_unavailable', error_type=type(exc).__name__, final=final,
                      observation='unknown', job_state=job_state)
            if final:
                self.emit('final_snapshot', snapshot=self.last_snapshot, cached=True, job_state=job_state,
                          observation='unknown' if self.last_snapshot is None else 'last_observed')
            return None

    def _poll(self, job_state, final):
        tick = self.clock()
        if self.last_poll is not None and tick - self.last_poll < self.interval:
            if final:
                self.emit('final_snapshot', snapshot=self.last_snapshot, cached=True,
                          snapshot_age_seconds=tick-self.last_poll, job_state=job_state)
            return self.last_snapshot
        self.last_poll = tick
        deadline = tick + MAX_SECONDS
        session = None
        snapshot = dict(observed_at=self.now().isoformat(), job_state=job_state,
                        stages=[], metrics=[], worker_sample=[], unavailable=[], partial=False,
                        memory_usage='unknown', source_jdbc_rows='unknown',
                        limitation='Stage/PCollection observations are not Oracle cursor counts; '
                                   'zero downstream output does not prove no JDBC reads. '
                                   'Unchanged observations do not establish a stall or retry.')
        try:
            if self.session_factory is None:
                from google.auth.transport.requests import AuthorizedSession
                session = AuthorizedSession(self.credentials, refresh_timeout=3, max_refresh_attempts=0)
            else:
                session = self.session_factory(self.credentials)
            base = ('https://dataflow.googleapis.com/v1b3/projects/' + quote(str(self.project_id), safe='')
                    + '/locations/' + quote(str(self.region), safe='')
                    + '/jobs/' + quote(str(self.job_id), safe=''))

            def get(path, **params):
                remaining = deadline - self.clock()
                if remaining <= 0:
                    raise TimeoutError('observer budget')
                response = session.request('GET', base + path, params=params,
                                           timeout=min(3.0, remaining), max_allowed_time=remaining, stream=True)
                try:
                    response.raise_for_status()
                    chunks, size = [], 0
                    for chunk in response.iter_content(chunk_size=8192):
                        size += len(chunk)
                        if size > MAX_RESPONSE_BYTES or self.clock() >= deadline:
                            raise TimeoutError('observer response budget')
                        chunks.append(chunk)
                    result = json.loads(b''.join(chunks))
                    if not isinstance(result, dict):
                        raise ValueError('invalid API object')
                    return result
                finally:
                    response.close()

            def unavailable(source, exc):
                snapshot['unavailable'].append(dict(source=source, error_type=type(exc).__name__))

            if not self.description_checked:
                self.description_checked = True
                try:
                    description = get('', view='JOB_VIEW_DESCRIPTION')
                    for stage in ((description.get('pipelineDescription') or {}).get('executionPipelineStage') or [])[:MAX_STAGES]:
                        sid = stage.get('id')
                        if sid is not None:
                            self.stage_names[sid] = dict(name=stage.get('name'),
                                transforms=[t.get('userName') for t in (stage.get('componentTransform') or [])[:10]],
                                prerequisites=stage.get('prerequisiteStage') or [])
                except Exception as exc:
                    unavailable('getJobDescription', exc)
            try:
                token = None
                for _ in range(2):
                    params = {'pageSize': MAX_STAGES}
                    if token:
                        params['pageToken'] = token
                    details = get('/executionDetails', **params)
                    for stage in details.get('stages') or []:
                        if not isinstance(stage, dict):
                            continue
                        sid = stage.get('stageId')
                        state = stage.get('state') or 'unknown'
                        self.first_seen.setdefault(sid, tick)
                        prior = self.previous_states.get(sid)
                        self.previous_states[sid] = state
                        snapshot['stages'].append(dict(
                            stage_id=sid, mapped_name=self.stage_names.get(sid, {}).get('name') or 'unknown',
                            stage_mapping=self.stage_names.get(sid), state=state,
                            previous_state=prior, state_changed=prior is not None and prior != state,
                            start_time=stage.get('startTime'), end_time_reported=stage.get('endTime'),
                            elapsed_seconds=_elapsed(stage.get('startTime'), stage.get('endTime'), state, self.now()),
                            observer_seen_seconds=tick-self.first_seen[sid],
                            progress=(stage.get('progress') or {}).get('currentProgress'),
                            metrics=_metrics(stage.get('metrics'), 'getExecutionDetails.stage.metrics', sid)[:4]))
                        if len(snapshot['stages']) >= MAX_STAGES:
                            snapshot['partial'] = True
                            break
                    token = details.get('nextPageToken')
                    if not token or len(snapshot['stages']) >= MAX_STAGES:
                        break
                if token:
                    snapshot['partial'] = True
            except Exception as exc:
                unavailable('getExecutionDetails', exc)
            try:
                metrics = get('/metrics')
                snapshot['metric_time'] = metrics.get('metricTime')
                snapshot['metrics'] = _metrics(metrics.get('metrics'), 'getMetrics')
                snapshot['metric_selection'] = 'element/byte/post-JDBC scalar subset; no totals; max40 per source'
            except Exception as exc:
                unavailable('getMetrics', exc)
            # At most one running stage / one page: a sample, not active-worker totals.
            running = next((s for s in snapshot['stages'] if s['state'] == 'EXECUTION_STATE_RUNNING' and s['stage_id']), None)
            if running:
                try:
                    details = get('/stages/' + quote(str(running['stage_id']), safe='') + '/executionDetails', pageSize=5)
                    for worker in (details.get('workers') or [])[:5]:
                        for item in (worker.get('workItems') or [])[:MAX_WORK_ITEMS]:
                            snapshot['worker_sample'].append(dict(
                                stage_id=running['stage_id'], worker_name=worker.get('workerName'),
                                task_id=item.get('taskId'), attempt_id=item.get('attemptId'),
                                state=item.get('state'), start_time=item.get('startTime'), end_time=item.get('endTime')))
                    snapshot['worker_sample_scope'] = 'one running stage; max5 workers/max10 work-items each; not active-worker count'
                    snapshot['worker_sample_partial'] = bool(details.get('nextPageToken'))
                except Exception as exc:
                    unavailable('stage.getExecutionDetails', exc)
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass
        self.last_snapshot = snapshot
        self.emit('final_snapshot' if final else 'snapshot', snapshot=snapshot, cached=False)
        return snapshot
