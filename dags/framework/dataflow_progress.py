# Author: LIM UI JIN
# Created: 2026-10-30
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
        if not re.search(r'element|byte|post_jdbc|rows_emitted', label, re.I):
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
                 session_factory=None, monotonic=time.monotonic, now=None):
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

    def emit(self, event, **fields):
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
            self.logger.info('Dataflow progress %s', rendered)
        except Exception:
            pass

    def poll(self, job_state=None, final=False):
        try:
            return self._poll(job_state, final)
        except Exception as exc:
            self.emit('observer_unavailable', error_type=type(exc).__name__, final=final,
                      observation='unknown', job_state=job_state)
            if final:
                self.emit('final_snapshot', snapshot=self.last_snapshot, cached=True,
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
