# Author: LIM UI JIN
# Created: 2026-10-23

"""Bounded, best-effort remote failure excerpts; never a complete log mirror."""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlencode

MAX_SECONDS = 20.0
MAX_PAGES = 2  # per API
PAGE_SIZE = 100
MAX_BYTES = 131072
MAX_LINE_BYTES = 8192
MAX_RESPONSE_BYTES = 262144
SOURCE_BYTES = MAX_BYTES // 3
OOM_SIGNAL_FILTER = '(' + ' OR '.join(json.dumps(term) for term in (
    'Successfully wrote OOM event', 'OutOfMemoryError', 'MemoryError', 'OOMKilled',
    'Java heap space', 'GC overhead limit exceeded', 'GC thrashing',
    'Memory cgroup out of memory', 'oom-kill', 'Killed process',
    'SDK harness disconnected', 'SDK harness connection lost',
    'SDK harness is disconnected', 'worker lost contact',
)) + ')'



def redact(value, secrets=()):
    text = str(value)
    text = re.sub(r"(?is)-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----", "[REDACTED KEY]", text)
    text = re.sub(r"(?i)(bearer\s+)[^\s\"',;]+", r"\1[REDACTED]", text)
    text = re.sub(r'''(?ix)((?:["']?)(?:password|passwd|pwd|access_token|refresh_token|id_token|client_secret|authorization|api_key|secret|token)(?:["']?)\s*[:=]\s*)(?:"[^"\n]*"|'[^'\n]*'|[^\s,;&}]+)''', r"\1[REDACTED]", text)
    text = re.sub(r'''(?i)(--(?:password|passwd|token|client-secret|api-key)\s+)(?:"[^"\n]*"|'[^'\n]*'|[^\s]+)''', r"\1[REDACTED]", text)
    # URLs can carry credentials in multiple JDBC dialects; omit the whole URL.
    text = re.sub(r"(?i)jdbc:[^\s\"']+", "[REDACTED JDBC URL]", text)
    text = re.sub(r"(https?://)[^\s/@]+:[^\s/@]+@", r"\1[REDACTED]@", text)
    text = re.sub(r"\bya29\.[A-Za-z0-9_.-]+|\bAIza[A-Za-z0-9_-]{20,}|\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", "[REDACTED TOKEN]", text)
    for secret in sorted((str(s) for s in secrets if s), key=len, reverse=True):
        text = text.replace(secret, "[REDACTED]")
    return text


def _stamp(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def collect_failure_diagnostics(*, logger, credentials, project_id, region, job_id,
                                job_name, launch_time, secrets=(), session_factory=None,
                                monotonic=time.monotonic, now=None):
    """No exception may replace the caller's original failure. No job-name broad search."""
    session = None
    try:
        deadline = monotonic() + MAX_SECONDS
        now = now or datetime.now(timezone.utc)
        start = launch_time - timedelta(seconds=30)
        project = quote(str(project_id), safe="")
        region_path = quote(str(region), safe="")
        console = f"https://console.cloud.google.com/dataflow/jobs/{region_path}"
        if job_id:
            console += "/" + quote(str(job_id), safe="")
        console += "?" + urlencode({"project": project_id})
        logger.warning("Dataflow failure diagnostics (bounded excerpt, not complete): %s", console)
        if not job_id:
            logger.warning("No verified Dataflow job ID for %s; skipping log lookup to avoid mixing concurrent launches. Inspect launcher logs in Dataflow/Cloud Logging.", redact(job_name, secrets))
            return
        query = '\n'.join([
            'resource.type="dataflow_step"',
            'resource.labels.project_id=' + json.dumps(str(project_id)),
            'resource.labels.region=' + json.dumps(str(region)),
            'resource.labels.job_id=' + json.dumps(str(job_id)),
            'timestamp>=' + json.dumps(_stamp(start)),
            'timestamp<=' + json.dumps(_stamp(now + timedelta(seconds=15))),
        ])
        logger.warning("Full Cloud Logging query: https://console.cloud.google.com/logs/query?%s", urlencode({"project": project_id, "query": query}))
        if session_factory is None:
            from google.auth.transport.requests import AuthorizedSession
            session_factory = lambda creds: AuthorizedSession(creds, refresh_timeout=5, max_refresh_attempts=0)
        session = session_factory(credentials)
        seen = set()
        used = 0
        source_used = 0

        def emit(source, value):
            nonlocal used, source_used
            rendered = redact(value, secrets)
            if rendered in seen or source_used >= SOURCE_BYTES:
                return
            seen.add(rendered)
            raw = rendered.encode("utf-8")
            limit = min(MAX_LINE_BYTES, SOURCE_BYTES - source_used)
            truncated = len(raw) > limit
            rendered = raw[:limit].decode("utf-8", errors="ignore")
            used += min(len(raw), limit)
            source_used += min(len(raw), limit)
            logger.warning("[Dataflow %s] %s%s", source, rendered, " [truncated]" if truncated else "")

        def request(method, url, **kwargs):
            remaining = min(deadline, source_deadline) - monotonic()
            if remaining <= 0 or source_used >= SOURCE_BYTES:
                raise TimeoutError("diagnostic budget exhausted")
            result = session.request(method, url, timeout=min(5.0, remaining),
                                     max_allowed_time=remaining, stream=True, **kwargs)
            try:
                result.raise_for_status()
                chunks = []
                size = 0
                for chunk in result.iter_content(chunk_size=8192):
                    size += len(chunk)
                    if size > MAX_RESPONSE_BYTES or monotonic() >= min(deadline, source_deadline):
                        raise TimeoutError("diagnostic response budget exhausted")
                    chunks.append(chunk)
                return json.loads(b"".join(chunks))
            finally:
                result.close()

        # Preserve sections 7/8 and reserve a bounded newest-first OOM signal excerpt.
        sources = (
            ("7 ALL Job Logs (bounded)", "", "timestamp asc"),
            ("8 JDBC QUERY RESULT", '(textPayload:"JDBC QUERY RESULT" OR jsonPayload.message:"JDBC QUERY RESULT")', "timestamp asc"),
            ("9 OOM GC SDK signal candidates (not diagnoses or event counts)", OOM_SIGNAL_FILTER, "timestamp desc"),
        )
        for source, extra_filter, order_by in sources:
            token = None
            source_used = 0
            source_deadline = min(deadline, monotonic() + MAX_SECONDS / len(sources))
            # Section 8 intentionally repeats result rows also present in section 7.
            seen.clear()
            if extra_filter == OOM_SIGNAL_FILTER:
                logger.warning("OOM/GC/SDK matches are context candidates, not unique OOM events. INFO detector markers, negated text and repeated attempts require investigation; no kernel cause is inferred.")
            entries_found = 0
            logger.warning("===== Dataflow [%s] =====", source)
            section_query = query + ("\n" + extra_filter if extra_filter else "")
            logger.warning("Dataflow [%s] complete query: https://console.cloud.google.com/logs/query?%s", source,
                           urlencode({"project": project_id, "query": section_query}))
            try:
                for _ in range(MAX_PAGES):
                    body = {"resourceNames": [f"projects/{project_id}"], "filter": section_query,
                            "orderBy": order_by, "pageSize": PAGE_SIZE}
                    if token:
                        body["pageToken"] = token
                    data = request("POST", "https://logging.googleapis.com/v2/entries:list", json=body)
                    entries = data.get("entries", [])[:PAGE_SIZE]
                    for entry in entries:
                        payload = entry.get("textPayload")
                        if payload is None:
                            payload = entry.get("jsonPayload", {}).get("message", "")
                        prefix = f"{entry.get('timestamp', '')} {entry.get('severity', '')} {entry.get('logName', '')}"
                        emit(source, f"{prefix} {payload}")
                    entries_found += len(entries)
                    token = data.get("nextPageToken")
                    if not token or source_used >= SOURCE_BYTES:
                        break
                logger.warning("Dataflow [%s]: fetched=%s; next_page=%s; output_limit=%s. Empty results may reflect delayed logging; this is a bounded snapshot.", source, entries_found, bool(token), source_used >= SOURCE_BYTES)
            except Exception as exc:
                # Do not print response bodies, request URLs, or potentially secret exceptions.
                logger.warning("Dataflow %s diagnostics unavailable (%s); original failure preserved.", source, type(exc).__name__)
        logger.warning("Dataflow diagnostic excerpt finished: %s payload bytes. Logs may be delayed, omitted, or truncated; use the links for complete investigation.", used)
    except Exception as exc:
        logger.warning("Dataflow diagnostics unavailable (%s); original failure preserved.", type(exc).__name__)
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                pass
