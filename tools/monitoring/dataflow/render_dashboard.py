#!/usr/bin/env python3
# Author: LIM UI JIN
# Created: 2026-10-30
"""Render/validate a reviewed dashboard subset locally. No network or GCP writes."""
import argparse
import json
from pathlib import Path
import re

DEFAULT_TEMPLATE = Path(__file__).with_name('dashboard.template.json')
METRICS = {
    'job/current_num_vcpus', 'job/is_failed', 'job/element_count',
    'job/estimated_byte_count', 'job/user_counter', 'job/total_shuffle_data_processed',
    'worker/memory/bytes_used', 'worker/memory/container_limit',
}
PLACEHOLDER = re.compile(r'__[A-Z][A-Z0-9_]*__')


def validate(document):
    """Strict local checks for this template, NOT the complete Google API schema."""
    if not isinstance(document, dict) or not isinstance(document.get('displayName'), str):
        raise ValueError('dashboard requires displayName')
    if set(document) - {'displayName', 'mosaicLayout', 'name', 'etag'}:
        raise ValueError('unsupported top-level key')
    if PLACEHOLDER.search(json.dumps(document)):
        raise ValueError('unresolved placeholder')
    layout = document.get('mosaicLayout', {})
    columns = layout.get('columns')
    if type(columns) is not int or not 1 <= columns <= 48:
        raise ValueError('columns must be 1..48')
    if not isinstance(layout.get('tiles'), list) or not layout['tiles']:
        raise ValueError('tiles required')
    rectangles = []
    for tile in layout['tiles']:
        for key in ('xPos', 'yPos', 'width', 'height'):
            if type(tile.get(key)) is not int:
                raise ValueError('tile geometry must be integer')
        x,y,w,h = [tile[k] for k in ('xPos','yPos','width','height')]
        if min(x,y) < 0 or min(w,h) <= 0 or x+w > columns:
            raise ValueError('invalid tile bounds')
        for a,b,c,d in rectangles:
            if x<a+c and a<x+w and y<b+d and b<y+h:
                raise ValueError('overlapping tiles')
        rectangles.append((x,y,w,h))
        widget = tile.get('widget', {})
        kinds = [k for k in ('text', 'xyChart', 'logsPanel') if k in widget]
        if len(kinds) != 1 or not isinstance(widget.get('title'), str):
            raise ValueError('one supported widget type and title required')
        if kinds[0] == 'text':
            if widget['text'].get('format') != 'MARKDOWN' or not isinstance(widget['text'].get('content'), str):
                raise ValueError('invalid text widget')
        elif kinds[0] == 'logsPanel':
            panel = widget['logsPanel']
            if not panel.get('filter') or not panel.get('resourceNames'):
                raise ValueError('logs must have a filter and project scope')
            if any(not re.fullmatch(r'projects/[a-z][a-z0-9-]{4,28}[a-z0-9]', name) for name in panel['resourceNames']):
                raise ValueError('invalid logging project scope')
        else:
            for dataset in widget['xyChart'].get('dataSets', []):
                series = dataset.get('timeSeriesQuery', {}).get('timeSeriesFilter', {})
                filter_ = series.get('filter', '')
                match = re.search(r'metric.type="dataflow.googleapis.com/([^"]+)"', filter_)
                if not match or match.group(1) not in METRICS:
                    raise ValueError('metric not in reviewed allowlist')
                worker = match.group(1).startswith('worker/')
                required = ['resource.labels.project_id=', 'resource.labels.job_name=']
                if worker:
                    required += ['resource.type="dataflow_worker"', 'resource.labels.job_id=']
                else:
                    required += ['resource.type="dataflow_job"', 'resource.labels.region=']
                    if 'resource.labels.job_id=' in filter_:
                        raise ValueError('dataflow_job has no job_id resource label')
                    if match.group(1) != 'job/is_failed':
                        required += ['metric.labels.job_id=']
                    elif 'metric.labels.job_id=' in filter_:
                        raise ValueError('is_failed has no documented job_id metric label')
                if any(token not in filter_ for token in required):
                    raise ValueError('missing metric scope')
                aggregation = series.get('aggregation', {})
                if aggregation != {'alignmentPeriod':'60s','perSeriesAligner':'ALIGN_MAX'}:
                    raise ValueError('keep gauge samples separate; only reviewed ALIGN_MAX allowed')
    return document


def render(*, project_id, region, job_id, job_name, template=DEFAULT_TEMPLATE,
           existing_dashboard=None, project_number=None):
    patterns = {'PROJECT_ID':r'[a-z][a-z0-9-]{4,28}[a-z0-9]',
                'REGION':r'[a-z]+-[a-z]+[0-9]+',
                'JOB_ID':r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',
                'JOB_NAME':r'[a-z][a-z0-9-]{0,62}'}
    values = dict(PROJECT_ID=project_id, REGION=region, JOB_ID=job_id, JOB_NAME=job_name)
    for key,value in values.items():
        if not isinstance(value,str) or not re.fullmatch(patterns[key], value):
            raise ValueError('invalid '+key+' (no query/filter injection allowed)')
    document = json.loads(Path(template).read_text(encoding='utf-8'))
    def replace(value):
        if isinstance(value,str):
            for key,item in values.items():
                value=value.replace('__'+key+'__',item)
            return value
        if isinstance(value,list): return [replace(v) for v in value]
        if isinstance(value,dict): return {k:replace(v) for k,v in value.items()}
        return value
    document=replace(document)
    if existing_dashboard is not None:
        existing=json.loads(Path(existing_dashboard).read_text(encoding='utf-8'))
        name,etag=existing.get('name'),existing.get('etag')
        allowed_projects={project_id}
        if project_number is not None:
            if not re.fullmatch(r'[0-9]+',str(project_number)):
                raise ValueError('invalid project number')
            allowed_projects.add(str(project_number))
        match=re.fullmatch(r'projects/([^/]+)/dashboards/([A-Za-z0-9_-]+)',name or '')
        if not match or match.group(1) not in allowed_projects or not isinstance(etag,str) or not etag:
            raise ValueError('existing dashboard needs matching project name and latest etag; pass verified --project-number if needed')
        document.update(name=name,etag=etag)
    return validate(document)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-id',default='gcp-prod-edp-sa-udw')
    parser.add_argument('--project-number')
    parser.add_argument('--region',default='asia-northeast3')
    parser.add_argument('--job-id')
    parser.add_argument('--job-name')
    parser.add_argument('--template',type=Path,default=DEFAULT_TEMPLATE)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--existing-dashboard',type=Path)
    parser.add_argument('--check',type=Path,help='validate an already rendered JSON locally only')
    args=parser.parse_args(argv)
    try:
        if args.check:
            validate(json.loads(args.check.read_text(encoding='utf-8')))
            print('Local subset validation PASS (not Google API validation)')
            return
        if not args.job_id or not args.job_name or args.output is None:
            parser.error('--job-id, --job-name and --output are required for rendering')
        document=render(project_id=args.project_id,region=args.region,job_id=args.job_id,
                        job_name=args.job_name,template=args.template,
                        existing_dashboard=args.existing_dashboard,project_number=args.project_number)
        args.output.write_text(json.dumps(document,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        print('Rendered locally: '+str(args.output)+' (no GCP changes)')
    except (ValueError,OSError) as exc:
        parser.error(str(exc))


if __name__=='__main__':
    main()
