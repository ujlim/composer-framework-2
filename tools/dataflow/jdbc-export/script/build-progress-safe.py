#!/usr/bin/env python3
"""Opt-in B image/template publishing. Default is an offline dry run."""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request

DOCKER_DIR = Path(__file__).resolve().parent.parent / 'docker'


class SafetyError(RuntimeError):
    pass


def run(command):
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode:
        # Do not expose credentials or arbitrary server response bodies.
        raise SafetyError(f'Command failed ({result.returncode}): {shlex.join(command)}; '
                          'outcome may be partial; inspect Cloud Build/targets before retrying.')
    return result.stdout


def read_json(url, missing_ok=False):
    token = run(['gcloud', 'auth', 'print-access-token']).strip()
    if not token or '\n' in token:
        raise SafetyError('Invalid access token response')
    request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read(1048577)
        if len(data) > 1048576:
            raise SafetyError('Read response too large')
        value = json.loads(data)
        if not isinstance(value, dict):
            raise SafetyError('Unexpected API response')
        return value
    except urllib.error.HTTPError as error:
        if error.code == 404 and missing_ok:
            return None
        raise SafetyError(f'Read check failed: HTTP {error.code}; no absence assumed') from None
    except (OSError, ValueError) as error:
        raise SafetyError(f'Read check failed: {type(error).__name__}; no absence assumed') from None


def targets(args):
    for name, pattern in (
        ('project', r'[a-z][a-z0-9-]{4,61}[a-z0-9]'),
        ('region', r'[a-z]+-[a-z]+[0-9]+'),
        ('repository', r'[a-z][a-z0-9-]{0,62}'),
        ('bucket', r'[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]'),
        ('version', r'[0-9]{8}-[a-z0-9][a-z0-9.-]{0,80}'),
    ):
        if not re.fullmatch(pattern, getattr(args, name)):
            raise SafetyError(f'Invalid {name}; version must be date-prefixed, e.g. 20261030-v1')
    if os.environ.get('IMAGE') or os.environ.get('TEMPLATE_PATH'):
        raise SafetyError('Unset IMAGE/TEMPLATE_PATH: legacy destination overrides are not accepted')
    build_region = args.build_region or args.region
    if not re.fullmatch(r'[a-z]+-[a-z]+[0-9]+', build_region):
        raise SafetyError('Invalid build region')
    for uri in (args.gcs_source_staging_dir, args.gcs_log_dir):
        if uri is not None and not re.fullmatch(r'gs://[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]/[A-Za-z0-9_./-]+', uri):
            raise SafetyError('Explicit staging/log directory must be a gs://bucket/prefix URI')
    if args.execute and not args.gcs_source_staging_dir:
        raise SafetyError('Execute requires --gcs-source-staging-dir in an approved existing bucket; no default US staging')
    image = f'{args.region}-docker.pkg.dev/{args.project}/{args.repository}/jdbc-export-progress:{args.version}'
    key = f'templates/jdbc-export-progress/{args.version}.json'
    template = f'gs://{args.bucket}/{key}'
    repo_url = f'https://artifactregistry.googleapis.com/v1/projects/{args.project}/locations/{args.region}/repositories/{args.repository}'
    tag_url = repo_url + '/packages/jdbc-export-progress/tags/' + args.version
    bucket_url = 'https://storage.googleapis.com/storage/v1/b/' + args.bucket
    object_url = bucket_url + '/o/' + urllib.parse.quote(key, safe='')
    return image, template, repo_url, tag_url, bucket_url, object_url


def require_immutable(repo_url):
    repo = read_json(repo_url)
    if repo.get('format') != 'DOCKER' or repo.get('mode', 'STANDARD_REPOSITORY') != 'STANDARD_REPOSITORY':
        raise SafetyError('A standard Docker Artifact Registry repository is required')
    if repo.get('dockerConfig', {}).get('immutableTags') is not True:
        raise SafetyError('Repository immutableTags is not true. STOP: obtain an approved immutable '
                          'B repository/policy from its owner; this script never changes policy or IAM.')


def publish(args):
    image, template, repo_url, tag_url, bucket_url, object_url = targets(args)
    build = ['gcloud', 'builds', 'submit', str(DOCKER_DIR), '--project=' + args.project, '--tag=' + image, '--region=' + (args.build_region or args.region)]
    if args.gcs_source_staging_dir:
        build.append('--gcs-source-staging-dir=' + args.gcs_source_staging_dir)
    if args.gcs_log_dir:
        build.append('--gcs-log-dir=' + args.gcs_log_dir)
    spec = ['gcloud', 'dataflow', 'flex-template', 'build', template, '--project=' + args.project,
            '--image=' + image, '--sdk-language=PYTHON', '--metadata-file=' + str(DOCKER_DIR / 'metadata.json'), '--print-only']
    print(json.dumps({'mode': 'EXECUTE' if args.execute else 'DRY_RUN_OFFLINE', 'image': image,
                      'template': template, 'immutable_tags_required': True, 'build_region': args.build_region or args.region,
                      'source_staging': args.gcs_source_staging_dir or '<REQUIRED FOR EXECUTE>',
                      'build_log_directory': args.gcs_log_dir or '<CLOUD BUILD DEFAULT; REVIEW POLICY>'}, indent=2))
    if not args.execute:
        print(shlex.join(build)); print(shlex.join(spec))
        print('gcloud storage cp <validated-local-spec.json> ' + shlex.quote(template) + ' --if-generation-match=0')
        print('No cloud checks or writes performed. Reuse of a version is rejected during execute.')
        return
    require_immutable(repo_url)
    read_json(bucket_url)  # Fail on missing bucket / permission / network errors before build.
    if read_json(tag_url, missing_ok=True) is not None:
        raise SafetyError('Image tag already exists; choose a new version')
    if read_json(object_url, missing_ok=True) is not None:
        raise SafetyError('Template already exists; choose a new version')
    # Explicit source/log buckets must already exist; never let gcloud create missing ones.
    for uri in (args.gcs_source_staging_dir, args.gcs_log_dir):
        if uri:
            read_json('https://storage.googleapis.com/storage/v1/b/' + uri.split('/')[2])
    # Print-only never writes GCS; validate before the paid Cloud Build operation.
    try:
        container_spec = json.loads(run(spec))
    except ValueError:
        raise SafetyError('Print-only output is not JSON; stopped before Cloud Build') from None
    if not isinstance(container_spec, dict) or container_spec.get('image') != image:
        raise SafetyError('Container spec image does not match the protected B target')
    if container_spec.get('sdkInfo', {}).get('language') != 'PYTHON' or not isinstance(container_spec.get('metadata'), dict):
        raise SafetyError('Unexpected container spec schema')
    # Recheck just before push. Registry immutable policy protects tag reassignment races.
    require_immutable(repo_url)
    run(build)
    tag = read_json(tag_url)
    if not isinstance(tag.get('version'), str) or '/versions/sha256:' not in tag['version']:
        raise SafetyError('Built image tag could not be verified; no template published')
    with tempfile.TemporaryDirectory(prefix='jdbc-progress-') as directory:
        local = Path(directory) / 'template.json'
        local.write_text(json.dumps(container_spec, indent=2) + '\n', encoding='utf-8')
        run(['gcloud', 'storage', 'cp', str(local), template, '--project=' + args.project,
             '--if-generation-match=0'])
    print('Published NEW image/tag and NEW template. No Dataflow Job launched or Vine changed.')
    print('TEMPLATE_PATH=' + template)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', default='gcp-prod-edp-sa-udw')
    parser.add_argument('--region', default='asia-northeast3')
    parser.add_argument('--repository', default='repo-prod-sa-udw-dataflow-tmplt-1')
    parser.add_argument('--bucket', default='bucket-prod-sa-udw-dataflow-system-1')
    parser.add_argument('--build-region', help='Cloud Build region (defaults to --region)')
    parser.add_argument('--gcs-source-staging-dir', help='Approved existing gs://bucket/prefix; required for execute')
    parser.add_argument('--gcs-log-dir', help='Optional approved existing gs://bucket/prefix for build logs')
    parser.add_argument('--version', required=True)
    parser.add_argument('--execute', action='store_true', help='Explicitly opt in to paid Cloud Build and new GCS object creation')
    args = parser.parse_args(argv)
    try:
        publish(args)
    except SafetyError as error:
        parser.exit(1, 'STOP: ' + str(error) + '\n')


if __name__ == '__main__':
    main()
