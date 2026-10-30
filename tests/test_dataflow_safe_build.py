import argparse
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error

PATH = Path(__file__).resolve().parents[1] / 'tools/dataflow/jdbc-export/script/build-progress-safe.py'
spec = importlib.util.spec_from_file_location('safe_build', PATH)
b = importlib.util.module_from_spec(spec); spec.loader.exec_module(b)


class SafeBuildTest(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {}, clear=True); self.env.start(); self.addCleanup(self.env.stop)
        self.args = argparse.Namespace(project='gcp-prod-edp-sa-udw', region='asia-northeast3',
            repository='repo-prod-sa-udw-dataflow-tmplt-1', bucket='bucket-prod-sa-udw-dataflow-system-1',
            version='20261030-v1', execute=True, build_region=None,
            gcs_source_staging_dir='gs://approved-source-bucket/build-source', gcs_log_dir=None)
        self.repo = {'format':'DOCKER','mode':'STANDARD_REPOSITORY','dockerConfig':{'immutableTags':True}}

    def publish(self, reads=None, spec_override=None):
        image=b.targets(self.args)[0]
        output=json.dumps(spec_override if spec_override is not None else {'image':image,'sdkInfo':{'language':'PYTHON'},'metadata':{}})
        reads = reads if reads is not None else [self.repo, {}, None, None] + ([{}] * (1 + bool(self.args.gcs_log_dir))) + [self.repo, {'version':'x/versions/sha256:abc'}]
        with patch.object(b,'read_json',side_effect=reads) as read, patch.object(b,'run',side_effect=[output,'','']) as run, contextlib.redirect_stdout(io.StringIO()):
            b.publish(self.args)
        return read,run

    def test_default_dry_run_no_cloud_call(self):
        with patch.object(b,'read_json') as read,patch.object(b,'run') as run,contextlib.redirect_stdout(io.StringIO()):
            b.main(['--version','20261030-v1'])
        read.assert_not_called();run.assert_not_called()

    def test_explicit_build_location_and_staging(self):
        self.args.build_region='asia-northeast1';self.args.gcs_log_dir='gs://approved-log-bucket/build-logs'
        _,run=self.publish();command=run.call_args_list[1].args[0]
        self.assertIn('--region=asia-northeast1',command)
        self.assertIn('--gcs-source-staging-dir=gs://approved-source-bucket/build-source',command)
        self.assertIn('--gcs-log-dir=gs://approved-log-bucket/build-logs',command)

    def test_missing_staging_blocks_execute(self):
        self.args.gcs_source_staging_dir=None
        with self.assertRaises(b.SafetyError):b.targets(self.args)

    def test_bad_build_locations_rejected(self):
        for name in ['build_region','gcs_source_staging_dir','gcs_log_dir']:
            old=getattr(self.args,name);setattr(self.args,name,'bad;injection')
            with self.subTest(name=name),self.assertRaises(b.SafetyError):b.targets(self.args)
            setattr(self.args,name,old)

    def test_distinct_fixed_targets(self):
        image,path,*_=b.targets(self.args)
        self.assertTrue(image.endswith('/jdbc-export-progress:20261030-v1'))
        self.assertTrue(path.endswith('/templates/jdbc-export-progress/20261030-v1.json'))

    def test_legacy_override_rejected(self):
        for key in ['IMAGE','TEMPLATE_PATH']:
            with self.subTest(key=key),patch.dict(os.environ,{key:'legacy'}),self.assertRaises(b.SafetyError):b.targets(self.args)

    def test_invalid_versions_rejected(self):
        for value in ['v1','latest','../v1','20261030-v1;echo x','20261030-a/b','20261030-']:
            self.args.version=value
            with self.subTest(value=value),self.assertRaises(b.SafetyError):b.targets(self.args)

    def test_identifier_injection_rejected(self):
        for name in ['project','region','repository','bucket']:
            old=getattr(self.args,name);setattr(self.args,name,'x;bad')
            with self.subTest(name=name),self.assertRaises(b.SafetyError):b.targets(self.args)
            setattr(self.args,name,old)

    def test_happy_path_preconditions_and_print_only(self):
        read,run=self.publish()
        commands=[c.args[0] for c in run.call_args_list]
        self.assertIn('--print-only',commands[0]);self.assertIn('--format=json',commands[0]);self.assertIn('--if-generation-match=0',commands[-1])
        self.assertEqual(commands[1][1:3],['builds','submit'])
        self.assertEqual(read.call_count,7)
        self.assertTrue(read.call_args_list[2].kwargs['missing_ok'])

    def test_mutable_repository_blocks_build(self):
        for repo in [{}, {'format':'DOCKER'}, {'format':'DOCKER','dockerConfig':{'immutableTags':'true'}},
                     dict(self.repo,mode='REMOTE_REPOSITORY')]:
            with self.subTest(repo=repo),self.assertRaises(b.SafetyError):self.publish([repo])

    def test_existing_tag_or_template_blocks(self):
        for reads in [[self.repo,{},{}],[self.repo,{},None,{}]]:
            with self.subTest(reads=reads),self.assertRaises(b.SafetyError):self.publish(reads)

    def test_check_failure_never_runs_build(self):
        for index in range(4):
            reads=[self.repo,{},None,None];reads[index]=b.SafetyError('HTTP403')
            with patch.object(b,'read_json',side_effect=reads),patch.object(b,'run') as run,contextlib.redirect_stdout(io.StringIO()),self.assertRaises(b.SafetyError):b.publish(self.args)
            run.assert_not_called()

    def test_sdk_json_string_wrapper_is_decoded_once(self):
        image=b.targets(self.args)[0]
        doc={'image':image,'sdkInfo':{'language':'PYTHON'},'metadata':{}}
        output=json.dumps(json.dumps(doc))
        reads=[self.repo,{},None,None,{},self.repo,{'version':'x/versions/sha256:a'}]
        with patch.object(b,'read_json',side_effect=reads),patch.object(b,'run',side_effect=[output,'','']) as run,contextlib.redirect_stdout(io.StringIO()):b.publish(self.args)
        self.assertEqual(run.call_count,3)

    def test_non_json_and_extra_wrappers_fail_before_build(self):
        image=b.targets(self.args)[0]
        doc={'image':image,'sdkInfo':{'language':'PYTHON'},'metadata':{}}
        for output in ['image: x\nmetadata: {}\n', 'NOTICE\n'+json.dumps(doc),
                       json.dumps(json.dumps(json.dumps(doc))), '[]', 'null',
                       json.dumps('not-json')]:
            with self.subTest(output=output),patch.object(b,'read_json',side_effect=[self.repo,{},None,None,{}]),patch.object(b,'run',return_value=output) as run,contextlib.redirect_stdout(io.StringIO()),self.assertRaises(b.SafetyError):b.publish(self.args)
            self.assertEqual(run.call_count,1)

    def test_bad_spec_rejected_before_build(self):
        for doc in [{}, {'image':'old'}, {'image':b.targets(self.args)[0],'sdkInfo':{'language':'JAVA'},'metadata':{}}]:
            with self.subTest(doc=doc),self.assertRaises(b.SafetyError):self.publish(spec_override=doc)

    def test_late_mutable_policy_blocks_build(self):
        with self.assertRaises(b.SafetyError):self.publish([self.repo,{},None,None,{},{}])

    def test_missing_built_tag_no_template(self):
        with self.assertRaises(b.SafetyError):self.publish([self.repo,{},None,None,{},self.repo,{}])

    def test_missing_source_bucket_blocks_before_build(self):
        with self.assertRaises(b.SafetyError):self.publish([self.repo,{},None,None,b.SafetyError('HTTP404')])

    def test_atomic_gcs_failure_propagates(self):
        image=b.targets(self.args)[0]
        output=json.dumps({'image':image,'sdkInfo':{'language':'PYTHON'},'metadata':{}})
        with patch.object(b,'read_json',side_effect=[self.repo,{},None,None,{},self.repo,{'version':'x/versions/sha256:a'}]),patch.object(b,'run',side_effect=[output,'',b.SafetyError('412')]),contextlib.redirect_stdout(io.StringIO()),self.assertRaises(b.SafetyError):b.publish(self.args)

    def test_http_only_authoritative_404_is_absent(self):
        for status in [401,403,404,429,500]:
            error=urllib.error.HTTPError('https://example',status,'error',{},None)
            with patch.object(b,'run',return_value='token'),patch.object(b.urllib.request,'urlopen',side_effect=error):
                if status==404:self.assertIsNone(b.read_json('https://example',missing_ok=True))
                else:
                    with self.assertRaises(b.SafetyError):b.read_json('https://example',missing_ok=True)

    def test_required_404_and_network_fail_closed(self):
        for error in [urllib.error.HTTPError('x',404,'error',{},None),urllib.error.URLError('timeout')]:
            with patch.object(b,'run',return_value='token'),patch.object(b.urllib.request,'urlopen',side_effect=error),self.assertRaises(b.SafetyError):b.read_json('https://example')

    def test_command_failure_not_treated_absence(self):
        with patch.object(b.subprocess,'run',return_value=argparse.Namespace(returncode=1,stdout='',stderr='secret')),self.assertRaises(b.SafetyError) as error:b.run(['gcloud','x'])
        self.assertNotIn('secret',str(error.exception))


if __name__ == '__main__':unittest.main()
