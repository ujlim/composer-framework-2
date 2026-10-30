# Author: LIM UI JIN
# Created: 2026-10-30
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('dashboard_renderer',ROOT/'tools/monitoring/dataflow/render_dashboard.py')
r=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(r)


class DashboardTests(unittest.TestCase):
    def render(self,**kwargs):
        values=dict(project_id='gcp-prod-edp-sa-udw',region='asia-northeast3',job_id='2026-10-30_00_00_00-123',job_name='jdbc-test')
        values.update(kwargs);return r.render(**values)
    def test_render_and_roundtrip(self):
        d=self.render();self.assertEqual(r.validate(json.loads(json.dumps(d))),d)
        self.assertNotIn('__JOB',json.dumps(d));self.assertNotIn('etag',d);self.assertNotIn('name',d)
        self.assertEqual(len(d['mosaicLayout']['tiles']),13)
    def test_filters_scope_each_job_correctly(self):
        for tile in self.render()['mosaicLayout']['tiles']:
            widget=tile['widget']
            for ds in widget.get('xyChart',{}).get('dataSets',[]):
                f=ds['timeSeriesQuery']['timeSeriesFilter']['filter']
                self.assertIn('project_id="gcp-prod-edp-sa-udw"',f)
                if 'dataflow_job' in f:self.assertNotIn('resource.labels.job_id',f)
                if 'job/is_failed' in f:self.assertNotIn('metric.labels.job_id',f)
                elif 'dataflow_job' in f:self.assertIn('metric.labels.job_id',f)
    def test_bad_identifiers_rejected(self):
        for key in ['project_id','region','job_id','job_name']:
            for value in ['a" OR true','x\nfoo','__JOB_ID__','',None]:
                with self.subTest(key=key,value=value),self.assertRaises(ValueError):self.render(**{key:value})
    def test_no_gauge_sum_rate_or_ratio(self):
        doc=json.dumps(self.render())
        for token in ['ALIGN_SUM','REDUCE_SUM','ALIGN_RATE','timeSeriesFilterRatio','total_memory_usage_time']:
            self.assertNotIn(token,doc)
    def test_real_counter_names_and_unknown_disclaimer(self):
        doc=json.dumps(self.render());source=(ROOT/'tools/dataflow/jdbc-export/docker/main.py').read_text()
        self.assertIn('post_jdbc_output_rows',source);self.assertIn('jdbc_export_progress',source)
        self.assertIn('post_jdbc_output_rows',doc);self.assertIn('No data = unknown',doc)
        self.assertNotIn('metric.labels.namespace',doc)
    def test_logs_global_search_payload_compatible(self):
        panels=[t['widget']['logsPanel'] for t in self.render()['mosaicLayout']['tiles'] if 'logsPanel' in t['widget']]
        self.assertEqual(len(panels),3)
        self.assertIn('"Dataflow progress" AND "2026-',panels[0]['filter'])
        self.assertEqual(panels[0]['resourceNames'],['projects/gcp-prod-edp-sa-udw'])
        self.assertIn('resource.labels.job_id',panels[1]['filter'])
    def test_unknown_placeholder_rejected(self):
        d=self.render();d['displayName']='__UNKNOWN__'
        with self.assertRaises(ValueError):r.validate(d)
    def test_invalid_geometry_rejected(self):
        d=self.render();d['mosaicLayout']['tiles'][1]['xPos']=99
        with self.assertRaises(ValueError):r.validate(d)
        d=self.render();d['mosaicLayout']['tiles'][1]['yPos']=0
        with self.assertRaises(ValueError):r.validate(d)
    def test_unsafe_aggregation_rejected(self):
        d=self.render();d['mosaicLayout']['tiles'][1]['widget']['xyChart']['dataSets'][0]['timeSeriesQuery']['timeSeriesFilter']['aggregation']['perSeriesAligner']='ALIGN_SUM'
        with self.assertRaises(ValueError):r.validate(d)
    def test_update_etag_scope_and_project_number(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'existing.json'
            p.write_text(json.dumps({'name':'projects/gcp-prod-edp-sa-udw/dashboards/abc','etag':'fresh'}))
            d=self.render(existing_dashboard=p);self.assertEqual(d['etag'],'fresh')
            p.write_text(json.dumps({'name':'projects/123456/dashboards/abc','etag':'fresh'}))
            with self.assertRaises(ValueError):self.render(existing_dashboard=p)
            self.assertEqual(self.render(existing_dashboard=p,project_number='123456')['etag'],'fresh')
    def test_missing_etag_wrong_project_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'existing.json'
            for d in [{'name':'projects/other-project/dashboards/a','etag':'x'}, {'name':'projects/gcp-prod-edp-sa-udw/dashboards/a'}]:
                p.write_text(json.dumps(d))
                with self.assertRaises(ValueError):self.render(existing_dashboard=p)
    def test_cli_render_and_check(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'rendered.json'
            r.main(['--job-id','test-id','--job-name','test-name','--output',str(p)])
            r.main(['--check',str(p)]);self.assertTrue(p.exists())
    def test_fake_metric_rejected(self):
        d=self.render();series=d['mosaicLayout']['tiles'][1]['widget']['xyChart']['dataSets'][0]['timeSeriesQuery']['timeSeriesFilter'];series['filter']=series['filter'].replace('job/current_num_vcpus','job/invented')
        with self.assertRaises(ValueError):r.validate(d)


if __name__=='__main__':unittest.main()
