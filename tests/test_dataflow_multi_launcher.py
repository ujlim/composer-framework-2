# Author: 임의진 (LIM UI JIN)
# Updated: 2026-10-31
"""Offline contract tests; not a Beam SDK or live Dataflow integration test."""
import argparse
import ast
import importlib.util
import os
import socket
import time
import csv
import io
import json
import logging
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch, MagicMock

SOURCE_DIR = Path(__file__).resolve().parents[1] / 'tools/dataflow/jdbc-export/docker'
spec = importlib.util.spec_from_file_location('multi_table_contract_test', SOURCE_DIR / 'multi_table.py')
contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract)
load_tables, validate_schema = contract.load_tables, contract.validate_schema

PREFIX = "gs://example-bucket/run/"
SCHEMA = {"fields": [{"name": "id", "type": "INTEGER"}]}


def documents():
    manifest = {"version": 1, "tables": []}
    assets = {}
    for name in ("emp", "dept", "sales"):
        manifest["tables"].append({"id": name, "query_uri": PREFIX + name + ".sql", "schema_uri": PREFIX + name + ".json", "destination": {"table": "sample-project:staging." + name, "write_disposition": "WRITE_TRUNCATE"}})
        assets[PREFIX + name + ".sql"] = 'SELECT 1 AS "id" FROM dual'
        assets[PREFIX + name + ".json"] = json.dumps(SCHEMA)
    return manifest, assets


class ValidationTests(unittest.TestCase):
    def load(self, change=lambda doc: None):
        manifest, assets = documents()
        change(manifest)
        assets[PREFIX + "manifest.json"] = json.dumps(manifest)
        return load_tables(PREFIX + "manifest.json", assets.__getitem__)

    def test_three_tables(self):
        self.assertEqual([x['id'] for x in self.load()], ['emp', 'dept', 'sales'])

    def test_duplicate_id(self):
        with self.assertRaises(ValueError):
            self.load(lambda d: d['tables'][1].update(id='emp'))

    def test_duplicate_destination(self):
        with self.assertRaises(ValueError):
            self.load(lambda d: d['tables'][1]['destination'].update(table='sample-project:staging.emp'))

    def test_path_and_cross_prefix(self):
        for uri in ('gs://example-bucket/run/../x', 'gs://other-bucket/run/x', 'gs://example-bucket/run2/x', 'gs://example-bucket/run/%2e%2e/x'):
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                self.load(lambda d: d['tables'][0].update(query_uri=uri))

    def test_secret_key_rejected_without_echo(self):
        with self.assertRaises(ValueError) as error:
            self.load(lambda d: d.update(password='SENSITIVE'))
        self.assertNotIn('SENSITIVE', str(error.exception))

    def test_strict_json_and_empty_manifest(self):
        for value in ('{"version":1,"version":1,"tables":[]}', '{"version":NaN,"tables":[]}',
                      '{"version":true,"tables":[]}', '{"version":2,"tables":[]}',
                      '{"version":1,"tables":[]}'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                load_tables(PREFIX+'manifest.json', lambda uri: value)

    def test_invalid_assets_and_unrendered_sql(self):
        for replacement in ('', 'SELECT {{ BUSINESS_DATE }} FROM dual', '{% set x = 1 %} SELECT 1 FROM dual'):
            manifest, assets = documents()
            assets[PREFIX+'manifest.json'] = json.dumps(manifest)
            assets[PREFIX+'emp.sql'] = replacement
            with self.subTest(query=replacement), self.assertRaises(ValueError):
                load_tables(PREFIX+'manifest.json', assets.__getitem__)
        for invalid in ('WRITE_UNKNOWN', [], None):
            with self.subTest(disposition=invalid), self.assertRaises(ValueError):
                self.load(lambda d: d['tables'][0]['destination'].update(write_disposition=invalid))

    def test_schema_validation(self):
        for schema in ({'fields': []}, {'fields': [{'name':'x', 'type':'STRING'}, {'name':'X', 'type':'STRING'}]}, {'fields':[{'name':'x','type':'MAGIC'}]}, {'fields':[{'name':'x','type':'STRING','mode':'WRONG'}]}):
            with self.subTest(schema=schema), self.assertRaises(ValueError):
                validate_schema(schema)
        nested = {'fields':[{'name':'x','type':'RECORD','fields':[{'name':'y','type':'DATE'}]}]}
        self.assertEqual(validate_schema(nested), nested)
        self.assertEqual(validate_schema(SCHEMA['fields']), SCHEMA)


class LauncherTests(unittest.TestCase):
    def environment(self):
        events = []
        class Transform:
            def __init__(self, kind, kwargs=None):
                self.kind, self.kwargs = kind, kwargs
            def __rrshift__(self, label):
                self.label = label
                return self
        class Node:
            def __or__(self, transform):
                events.append((transform.kind, transform.label, transform.kwargs))
                if transform.kind == 'write':
                    return SimpleNamespace(destination_file_pairs=Node(), destination_load_jobid_pairs=Node(), destination_copy_jobid_pairs=Node())
                return Node()
        class Pipeline(Node):
            def __init__(self, **kwargs):
                events.append(('pipeline',))
            def __enter__(self):
                return self
            def __exit__(self, *args):
                events.append(('submit',))
        def writer(**kwargs):
            return Transform('write', kwargs)
        writer.Method = SimpleNamespace(FILE_LOADS='FILE_LOADS')
        beam = SimpleNamespace(DoFn=object, ParDo=lambda fn: Transform("observe", fn), Pipeline=Pipeline, Map=lambda fn: Transform('map'), io=SimpleNamespace(WriteToText=lambda *a, **k: Transform('gcs')))
        pa = SimpleNamespace(**{name:lambda *a:None for name in ('string','binary','bool_','int64','float64','date32','timestamp','decimal128','decimal256')})
        metrics = MagicMock()
        env = dict(os=os, socket=socket, time=time, Metrics=metrics, GoogleCloudOptions=object, argparse=argparse, csv=csv, io=io, json=json, logging=logging, beam=beam, pa=pa,
                   google=SimpleNamespace(auth=SimpleNamespace(default=lambda: (object(),'project'))),
                   WriteToBigQuery=writer, ReadFromJdbc=lambda **k:Transform('read', k),
                   PipelineOptions=lambda *a, **k:SimpleNamespace(view_as=lambda cls:SimpleNamespace(job_name="test-job", temp_location="gs://bucket/temp")), __name__='test_launcher')
        source = ast.parse((SOURCE_DIR / 'main.py').read_text())
        source.body = [n for n in source.body if not isinstance(n, (ast.Import, ast.ImportFrom))]
        exec(compile(source, 'main.py', 'exec'), env)
        manifest, assets = documents()
        assets[PREFIX + 'manifest.json'] = json.dumps(manifest)
        def read(uri):
            events.append(('asset',uri))
            return assets[uri]
        def secret(resource):
            events.append(('secret',))
            return 'test-secret'
        env.update(read_gcs_text=read, access_secret=secret)
        env['_emit_progress'] = lambda event: events.append(('progress', event))
        env['_test_metrics'] = metrics
        return env, events

    def invoke(self, args):
        env, events = self.environment()
        base = ['main.py','--db_type','oracle','--connection_url','jdbc:oracle:example','--username','test','--password_secret','test','--driver_jars','gs://bucket/driver.jar']
        with patch.object(sys, 'argv', base+args), patch.dict(sys.modules, {'multi_table': contract}):
            env['run']()
        return events

    def test_launcher_helper_not_in_saved_main_globals(self):
        source = ast.parse((SOURCE_DIR / 'main.py').read_text())
        self.assertFalse(any(isinstance(n, ast.ImportFrom) and n.module == 'multi_table'
                             for n in source.body))
        env, _ = self.environment()
        self.assertNotIn('load_tables', env)
        self.assertNotIn('multi_table', env)
        args = ['main.py', '--db_type', 'oracle', '--connection_url', 'jdbc:oracle:x',
                '--username', 'test', '--password_secret', 'test', '--driver_jars', 'gs://bucket/driver.jar',
                '--tables_manifest_uri', PREFIX+'manifest.json']
        with patch.object(sys, 'argv', args), patch.dict(sys.modules, {'multi_table': contract}):
            env['run']()
        self.assertNotIn('load_tables', env)
        self.assertNotIn('multi_table', env)

    def test_one_pipeline_three_independent_branches(self):
        events = self.invoke(['--tables_manifest_uri',PREFIX+'manifest.json'])
        self.assertEqual(sum(x[0]=='pipeline' for x in events), 1)
        self.assertEqual(sum(x[0]=='submit' for x in events), 1)
        self.assertEqual(sum(x[0]=='secret' for x in events), 1)
        self.assertEqual(sum(x[0]=='asset' for x in events), 7)
        self.assertLess(max(i for i,x in enumerate(events) if x[0]=='asset'), next(i for i,x in enumerate(events) if x[0]=='secret'))
        reads = [x for x in events if x[0]=='read']
        writes = [x for x in events if x[0]=='write']
        self.assertEqual(len(reads),3)
        self.assertEqual(len(writes),3)
        self.assertEqual(len({x[1] for x in events if x[0] in ('read','write','map','observe')}),21)
        observers = [x[2] for x in events if x[0] == 'observe']
        self.assertEqual(len(observers),12)
        side_observers = [x for x in observers if hasattr(x, 'table_id')]
        self.assertEqual([x.table_id for x in side_observers], ['emp']*3 + ['dept']*3 + ['sales']*3)
        self.assertTrue(all(x[2]['jdbc_url']=='jdbc:oracle:example' for x in reads))
        self.assertEqual([x[2]['table'] for x in writes], ['sample-project:staging.'+name for name in ('emp','dept','sales')])
        self.assertTrue(all(x[2]['method']=='FILE_LOADS' for x in writes))

    def test_legacy_bigquery(self):
        events = self.invoke(['--query_uri',PREFIX+'emp.sql','--destination_type','bigquery','--output_table','sample-project:staging.emp','--bq_schema',json.dumps(SCHEMA)])
        self.assertEqual(sum(x[0]=='pipeline' for x in events),1)
        self.assertEqual(sum(x[0]=='read' for x in events),1)
        write = next(x for x in events if x[0]=='write')
        self.assertEqual(write[2]['write_disposition'],'WRITE_APPEND')

    def test_legacy_gcs(self):
        events = self.invoke(['--query_uri',PREFIX+'emp.sql','--destination_type','gcs','--output_path','gs://bucket/out'])
        self.assertEqual(sum(x[0]=='gcs' for x in events),1)

    def test_mixed_modes_fail(self):
        for flag, value in (('--query_uri',PREFIX+'emp.sql'), ('--destination_type','bigquery'),
                            ('--output_format','json'), ('--write_disposition','WRITE_APPEND'),
                            ('--create_disposition','CREATE_IF_NEEDED'), ('--output_table','sample-project:staging.x')):
            with self.subTest(flag=flag), self.assertRaises(ValueError):
                self.invoke(['--tables_manifest_uri',PREFIX+'manifest.json',flag,value])

    def test_per_table_counter_and_event_contract(self):
        env, events = self.environment()
        row = {'do_not_log': 'secret-row'}
        for name in ('emp', 'dept', 'sales'):
            observer = env['ObservePostJdbcRows'](name)
            self.assertIs(list(observer.process(row))[0], row)
        self.assertEqual([call.args for call in env['_test_metrics'].counter.call_args_list], [
            ('jdbc_export_progress.' + name, 'post_jdbc_output_rows') for name in ('emp','dept','sales')])
        self.assertEqual(env['_test_metrics'].counter.return_value.inc.call_count, 3)
        for name in ('unsafe/id', 'secret value', '', 'A', 123):
            with self.subTest(name=name), self.assertRaises(ValueError):
                env['ObservePostJdbcRows'](name)
        observer = env['ObserveFileLoadOutput']('bq_load_reference_observed', table_id='emp')
        list(observer.process(('sample-project:staging.emp', {'jobId':'j'})))
        event = next(x[1] for x in events if x[0]=='progress')
        self.assertEqual(event['table_id'], 'emp')
        self.assertEqual(event['current_state'], 'unknown')
        self.assertEqual(event['state'], 'SUBMITTED_REFERENCE_OBSERVED')
        self.assertNotIn('secret-row', json.dumps(event))

    def test_invalid_manifest_never_reads_secret_or_builds_pipeline(self):
        env, events = self.environment()
        env['read_gcs_text'] = lambda uri: '{"version":1,"tables":[],"password":"SECRET"}'
        base = ['main.py','--db_type','oracle','--connection_url','x','--username','x',
                '--password_secret','x','--driver_jars','x','--tables_manifest_uri',PREFIX+'manifest.json']
        with patch.object(sys, 'argv', base), patch.dict(sys.modules, {'multi_table': contract}), self.assertRaises(ValueError) as error:
            env['run']()
        self.assertEqual(events, [])
        self.assertNotIn('SECRET', str(error.exception))


if __name__ == '__main__':
    unittest.main()
