# Author: 임의진 (LIM UI JIN)
# Created: 2026-11-02
# Updated: 2026-11-02
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('pipeline_names_cli', Path(__file__).resolve().parents[1] / 'tools/validate_pipeline_names.py')
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)


class PipelineNamesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)

    def config(self, group, folder, dag_id=None, text=None):
        kind = group[:-1]
        path = self.base / group / folder / 'config.yaml'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text if text is not None else f'{kind}:\n  {kind}_id: "{dag_id or folder}"\n')
        return path

    def inspect(self, **kwargs):
        return cli.inspect_catalog(self.base, **kwargs)

    def run_cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = cli.main(['--pipelines-dir', str(self.base), *args])
        return status, out.getvalue(), err.getvalue()

    def test_valid_catalog(self):
        self.config('roots', 'root_a')
        self.config('vines', 'vine_a')
        self.assertEqual(self.inspect()['errors'], [])
        self.assertEqual(self.run_cli()[0], 0)

    def test_mismatch_lookup_by_either_alias(self):
        path = self.config('vines', 'physical', 'logical')
        for name in ('physical', 'logical'):
            status, out, _ = self.run_cli('--name', name)
            self.assertEqual(status, 0)
            self.assertEqual(out.strip(), str(path))
        self.assertEqual(len(self.inspect()['warnings']), 1)
        self.assertEqual(self.run_cli('--require-id-folder-match')[0], 1)

    def test_normalizes_id_whitespace(self):
        self.config('vines', 'logical', ' logical ')
        self.assertEqual(self.inspect()['warnings'], [])
        self.assertEqual(self.run_cli('--name', 'logical')[0], 0)

    def test_duplicate_ids_cross_kind(self):
        self.config('roots', 'one', 'same')
        self.config('vines', 'two', ' same ')
        self.assertTrue(any('Duplicate DAG ID' in x for x in self.inspect()['errors']))
        self.assertEqual(self.run_cli('--name', 'one')[0], 1)

    def test_duplicate_folder_cross_kind(self):
        self.config('roots', 'same', 'root_a')
        self.config('vines', 'same', 'vine_a')
        self.assertTrue(any('Duplicate folder name' in x for x in self.inspect()['errors']))

    def test_cross_namespace_alias_collision(self):
        self.config('roots', 'one', 'two')
        self.config('vines', 'two', 'three')
        self.assertTrue(any("Ambiguous name 'two'" in x for x in self.inspect()['errors']))
        self.assertEqual(self.run_cli('--name', 'three')[0], 1)

    def test_unknown_lookup_and_traversal(self):
        self.config('roots', 'one')
        for name in ('missing', '../one', '/etc/passwd'):
            status, out, err = self.run_cli('--name', name)
            self.assertEqual(status, 1)
            self.assertEqual(out, '')
            self.assertIn('Name not found', err)

    def test_invalid_configs_are_not_skipped(self):
        cases = ['[', '[]', '{}', 'vine: []', 'vine: {}', 'vine: {vine_id: 2}',
                 'vine: {vine_id: ""}', 'vine: {vine_id: "bad/id"}',
                 'vine: {vine_id: "' + 'a' * 251 + '"}',
                 'root: {root_id: r}\nvine: {vine_id: v}',
                 'vine: {vine_id: a, vine_id: b}']
        for index, text in enumerate(cases):
            self.config('vines', str(index), text=text)
        self.config('vines', 'good')
        report = self.inspect()
        self.assertEqual(len(report['errors']), len(cases))
        self.assertEqual(len(report['entries']), 1)
        self.assertEqual(self.run_cli('--name', 'good')[0], 1)

    def test_yaml_merge_allows_explicit_override(self):
        self.config('vines', 'actual', text=(
            'defaults: &defaults\n  vine_id: inherited\n  owner: default_owner\n'
            'vine:\n  <<: *defaults\n  vine_id: actual\n'))
        report = self.inspect()
        self.assertEqual(report['errors'], [])
        self.assertEqual(report['entries'][0]['dag_id'], 'actual')

    def test_yaml_merge_does_not_hide_explicit_duplicate(self):
        self.config('vines', 'actual', text=(
            'defaults: &defaults {vine_id: inherited}\n'
            'vine:\n  <<: *defaults\n  vine_id: actual\n  vine_id: other\n'))
        self.assertTrue(any('Duplicate YAML key' in x for x in self.inspect()['errors']))

    def test_empty_and_missing_catalog(self):
        self.assertTrue(self.inspect()['errors'])
        self.assertTrue(cli.inspect_catalog(self.base / 'missing')['errors'])

    def test_json_output_and_failure(self):
        self.config('vines', 'good')
        status, out, err = self.run_cli('--json', '--name', 'good')
        self.assertEqual(status, 0)
        self.assertTrue(json.loads(out)['valid'])
        self.assertEqual(json.loads(out)['lookup']['dag_id'], 'good')
        self.assertEqual(err, '')
        status, out, _ = self.run_cli('--json', '--name', 'missing')
        self.assertEqual(status, 1)
        self.assertFalse(json.loads(out)['valid'])
        self.assertIsNone(json.loads(out)['lookup'])

    def test_no_airflow_import(self):
        source = Path(SPEC.origin).read_text()
        self.assertNotIn('from airflow', source)
        self.assertNotIn('import airflow', source)

    def test_symlink_escape_rejected(self):
        with tempfile.TemporaryDirectory() as outside:
            path = Path(outside) / 'config.yaml'
            path.write_text('vine: {vine_id: outside}')
            group = self.base / 'vines'
            group.mkdir()
            (group / 'outside').symlink_to(outside, target_is_directory=True)
            self.assertTrue(any('escapes' in x for x in self.inspect()['errors']))


if __name__ == '__main__':
    unittest.main()
