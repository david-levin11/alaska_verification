"""Run the real shell wrapper with simulated archivers; no network or archives."""
import json
import os
from pathlib import Path
import subprocess
import pytest

ELEMENTS = ['wind', 'rh', 'precip6hr', 'snow6hr', 'temp2m']


def run_wrapper(tmp_path, wrapper_args=None, **settings):
    script = tmp_path / 'run_daily_archives.sh'
    script.write_text((Path(__file__).parents[1] / script.name).read_text())
    fake = tmp_path / 'python'
    fake.write_text('''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ['CALL_LOG'], 'a') as log:
    log.write(json.dumps(args) + '\\n')
def option(name):
    return args[args.index(name) + 1] if name in args else ''
if args[0] == 'run_model_archiver.py':
    if (option('--model') == os.environ.get('FAIL_MODEL', '')
            and option('--element').lower() == os.environ.get('FAIL_ELEMENT', '').lower()):
        sys.exit(1)
if args[0] == 'run_refs_processing.py':
    if option('--elements') == os.environ.get('FAIL_REFS', ''):
        sys.exit(1)
''')
    fake.chmod(0o755)
    calls = tmp_path / 'calls.jsonl'
    env = dict(os.environ, PATH=str(tmp_path) + ':' + os.environ['PATH'],
               CALL_LOG=str(calls), LOG_DIR=str(tmp_path / 'logs'), MODELS='hrrr rrfs rrfsens',
               RUN_NDFD='0', RUN_OBS='0', RUN_REFS='1', DRY_RUN='0',
               REGION='alaska', REFS_ELEMENTS='Wind rh precip6hr snow6hr temp2m',
               REFS_LOOKBACK_DAYS='2', SOURCE_STORAGE='local',
               REFS_THRESHOLD_CONFIG='/tmp/thresholds.json', FAIL_MODEL='',
               FAIL_ELEMENT='', FAIL_REFS='')
    env.update(settings)
    result = subprocess.run(['bash', str(script)] + (wrapper_args or ['2026-10-12']), env=env,
                            capture_output=True, text=True)
    records = [json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
    refs = [r for r in records if r[0] == 'run_refs_processing.py']
    log = next((tmp_path / 'logs').glob('archive_*.log')).read_text()
    return result, records, refs, log


def derived_elements(refs):
    return [r[r.index('--elements') + 1] for r in refs]


def test_all_elements_follow_all_sources(tmp_path):
    result, records, refs, log = run_wrapper(tmp_path)
    assert result.returncode == 0, result.stderr
    assert derived_elements(refs) == ELEMENTS
    assert all(r[0] == 'run_model_archiver.py' for r in records[:-5])
    assert '--start' in records[0] and '202610101800' in records[0]
    for command in refs:
        assert '2026-10-12' in command and '--lookback-days' in command
        assert '/tmp/thresholds.json' in command
        assert len(command[command.index('--elements') + 1:command.index('--date')]) == 1


@pytest.mark.parametrize('model', ['hrrr', 'rrfs', 'rrfsens'])
def test_source_failure_only_skips_affected_element(tmp_path, model):
    result, _, refs, log = run_wrapper(tmp_path, FAIL_MODEL=model, FAIL_ELEMENT='precip6hr')
    assert result.returncode == 1
    assert derived_elements(refs) == ['wind', 'rh', 'snow6hr', 'temp2m']
    assert 'Skipping REFS element=precip6hr' in log
    assert model in log
    assert f'FAILED: model={model} element=precip6hr' in log
    assert 'OK: REFS element=temp2m' in log


def test_derived_failure_does_not_stop_later_elements(tmp_path):
    result, _, refs, log = run_wrapper(tmp_path, FAIL_REFS='wind')
    assert result.returncode == 1
    assert derived_elements(refs) == ELEMENTS
    assert 'FAILED: REFS element=wind' in log
    assert 'OK: REFS element=temp2m' in log


def test_non_refs_source_failure_does_not_skip_refs(tmp_path):
    result, _, refs, log = run_wrapper(tmp_path, MODELS='nbmqmd hrrr rrfs rrfsens',
                                      FAIL_MODEL='nbmqmd', FAIL_ELEMENT='rh')
    assert result.returncode == 1
    assert derived_elements(refs) == ELEMENTS


def test_disabled_refs(tmp_path):
    result, _, refs, _ = run_wrapper(tmp_path, RUN_REFS='0')
    assert result.returncode == 0 and not refs


def test_override_case_and_flags(tmp_path):
    result, _, refs, log = run_wrapper(tmp_path, REFS_ELEMENTS='Wind rh',
                                      FAIL_MODEL='rrfsens', FAIL_ELEMENT='wind',
                                      REFS_ALLOW_INCOMPLETE='1', REFS_AWS_PROFILE='example')
    assert result.returncode == 1
    assert derived_elements(refs) == ['rh']
    assert '--allow-incomplete' in refs[0] and 'example' in refs[0]
    assert 'Skipping REFS element=wind' in log


def test_dry_run_invokes_no_python(tmp_path):
    result, records, _, log = run_wrapper(tmp_path, DRY_RUN='1')
    assert result.returncode == 0 and records == []
    assert log.count('DRY_RUN: python run_refs_processing.py') == 5


def test_explicit_range_is_preserved_for_each_element(tmp_path):
    result, _, refs, _ = run_wrapper(tmp_path, wrapper_args=[
        '--start', '202610110000', '--end', '202610130000'])
    assert result.returncode == 0
    for command in refs:
        assert '202610110000' in command and '202610130000' in command
        assert '--date' not in command and '--lookback-days' not in command


def test_hawaii_supported_elements_continue(tmp_path):
    result, _, refs, log = run_wrapper(tmp_path, REGION='hawaii', MODELS='rrfs rrfsens',
        REFS_ELEMENTS='Wind rh precip6hr temp2m', NDFD_ELEMENTS='Wind', OBS_ELEMENTS='Wind',
        FAIL_MODEL='rrfsens', FAIL_ELEMENT='rh')
    assert result.returncode == 1
    assert derived_elements(refs) == ['wind', 'precip6hr', 'temp2m']
    assert all('hawaii' in command for command in refs)
    assert 'Skipping REFS element=rh' in log


@pytest.mark.parametrize('region', ['alaska', 'hawaii'])
def test_nbm_deterministic_qmd_uses_zero_hour_window(tmp_path, region):
    result, records, _, _ = run_wrapper(tmp_path, REGION=region, MODELS='nbm',
        RUN_REFS='0', REFS_ELEMENTS='Wind rh precip6hr temp2m',
        NDFD_ELEMENTS='Wind', OBS_ELEMENTS='Wind')
    assert result.returncode == 0, result.stderr
    commands = {r[r.index('--element')+1]: r for r in records if r[0] == 'run_model_archiver.py'}
    for element in ('precip6hr', 'precip24hr', 'maxt', 'mint'):
        assert commands[element][commands[element].index('--start')+1] == '202610120000'
        assert commands[element][commands[element].index('--end')+1] == '202610130000'
    assert commands['Wind'][commands['Wind'].index('--start')+1] == '202610120100'
