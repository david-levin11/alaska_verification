"""Exercise real daily shell orchestration using the simulated archivers."""
from test_daily_refs_elements import run_wrapper
import pytest


def experimental(records):
    imports=[r for r in records if r[0]=='import_ndfd_experimental.py']
    obs=[r for r in records if r[0]=='run_obs_forecast_windows.py']
    return imports,obs


def setup(tmp_path):
    inputs=tmp_path/'incoming exports';inputs.mkdir()
    return dict(MODELS='urma',RUN_REFS='0',RUN_NDFD_EXP='1',NDFD_EXP_INPUT_DIR=str(inputs),
        NDFD_EXP_ARCHIVE_ROOT=str(tmp_path/'model with spaces'),RUN_OBS_EXP='1',RUN_OBS_12HR='1')


def test_import_and_both_obs_profiles_run_on_empty_input(tmp_path):
    settings=setup(tmp_path)
    result,records,_,log=run_wrapper(tmp_path,wrapper_args=['2026-11-30'],**settings)
    assert result.returncode==0,result.stdout+result.stderr
    imports,obs=experimental(records)
    assert len(imports)==1 and len(obs)==6
    assert records.index(imports[0])<records.index(obs[0])
    assert '--allow-empty' in imports[0] and '--apply' in imports[0]
    assert imports[0][imports[0].index('--input-dir')+1]==settings['NDFD_EXP_INPUT_DIR']
    for command in imports+obs:
        assert command[command.index('--archive-root')+1]==settings['NDFD_EXP_ARCHIVE_ROOT']
    for command in obs:
        assert command[command.index('--start')+1]=='202611240000'
        assert command[command.index('--end')+1]=='202612010000'
        assert '--allow-empty' in command and '--replace-conflicts' in command
    assert {r[r.index('--profile')+1] for r in obs}=={'experimental','12hr'}
    assert 'OK: experimental NDFD import' in log


def test_new_import_failure_keeps_old_forecast_observation_jobs(tmp_path):
    settings=setup(tmp_path)
    result,records,_,log=run_wrapper(tmp_path,FAIL_EXPERIMENTAL='1',**settings)
    assert result.returncode==1
    assert len(experimental(records)[1])==6
    assert 'FAILED: experimental NDFD import' in log
    assert 'OK: observations profile=12hr element=minrh' in log


def test_single_obs_failure_does_not_stop_other_elements_or_profile(tmp_path):
    settings=setup(tmp_path)
    result,records,_,log=run_wrapper(tmp_path,FAIL_EXP_PROFILE='experimental',FAIL_EXP_ELEMENT='maxt',**settings)
    assert result.returncode==1
    assert len(experimental(records)[1])==6
    assert 'FAILED: observations profile=experimental element=maxt' in log
    assert 'OK: observations profile=experimental element=mint' in log


def test_profiles_can_run_without_new_import_and_preserve_explicit_range(tmp_path):
    result,records,_,_=run_wrapper(tmp_path,wrapper_args=['--start','202611300000','--end','202612020000'],
        MODELS='urma',RUN_REFS='0',RUN_NDFD_EXP='0',RUN_OBS_EXP='1',RUN_OBS_12HR='0')
    assert result.returncode==0
    imports,obs=experimental(records)
    assert not imports and len(obs)==3
    assert all('--start' in c and '202611300000' in c and '202612020000' in c for c in obs)


def test_default_observation_profiles_run_without_experimental_import_setting(tmp_path):
    result,records,_,_=run_wrapper(tmp_path,MODELS='urma',RUN_REFS='0',RUN_OBS='1',
        RUN_NDFD_EXP='0',RUN_OBS_EXP='',RUN_OBS_12HR='')
    assert result.returncode==0
    imports,obs=experimental(records)
    assert not imports and len(obs)==6
    assert {c[c.index('--profile')+1] for c in obs}=={'experimental','12hr'}


def test_dry_run_and_hawaii_root(tmp_path):
    settings=setup(tmp_path)
    settings.pop('NDFD_EXP_ARCHIVE_ROOT')
    result,records,_,log=run_wrapper(tmp_path,wrapper_args=['--region','hawaii','2026-11-30'],
        DRY_RUN='1',REFS_ELEMENTS='Wind rh precip6hr temp2m',**settings)
    assert result.returncode==0 and not records
    assert log.count('DRY_RUN: python import_ndfd_experimental.py')==1
    assert log.count('DRY_RUN: python run_obs_forecast_windows.py')==6
    assert '--archive-root hawaii/model' in log
    assert '--region hawaii' in log


@pytest.mark.parametrize('bad',[
    {'NDFD_EXP_INPUT_DIR':'/nonexistent/exports'},
    {'OBS_EXP_LOOKBACK_DAYS':'0'},
    {'NDFD_EXP_ARCHIVE_ROOT':'s3://bucket/model'},
])
def test_bad_configuration_fails_before_downloads(tmp_path,bad):
    settings=setup(tmp_path);settings.update(bad)
    # Configuration validation happens before the helper's log file is made.
    import os,subprocess
    from pathlib import Path
    script=Path(__file__).parents[1]/'run_daily_archives.sh'
    result=subprocess.run(['bash',str(script),'2026-11-30'],env=dict(os.environ,**settings),capture_output=True,text=True)
    assert result.returncode==2
    assert 'requires an existing' in result.stderr or 'must be' in result.stderr
