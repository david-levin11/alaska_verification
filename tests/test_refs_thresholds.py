import json
import pandas as pd
import pytest
from ensemble_processing import summarize_refs
from refs_threshold_config import load_threshold_config, resolve_threshold_config
from run_refs_processing import process_range
from test_refs_batch import make_sources


def test_strict_comparison_and_missing():
    cycle = pd.Timestamp('2026-10-01', tz='UTC')
    members = pd.DataFrame(dict(station_id=['PAOU']*3, ensemble_init_time=[cycle]*3,
        valid_time=[cycle]*3, ensemble_forecast_hour=[6]*3,
        ensemble_member_id=['a','b','c'], source_available=[True]*3, rh=[20.,30.,40.]))
    for operator, column in [('>', 'prob_gt_30'), ('<', 'prob_lt_30')]:
        result = summarize_refs(members, 'rh', thresholds=[30], probability_operator=operator)
        assert result.iloc[0][column] == pytest.approx(1/3)
        missing = members.copy(); missing.loc[2,'rh'] = float('nan')
        assert pd.isna(summarize_refs(missing,'rh',thresholds=[30],probability_operator=operator).iloc[0][column])
        partial = summarize_refs(missing,'rh',thresholds=[30],probability_operator=operator,require_complete=False)
        assert partial.iloc[0][column] == (0 if operator == '>' else .5)


@pytest.mark.parametrize('change', [
    {'units':'mph'}, {'operator':'>='}, {'thresholds':[float('nan')]},
    {'thresholds':[-1]}, {'thresholds':[True]}, {'thresholds':[101]}])
def test_invalid_config(tmp_path,change):
    config=load_threshold_config(); config['rh'].update(change)
    path=tmp_path/'thresholds.json'; path.write_text(json.dumps(config))
    with pytest.raises(ValueError): load_threshold_config(path)


def test_overrides_require_variable():
    with pytest.raises(ValueError,match='value-column'):
        resolve_threshold_config(thresholds=[20])
    assert resolve_threshold_config(thresholds=[],value_column='rh')['rh']['thresholds']==[]


def test_variable_recipes_subset_rerun_and_conflict(tmp_path):
    cycle=pd.Timestamp('2026-10-01',tz='UTC'); end=cycle+pd.Timedelta(hours=6)
    make_sources(tmp_path/'model',[cycle])
    kw=dict(archive_root=tmp_path/'model',output_root=tmp_path/'out',elements=['Wind'],forecast_hours=[6])
    process_range(cycle,end,**kw)
    path=tmp_path/'out/wind/2026_10_archive.parquet'
    result=pd.read_parquet(path).set_index('value_column')
    assert result.loc['wind_speed_kt','prob_gt_15']==1
    assert result.loc['wind_speed_kt','prob_gt_20']==0
    assert result.loc['wind_gust_kt','prob_gt_25']==1
    assert pd.isna(result.loc['wind_gust_kt','prob_gt_15'])
    assert result.statistics_config.nunique()==2
    process_range(cycle,end,value_column='wind_speed_kt',**kw)
    assert len(pd.read_parquet(path))==2
    with pytest.raises(ValueError,match='recipe differs'):
        process_range(cycle,end,value_column='wind_speed_kt',thresholds=[99],**kw)
    old=pd.read_parquet(path).assign(statistics_config='legacy recipe')
    old.to_parquet(path,index=False)
    with pytest.raises(ValueError,match='recipe differs'):
        process_range(cycle,end,**kw)
