import pandas as pd
import pytest
import archiver_config as config
from ensemble_processing import add_interval_precip_from_total


@pytest.mark.parametrize('model',['hrrr','rrfs','rrfsens'])
@pytest.mark.parametrize('element',['precip6hr','snow6hr'])
def test_downloads_include_f003_endpoint(model,element):
    leads=config.HERBIE_FORECASTS[model][element]
    assert all(lead in leads for lead in (3,6,9))


@pytest.mark.parametrize('total,interval',[('precip_accum','precip_6h'),('snow_accum','snow_6h')])
def test_f009_uses_f003_same_run_and_member(total,interval):
    init=pd.Timestamp('2026-10-01',tz='UTC')
    rows=[]
    for member,values in [('m001',[.2,.5,.9]),('m002',[1.,2.,4.])]:
        for lead,value in zip([3,6,9],values):
            rows.append(dict(station_id='PAJN',member_id=member,init_time=init,
                valid_time=init+pd.Timedelta(hours=lead),forecast_hour=lead,**{total:value}))
    frame=pd.DataFrame(rows)
    result=add_interval_precip_from_total(frame,total_col=total,out_col=interval)
    assert result.loc[result.forecast_hour==3,interval].isna().all()
    assert result.loc[result.forecast_hour==6,interval].tolist()==[.5,2.]
    assert result.loc[result.forecast_hour==9,interval].tolist()==[.7,3.]
    missing=add_interval_precip_from_total(frame[frame.forecast_hour!=3],total_col=total,out_col=interval)
    assert missing.loc[missing.forecast_hour==9,interval].isna().all()
