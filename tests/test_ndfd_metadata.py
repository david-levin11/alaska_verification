"""Exercise fresh precipitation metadata through the real Synoptic helper."""
from types import SimpleNamespace
import pytest
import ast
from pathlib import Path
import pandas as pd


def metadata_class(monkeypatch, get):
    # Execute production metadata code without optional GRIB dependencies.
    root=Path(__file__).parents[1]
    tree=ast.parse((root/'utils.py').read_text())
    helpers=[n for n in tree.body if isinstance(n,ast.FunctionDef)
             and n.name in ('create_precip_metadata','parse_metadata')]
    ns=dict(requests=SimpleNamespace(get=get),pd=pd,Path=Path)
    exec(compile(ast.Module(body=helpers,type_ignores=[]),'utils.py','exec'),ns)
    tree=ast.parse((root/'ndfd_archiver.py').read_text())
    node=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='NDFDArchiver')
    class Archiver:
        def __init__(self,config): self.config=config
    ns['Archiver']=Archiver
    exec(compile(ast.Module(body=[node],type_ignores=[]),'ndfd_archiver.py','exec'),ns)
    return ns['NDFDArchiver']


@pytest.mark.parametrize('region,state', [('alaska','ak'),('hawaii','hi')])
def test_fresh_precip_metadata_then_cached(tmp_path,monkeypatch,region,state):
    calls=[]
    def get(url,params):
        calls.append(params)
        return SimpleNamespace(status_code=200,json=lambda:{'STATION':[
            {'STID':'TEST','LATITUDE':'21.3','LONGITUDE':'-157.9'}]})
    NDFDArchiver=metadata_class(monkeypatch,get)
    config=SimpleNamespace(REGION=region,STATE=state,OBS=str(tmp_path),
        API_KEY='test-token',METADATA_URL='https://example.test/metadata',
        NETWORK='1,107',ELEMENT='precip6hr',OBS_START='202610060000')
    first=NDFDArchiver(config)
    assert first.station_df.stid.tolist()==['TEST']
    assert calls==[dict(token='test-token',precip='1',network='1,107',state=state,output='json')]
    assert (tmp_path/f'{region}_precip6hr_obs_metadata.csv').exists()
    NDFDArchiver(config)
    assert len(calls)==1
