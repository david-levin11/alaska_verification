"""NDFD uses fresh regional stations regardless of element, network or cache."""
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
import ast
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).parents[1]
ELEMENTS = ['Wind', 'Gust', 'rh', 'precip6hr', 'maxt', 'mint', 'snow6hr']


def metadata_class(get, extract=None):
    # Execute production class/metadata parser without optional GRIB libraries.
    tree = ast.parse((ROOT/'utils.py').read_text())
    helper = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'parse_metadata')
    ns = dict(requests=SimpleNamespace(get=get), pd=pd, Path=Path)
    exec(compile(ast.Module(body=[helper], type_ignores=[]), 'utils.py', 'exec'), ns)
    tree = ast.parse((ROOT/'ndfd_archiver.py').read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'NDFDArchiver')
    class Archiver:
        def __init__(self, config): self.config = config
    ns['Archiver'] = Archiver
    ns['extract_ndfd_forecasts_parallel'] = extract
    exec(compile(ast.Module(body=[node], type_ignores=[]), 'ndfd_archiver.py', 'exec'), ns)
    return ns['NDFDArchiver']


def station(stid, network=106, **extra):
    return dict(STID=stid, LATITUDE='58.38', LONGITUDE='-134.64', STATE='AK',
                STATUS='ACTIVE', MNET_ID=str(network), **extra)


def response(rows, code=1):
    return SimpleNamespace(raise_for_status=lambda: None,
        json=lambda: {'SUMMARY': {'RESPONSE_CODE': code}, 'STATION': rows})


def config(tmp_path, element='precip6hr', region='alaska', state='ak'):
    return SimpleNamespace(REGION=region, STATE=state, OBS=str(tmp_path),
        API_KEY='TEST_TOKEN', METADATA_URL='https://example.test/metadata',
        NETWORK='1,107', ELEMENT=element, OBS_START='202610060000', HFMETAR='0',
        NDFD_FILE_STRINGS={e: ['value', 'direction'] if e == 'Wind' else ['value'] for e in ELEMENTS},
        TMP=str(tmp_path/'tmp'))


@pytest.mark.parametrize('element', ELEMENTS)
def test_all_elements_include_excluded_networks_and_ignore_old_cache(tmp_path, element):
    rows = [station('ABYA2', 106), station('HADA2', 2), station('JTMA2', 12),
            station('LLKA2', 25), station('ECEA2', 65), station('GTSA2', 123),
            station('CSMA2', 293), station('HAOA2', 323)]
    rows += [{**station('NKXA2', 332), 'STATUS': 'INACTIVE'},
             {**station('NDBCRIXA2', 286), 'STATE': None},
             {**station('ALEUTIAN'), 'LONGITUDE': '173', 'LATITUDE': '52'},
             {**station('OUTSIDE'), 'LONGITUDE': '4.2'}]
    get = Mock(return_value=response(rows))  # Includes duplicates across boxes.
    extract = Mock(return_value=pd.DataFrame({'success': [True]}))
    cls = metadata_class(get, extract)
    legacy = tmp_path/f'alaska_{element}_obs_metadata.csv'
    legacy.write_text('stid,latitude,longitude\nOLD,58,-135\n')
    original = legacy.read_bytes()
    cfg = config(tmp_path, element)
    archiver = cls(cfg)
    expected = {r['STID'] for r in rows} - {'OUTSIDE'}
    assert set(archiver.station_df.stid) == expected
    assert not archiver.station_df.stid.duplicated().any()
    assert legacy.read_bytes() == original
    snapshot = tmp_path/f'ndfd_{element.lower()}_model_sites.csv'
    assert set(pd.read_csv(snapshot).stid) == expected
    assert get.call_count == 2
    for call in get.call_args_list:
        params = call.kwargs['params']
        assert not {'network', 'status', 'state', 'vars', 'precip'} & params.keys()
    assert [c.kwargs['params']['bbox'] for c in get.call_args_list] == [
        '-179.999999,50,-129,73', '170,50,179.999999,73']
    # The production dispatcher forwards the full station set to extraction.
    archiver.process_files({'value': ['test.grib2']})
    assert set(extract.call_args.args[2].stid) == expected
    # New stations are discovered even though both snapshots and old caches exist.
    get.return_value = response(rows + [station('NEW_SITE')])
    assert 'NEW_SITE' in set(cls(cfg).station_df.stid)
    assert get.call_count == 4


def test_hawaii_keeps_state_selection_but_includes_all_statuses(tmp_path):
    rows = [dict(STID='PHNL', STATE='HI', LATITUDE='21.3', LONGITUDE='-157.9', STATUS='ACTIVE'),
            dict(STID='OLD_HI', STATE='HI', LATITUDE='21.4', LONGITUDE='-157.8', STATUS='INACTIVE')]
    get = Mock(return_value=response(rows))
    archiver = metadata_class(get)(config(tmp_path, region='hawaii', state='hi'))
    assert set(archiver.station_df.stid) == {'PHNL', 'OLD_HI'}
    assert get.call_count == 1
    params = get.call_args.kwargs['params']
    assert params['state'] == 'hi'
    assert not {'network', 'status', 'vars', 'precip'} & params.keys()


def test_failure_in_second_box_stops_without_using_legacy_cache(tmp_path):
    legacy = tmp_path/'alaska_precip6hr_obs_metadata.csv'
    legacy.write_text('stid,latitude,longitude\nOLD,58,-135\n')
    get = Mock(side_effect=[response([station('ABYA2')]), response([], code=200)])
    with pytest.raises(ValueError, match='metadata response'):
        metadata_class(get)(config(tmp_path))
    assert not (tmp_path/'ndfd_precip6hr_model_sites.csv').exists()


def test_empty_inventory_stops_before_extraction(tmp_path):
    get = Mock(return_value=response([], code=2))
    with pytest.raises(ValueError, match='No stations'):
        metadata_class(get)(config(tmp_path))


def test_locked_diagnostic_snapshot_does_not_block_extraction(tmp_path, monkeypatch, capsys):
    get = Mock(return_value=response([station('ABYA2')]))
    def locked(*args, **kwargs):
        raise PermissionError('snapshot is open elsewhere')
    monkeypatch.setattr(pd.DataFrame, 'to_csv', locked)
    archiver = metadata_class(get)(config(tmp_path))
    assert archiver.station_df.stid.tolist() == ['ABYA2']
    assert 'WARNING' in capsys.readouterr().out


def test_missing_key_does_not_fall_back_to_cached_inventory(tmp_path):
    cfg = config(tmp_path)
    cfg.API_KEY = None
    get = Mock()
    with pytest.raises(ValueError, match='SYNOPTIC_API_KEY'):
        metadata_class(get)(cfg)
    get.assert_not_called()
