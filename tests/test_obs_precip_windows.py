"""Exercise API requests, actual interval boundaries and monthly archive writes."""
import ast
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from dateutil.relativedelta import relativedelta

import obs_archiver
from obs_archiver import ObsArchiver


@pytest.fixture
def source(monkeypatch):
    archiver = ObsArchiver.__new__(ObsArchiver)
    archiver.api_token = 'TEST_TOKEN'
    archiver.max_retries = 2
    archiver.initial_wait = 0
    archiver.station_metadata = {'PAJN': {'zone': '1', 'cwa': 'AJK'}}
    calls = []
    mutate = [lambda rows: rows]

    def get(url, params, timeout):
        calls.append(params)
        start = pd.to_datetime(params['start'], format='%Y%m%d%H%M', utc=True)
        end = pd.to_datetime(params['end'], format='%Y%m%d%H%M', utc=True)
        rows = []
        duration = pd.Timedelta(hours=int(params['interval']))
        for index, endpoint in enumerate(pd.date_range(start + duration, end, freq=duration), 1):
            actual_end = endpoint - pd.Timedelta(minutes=7)
            rows.append(dict(first_report=str(actual_end-duration),
                last_report=str(actual_end), total=float(endpoint.day*4+endpoint.hour//6)+0.37, interval=index,
                report_type='precip_accum_one_hour'))
        payload = {'SUMMARY': {'RESPONSE_CODE': 1}, 'UNITS': {'precipitation': 'in'},
            'STATION': [dict(STID='PAJN', LATITUDE='58', LONGITUDE='-135', ELEVATION='10',
                            OBSERVATIONS={'precipitation': mutate[0](rows)})]}
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)
    monkeypatch.setattr(obs_archiver.requests, 'get', get)
    monkeypatch.setattr(obs_archiver, 'sleep', lambda wait: None)
    return archiver, calls, mutate


def fetch(archiver, start='202606010000', end='202606012359', accum_hours=24):
    return archiver.fetch_precip_rolling(['PAJN'], start, end, accum_hours=accum_hours, step_hours=6)


def test_daily_four_direct_totals_and_actual_bounds(source):
    archiver, calls, _ = source
    df = fetch(archiver)
    assert len(calls) == 4
    assert [c['start'] for c in calls] == ['202605310000','202605310600','202605311200','202605311800']
    assert [c['end'] for c in calls] == ['202606010000','202606010600','202606011200','202606011800']
    assert all(c['interval'] == '24' and c['pmode'] == 'intervals' for c in calls)
    assert df.nominal_end_time.dt.hour.tolist() == [0, 6, 12, 18]
    assert df.nominal_end_time.dt.day.tolist() == [1]*4
    assert (df.end_time == df.nominal_end_time-pd.Timedelta(minutes=7)).all()
    assert (df.start_time == df.end_time-pd.Timedelta(hours=24)).all()
    assert (df.first_report == df.start_time).all()
    # Each amount is the API's independent total; no shorter amounts are summed.
    assert df.precip_total.tolist() == [4.37,5.37,6.37,7.37]
    assert df.precip_units.tolist() == ['in']*4


def test_daily_and_monthly_requests_produce_identical_windows(source):
    archiver, calls, _ = source
    whole = fetch(archiver, '202605310000', '202606022359')
    assert len(calls) == 4
    for params in calls:
        assert params['interval'] == '24'
        first = pd.to_datetime(params['start'], format='%Y%m%d%H%M')
        last = pd.to_datetime(params['end'], format='%Y%m%d%H%M')
        assert last-first == pd.Timedelta(hours=72)
    days = pd.concat([fetch(archiver, f'{date}0000', f'{date}2359')
                      for date in ('20260531','20260601','20260602')], ignore_index=True)
    pd.testing.assert_frame_equal(whole, days)


def test_non_aligned_request_is_trimmed_to_requested_endpoints(source):
    archiver, calls, _ = source
    df = fetch(archiver, '202606010101', '202606011202')
    assert len(calls) == 2
    assert df.nominal_end_time.dt.hour.tolist() == [6,12]


@pytest.mark.parametrize('renumber', [False, True])
@pytest.mark.parametrize('accum_hours', [6,24])
def test_missing_daily_interval_keeps_later_interval_positions(source, renumber, accum_hours):
    archiver, _, mutate = source
    def remove_middle(rows):
        kept = [row for row in rows if row['interval'] != 2]
        if renumber:
            for number, row in enumerate(kept, 1):
                row['interval'] = number
        return kept
    mutate[0] = remove_middle
    df = fetch(archiver, '202606010000', '202606032359', accum_hours=accum_hours)
    if accum_hours == 24:
        assert df.nominal_end_time.dt.day.tolist() == [1]*4+[3]*4
        assert df.precip_total.tolist() == [4.37,5.37,6.37,7.37,12.37,13.37,14.37,15.37]
    else:
        expected = pd.date_range('2026-06-01', '2026-06-03 18:00', freq='6h', tz='UTC').delete(1)
        assert df.nominal_end_time.tolist() == expected.tolist()


def test_monthly_request_recovers_after_brief_gaps_at_pajn_endpoints(source):
    archiver, _, mutate = source
    def brief_gaps(rows):
        kept = []
        for row in rows:
            endpoint = pd.Timestamp(row['last_report']) + pd.Timedelta(minutes=7)
            if (endpoint.day, endpoint.hour) in ((12,6), (6,18)):
                continue
            kept.append(row)
        for number, row in enumerate(kept, 1):
            row['interval'] = number
        return kept
    mutate[0] = brief_gaps
    df = fetch(archiver, '202609010000', '202609302359')
    # Reproduce the uploaded file's 76-row PAJN pattern under the old mapping.
    # Two missing totals must not discard the 42 later, independently valid ones.
    assert len(df) == 118
    assert df.groupby(df.nominal_end_time.dt.hour).size().to_dict() == {0:30,6:29,12:30,18:29}
    daily = pd.concat([fetch(archiver, f'202609{day:02}0000', f'202609{day:02}2359')
                       for day in range(1,31)], ignore_index=True)
    pd.testing.assert_frame_equal(df, daily)


@pytest.mark.parametrize('damage', ['missing', 'duplicate', 'conflict', 'late_start', 'early_start',
                                  'short', 'missing_start', 'missing_end', 'null', 'negative',
                                  'infinite', 'late_end'])
def test_bad_direct_total_does_not_invalidate_other_endpoints(source, damage):
    archiver, _, mutate = source
    def damage_rows(rows):
        row = rows[0]
        if pd.Timestamp(row['last_report']).hour != 23:
            return rows
        if damage == 'missing': rows.pop(0)
        elif damage == 'duplicate': rows.append(dict(row))
        elif damage == 'conflict': rows.append({**row, 'total': 999.})
        elif damage in ('late_start', 'early_start', 'short'):
            minutes = {'late_start': 60, 'early_start': -60, 'short': 120}[damage]
            row['first_report'] = str(pd.Timestamp(row['first_report'])+pd.Timedelta(minutes=minutes))
        elif damage == 'missing_start': row.pop('first_report')
        elif damage == 'missing_end': row.pop('last_report')
        elif damage == 'null': row['total'] = None
        elif damage == 'negative': row['total'] = -1
        elif damage == 'infinite': row['total'] = float('inf')
        elif damage == 'late_end': row['last_report'] = str(pd.Timestamp(row['last_report'])+pd.Timedelta(hours=1))
        return rows
    mutate[0] = damage_rows
    df = fetch(archiver)
    assert len(df) == (4 if damage == 'duplicate' else 3)
    assert df.nominal_end_time.dt.hour.tolist() == ([0,6,12,18] if damage == 'duplicate' else [6,12,18])


@pytest.mark.parametrize('ordinal', [None, 0, 2, 'not-a-number'])
def test_calendar_alignment_does_not_depend_on_interval_ordinal(source, ordinal):
    archiver, _, mutate = source
    def ordinals(rows):
        for row in rows:
            if ordinal is None:
                row.pop('interval')
            else:
                row['interval'] = ordinal
        return rows
    mutate[0] = ordinals
    df = fetch(archiver)
    assert df.nominal_end_time.dt.hour.tolist() == [0,6,12,18]


def test_zero_totals_are_valid(source):
    archiver, _, mutate = source
    def dry(rows):
        for row in rows:
            row['total'] = 0
        return rows
    mutate[0] = dry
    assert fetch(archiver).precip_total.tolist() == [0.]*4


def test_six_hour_totals_use_same_validated_boundaries(source):
    archiver, calls, _ = source
    df = fetch(archiver, accum_hours=6)
    assert len(calls) == 1
    assert calls[0]['start'] == '202605311800'
    assert calls[0]['end'] == '202606011800'
    assert calls[0]['interval'] == '6'
    assert df.nominal_end_time.dt.hour.tolist() == [0,6,12,18]
    assert (df.end_time-df.start_time == pd.Timedelta(hours=6)).all()


def test_api_failure_is_not_silently_saved_as_missing_stations(source, monkeypatch):
    archiver, calls, _ = source
    monkeypatch.setattr(obs_archiver.requests, 'get', lambda *a, **k: SimpleNamespace(
        raise_for_status=lambda: None, json=lambda: {'SUMMARY': {'RESPONSE_CODE': 99}}))
    with pytest.raises(RuntimeError, match='exhausted retries'):
        fetch(archiver)


def test_no_data_response_is_empty(source, monkeypatch):
    archiver, _, _ = source
    monkeypatch.setattr(obs_archiver.requests, 'get', lambda *a, **k: SimpleNamespace(
        raise_for_status=lambda: None, json=lambda: {'SUMMARY': {'RESPONSE_CODE': 2}}))
    assert fetch(archiver).empty


def test_missing_report_times_are_not_invented(source):
    archiver, _, mutate = source
    def remove_times(rows):
        for row in rows:
            row.pop('last_report')
        return rows
    mutate[0] = remove_times
    assert fetch(archiver).empty


@pytest.mark.parametrize('legacy_archive', [False, True])
def test_runner_partitions_midmonth_start_and_repeated_writes(source, tmp_path, legacy_archive):
    archiver, _, _ = source
    archiver.get_station_metadata = lambda: ['PAJN']
    cfg = SimpleNamespace(configure_region=Mock(), OBS_VARS={'precip24hr': []},
        OBS=str(tmp_path), MODEL_DIR=str(tmp_path))
    tree = ast.parse((Path(__file__).parents[1]/'run_obs_archiver.py').read_text())
    method = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                  and n.name == 'run_monthly_obs_archiving')
    ns = dict(config=cfg, pd=pd, os=os, sys=sys, relativedelta=relativedelta,
              ObsArchiver=lambda config: archiver)
    exec(compile(ast.Module(body=[method], type_ignores=[]), 'runner', 'exec'), ns)
    if legacy_archive:
        old = fetch(archiver, '202606290000', '202606302359').iloc[:, :12]
        old['precip_total'] = 999.
        path = tmp_path/'obs'/'precip24hr'/'2026_06_archive.parquet'
        path.parent.mkdir(parents=True)
        old.to_parquet(path, index=False)
    for _ in range(2):
        ns[method.name](pd.Timestamp('2026-06-30'), pd.Timestamp('2026-07-01 23:59'),
                        'precip24hr', True)
    for month in ('2026_06', '2026_07'):
        df = pd.read_parquet(tmp_path/'obs'/'precip24hr'/f'{month}_archive.parquet')
        assert len(df) == (8 if legacy_archive and month == '2026_06' else 4)
        if legacy_archive and month == '2026_06':
            assert df.precip_total.tolist()[:4] == [999.]*4
            df = df.iloc[4:]
        assert not df.duplicated(['stid','accum_hours','end_time']).any()
        assert (df.precip_total != 999.).all()
        assert df.nominal_end_time.dt.month.tolist() == [int(month[-2:])]*4
        assert df.nominal_end_time.dt.hour.tolist() == [0,6,12,18]
