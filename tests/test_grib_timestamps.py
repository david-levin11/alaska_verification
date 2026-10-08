"""Regression tests including production extraction from optional retained GRIBs.

Set NBM_TIMESTAMP_REPORTS to comma-separated audit report paths for live tests.
"""
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import gc
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import unittest
from types import SimpleNamespace
import numpy as np
import pandas as pd
sys.path.insert(0, str(Path(__file__).parents[1]))
from grib_timestamps import percentile_times


def message(duration, instant=False):
    ref = datetime(2026,10,1)
    end = ref+timedelta(hours=30)
    m = dict(dataDate=20261001,dataTime=0,stepType='instant' if instant else 'accum',
             validityDate=20261002,validityTime=600)
    if not instant:
        for name,value in zip(('year','month','day','hour','minute','second'),(end.year,end.month,end.day,end.hour,0,0)):
            m[name+'OfEndOfOverallTimeInterval'] = value
    return m


class GribTimes(unittest.TestCase):
    def test_interval_and_instant_endpoint(self):
        for instant in (False,True):
            self.assertEqual(percentile_times(message(18,instant),30),
                             (datetime(2026,10,1),datetime(2026,10,2,6)))

    def test_missing_and_inconsistent_metadata_rejected(self):
        for m,lead in [({},30),(message(18),24),({'dataDate':20261001,'dataTime':0,'stepType':'accum'},30)]:
            with self.assertRaises(ValueError):
                percentile_times(m,lead)

    def test_reported_regressions(self):
        for hours in (6,18,24,48,72):
            m=message(hours)
            # The helper intentionally never uses pygrib.validDate or forecastTime.
            m['forecastTime']=30-hours
            self.assertEqual(percentile_times(m,30)[0],datetime(2026,10,1))

    @unittest.skipUnless(os.environ.get('NBM_TIMESTAMP_REPORTS'), 'Supply retained live audit reports')
    def test_live_production_extraction_and_parquet_roundtrip(self):
        import pygrib
        from archiver_base import Archiver
        root=Path(__file__).parents[1]
        wanted={'extract_model_subset_parallel','parse_date_and_time_from_url','ll_to_index',
                'normalize_lons_to_minus180_180','K_to_F','MS_to_KTS','M_to_IN'}
        tree=ast.parse((root/'utils.py').read_text())
        ns=dict(globals(),pygrib=pygrib)
        exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in wanted],type_ignores=[]),'utils.py','exec'),ns)
        tested=0
        with tempfile.TemporaryDirectory() as work:
            for report_path in os.environ['NBM_TIMESTAMP_REPORTS'].split(','):
                report_path=Path(report_path)
                for row in json.loads(report_path.read_text())['results']:
                    if row['status'] not in ('OFFSET','PASS'):
                        continue
                    model,element=row['model'],row['element']
                    field={'wind':'Wind','gust':'Gust'}.get(element,element)
                    case=f"{row['region']}-{model}-{row['date'].replace('-','')}-{row['cycle']:02d}-f{row['lead']:03d}"
                    subset=report_path.parent/case/f'{element}.grib2'
                    def download_subset(remote_url,local_filename,**kwargs):
                        shutil.copyfile(subset,local_filename)
                        return local_filename
                    ns['download_subset']=download_subset
                    conf=SimpleNamespace(MAX_WORKERS=1,REGION=row['region'],
                         HERBIE_RENAME_MAP={field:{model:{}}},
                         HERBIE_UNIT_CONVERSIONS={field:{model:{field:1}}},
                         HERBIE_REQUIRED_PHRASES={field:{model:[]}},
                         HERBIE_EXCLUDE_PHRASES={field:{model:[]}},
                         PROBABILISTIC_ELEMENTS={model:[field]})
                    ns['config']=conf
                    site=('PHNL',21.32,-157.92) if row['region']=='hawaii' else ('PANC',61.17,-150.0)
                    stations=pd.DataFrame([site],columns=['stid','latitude','longitude'])
                    df=ns['extract_model_subset_parallel']([row['source_url']],stations,[],field,model,conf)
                    self.assertEqual(len(df),1,case+' '+element)
                    self.assertEqual(pd.Timestamp(df.init_time.iloc[0]),pd.Timestamp(row['reference_time']))
                    self.assertEqual(pd.Timestamp(df.valid_time.iloc[0]),pd.Timestamp(row['endpoint']))
                    self.assertEqual(df.forecast_hour.iloc[0],row['lead'])
                    path=Path(work)/f'{tested}.parquet'
                    # Use the production local writer twice, verifying idempotence.
                    writer=SimpleNamespace()
                    for _ in range(2):
                        Archiver.write_local_output(writer,df,path,dedup_columns=['station_id','init_time','valid_time','forecast_hour'])
                    pd.testing.assert_frame_equal(df,pd.read_parquet(path))
                    tested+=1
            self.assertGreater(tested,0)
            print(f'Live production extraction + repeat Parquet write: {tested} subsets passed')

if __name__=='__main__':
    unittest.main()
