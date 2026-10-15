"""Test the production downloader without requiring GRIB libraries or network."""
import ast
import os
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).parents[1]


def downloader(index):
    tree=ast.parse((ROOT/'utils.py').read_text())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('parse_forecast_hour','download_subset')]
    ranges=[]
    def get(url,**kwargs):
        if url.endswith('.idx'):
            return SimpleNamespace(ok=True,text=index,close=lambda: None)
        ranges.append(kwargs['headers']['Range'])
        return SimpleNamespace(status_code=206,content=b'GRIB-test',close=lambda: None)
    ns=dict(os=os,re=re,get_with_retry=get)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'utils.py','exec'),ns)
    return ns['download_subset'],ranges


class PrecipSelectionTest(unittest.TestCase):
    def select(self,index,lead=51,model='rrfs',element='precip6hr',variable='APCP'):
        download,ranges=downloader(index)
        with tempfile.TemporaryDirectory() as tmp:
            result=download(f'https://example.test/model.f{lead:03}.ak.grib2',str(Path(tmp)/'subset.grib2'),
                            [f':{variable}:surface:'],model,element)
            return result is not None,ranges

    def test_real_f051_index_offsets(self):
        # Exact APCP records/offsets from the user's 2026100318 RRFS f051 index.
        index=('97:81987625:d=2026100318:APCP:surface:50-51 hour acc fcst:\n'
               '98:82242714:d=2026100318:APCP:surface:0-51 hour acc fcst:\n'
               '99:90000000:d=2026100318:NEXT:surface:51 hour fcst:')
        ok,ranges=self.select(index)
        self.assertTrue(ok)
        self.assertEqual(ranges,['bytes=82242714-89999999'])

    def test_member_suffix_and_missing_total(self):
        one='1:0:d=2026100318:APCP:surface:50-51 hour acc fcst:ENS=+1'
        total='2:100:d=2026100318:APCP:surface:0-51 hour acc fcst:ENS=+1'
        self.assertEqual(self.select(one+'\n'+total)[1],['bytes=100-'])
        self.assertEqual(self.select(one),(False,[]))

    def test_hour_and_day_labels_hrrr_rrfs(self):
        for model in ['hrrr','rrfs']:
            for lead in [6,21,24,48,51]:
                labels=[f'0-{lead} hour acc fcst']
                if lead%24==0: labels.append(f'0-{lead//24} day acc fcst')
                for label in labels:
                    with self.subTest(model=model,lead=lead,label=label):
                        index=(f'1:0:d=2026100318:APCP:surface:{lead-1}-{lead} hour acc fcst:\n'
                               f'2:100:d=2026100318:APCP:surface:{label}:')
                        self.assertEqual(self.select(index,lead,model)[1],['bytes=100-'])

    def test_cumulative_snow_still_selected(self):
        index='1:0:d=2026100318:ASNOW:surface:0-51 hour acc fcst:'
        self.assertEqual(self.select(index,element='snow6hr',variable='ASNOW')[1],['bytes=0-'])


if __name__=='__main__':
    unittest.main()

