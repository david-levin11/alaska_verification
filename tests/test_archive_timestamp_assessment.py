from pathlib import Path
import sys
import tempfile
import unittest
import pandas as pd
sys.path.insert(0,str(Path(__file__).parents[1]))
from assess_archive_timestamps import assess

class Assessment(unittest.TestCase):
    def test_read_only_boundary_inventory(self):
        with tempfile.TemporaryDirectory() as work:
            path=Path(work)/'history.parquet'
            pd.DataFrame({'station_id':['A'],'init_time':[pd.Timestamp('2026-09-30 18:00')],
                          'valid_time':[pd.Timestamp('2026-10-01 00:00')],'forecast_hour':[6],
                          'qpf_p50':[0.1]}).to_parquet(path,index=False)
            before=path.read_bytes()
            result=assess(path,'precip6hr','nbmqmd')
            self.assertEqual(result['candidate_cross_month_rows'],1)
            self.assertEqual(result['lead_mismatch_rows'],0)
            self.assertEqual(result['off_schedule_init_rows'],0)
            self.assertEqual(path.read_bytes(),before)
            self.assertIn('REQUIRES_PROVENANCE_CHECK',result['assessment'])

if __name__=='__main__':unittest.main()
