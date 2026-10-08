import sys
from pathlib import Path
from datetime import datetime
import unittest
sys.path.insert(0, str(Path(__file__).parents[1]))
from audit_nbm_timestamps import matching_ranges, assess

class Message(dict):
    pass

class TimestampAudit(unittest.TestCase):
    def test_select_percentile_not_probability(self):
        idx = ('1:0:d=x:TMP:2 m above ground:12-30 hour max fcst:prob <255\n'
               '2:100:d=x:TMP:2 m above ground:12-30 hour max fcst:50% level\n'
               '3:200:d=x:TMP:2 m above ground:12-30 hour max fcst:90% level\n')
        self.assertEqual(list(matching_ranges(idx,'maxt',30,50))[0][1:], (100,199))
        self.assertEqual(list(matching_ranges(idx,'mint',30,50)), [])

    def test_annotated_snow_accumulation(self):
        idx = '1:0:d=x:ASNOW:surface:5-29 hour acc@(fcst,dt=24 hour),missing=0:50% level\n'
        self.assertEqual(len(list(matching_ranges(idx,'snow24hr',29,50))),1)
        self.assertEqual(list(matching_ranges(idx,'snow6hr',29,50)),[])

    def test_day_units(self):
        idx = '1:0:d=x:APCP:surface:1-2 day acc fcst:50% level\n'
        self.assertEqual(len(list(matching_ranges(idx,'precip24hr',48,50))),1)
        self.assertEqual(list(matching_ranges(idx,'precip6hr',48,50)),[])

    def test_known_shift_and_correct_endpoint(self):
        m = Message(dataDate=20261006,dataTime=0,percentileValue=50,
                    validityDate=20261007,validityTime=600,
                    yearOfEndOfOverallTimeInterval=2026,monthOfEndOfOverallTimeInterval=10,
                    dayOfEndOfOverallTimeInterval=7,hourOfEndOfOverallTimeInterval=6,
                    minuteOfEndOfOverallTimeInterval=0,secondOfEndOfOverallTimeInterval=0)
        m.validDate = datetime(2026,10,6,12)
        r = assess(m,datetime(2026,10,6),30,50)
        self.assertEqual(r['status'],'OFFSET')
        self.assertEqual(r['init_offset_hours'],-18)
        self.assertEqual(r['valid_offset_hours'],-18)
        m.validDate = datetime(2026,10,7,6)
        self.assertEqual(assess(m,datetime(2026,10,6),30,50)['status'],'PASS')
        self.assertEqual(assess(m,datetime(2026,10,6),24,50)['status'],'METADATA_MISMATCH')

    def test_instantaneous_and_wrong_percentile(self):
        m = Message(dataDate=20261006,dataTime=0,percentileValue=50,
                    validityDate=20261006,validityTime=600)
        m.validDate = datetime(2026,10,6,6)
        self.assertEqual(assess(m,datetime(2026,10,6),6,50)['status'],'PASS')
        with self.assertRaises(ValueError):
            assess(m,datetime(2026,10,6),6,90)

if __name__ == '__main__':
    unittest.main()
