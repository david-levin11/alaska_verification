import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).parents[1]))
from diagnose_hrrr_snow import select_record,compare_bounds,resolve_units,precip_units

class DiagnosticTests(unittest.TestCase):
    def test_exact_period(self):
        idx='1:0:d=2026100412:ASNOW:surface:50-51 hour acc fcst:\n2:100:d=2026100412:ASNOW:surface:0-51 hour acc fcst:\n3:200:d=2026100412:TMP:surface:51 hour fcst:'
        self.assertEqual(select_record(idx,51)[1:],(100,199))
    def test_day_and_ambiguous(self):
        line='1:0:d=2026100412:ASNOW:surface:0-1 day acc fcst:'
        self.assertEqual(select_record(line,24)[1:],(0,None))
        with self.assertRaises(ValueError): select_record(line+'\n'+line,24)
        with self.assertRaises(ValueError): select_record(line,6)
    def test_unknown_units_require_explicit_assumption(self):
        self.assertIn('Decoded',resolve_units('m'))
        with self.assertRaises(ValueError): resolve_units('unknown')
        self.assertIn('ASSUMED',resolve_units('unknown',True))
        with self.assertRaises(ValueError): resolve_units('kg m**-2',True)

    def test_precip_exact_window_and_variable(self):
        idx=('1:0:d=2026100100:ASNOW:surface:0-1 day acc fcst:\n'
             '2:100:d=2026100100:APCP:surface:23-24 hour acc fcst:\n'
             '3:200:d=2026100100:APCP:surface:0-1 day acc fcst:\n'
             '4:300:d=2026100100:TMP:surface:24 hour fcst:')
        self.assertEqual(select_record(idx,24,'APCP')[1:],(200,299))
        self.assertEqual(select_record(idx,24)[1:],(0,99))
        hourly='1:0:d=2026100100:APCP:surface:0-18 hour acc fcst:'
        self.assertEqual(select_record(hourly,18,'APCP')[1:],(0,None))
        with self.assertRaises(ValueError): select_record(hourly.replace('0-18','17-18'),18,'APCP')

    def test_precip_conversion(self):
        for unit in ['kg m**-2','kg m-2','kg/m^2','mm']:
            self.assertEqual(precip_units(unit)[1],1)
        self.assertEqual(precip_units('m')[1],1000)
        self.assertAlmostEqual((8.25*precip_units('kg m**-2')[1])/25.4,0.3248031496062992)
        with self.assertRaises(ValueError): precip_units('unknown')
        with self.assertRaises(ValueError): precip_units('K')

    def test_bounds(self):
        self.assertTrue(compare_bounds(-.00001,{'packingError':.000006},{'packingError':.000006})['within_reported_packing_error'])
        self.assertFalse(compare_bounds(-.001,{'packingError':.000006},{'packingError':.000006})['within_reported_packing_error'])
        self.assertIsNone(compare_bounds(-.00001,{}, {})['within_reported_packing_error'])

if __name__=='__main__': unittest.main()
