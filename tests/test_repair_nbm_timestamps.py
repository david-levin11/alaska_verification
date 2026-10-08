from pathlib import Path
import sys
import tempfile
import unittest
import pandas as pd
sys.path.insert(0,str(Path(__file__).parents[1]))
from repair_nbm_timestamps import repair,repair_in_place,digest,MARKER,STATE_FILE
import pyarrow.parquet as pq


def source_file(root,month,init):
    p=Path(root)/'nbmqmd'/'precip6hr'/f'{month}_archive.parquet'
    p.parent.mkdir(parents=True,exist_ok=True)
    t=pd.Timestamp(init)
    pd.DataFrame({'station_id':['PAJN'],'init_time':[t],
                  'valid_time':[t+pd.Timedelta(hours=6)],'forecast_hour':[6],
                  'qpf_p50':[0.25]}).to_parquet(p,index=False)
    return p


class Repair(unittest.TestCase):
    def test_boundary_repartition_and_preserve_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'model';out=Path(temp)/'corrected'
            a=source_file(root,'2026_09','2026-09-30 18:00')
            b=source_file(root,'2026_10','2026-10-01 06:00')
            hashes=[digest(a),digest(b)]
            m=repair(root,out)
            self.assertEqual(m['status'],'COMPLETE')
            d=pd.read_parquet(out/'nbmqmd/precip6hr/2026_10_archive.parquet')
            self.assertEqual(d.init_time.tolist(),[pd.Timestamp('2026-10-01'),pd.Timestamp('2026-10-01 12:00')])
            self.assertEqual(d.qpf_p50.tolist(),[.25,.25])
            self.assertEqual([digest(a),digest(b)],hashes)
            self.assertIn(MARKER,pq.read_metadata(out/'nbmqmd/precip6hr/2026_10_archive.parquet').metadata)
            with self.assertRaises(ValueError):repair(root,out)
            with self.assertRaises(ValueError):repair(out,Path(temp)/'twice')

    def test_conflicting_keys_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'model';out=Path(temp)/'corrected'
            source_file(root,'2026_09','2026-09-30 18:00')
            source_file(root,'2026_10','2026-09-30 18:00')
            with self.assertRaisesRegex(ValueError,'Cross-file duplicate'):repair(root,out)
            import json
            self.assertEqual(json.loads((out/'manifest.json').read_text())['status'],'INCOMPLETE')

    def test_in_place_backups_and_repeat_block(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'model'
            path=source_file(root,'2026_10','2026-10-01')
            original=digest(path)
            wind=root/'nbm/wind/untouched.txt'
            wind.parent.mkdir(parents=True)
            wind.write_text('unchanged')
            backup=repair_in_place(root)
            self.assertEqual(digest(backup/'originals/nbmqmd/precip6hr/2026_10_archive.parquet'),original)
            self.assertEqual(pd.read_parquet(path).init_time.iloc[0],pd.Timestamp('2026-10-01 06:00'))
            self.assertEqual(wind.read_text(),'unchanged')
            self.assertTrue((root/STATE_FILE).exists())
            with self.assertRaises(FileExistsError):repair_in_place(root)
            with self.assertRaises(ValueError):repair(root,Path(temp)/'double')

    def test_install_failure_restores_original(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'model'
            path=source_file(root,'2026_10','2026-10-01')
            original=digest(path)
            rename=Path.rename
            def fail_install(p,target):
                if p.name=='precip6hr' and 'corrected_staging' in p.parts:
                    raise OSError('simulated installation failure')
                return rename(p,target)
            with patch.object(Path,'rename',fail_install):
                with self.assertRaises(OSError):repair_in_place(root)
            self.assertEqual(digest(path),original)
            self.assertFalse((root/STATE_FILE).exists())

    def test_nested_output_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'model'
            source_file(root,'2026_10','2026-10-01')
            with self.assertRaises(ValueError):repair(root,root/'corrected')

if __name__=='__main__':unittest.main()
