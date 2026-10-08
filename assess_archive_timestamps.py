"""Read-only inventory of local NBM percentile Parquet timestamps.

Reports candidate offsets, never applies them or declares older rows affected
solely from their timestamps. Keep the report outside model archive directories.
"""
import argparse
import json
from pathlib import Path
from uuid import uuid4
import pandas as pd

OFFSETS={'maxt':18,'mint':18,'precip6hr':6,'precip24hr':24,
         'snow6hr':6,'snow24hr':24,'snow48hr':48,'snow72hr':72}
MODELS={'nbm','nbmqmd','nbm_exp','nbmqmd_exp'}


def assess(path,element=None,model=None):
    d=pd.read_parquet(path)
    path=Path(path)
    element=element or next((x for x in path.parts if x in OFFSETS),None)
    model=model or next((x for x in path.parts if x in MODELS),None)
    result=dict(path=str(path),model=model,element=element,rows=len(d),
                assessment='REQUIRES_PROVENANCE_CHECK; archive unchanged')
    required={'init_time','valid_time','forecast_hour','station_id'}
    if not required.issubset(d):
        return dict(result,assessment='NOT_FORECAST_SCHEMA')
    init=pd.to_datetime(d.init_time,utc=True,errors='coerce')
    valid=pd.to_datetime(d.valid_time,utc=True,errors='coerce')
    result.update(init_min=str(init.min()),init_max=str(init.max()),
                  init_hour_counts={str(k):int(v) for k,v in init.dt.hour.value_counts().sort_index().items()},
                  valid_hour_counts={str(k):int(v) for k,v in valid.dt.hour.value_counts().sort_index().items()},
                  invalid_timestamps=int((init.isna()|valid.isna()).sum()),
                  lead_mismatch_rows=int(((valid-init).dt.total_seconds()!=pd.to_numeric(d.forecast_hour,errors='coerce')*3600).sum()),
                  duplicate_keys=int(d.duplicated(['station_id','init_time','valid_time','forecast_hour']).sum()))
    offset=OFFSETS.get(element)
    if offset is not None:
        shifted=init+pd.Timedelta(hours=offset)
        result.update(candidate_forward_shift_hours=offset,
                      candidate_init_min=str(shifted.min()),candidate_init_max=str(shifted.max()),
                      candidate_cross_month_rows=int(((init.dt.year!=shifted.dt.year)|(init.dt.month!=shifted.dt.month)).sum()),
                      warning='A matching cycle hour does not prove correctness: 6/24/48/72-hour errors can preserve cycle schedules.')
        expected={1,7,13,19} if model in ('nbm','nbm_exp') else {0,6,12,18}
        result['off_schedule_init_rows']=int((~init.dt.hour.isin(expected)).sum())
        # Report records that would collide with existing keys if shifted alone.
        keys=['station_id','init_time','valid_time','forecast_hour']
        original=d[keys].copy()
        original['init_time']=init
        original['valid_time']=valid
        candidate=original.copy()
        candidate['init_time']+=pd.Timedelta(hours=offset)
        candidate['valid_time']+=pd.Timedelta(hours=offset)
        existing=pd.MultiIndex.from_frame(original)
        result['candidate_keys_already_present']=int(pd.MultiIndex.from_frame(candidate).isin(existing).sum())
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--roots',nargs='*',default=[],help='Model roots, e.g. model hawaii/model')
    p.add_argument('--files',nargs='*',default=[])
    p.add_argument('--element',choices=list(OFFSETS),help='For explicitly supplied files only')
    p.add_argument('--model',choices=sorted(MODELS),help='For explicitly supplied files only')
    p.add_argument('--report',default=None)
    a=p.parse_args()
    if not a.roots and not a.files:p.error('Supply --roots or --files')
    paths={Path(f):(a.element,a.model) for f in a.files}
    for root in a.roots:
        root=Path(root)
        if not root.is_dir():p.error(f'Root does not exist: {root}')
        for model in MODELS:
            for element in OFFSETS:
                for f in (root/model/element).glob('*.parquet'):
                    paths[f]=(element,model)
    results=[]
    for path,(element,model) in sorted(paths.items()):
        try:row=assess(path,element,model)
        except Exception as exc:row=dict(path=str(path),assessment='ERROR',detail=str(exc))
        results.append(row)
        print(f"{path}: {row['assessment']}; rows={row.get('rows','?')}")
    report=Path(a.report or f'timestamp_diagnostics/history-{uuid4().hex[:12]}.json')
    if report.exists():p.error(f'Refusing to overwrite existing report: {report}')
    if report.suffix.lower() != '.json':p.error('Report must have .json extension')
    report.parent.mkdir(parents=True,exist_ok=True)
    report.write_text(json.dumps({'files':results,'note':'Inventory only. Confirm decoder/version and source metadata before any timestamp correction.'},indent=2)+'\n')
    print(f'Report: {report}')
    return 1 if not results or any(r['assessment']=='ERROR' for r in results) else 0

if __name__=='__main__':raise SystemExit(main())
