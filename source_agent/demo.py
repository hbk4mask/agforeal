from pathlib import Path
import uuid
import pandas as pd

def create_demo(path):
    rows = []
    for i in range(1600):
        rows.append({'advertising_id': str(uuid.uuid5(uuid.NAMESPACE_DNS, f'demo-device-{i%800}')), 'ip_address': f'203.0.113.{i%240+1}', 'email_address': f'person{i%950}@example.com', 'device': ['smartphone','tablet','laptop'][i%3], 'os': ['iOS','iPad','Windows'][i%3], 'event_time': f'2026-10-21T{(i//60)%24:02d}:{i%60:02d}:00Z', 'source_label': 'synthetic-demo'})
    for i in range(0,1600,17): rows[i]['ip_address']='999.1.2.3'
    for i in range(0,1600,29): rows[i]['email_address']='invalid-email'
    for i in range(0,1600,13): rows[i]['device']='mobile'; rows[i]['os']='iPad'
    for i in range(0,1600,41): rows[i]['advertising_id']='00000000-0000-0000-0000-000000000000'
    for i in range(0,1600,97): rows[i]['event_time']='not-a-date'
    for i in range(150): rows[i]['advertising_id']=str(uuid.uuid5(uuid.NAMESPACE_DNS,'demo-high-runner'))
    rows.extend([r.copy() for r in rows[500:540]])
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path,index=False)
