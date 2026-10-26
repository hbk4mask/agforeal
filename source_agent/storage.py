"""Durable JSON records; GCS generations prevent lost updates across instances."""
import json
import re
import threading
from pathlib import Path

TERMINAL = {'completed', 'failed', 'cancelled'}
RANK = {'prepared':0,'submitted':1,'pending':2,'running':3,'completed':4,'failed':4,'cancelled':4}

def merge_record(current, incoming):
    if current and RANK.get(current.get('status'),-1) > RANK.get(incoming.get('status'),-1):
        return current
    if current and current.get('status') in TERMINAL and incoming.get('status') != current['status']:
        return current
    return incoming

def key_path(key):
    if not re.fullmatch(r'(plans|sources)/[a-zA-Z0-9_-]+',key):raise ValueError('Invalid record key')
    return key+'.json'

class FileStore:
    def __init__(self,directory):
        self.directory=Path(directory);self.directory.mkdir(parents=True,exist_ok=True);self.lock=threading.RLock()
    def get(self,key):
        path=self.directory/key_path(key)
        with self.lock:return json.loads(path.read_text()) if path.exists() else None
    def put(self,key,value):
        with self.lock:
            value=merge_record(self.get(key),value)
            path=self.directory/key_path(key);path.parent.mkdir(parents=True,exist_ok=True)
            tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(value));tmp.chmod(0o600);tmp.replace(path)
            return value
    def list(self,collection):
        if collection not in {'plans','sources'}:raise ValueError('Invalid collection')
        with self.lock:return [json.loads(p.read_text()) for p in (self.directory/collection).glob('*.json')]

class GCSStore:
    def __init__(self,bucket,prefix='sourcework',project=None):
        from google.cloud import storage
        self.client=storage.Client(project=project);self.bucket=self.client.bucket(bucket);self.prefix=prefix.strip('/')
    def blob(self,key):return self.bucket.blob(self.prefix+'/'+key_path(key))
    def get(self,key):
        from google.api_core.exceptions import NotFound
        try:return json.loads(self.blob(key).download_as_bytes(timeout=30))
        except NotFound:return None
    def put(self,key,value):
        from google.api_core.exceptions import NotFound,PreconditionFailed
        for _ in range(8):
            blob=self.blob(key)
            try:
                blob.reload(timeout=30);generation=int(blob.generation)
                current=json.loads(blob.download_as_bytes(if_generation_match=generation,timeout=30))
            except NotFound:current=None;generation=0
            except PreconditionFailed:continue
            merged=merge_record(current,value)
            if merged==current:return current
            try:
                blob.upload_from_string(json.dumps(merged),content_type='application/json',if_generation_match=generation,timeout=30)
                return merged
            except PreconditionFailed:continue
        raise RuntimeError('Record changed concurrently; retry the request')
    def list(self,collection):
        if collection not in {'plans','sources'}:raise ValueError('Invalid collection')
        # Individual records, not an overwritten shared jobs.json document.
        prefix=self.prefix+'/'+collection+'/'
        return [json.loads(blob.download_as_bytes(timeout=30)) for blob in self.client.list_blobs(self.bucket,prefix=prefix) if blob.name.endswith('.json')]
    def upload_source(self,path,source_id):
        name=self.prefix+'/uploads/'+source_id+'.parquet'
        self.bucket.blob(name).upload_from_filename(str(path),content_type='application/octet-stream',if_generation_match=0,timeout=120)
        return 'gs://'+self.bucket.name+'/'+name
