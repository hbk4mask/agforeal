import json
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    cloud: bool = False
    connection: dict | None = None
    state_bucket: str = ''
    state_prefix: str = 'sourcework'
    allowed_hosts: tuple = ('localhost','127.0.0.1','testserver')
    allowed_origins: tuple = ()
    output_uri: str = ''

    @classmethod
    def from_env(cls):
        mode=os.getenv('SOURCEWORK_MODE','cloud' if os.getenv('K_SERVICE') else 'local')
        if mode not in {'cloud','local'}:raise RuntimeError('SOURCEWORK_MODE must be cloud or local')
        if os.getenv('K_SERVICE') and mode!='cloud':raise RuntimeError('Cloud Run requires cloud mode')
        cloud=mode=='cloud'
        if not cloud:return cls()
        if os.getenv('SOURCEWORK_AUTH_MODE')!='cloud-run-iam':
            raise RuntimeError('Cloud mode requires SOURCEWORK_AUTH_MODE=cloud-run-iam and a private IAM-protected Cloud Run deployment')
        bucket=os.getenv('SOURCEWORK_STATE_BUCKET','')
        if not bucket:raise RuntimeError('SOURCEWORK_STATE_BUCKET is required in cloud mode')
        from .cloud import Connection
        connection=Connection(**json.loads(os.environ.get('SOURCEWORK_CONNECTION_JSON','{}'))).model_dump()
        if not connection['staging_uri'].startswith('gs://'):raise RuntimeError('Cloud deployment requires a GCS staging URI')
        hosts=tuple(h.strip() for h in os.getenv('SOURCEWORK_ALLOWED_HOSTS','.run.app,localhost,127.0.0.1').split(',') if h.strip())
        origins=tuple(o.strip().rstrip('/') for o in os.getenv('SOURCEWORK_ALLOWED_ORIGINS','http://localhost:8080,http://127.0.0.1:8080').split(',') if o.strip())
        return cls(True,connection,bucket,os.getenv('SOURCEWORK_STATE_PREFIX','sourcework'),hosts,origins,os.getenv('SOURCEWORK_OUTPUT_URI',''))

    def permits_host(self,host):
        return any(host==h or (h.startswith('.') and host.endswith(h) and host!=h[1:]) for h in self.allowed_hosts)
