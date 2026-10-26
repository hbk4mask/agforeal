"""GCP REST connectors. Credentials are resolved on the server through ADC."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import re
import threading
import uuid
from urllib.parse import quote
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from .engine import FIELDS, policy_with_defaults, InputError
from .codegen import generate_bigquery, ctes

class CloudError(ValueError): pass

class Connection(BaseModel):
    model_config = ConfigDict(extra='forbid')
    project: str = Field(pattern=r'^[a-z][a-z0-9-]{4,61}[a-z0-9]$')
    dataset: str = Field(default='sourcework_onboarding', pattern=r'^[A-Za-z_][A-Za-z0-9_]{0,1023}$')
    location: str = Field(default='US', pattern=r'^[A-Za-z0-9-]+$')
    region: str = Field(default='us-central1', pattern=r'^[a-z]+-[a-z]+[0-9]+$')
    runtime: str = Field(default='2.2', pattern=r'^2\.[0-9]+(?:\.[0-9]+)?$')
    service_account: str = ''
    subnet: str = ''
    jar_uri: str = ''
    staging_uri: str = ''
    maximum_bytes_billed: int = Field(default=10000000000, ge=1, le=9007199254740991)
    ttl_seconds: int = Field(default=3600, ge=600, le=86400)

class Prepare(BaseModel):
    backend: str
    table: str = ''
    input_uri: str = ''
    output_uri: str = ''
    policy: dict = Field(default_factory=dict)
    mapping: dict[str, str | None] = Field(default_factory=lambda:{f:f for f in FIELDS})

class SourceRegistration(BaseModel):
    source_id: str = ''
    uri: str = ''

class Submission(BaseModel):
    plan_id: str

class GCP:
    def __init__(self):
        try:
            import google.auth
            from google.auth.transport.requests import AuthorizedSession
            credentials,_=google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
            self.session=AuthorizedSession(credentials)
        except ImportError as e:
            raise CloudError('Install cloud dependencies: pip install -r requirements-cloud.txt') from e
        except Exception as e:
            raise CloudError('Application Default Credentials unavailable. Run gcloud auth application-default login on the service host, or configure a workload identity.') from e

    def request(self, method, url, **kwargs):
        try: r=self.session.request(method,url,timeout=40,**kwargs)
        except Exception as e: raise CloudError('GCP request failed. Check server credentials and network access.') from e
        if not r.ok:
            try: message=r.json().get('error',{}).get('message','GCP request rejected')
            except ValueError: message='GCP request rejected'
            raise CloudError(f'GCP {r.status_code}: {message}')
        return r.json() if r.content else {}


def gs(uri):
    match=re.fullmatch(r'gs://([a-z0-9][a-z0-9._-]+)/(\S+)',uri)
    if not match: raise CloudError('Use a gs://bucket/object-or-prefix URI')
    return match.groups()


def checked_mapping(mapping):
    if set(mapping)-set(FIELDS): raise CloudError('Mapping contains unknown canonical fields')
    result={f:mapping.get(f) for f in FIELDS}
    if not result['timestamp'] or not any(result[f] for f in ['maid','ip','email']):
        raise CloudError('Map timestamp and at least one identifier')
    selected=[v for v in result.values() if v]
    if len(selected)!=len(set(selected)): raise CloudError('Each source column can only be mapped once')
    return result


def decode_rows(result):
    def value(field, raw):
        if raw is None:return None
        if field.get('mode')=='REPEATED':
            return [value({**field,'mode':'NULLABLE'},v.get('v')) for v in raw]
        if field['type'] in ['RECORD','STRUCT']:
            return {f['name']:value(f,v.get('v')) for f,v in zip(field['fields'],raw['f'])}
        if field['type'] in ['INTEGER','INT64']:return int(raw)
        if field['type'] in ['FLOAT','FLOAT64']:return float(raw)
        if field['type']=='BOOLEAN':return raw=='true'
        return raw
    return [{f['name']:value(f,v.get('v')) for f,v in zip(result.get('schema',{}).get('fields',[]),r['f'])} for r in result.get('rows',[])]


def cloud_router(directory: Path, transport_factory=GCP, source_lookup=None, store=None, managed_connection=None):
    router=APIRouter(prefix='/api/cloud')
    directory.mkdir(parents=True,exist_ok=True)
    lock=threading.RLock()
    config_path=directory/'connection.json'
    records_path=directory/'plans.json'
    from .storage import FileStore
    store=store or FileStore(directory/'records')
    if records_path.exists() and managed_connection is None:
        for record in json.loads(records_path.read_text()).values():
            if store.get('plans/'+record['id']) is None:store.put('plans/'+record['id'],record)
        records_path.rename(directory/'plans.migrated.json')

    def save(path,data):
        tmp=path.with_suffix('.tmp')
        tmp.write_text(json.dumps(data,indent=2));tmp.chmod(0o600);tmp.replace(path)

    def config():
        if managed_connection is not None:return managed_connection
        if not config_path.exists():raise CloudError('Save your GCP connection settings first')
        return Connection(**json.loads(config_path.read_text())).model_dump()

    def api_base(c,backend):
        if backend=='bigquery':return f"https://bigquery.googleapis.com/bigquery/v2/projects/{c['project']}"
        return f"https://dataproc.googleapis.com/v1/projects/{c['project']}/locations/{c['region']}/batches"

    def handle(fn):
        try:return fn()
        except (CloudError,InputError) as e:raise HTTPException(400,str(e)) from e

    @router.get('/connection')
    def get_connection():
        return {'configured':managed_connection is not None or config_path.exists(),'managed':managed_connection is not None,'settings':config() if managed_connection is not None or config_path.exists() else {},'authentication':'Cloud Run service identity' if managed_connection is not None else 'Application Default Credentials (server-side)'}

    @router.post('/connection')
    def set_connection(body: Connection):
        if managed_connection is not None:raise HTTPException(403,'Connection settings are managed at deployment')
        with lock:save(config_path,body.model_dump())
        return {'saved':True,'message':'Configuration saved. Test access separately.'}

    @router.post('/test/{backend}')
    def test(backend: str):
        def run():
            if backend not in ['bigquery','spark']:raise CloudError('Unknown connector')
            c=config();gcp=transport_factory()
            if backend=='bigquery':
                result=gcp.request('POST',api_base(c,backend)+'/jobs',json={'jobReference':{'projectId':c['project'],'location':c['location']},'configuration':{'dryRun':True,'query':{'query':'SELECT 1','useLegacySql':False}}})
                return {'ok':True,'message':'BigQuery authentication and query dry-run succeeded. Prepare analysis to verify source access.'}
            gcp.request('GET',api_base(c,backend),params={'pageSize':1})
            if c['jar_uri']:
                bucket,obj=gs(c['jar_uri'])
                gcp.request('GET',f'https://storage.googleapis.com/storage/v1/b/{quote(bucket,safe="")}/o/{quote(obj,safe="")}')
            return {'ok':True,'message':'Spark batch-list access verified'+(' and JAR object found.' if c['jar_uri'] else '.')+' Submission, service-account and network permissions are checked when running a batch.'}
        return handle(run)

    @router.post('/sources')
    def register_cloud_source(body: SourceRegistration):
        def run():
            from .engine import ALIASES, source_hash
            import base64
            if bool(body.source_id) == bool(body.uri):
                raise CloudError('Provide either an uploaded source ID or a GCS path')
            c=config();gcp=transport_factory();local=None
            if body.source_id:
                if source_lookup is None:raise CloudError('Local source lookup is unavailable')
                local=source_lookup(body.source_id)
                if local.get('gcs_uri'):
                    uri=local['gcs_uri'];local=None
                else:
                    digest=source_hash(local['path'])
                    gs(c['staging_uri'])
                    uri=c['staging_uri'].rstrip('/')+'/uploads/'+digest+'.parquet'
            else:
                uri=body.uri.strip();gs(uri)
                if any(x in uri for x in ['?', '#', '[', ']']) or uri.count('*')>1:
                    raise CloudError('Use a Parquet object, folder, or one * wildcard; query strings and other glob patterns are unsupported')
                if '*' not in uri and not uri.lower().endswith('.parquet'):
                    uri=uri.rstrip('/')+'/*.parquet'
            base=api_base(c,'bigquery')
            dataset_url=base+'/datasets/'+c['dataset']
            try:dataset=gcp.request('GET',dataset_url)
            except CloudError as e:
                if 'GCP 404:' not in str(e):raise
                try:
                    dataset=gcp.request('POST',base+'/datasets',json={'datasetReference':{'projectId':c['project'],'datasetId':c['dataset']},'location':c['location'],'labels':{'application':'sourcework'}})
                except CloudError as race:
                    if 'GCP 409:' not in str(race):raise
                    dataset=gcp.request('GET',dataset_url)
            if dataset.get('location','').lower()!=c['location'].lower():
                raise CloudError('Dataset location differs from the configured BigQuery location. Use a matching dataset/location and colocated GCS bucket.')
            table_id='source_'+hashlib.sha256(uri.encode()).hexdigest()[:24]
            table_url=dataset_url+'/tables/'+table_id
            created=False
            try:table=gcp.request('GET',table_url)
            except CloudError as e:
                if 'GCP 404:' not in str(e):raise
                if local:
                    bucket,obj=gs(uri)
                    with local['path'].open('rb') as data:
                        try:
                            gcp.request('POST',f'https://storage.googleapis.com/upload/storage/v1/b/{quote(bucket,safe="")}/o',params={'uploadType':'media','name':obj,'ifGenerationMatch':'0'},headers={'Content-Type':'application/octet-stream'},data=data)
                        except CloudError as upload_error:
                            if 'GCP 412:' not in str(upload_error):raise
                            metadata=gcp.request('GET',f'https://storage.googleapis.com/storage/v1/b/{quote(bucket,safe="")}/o/{quote(obj,safe="")}')
                            data.seek(0)
                            checksum=base64.b64encode(hashlib.file_digest(data,'md5').digest()).decode()
                            if metadata.get('md5Hash')!=checksum:raise CloudError('Staging object exists with different content; choose a different staging prefix')
                definition={'tableReference':{'projectId':c['project'],'datasetId':c['dataset'],'tableId':table_id},'externalDataConfiguration':{'sourceFormat':'PARQUET','sourceUris':[uri],'autodetect':True},'labels':{'application':'sourcework'},'description':'Sourcework onboarding source; generated automatically from Parquet.'}
                try:
                    table=gcp.request('POST',dataset_url+'/tables',json=definition);created=True
                except CloudError as race:
                    if 'GCP 409:' not in str(race):raise
                    table=gcp.request('GET',table_url)
            external=table.get('externalDataConfiguration',{})
            if external.get('sourceFormat')!='PARQUET' or external.get('sourceUris')!=[uri]:
                raise CloudError('Generated table name already exists with a different definition; no table was overwritten')
            if not table.get('schema',{}).get('fields'):
                table=gcp.request('GET',table_url)
            schema=table.get('schema',{}).get('fields',[])
            mapping={};warnings=[]
            for field in FIELDS:
                candidates=[col['name'] for col in schema if col['name'].lower() in ALIASES[field]]
                mapping[field]=candidates[0] if len(candidates)==1 else None
                if len(candidates)>1:warnings.append('Choose a source column for '+field+': '+', '.join(candidates))
            if not schema:warnings.append('BigQuery has not returned a schema yet. Set source mappings before preparing analysis.')
            return {'table':'.'.join([c['project'],c['dataset'],table_id]),'uri':uri,'created':created,'schema':schema,'mapping':mapping,'warnings':warnings,'source_kind':'uploaded_sample' if body.source_id else 'gcs','location':c['location']}
        return handle(run)

    @router.post('/prepare')
    def prepare(body: Prepare):
        def run():
            c=config();policy=policy_with_defaults(body.policy);mapping=checked_mapping(body.mapping)
            plan_id=uuid.uuid4().hex
            record={'id':plan_id,'backend':body.backend,'config':c,'policy':policy,'mapping':mapping,'status':'prepared'}
            if body.backend=='bigquery':
                sql=generate_bigquery(body.table,mapping,policy)
                result=transport_factory().request('POST',api_base(c,'bigquery')+'/jobs',json={'jobReference':{'projectId':c['project'],'location':c['location']},'configuration':{'dryRun':True,'query':{'query':sql,'useLegacySql':False,'maximumBytesBilled':str(c['maximum_bytes_billed'])}}})
                estimated=int(result.get('statistics',{}).get('query',{}).get('totalBytesProcessed',result.get('statistics',{}).get('totalBytesProcessed',0)))
                if estimated>c['maximum_bytes_billed']:raise CloudError(f'Estimated {estimated:,} bytes exceeds your configured billing cap')
                record.update(sql=sql,source=body.table,estimated_bytes=estimated,job_id='sourcework_'+plan_id)
            elif body.backend=='spark':
                gs(body.input_uri);gs(body.output_uri);gs(c['staging_uri']);gs(c['jar_uri'])
                if not c['jar_uri'].endswith('.jar'):raise CloudError('Spark application URI must point to the current SourceOnboarding .jar')
                if not re.fullmatch(r'[A-Za-z0-9._-]+@[A-Za-z0-9.-]+\.iam\.gserviceaccount\.com',c['service_account']):raise CloudError('Enter a Spark workload service-account email')
                if c['subnet'] and not re.fullmatch(r'projects/[a-z0-9-]+/regions/[a-z0-9-]+/subnetworks/[a-z0-9-]+',c['subnet']):raise CloudError('Subnet must use projects/PROJECT/regions/REGION/subnetworks/NAME')
                batch_id='sourcework-'+plan_id
                output=body.output_uri.rstrip('/')+'/'+batch_id
                staging=c['staging_uri'].rstrip('/')+'/'+batch_id+'/query.sql'
                sql=ctes(mapping,policy,'spark','source_input')+'\nSELECT * FROM flagged'
                execution={'serviceAccount':c['service_account'],'ttl':str(c['ttl_seconds'])+'s'}
                if c['subnet']:execution['subnetworkUri']=c['subnet']
                payload={'sparkBatch':{'mainClass':'SourceOnboarding','jarFileUris':[c['jar_uri']],'args':[body.input_uri,output,staging,str(policy['deduplicate']).lower()]},'runtimeConfig':{'version':c['runtime']},'environmentConfig':{'executionConfig':execution},'labels':{'application':'sourcework'}}
                record.update(sql=sql,source=body.input_uri,output_uri=output,staging_uri=staging,payload=payload,job_id=batch_id,request_id=str(uuid.uuid4()))
            else:raise CloudError('Unknown connector')
            record['policy_sha256']=hashlib.sha256(json.dumps({'mapping':mapping,'policy':policy},sort_keys=True).encode()).hexdigest()
            store.put('plans/'+plan_id,record)
            return record
        return handle(run)

    @router.post('/submit')
    def submit(body: Submission):
        def run():
            # Keep stable IDs across retries, including uncertain network outcomes.
            with lock:
                r=store.get('plans/'+body.plan_id)
                if not r:raise CloudError('Prepare a job first')
                if r['status']!='prepared':return r
                c=r['config'];gcp=transport_factory()
                if r['backend']=='bigquery':
                    payload={'jobReference':{'projectId':c['project'],'location':c['location'],'jobId':r['job_id']},'configuration':{'query':{'query':r['sql'],'useLegacySql':False,'maximumBytesBilled':str(c['maximum_bytes_billed'])}}}
                    try:gcp.request('POST',api_base(c,'bigquery')+'/jobs',json=payload)
                    except CloudError as e:
                        if 'GCP 409:' not in str(e):raise
                else:
                    bucket,obj=gs(r['staging_uri'])
                    # Content is immutable for this plan; a retry uploads identical SQL.
                    gcp.request('POST',f'https://storage.googleapis.com/upload/storage/v1/b/{quote(bucket,safe="")}/o',params={'uploadType':'media','name':obj},headers={'Content-Type':'text/plain'},data=r['sql'].encode())
                    try:gcp.request('POST',api_base(c,'spark'),params={'batchId':r['job_id'],'requestId':r['request_id']},json=r['payload'])
                    except CloudError as e:
                        if 'GCP 409:' not in str(e):raise
                r['status']='submitted';r=store.put('plans/'+body.plan_id,r)
                return r
        return handle(run)

    @router.get('/jobs')
    def jobs():
        with lock:return [{'id':r['id'],'backend':r['backend'],'job_id':r['job_id'],'status':r['status'],'source':r['source']} for r in store.list('plans') if r['status']!='prepared']

    @router.get('/jobs/{plan_id}')
    def status(plan_id: str):
        def run():
            with lock:
                r=store.get('plans/'+plan_id)
                if r is None:raise CloudError('Job not found')
            if r['status']=='prepared':return r
            c=r['config'];gcp=transport_factory()
            if r['backend']=='bigquery':
                result=gcp.request('GET',api_base(c,'bigquery')+'/jobs/'+r['job_id'],params={'location':c['location']})
                state=result.get('status',{});r['status']='failed' if state.get('errorResult') else {'DONE':'completed','RUNNING':'running','PENDING':'pending'}.get(state.get('state'),'pending')
                r['error']=state.get('errorResult');r['statistics']=result.get('statistics',{})
                r['console_url']=f"https://console.cloud.google.com/bigquery?project={c['project']}&j=bq:{c['location']}:{r['job_id']}&page=queryresults"
                if r['status']=='completed':
                    data=gcp.request('GET',api_base(c,'bigquery')+'/queries/'+r['job_id'],params={'location':c['location'],'maxResults':1})
                    r['report']=decode_rows(data)
            else:
                result=gcp.request('GET',api_base(c,'spark')+'/'+r['job_id'])
                r['status']={'SUCCEEDED':'completed','FAILED':'failed','CANCELLED':'cancelled','RUNNING':'running'}.get(result.get('state'),'pending')
                r['message']=result.get('stateMessage','');r['runtime_info']=result.get('runtimeInfo',{})
                r['console_url']=f"https://console.cloud.google.com/dataproc/batches/{c['region']}/{r['job_id']}?project={c['project']}"
            r=store.put('plans/'+plan_id,r)
            return r
        return handle(run)
    return router
