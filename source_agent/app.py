from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import os
import threading
import uuid
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from .engine import analyze, load, write_outputs, policy_with_defaults, InputError, FIELDS
from .adapters import BACKENDS, make_plan
from .demo import create_demo

ROOT = Path(__file__).resolve().parent.parent

class AnalysisRequest(BaseModel):
    source_id: str
    policy: dict = Field(default_factory=dict)
    mapping: dict = Field(default_factory=dict)

class PlanRequest(AnalysisRequest):
    backend: str = 'local'
    source_uri: str = ''

def create_app(adapters=None, settings=None, store=None, runtime_dir=None):
    from .settings import Settings
    from .storage import GCSStore
    from starlette.concurrency import run_in_threadpool
    settings = settings or Settings.from_env()
    RUNTIME = Path(runtime_dir or ('/tmp/sourcework' if settings.cloud else ROOT/'.runtime'))
    RUNTIME.mkdir(parents=True, exist_ok=True)
    os.chmod(RUNTIME, 0o700)
    if settings.cloud and store is None:
        store = GCSStore(settings.state_bucket, settings.state_prefix, settings.connection['project'])
    upload_limit = (24 if settings.cloud else 128)*1024*1024
    app = FastAPI(title='Sourcework — onboarding agent', version='0.1.0')
    sources, jobs, lock = {}, {}, threading.Lock()
    pool = None if settings.cloud else ThreadPoolExecutor(max_workers=2)
    adapters = adapters or {}

    @app.middleware('http')
    async def local_guard(request: Request, call_next):
        # Cloud Run IAM authenticates callers before requests reach this container.
        host = request.headers.get('host','').split(':')[0]
        if not settings.permits_host(host):
            from fastapi.responses import JSONResponse
            return JSONResponse({'detail':'Host is not allowed'}, status_code=403)
        origin = request.headers.get('origin')
        if request.method not in {'GET','HEAD','OPTIONS'} and origin and origin not in (str(request.base_url).rstrip('/'), *settings.allowed_origins):
            from fastapi.responses import JSONResponse
            return JSONResponse({'detail':'Cross-origin writes are disabled'},status_code=403)
        response = await call_next(request)
        response.headers['Cache-Control']='no-store'
        response.headers['X-Content-Type-Options']='nosniff'
        return response

    @app.get('/healthz')
    def health(): return {'status':'ok'}

    @app.get('/api/runtime')
    def runtime():
        return {'cloud':settings.cloud,'local_analysis':not settings.cloud,'upload_limit':upload_limit,'output_uri':settings.output_uri}

    def get_source(source_id):
        if settings.cloud:
            import re
            if not re.fullmatch(r'[a-f0-9]{32}',source_id): raise HTTPException(404,'Source not found')
            result=store.get('sources/'+source_id)
            if result is None: raise HTTPException(404,'Source not found')
            return result
        if source_id not in sources: raise HTTPException(404,'Source not found; re-upload after restarting the service')
        return sources[source_id]

    def register(path, name):
        try: frame=load(path)
        except Exception as e:
            path.unlink(missing_ok=True)
            raise HTTPException(400,str(e)) from e
        source_id=path.stem
        entry={'id':source_id,'name':name,'rows':len(frame),'columns':list(frame.columns),'bytes':path.stat().st_size,'path':path}
        if settings.cloud:
            try:
                entry.pop('path')
                entry['gcs_uri']=store.upload_source(path,source_id)
                store.put('sources/'+source_id,entry)
            finally:
                path.unlink(missing_ok=True)
        else:
            sources[source_id]=entry
        return {k:v for k,v in entry.items() if k!='path'}

    @app.get('/api/backends')
    def backends():
        return [{**b,'ready':b['ready'] or b['id'] in adapters} for b in BACKENDS]

    @app.post('/api/sources')
    async def upload(request: Request):
        from urllib.parse import unquote
        name = unquote(request.headers.get('x-file-name', 'source.parquet'))
        if not name.lower().endswith('.parquet'): raise HTTPException(400,'Choose a .parquet file')
        path=RUNTIME/f'{uuid.uuid4().hex}.parquet'
        total=0
        try:
            with path.open('wb') as out:
                async for chunk in request.stream():
                    total+=len(chunk)
                    if total>upload_limit: raise HTTPException(413,f'Upload limit is {upload_limit//1024//1024} MiB; use a GCS path for larger sources')
                    out.write(chunk)
            return await run_in_threadpool(register,path,Path(name).name)
        except Exception:
            path.unlink(missing_ok=True); raise

    @app.post('/api/demo')
    def demo():
        path=RUNTIME/f'{uuid.uuid4().hex}.parquet'
        create_demo(path)
        return register(path,'mobile_events_demo.parquet')

    def work(job_id, source, policy, mapping):
        try:
            with lock: jobs[job_id]['status']='running'
            report, accepted, rejected=analyze(source['path'],policy,mapping)
            write_outputs(report,accepted,rejected,RUNTIME/job_id)
            with lock: jobs[job_id].update(status='review',report=report)
        except Exception as e:
            with lock: jobs[job_id].update(status='failed',error=str(e))

    @app.post('/api/analyze',status_code=202)
    def start(body: AnalysisRequest):
        if settings.cloud: raise HTTPException(409,'Use BigQuery analysis and serverless Spark in cloud mode')
        source=get_source(body.source_id)
        try: policy=policy_with_defaults(body.policy)
        except InputError as e: raise HTTPException(400,str(e))
        with lock:
            if sum(j['status'] in {'queued','running'} for j in jobs.values())>=2:
                raise HTTPException(429,'Two analyses are already running; wait for one to finish')
            job_id=uuid.uuid4().hex
            jobs[job_id]={'id':job_id,'status':'queued','source_id':body.source_id,'source_name':source['name']}
        pool.submit(work,job_id,source,policy,body.mapping)
        return {'id':job_id,'status':'queued'}

    @app.get('/api/jobs/{job_id}')
    def status(job_id: str):
        with lock:
            if job_id not in jobs: raise HTTPException(404,'Job not found')
            return dict(jobs[job_id])

    @app.post('/api/jobs/{job_id}/approve')
    def approve(job_id: str):
        with lock:
            job=jobs.get(job_id)
            if not job: raise HTTPException(404,'Job not found')
            if job['status'] not in {'review','completed'}: raise HTTPException(409,'Analysis must finish before approval')
            job['status']='completed'
            return {'id':job_id,'status':'completed','artifacts':['clean.parquet','quarantine.parquet','report.json']}

    @app.get('/api/jobs/{job_id}/artifacts/{name}')
    def artifact(job_id: str,name: str):
        job=jobs.get(job_id)
        if not job: raise HTTPException(404,'Job not found')
        if name not in {'clean.parquet','quarantine.parquet','report.json'}: raise HTTPException(404,'Artifact not found')
        if job['status']!='completed' and not(name=='report.json' and job['status']=='review'):
            raise HTTPException(409,'Approve this run before exporting cleaned data')
        return FileResponse(RUNTIME/job_id/name,filename=name)

    @app.post('/api/plan')
    def plan(body: PlanRequest):
        source=get_source(body.source_id)
        if body.backend not in [b['id'] for b in BACKENDS]: raise HTTPException(400,'Unknown backend')
        try: policy=policy_with_defaults(body.policy)
        except InputError as e: raise HTTPException(400,str(e))
        from .engine import resolve_mapping
        try: mapping=resolve_mapping(source['columns'],body.mapping)
        except InputError as e: raise HTTPException(400,str(e))
        return make_plan(body.backend,body.source_uri or source['name'],policy,mapping)

    @app.post('/api/generate')
    def generate(body: PlanRequest):
        from .codegen import generate_bigquery, generate_scala
        from .engine import resolve_mapping
        try:
            mapping = body.mapping or {f:f for f in FIELDS}
            if body.source_id:
                mapping = resolve_mapping(get_source(body.source_id)['columns'], body.mapping)
            policy=policy_with_defaults(body.policy)
            return {'bigquery_sql':generate_bigquery(body.source_uri,mapping,policy), 'spark_scala':generate_scala(mapping,policy), 'policy':policy, 'mapping':mapping}
        except InputError as e: raise HTTPException(400,str(e))

    @app.post('/api/remote/submit')
    def remote(body: PlanRequest):
        if body.backend not in adapters: raise HTTPException(409,'This backend has no execution adapter installed')
        if not body.source_uri: raise HTTPException(400,'A cloud source URI is required')
        return adapters[body.backend].submit(plan(body))

    @app.get('/api/remote/{backend}/{job_id}')
    def remote_status(backend: str,job_id: str):
        if backend not in adapters: raise HTTPException(404,'Adapter not installed')
        return adapters[backend].status(job_id)

    runner_path = ROOT/'spark/target/scala-2.13/source-onboarding_2.13-0.1.0.jar'

    @app.get('/api/cloud/runner')
    def runner_info():
        ready = runner_path.exists() and runner_path.stat().st_mtime >= (ROOT/'examples/default/SourceOnboarding.scala').stat().st_mtime
        return {'ready':ready,'scala':'2.13','spark':'3.5.3','runtime':'2.2'}

    @app.get('/api/cloud/runner.jar')
    def runner_download():
        if not runner_info()['ready']:
            raise HTTPException(404,'Build the current runner using the instructions in README.md')
        return FileResponse(runner_path,filename=runner_path.name,media_type='application/java-archive')

    from .cloud import cloud_router
    app.include_router(cloud_router(RUNTIME/'cloud', source_lookup=get_source, store=store, managed_connection=settings.connection if settings.cloud else None))

    @app.get('/')
    def index(): return FileResponse(ROOT/'source_agent/static/index.html')
    app.mount('/static',StaticFiles(directory=ROOT/'source_agent/static'),name='static')
    return app

app=create_app()
