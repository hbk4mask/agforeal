import tempfile
import unittest
from pathlib import Path
from fastapi import FastAPI
from fastapi.testclient import TestClient
from source_agent.cloud import cloud_router, CloudError, decode_rows
from source_agent.codegen import generate_scala
from source_agent.engine import DEFAULTS,FIELDS

class FakeGCP:
    def __init__(self):self.calls=[];self.fail=False;self.bytes=1234
    def request(self,method,url,**kw):
        self.calls.append((method,url,kw))
        if self.fail:raise CloudError('GCP 403: Access denied')
        if method=='POST' and url.endswith('/jobs'):
            return {'statistics':{'query':{'totalBytesProcessed':str(self.bytes)}}}
        if '/queries/' in url:
            return {'schema':{'fields':[{'name':'total_rows','type':'INTEGER'}]},'rows':[{'f':[{'v':'42'}]}]}
        if method=='GET' and '/jobs/' in url:return {'status':{'state':'DONE'}}
        if method=='GET' and '/batches/' in url:return {'state':'SUCCEEDED','runtimeInfo':{'outputUri':'gs://logs/batch'}}
        return {}

class CloudTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name);self.gcp=FakeGCP()
        app=FastAPI();app.include_router(cloud_router(self.path,lambda:self.gcp));self.client=TestClient(app)
        self.config={'project':'demo-project','jar_uri':'gs://demo-bucket/runner.jar','staging_uri':'gs://demo-bucket/staging','service_account':'runner@demo-project.iam.gserviceaccount.com'}
        self.assertEqual(self.client.post('/api/cloud/connection',json=self.config).status_code,200)
    def tearDown(self):self.temp.cleanup()
    def prepare(self,backend='bigquery',**kw):
        body={'backend':backend,'table':'demo-project.raw.events','input_uri':'gs://demo-bucket/input/*.parquet','output_uri':'gs://demo-bucket/output',**kw}
        return self.client.post('/api/cloud/prepare',json=body)
    def test_bq_dry_run_billing_and_results(self):
        p=self.prepare().json();self.assertEqual(p['estimated_bytes'],1234)
        self.assertTrue(self.gcp.calls[-1][2]['json']['configuration']['dryRun'])
        result=self.client.post('/api/cloud/submit',json={'plan_id':p['id']});self.assertEqual(result.status_code,200)
        payload=self.gcp.calls[-1][2]['json'];self.assertEqual(payload['configuration']['query']['maximumBytesBilled'],'10000000000')
        self.assertNotIn('dryRun',payload['configuration'])
        count=len(self.gcp.calls);self.client.post('/api/cloud/submit',json={'plan_id':p['id']});self.assertEqual(len(self.gcp.calls),count)
        result=self.client.get('/api/cloud/jobs/'+p['id']).json();self.assertEqual(result['status'],'completed');self.assertEqual(result['report'],[{'total_rows':42}])
    def test_spark_payload_uses_current_policy(self):
        p=self.prepare('spark',policy={'max_ips_per_maid':7,'deduplicate':False}).json()
        self.assertIn('fanout > 7',p['sql']);self.assertEqual(p['payload']['sparkBatch']['args'][3],'false')
        self.assertTrue(p['output_uri'].endswith(p['job_id']))
        self.assertEqual(p['payload']['environmentConfig']['executionConfig']['ttl'],'3600s')
        self.client.post('/api/cloud/submit',json={'plan_id':p['id']})
        self.assertIn('/upload/storage/',self.gcp.calls[-2][1]);self.assertIn('/batches',self.gcp.calls[-1][1])
        self.assertIn('requestId',self.gcp.calls[-1][2]['params'])
        self.assertEqual(self.client.get('/api/cloud/jobs/'+p['id']).json()['status'],'completed')
    def test_persistence_and_config_snapshot(self):
        p=self.prepare().json()
        self.client.post('/api/cloud/connection',json={**self.config,'project':'another-project'})
        app=FastAPI();app.include_router(cloud_router(self.path,lambda:self.gcp));client=TestClient(app)
        self.assertEqual(client.post('/api/cloud/submit',json={'plan_id':p['id']}).status_code,200)
        self.assertIn('/demo-project/',self.gcp.calls[-1][1]);self.assertEqual(len(client.get('/api/cloud/jobs').json()),1)
    def test_cap_and_errors(self):
        self.gcp.bytes=10000000001;self.assertEqual(self.prepare().status_code,400)
        self.gcp.fail=True;self.assertEqual(self.client.post('/api/cloud/test/bigquery').status_code,400)
    def test_invalid_mapping_and_paths(self):
        self.assertEqual(self.prepare(mapping={'timestamp':'ts'}).status_code,400)
        self.assertEqual(self.prepare('spark',input_uri='https://evil.example').status_code,400)
        self.assertEqual(self.prepare(table='x;DROP TABLE y').status_code,400)
        self.assertEqual(self.client.post('/api/cloud/connection',json={**self.config,'token':'secret'}).status_code,422)
    def test_connection_checks_do_not_submit_batch(self):
        self.assertEqual(self.client.post('/api/cloud/test/spark').status_code,200)
        self.assertTrue(all(c[0]=='GET' for c in self.gcp.calls))
    def test_nested_bigquery_report(self):
        result={'schema':{'fields':[{'name':'issues','type':'RECORD','mode':'REPEATED','fields':[{'name':'rows','type':'INTEGER'}]}]},'rows':[{'f':[{'v':[{'v':{'f':[{'v':'5'}]}}]}]}]}
        self.assertEqual(decode_rows(result),[{'issues':[{'rows':5}]}])
    def test_runner_runtime_policy_contract(self):
        scala=generate_scala({f:f for f in FIELDS},DEFAULTS)
        self.assertIn('spark.read.textFile(args(2)).collect().mkString("\\n")',scala)
        self.assertIn('args(3).toBoolean',scala)
        self.assertIn('spark.sql(runtimeQuery)',scala)

class RegistrationGCP(FakeGCP):
    def __init__(self):
        super().__init__();self.dataset=None;self.table=None;self.deny=False
    def request(self,method,url,**kw):
        self.calls.append((method,url,kw))
        if self.deny:raise CloudError('GCP 403: denied')
        if '/upload/storage/' in url:
            self.uploaded=kw['data'].read();return {}
        if method=='GET' and '/tables/' in url:
            if self.table is None:raise CloudError('GCP 404: not found')
            return self.table
        if method=='POST' and url.endswith('/tables'):
            self.table={**kw['json'],'schema':{'fields':[{'name':'advertising_id','type':'STRING'},{'name':'ip_address','type':'STRING'},{'name':'event_time','type':'TIMESTAMP'}]}}
            return self.table
        if method=='POST' and url.endswith('/datasets'):
            self.dataset=kw['json'];return self.dataset
        if method=='GET' and '/datasets/' in url:
            if self.dataset is None:raise CloudError('GCP 404: not found')
            return self.dataset
        return {}

class RegistrationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name);self.gcp=RegistrationGCP()
        self.sample=self.path/'sample.parquet';self.sample.write_bytes(b'PAR1-test-content')
        app=FastAPI();app.include_router(cloud_router(self.path/'cloud',lambda:self.gcp,lambda source_id:{'path':self.sample,'name':'sample.parquet'}))
        self.client=TestClient(app)
        self.client.post('/api/cloud/connection',json={'project':'demo-project','staging_uri':'gs://demo-bucket/staging'})
    def tearDown(self):self.temp.cleanup()
    def test_create_dataset_and_external_table_and_mapping(self):
        r=self.client.post('/api/cloud/sources',json={'uri':'gs://demo-bucket/events/'});self.assertEqual(r.status_code,200,r.text)
        result=r.json();self.assertTrue(result['created']);self.assertEqual(result['uri'],'gs://demo-bucket/events/*.parquet')
        self.assertEqual(result['mapping']['maid'],'advertising_id');self.assertEqual(result['mapping']['timestamp'],'event_time')
        self.assertEqual(self.gcp.table['externalDataConfiguration']['sourceFormat'],'PARQUET')
        self.assertEqual(self.gcp.dataset['location'],'US')
    def test_retry_reuses_table(self):
        body={'uri':'gs://demo-bucket/events.parquet'}
        first=self.client.post('/api/cloud/sources',json=body).json()
        second=self.client.post('/api/cloud/sources',json=body).json()
        self.assertEqual(first['table'],second['table']);self.assertFalse(second['created'])
        self.assertEqual(sum(c[0]=='POST' and c[1].endswith('/tables') for c in self.gcp.calls),1)
    def test_upload_sample_and_reuse(self):
        body={'source_id':'local-sample'}
        result=self.client.post('/api/cloud/sources',json=body).json()
        self.assertEqual(result['source_kind'],'uploaded_sample');self.assertIn('/uploads/',result['uri'])
        self.assertEqual(self.gcp.uploaded,b'PAR1-test-content')
        self.client.post('/api/cloud/sources',json=body)
        self.assertEqual(sum('/upload/storage/' in c[1] for c in self.gcp.calls),1)
        upload=next(c for c in self.gcp.calls if '/upload/storage/' in c[1]);self.assertEqual(upload[2]['params']['ifGenerationMatch'],'0')
    def test_no_overwrite_on_collision(self):
        self.gcp.dataset={'location':'US'};self.gcp.table={'externalDataConfiguration':{'sourceFormat':'CSV','sourceUris':['gs://other/path']}}
        r=self.client.post('/api/cloud/sources',json={'uri':'gs://demo-bucket/events.parquet'})
        self.assertEqual(r.status_code,400);self.assertTrue(all(c[0]=='GET' for c in self.gcp.calls))
    def test_location_and_permission_failures(self):
        self.gcp.dataset={'location':'EU'}
        self.assertEqual(self.client.post('/api/cloud/sources',json={'uri':'gs://demo-bucket/events.parquet'}).status_code,400)
        self.gcp.deny=True
        self.assertEqual(self.client.post('/api/cloud/sources',json={'uri':'gs://demo-bucket/events.parquet'}).status_code,400)
        self.assertFalse(any(c[0]=='POST' for c in self.gcp.calls))
    def test_source_validation(self):
        for body in [{},{'uri':'https://invalid/path'},{'source_id':'x','uri':'gs://demo-bucket/x'},{'uri':'gs://demo-bucket/a/*/*.parquet'}]:
            self.assertEqual(self.client.post('/api/cloud/sources',json=body).status_code,400)
