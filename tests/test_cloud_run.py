import tempfile
import unittest
from pathlib import Path
from fastapi.testclient import TestClient
from source_agent.app import create_app
from source_agent.settings import Settings
from source_agent.storage import FileStore
from source_agent.cloud import Connection

class SampleStore(FileStore):
    def upload_source(self,path,source_id):
        assert path.read_bytes()[:4] == b'PAR1'
        return 'gs://test-state/uploads/'+source_id+'.parquet'

class CloudRunTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.store=SampleStore(self.root/'durable')
        self.settings=Settings(cloud=True,connection=Connection(project='test-project',staging_uri='gs://test-stage/staging').model_dump(),state_bucket='test-state',allowed_hosts=('testserver','.run.app'),allowed_origins=('http://localhost:8080',))
        self.client=self.client_for('one')
    def client_for(self,name):
        return TestClient(create_app(settings=self.settings,store=self.store,runtime_dir=self.root/name))
    def test_managed_settings_and_execution(self):
        self.assertEqual(self.client.get('/healthz').status_code,200)
        self.assertFalse(self.client.get('/api/runtime').json()['local_analysis'])
        self.assertTrue(self.client.get('/api/cloud/connection').json()['managed'])
        self.assertEqual(self.client.post('/api/cloud/connection',json=self.settings.connection).status_code,403)
        self.assertEqual(self.client.post('/api/analyze',json={'source_id':'anything'}).status_code,409)
    def test_upload_survives_instance_replacement(self):
        response=self.client.post('/api/demo')
        self.assertEqual(response.status_code,200,response.text)
        source=response.json()
        self.assertTrue(source['gcs_uri'].startswith('gs://'))
        self.assertFalse(list((self.root/'one').glob('*.parquet')))
        second=self.client_for('two')
        result=second.post('/api/generate',json={'source_id':source['id'],'source_uri':'test-project.dataset.events'})
        self.assertEqual(result.status_code,200,result.text)
    def test_host_and_origin(self):
        self.assertEqual(self.client.get('/healthz',headers={'host':'service-123.run.app'}).status_code,200)
        self.assertEqual(self.client.get('/healthz',headers={'host':'evil.example'}).status_code,403)
        self.assertEqual(self.client.post('/api/demo',headers={'origin':'https://evil.example'}).status_code,403)
        self.assertEqual(self.client.post('/api/demo',headers={'origin':'http://localhost:8080'}).status_code,200)
    def test_records_do_not_regress_and_survive_restart(self):
        self.store.put('plans/abc',{'id':'abc','status':'completed','report':{'rows':10}})
        self.store.put('plans/abc',{'id':'abc','status':'running'})
        other=FileStore(self.root/'durable')
        self.assertEqual(other.get('plans/abc')['report']['rows'],10)
        self.assertEqual(len(other.list('plans')),1)
        with self.assertRaises(ValueError):other.get('plans/../../secrets')

    def test_gcs_compare_and_swap_retries_conflicts(self):
        import json
        from unittest.mock import Mock
        from google.api_core.exceptions import PreconditionFailed
        from source_agent.storage import GCSStore
        store=object.__new__(GCSStore)
        store.prefix='state'
        blob=Mock(generation=7)
        blob.download_as_bytes.return_value=json.dumps({'id':'abc','status':'running'}).encode()
        blob.upload_from_string.side_effect=[PreconditionFailed('conflict'),None]
        store.bucket=Mock()
        store.bucket.blob.return_value=blob
        result=store.put('plans/abc',{'id':'abc','status':'completed'})
        self.assertEqual(result['status'],'completed')
        self.assertEqual(blob.reload.call_count,2)
        self.assertEqual(blob.upload_from_string.call_args.kwargs['if_generation_match'],7)

    def test_cloud_run_cannot_start_in_local_mode(self):
        from unittest.mock import patch
        with patch.dict('os.environ',{'K_SERVICE':'sourcework','SOURCEWORK_MODE':'local'}):
            with self.assertRaises(RuntimeError):Settings.from_env()
