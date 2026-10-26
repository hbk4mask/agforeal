import time
import unittest
from fastapi.testclient import TestClient
from source_agent.app import create_app

class ApiTests(unittest.TestCase):
    def setUp(self): self.client=TestClient(create_app())
    def test_review_and_export(self):
        source=self.client.post('/api/demo').json()
        response=self.client.post('/api/analyze',json={'source_id':source['id']})
        self.assertEqual(response.status_code,202)
        job=response.json()['id']
        self.assertEqual(self.client.get(f'/api/jobs/{job}/artifacts/clean.parquet').status_code,409)
        for _ in range(100):
            state=self.client.get(f'/api/jobs/{job}').json()
            if state['status'] not in ['queued','running']:break
            time.sleep(.05)
        self.assertEqual(state['status'],'review',state)
        self.assertEqual(state['report']['rows'],1640)
        self.assertEqual(self.client.post(f'/api/jobs/{job}/approve').status_code,200)
        self.assertEqual(self.client.get(f'/api/jobs/{job}/artifacts/clean.parquet').content[:4],b'PAR1')
        self.assertEqual(self.client.get(f'/api/jobs/{job}/artifacts/report.json').status_code,200)
    def test_codegen_without_upload(self):
        r=self.client.post('/api/generate',json={'source_id':'','source_uri':'project.dataset.events'})
        self.assertEqual(r.status_code,200,r.text)
        self.assertIn('object SourceOnboarding',r.json()['spark_scala'])
    def test_origin_and_invalid_inputs(self):
        self.assertEqual(self.client.post('/api/demo',headers={'origin':'https://evil.example'}).status_code,403)
        self.assertEqual(self.client.post('/api/sources',content=b'not parquet',headers={'x-file-name':'bad.parquet'}).status_code,400)
        self.assertEqual(self.client.get('/api/jobs/missing').status_code,404)
        r=self.client.post('/api/remote/submit',json={'source_id':'','backend':'spark'})
        self.assertEqual(r.status_code,409)
