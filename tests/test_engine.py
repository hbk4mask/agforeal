import tempfile
import unittest
from pathlib import Path
import uuid
import pandas as pd
from source_agent.engine import analyze, InputError, DEFAULTS, write_outputs
from source_agent.codegen import generate_bigquery, generate_scala

class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.path=Path(self.temp.name)/'input.parquet'
    def tearDown(self): self.temp.cleanup()
    def row(self, n=1, **changes):
        return {'maid':str(uuid.UUID(int=n)), 'ip':'1.2.3.4','email':'A@Example.com','device_os':'iOS','device_type':'phone','timestamp':'2026-10-21T12:00:00Z',**changes}
    def run_rows(self, rows, policy=None):
        pd.DataFrame(rows).to_parquet(self.path,index=False)
        return analyze(self.path,policy)
    def test_normalization_and_duplicate_accounting(self):
        report,clean,bad=self.run_rows([self.row(),self.row()])
        self.assertEqual((report['rows'],len(clean),len(bad)),(2,1,1))
        self.assertEqual(clean.iloc[0].email,'a@example.com')
        self.assertEqual(clean.iloc[0].device_type,'mobile')
        self.assertTrue(report['post_checks']['row_conservation'])
        write_outputs(report,clean,bad,Path(self.temp.name)/'out')
    def test_conflict_repair_and_ambiguous_conflict(self):
        rows=[self.row(device_os='ipad'),self.row(2,device_os='android',device_type='desktop')]
        report,clean,bad=self.run_rows(rows,{'mismatch_action':'repair'})
        self.assertEqual(report['repaired'],1);self.assertEqual(clean.iloc[0].device_type,'tablet');self.assertEqual(len(bad),1)
    def test_strict_maid_threshold(self):
        rows=[self.row(ip=f'1.2.3.{i}') for i in [1,2]]
        self.assertEqual(self.run_rows(rows,{'max_ips_per_maid':2})[0]['quarantined'],0)
        self.assertEqual(self.run_rows(rows,{'max_ips_per_maid':1})[0]['quarantined'],2)
    def test_ip_and_email_threshold(self):
        rows=[self.row(i) for i in [1,2,3]]
        report,_,_=self.run_rows(rows,{'max_maids_per_ip':2,'max_maids_per_email':2})
        issues={x['rule']:x['rows'] for x in report['issues']}
        self.assertEqual(issues['high_maids_per_ip'],3);self.assertEqual(issues['high_maids_per_email'],3)
    def test_daily_vs_source(self):
        rows=[self.row(ip='1.2.3.4'),self.row(ip='1.2.3.5',timestamp='2026-10-22T00:00:00Z')]
        self.assertEqual(self.run_rows(rows,{'max_ips_per_maid':1,'window':'day'})[0]['quarantined'],0)
        self.assertEqual(self.run_rows(rows,{'max_ips_per_maid':1,'window':'source'})[0]['quarantined'],2)
    def test_invalid_inputs_and_privacy(self):
        report,clean,bad=self.run_rows([self.row(maid='bad',ip='999.0.0.1',email='bad',timestamp='bad')])
        self.assertEqual(len(clean),0);self.assertEqual(len(bad),1)
        self.assertNotIn('999.0.0.1',str(report));self.assertEqual(bad.iloc[0].ip,'999.0.0.1')
    def test_epoch_and_ipv6(self):
        report,clean,_=self.run_rows([self.row(ip='2001:0db8::1',timestamp='1792584000000')])
        self.assertEqual(len(clean),1);self.assertEqual(clean.iloc[0].ip,'2001:db8::1')
    def test_ambiguous_mapping(self):
        with self.assertRaises(InputError):self.run_rows([self.row(adid='x')])
    def test_empty_file(self):
        pd.DataFrame(columns=list(self.row())).to_parquet(self.path,index=False)
        report,clean,bad=analyze(self.path)
        self.assertEqual(report['rows'],0);write_outputs(report,clean,bad,Path(self.temp.name)/'out')
    def test_codegen(self):
        mapping={k:k for k in self.row()}
        sql=generate_bigquery('project.dataset.events',mapping,DEFAULTS)
        scala=generate_scala(mapping,DEFAULTS)
        self.assertIn('fanout > 30000',sql);self.assertIn('high_maids_per_email',scala)
        self.assertIn('COUNT(DISTINCT ip)',sql);self.assertIn('_ONBOARDING_COMPLETE',scala)
        with self.assertRaises(InputError):generate_bigquery('x`; DROP TABLE x',mapping,DEFAULTS)

if __name__=='__main__': unittest.main()
