"""python -m source_agent {analyze,generate} ..."""
import argparse
import json
from pathlib import Path
from .engine import analyze, write_outputs, FIELDS, policy_with_defaults
from .codegen import generate_bigquery, generate_scala

parser=argparse.ArgumentParser(description='Source onboarding agent')
sub=parser.add_subparsers(dest='command',required=True)
local=sub.add_parser('analyze');local.add_argument('source');local.add_argument('--output',required=True);local.add_argument('--policy');local.add_argument('--mapping')
gen=sub.add_parser('generate');gen.add_argument('--table',required=True);gen.add_argument('--output',required=True);gen.add_argument('--policy');gen.add_argument('--mapping')
args=parser.parse_args()
policy=policy_with_defaults(json.loads(Path(args.policy).read_text()) if args.policy else {})
mapping=json.loads(Path(args.mapping).read_text()) if args.mapping else None
out=Path(args.output)
if out.exists(): parser.error('Choose a new output directory; existing destinations are never overwritten')
if args.command=='analyze':
    report,clean,bad=analyze(args.source,policy,mapping)
    write_outputs(report,clean,bad,out)
    print(json.dumps({k:report[k] for k in ['rows','accepted','quarantined','repaired']},indent=2))
else:
    mapping=mapping or {f:f for f in FIELDS}
    sql=generate_bigquery(args.table,mapping,policy);scala=generate_scala(mapping,policy)
    out.mkdir(parents=True)
    (out/'analysis.sql').write_text(sql);(out/'SourceOnboarding.scala').write_text(scala)
    (out/'policy.json').write_text(json.dumps(policy,indent=2));(out/'mapping.json').write_text(json.dumps(mapping,indent=2))
    print(f'Generated BigQuery and Spark artifacts in {out}')
