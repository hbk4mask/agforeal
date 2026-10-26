"""Execution boundary for customer-owned compute tools.

Cloud adapters are intentionally not advertised as executable until installed.
Implement the protocol and register through create_app(adapters={...}) to integrate
an existing Spark submitter, BigQuery job service, or an internal agent tool.
"""
from typing import Protocol

class ExecutionAdapter(Protocol):
    def submit(self, plan: dict) -> dict:
        """Return {job_id, status}; submit with a customer-managed identity."""
        ...
    def status(self, job_id: str) -> dict:
        """Return status and artifact references once complete."""
        ...

BACKENDS = [
    {'id': 'local', 'name': 'Local Parquet', 'ready': True, 'description': 'Exact analysis • up to 500,000 rows / 256 MiB decoded'},
    {'id': 'spark', 'name': 'Apache Spark', 'ready': False, 'description': 'Execution contract available • connect your job submitter'},
    {'id': 'bigquery', 'name': 'BigQuery', 'ready': False, 'description': 'Execution contract available • connect your job service'},
]

def make_plan(backend, source, policy, mapping):
    return {'contract_version': 1, 'backend': backend, 'source': source, 'policy': policy, 'mapping': mapping, 'canonical_schema': {'maid': 'nullable string', 'ip': 'nullable string', 'email': 'nullable string', 'device_type': 'nullable string', 'device_os': 'nullable string', 'timestamp': 'UTC timestamp'}, 'stages': [
        {'id': 'inspect', 'operation': 'validate_schema_and_resolve_mapping', 'on_ambiguity': 'fail'},
        {'id': 'profile', 'operation': 'exact_cardinality_nulls_frequency', 'scope': 'full_source'},
        {'id': 'standardize', 'operation': 'normalize_nulls_identifiers_devices_and_utc_timestamps'},
        {'id': 'validate', 'operation': 'validate_uuid_ip_email_and_device_cross_fields'},
        {'id': 'high_runners', 'operation': 'group_counts_and_distinct_fanout', 'window': policy['window'], 'population': 'normalized_before_filtering_and_deduplication', 'comparison': 'strictly_greater_than'},
        {'id': 'clean', 'operation': 'quarantine_invalid_and_high_runner_rows_then_deduplicate_eligible_rows'},
        {'id': 'verify', 'operation': 'reprofile_and_check_row_conservation_and_device_consistency'},
        {'id': 'publish', 'operation': 'write_new_clean_quarantine_and_report_artifacts', 'requires_approval': True}
    ], 'cloud_execution': 'Requires a registered adapter; this contract is not executable SQL.'}
