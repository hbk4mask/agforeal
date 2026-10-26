"""Deterministic onboarding agent. Raw values stay in quarantine, never in reports."""
from __future__ import annotations
from collections import Counter
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

FIELDS = ['maid', 'ip', 'email', 'device_type', 'device_os', 'timestamp']
ALIASES = {'maid': ['maid', 'adid', 'idfa', 'gaid', 'advertising_id'], 'ip': ['ip', 'ip_address', 'client_ip'], 'email': ['email', 'email_address'], 'device_type': ['device_type', 'device', 'platform_type'], 'device_os': ['device_os', 'os', 'operating_system'], 'timestamp': ['timestamp', 'event_timestamp', 'event_time', 'ts']}
NULLS = {'', 'null', 'none', 'n/a', 'na', 'unknown', 'undefined', '-'}
OS = {'iphone': 'ios', 'ipad': 'ipados', 'ipad os': 'ipados', 'ipados': 'ipados', 'ios': 'ios', 'android': 'android', 'windows': 'windows', 'win': 'windows', 'mac': 'macos', 'mac os': 'macos', 'osx': 'macos', 'macos': 'macos', 'linux': 'linux'}
TYPES = {'phone': 'mobile', 'smartphone': 'mobile', 'mobile': 'mobile', 'tablet': 'tablet', 'ipad': 'tablet', 'desktop': 'desktop', 'pc': 'desktop', 'laptop': 'desktop', 'tv': 'tv', 'smart tv': 'tv'}
ALLOWED = {'ios': {'mobile', 'tablet'}, 'ipados': {'tablet'}, 'android': {'mobile', 'tablet', 'tv'}, 'windows': {'desktop', 'tablet'}, 'macos': {'desktop'}, 'linux': {'desktop'}}
DEFAULTS = {'max_ips_per_maid': 1000, 'max_maids_per_ip': 20000, 'max_maids_per_email': 30000, 'window': 'source', 'deduplicate': True, 'mismatch_action': 'quarantine', 'timestamp_unit': 'auto', 'require_identifier': True}
MAX_ROWS = 500000

class InputError(ValueError):
    pass

def policy_with_defaults(config=None):
    result = {**DEFAULTS, **(config or {})}
    for key in ['max_ips_per_maid', 'max_maids_per_ip', 'max_maids_per_email']:
        if isinstance(result[key], bool) or not isinstance(result[key], int) or result[key] < 1:
            raise InputError(f'{key} must be a positive integer')
    if result['window'] not in ['source', 'day']:
        raise InputError('window must be source or day')
    if result['mismatch_action'] not in ['quarantine', 'repair']:
        raise InputError('mismatch_action must be quarantine or repair')
    if result['timestamp_unit'] not in ['auto', 's', 'ms', 'us', 'ns']:
        raise InputError('Unsupported timestamp unit')
    for key in ['deduplicate', 'require_identifier']:
        if not isinstance(result[key], bool):
            raise InputError(f'{key} must be a boolean')
    return result

def text(value):
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    return None if value.lower() in NULLS else value

def resolve_mapping(columns, requested=None):
    if len(columns) != len(set(columns)):
        raise InputError('Duplicate column names are unsupported; rename them at the source')
    unknown = set(requested or {}) - set(FIELDS)
    if unknown:
        raise InputError(f'Unknown canonical fields: {sorted(unknown)}')
    mapping = {}
    for field in FIELDS:
        candidates = [c for c in columns if c.lower() in ALIASES[field]]
        if requested and field in requested:
            source = requested[field]
            if source is not None and source not in columns:
                raise InputError(f'Missing source column: {source}')
            mapping[field] = source
        elif len(candidates) > 1:
            raise InputError(f'Ambiguous mapping for {field}: {candidates}. Choose a column explicitly.')
        else:
            mapping[field] = candidates[0] if candidates else None
    chosen = [c for c in mapping.values() if c is not None]
    if len(chosen) != len(set(chosen)):
        raise InputError('A source column cannot map to multiple canonical fields')
    if not mapping['timestamp']:
        raise InputError('A timestamp column is required to calculate daily high runners')
    if not any(mapping[k] for k in ['maid', 'ip', 'email']):
        raise InputError('Map at least one identifier: maid, ip, or email')
    return mapping

def load(path):
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows > MAX_ROWS:
        raise InputError(f'Local execution supports up to {MAX_ROWS:,} rows. Use a distributed adapter for larger sources.')
    # Reject huge decoded tables before conversion to pandas.
    decoded = sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups))
    if decoded > 256 * 1024 * 1024:
        raise InputError('Decoded Parquet exceeds the 256 MiB local limit')
    table = parquet.read()
    for field in table.schema:
        if pa.types.is_nested(field.type) or pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
            raise InputError(f'Unsupported nested or binary column: {field.name}')
    return table.to_pandas()

def parse_timestamp(value, unit):
    if value is None:
        return None
    try:
        if re.fullmatch(r'-?\d+(\.\d+)?', value):
            n = float(value)
            selected = unit if unit != 'auto' else ('s' if abs(n) < 1e11 else 'ms' if abs(n) < 1e14 else 'us' if abs(n) < 1e17 else 'ns')
            stamp = pd.to_datetime(n, unit=selected, utc=True)
        else:
            stamp = pd.to_datetime(value, utc=True)
        if pd.isna(stamp):
            return None
        # Arrow nanosecond timestamps must be representable.
        stamp.value
        return stamp
    except (ValueError, OverflowError, TypeError):
        return None

def normalize(raw, mapping, policy):
    clean = pd.DataFrame(index=raw.index)
    reasons = [[] for _ in range(len(raw))]
    repairs = 0
    for field in FIELDS:
        clean[field] = raw[mapping[field]].map(text) if mapping[field] else None
    for i in range(len(clean)):
        r = clean.loc[i].to_dict()
        for field in ['maid', 'email', 'device_os', 'device_type']:
            if r[field]: r[field] = r[field].lower()
        if r['maid']:
            try:
                if not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', r['maid']): raise ValueError()
                identifier = uuid.UUID(r['maid'])
                if identifier.int == 0: raise ValueError()
                r['maid'] = str(identifier)
            except ValueError:
                reasons[i].append('invalid_maid'); r['maid'] = None
        if r['ip']:
            try: r['ip'] = str(ipaddress.ip_address(r['ip']))
            except ValueError:
                reasons[i].append('invalid_ip'); r['ip'] = None
        if r['email'] and (len(r['email']) > 254 or not re.fullmatch(r"[^\s@]+@[^\s@.]+(?:\.[^\s@.]+)+", r['email'])):
            reasons[i].append('invalid_email'); r['email'] = None
        for field, choices in [('device_type', TYPES), ('device_os', OS)]:
            if r[field] and r[field] not in choices:
                reasons[i].append('invalid_' + field); r[field] = None
            elif r[field]: r[field] = choices[r[field]]
        r['timestamp'] = parse_timestamp(r['timestamp'], policy['timestamp_unit'])
        if r['timestamp'] is None: reasons[i].append('invalid_timestamp')
        if policy['require_identifier'] and not any(r[f] for f in ['maid', 'ip', 'email']):
            reasons[i].append('missing_identifier')
        if r['device_os'] and r['device_type'] and r['device_type'] not in ALLOWED[r['device_os']]:
            if policy['mismatch_action'] == 'repair' and len(ALLOWED[r['device_os']]) == 1:
                r['device_type'] = next(iter(ALLOWED[r['device_os']]))
                repairs += 1
            else: reasons[i].append('device_os_type_mismatch')
        clean.loc[i] = r
    clean['timestamp'] = pd.to_datetime(clean['timestamp'], utc=True)
    return clean, reasons, repairs

def metrics(frame):
    result = []
    for name in frame.columns:
        s = frame[name]
        nonnull = s.dropna()
        # Treat blank and placeholder text as null in both profiles.
        values = nonnull.map(text).dropna()
        count = len(values)
        result.append({'field': name, 'dtype': str(s.dtype), 'null_count': len(s)-count, 'null_pct': round(100*(len(s)-count)/max(len(s),1), 2), 'distinct': int(values.nunique()), 'cardinality_ratio': round(values.nunique()/max(count,1),4), 'max_frequency': int(values.value_counts().max()) if count else 0})
    return result

def source_hash(path):
    with open(path, 'rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()

def analyze(path, config=None, requested_mapping=None):
    policy = policy_with_defaults(config)
    raw = load(path).reset_index(drop=True)
    mapping = resolve_mapping(list(raw.columns), requested_mapping)
    clean, reasons, repairs = normalize(raw, mapping, policy)
    clean['_day'] = clean['timestamp'].dt.floor('D')
    checks = [('max_ips_per_maid', 'maid', 'ip', 'high_ips_per_maid'), ('max_maids_per_ip', 'ip', 'maid', 'high_maids_per_ip'), ('max_maids_per_email', 'email', 'maid', 'high_maids_per_email')]
    runners = []
    # Fanout counts use all normalized rows, before filtering or deduplication.
    for setting, key, value, reason in checks:
        group_columns = [key, '_day'] if policy['window'] == 'day' else [key]
        grouped = clean.groupby(group_columns, dropna=True)
        counts = grouped[value].transform('nunique') if value else grouped[key].transform('size')
        flagged = counts.gt(policy[setting])
        for i in clean.index[flagged]: reasons[i].append(reason)
        runners.append({'rule': reason, 'threshold': policy[setting], 'rows': int(flagged.sum()), 'groups': len(clean.loc[flagged, group_columns].drop_duplicates())})
    clean = clean[FIELDS]
    # Do not let a rejected copy cause a valid row to be discarded.
    eligible = pd.Series([not r for r in reasons], index=clean.index, dtype=bool)
    if policy['deduplicate']:
        duplicate_indices = clean.loc[eligible].index[clean.loc[eligible].duplicated(keep='first')]
        for i in duplicate_indices: reasons[i].append('duplicate_row')
    accepted_mask = pd.Series([not r for r in reasons], index=clean.index, dtype=bool)
    accepted = clean.loc[accepted_mask].reset_index(drop=True)
    rejected = raw.loc[~accepted_mask].copy()
    if '_rejection_reasons' in rejected.columns or '_source_row' in rejected.columns:
        raise InputError('Source uses reserved columns _rejection_reasons or _source_row')
    rejected['_source_row'] = rejected.index.astype('int64')
    rejected['_rejection_reasons'] = [json.dumps(reasons[i]) for i in rejected.index]
    tally = Counter(reason for row in reasons for reason in row)
    remaining_mismatches = sum(bool(row.device_os and row.device_type and row.device_type not in ALLOWED[row.device_os]) for row in accepted.itertuples())
    report = {'version': 1, 'created_at': datetime.now(timezone.utc).isoformat(), 'source_sha256': source_hash(path), 'rows': len(raw), 'accepted': len(accepted), 'quarantined': len(rejected), 'acceptance_pct': round(100*len(accepted)/max(len(raw),1),2), 'repaired': repairs, 'mapping': mapping, 'policy': policy, 'before': metrics(raw), 'after': metrics(accepted), 'issues': [{'rule': k, 'rows': v} for k,v in tally.most_common()], 'high_runners': runners, 'post_checks': {'remaining_device_mismatches': remaining_mismatches, 'row_conservation': len(accepted)+len(rejected)==len(raw)}, 'notes': ['Exact full-file cardinality; no identifier values are included in this report.', 'High runners use the configured source or UTC-day window, strict greater-than thresholds, and pre-deduplication counts.', 'iPad is normalized to iPadOS. Repair changes device type only when the OS allows exactly one type.', 'Invalid non-null values quarantine the entire row; optional absent values stay null.', 'Email validation checks syntax only. MAIDs must be nonzero UUIDs. Private IPs are permitted.', 'Naive timestamps are interpreted as UTC; automatic numeric units use magnitude. Override when known.']}
    return report, accepted, rejected

def write_outputs(report, accepted, rejected, destination):
    destination = Path(destination); destination.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(f, pa.timestamp('ns', tz='UTC') if f == 'timestamp' else pa.string()) for f in FIELDS])
    pq.write_table(pa.Table.from_pandas(accepted, schema=schema, preserve_index=False), destination/'clean.parquet')
    rejected.to_parquet(destination/'quarantine.parquet', index=False)
    (destination/'report.json').write_text(json.dumps(report, indent=2))
