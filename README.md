# Sourcework — source onboarding agent

A runnable onboarding workbench for Parquet event sources. The intended production path is **BigQuery analysis → review policy → Spark Scala cleaning → post-cleaning validation**. An exact local Parquet engine makes the workflow testable without cloud credentials.

The workspace did not contain the existing product, so this is an independent FastAPI service and browser dashboard with an API integration boundary. It uses deterministic rules and code generation, not an LLM making unsupervised decisions about identifiers.

## Start

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m uvicorn source_agent.app:app --host 127.0.0.1 --port 8787
```

Open http://127.0.0.1:8787. Choose **Load demo source**, then **Analyze local source**, inspect the report, and approve exports. The demo is synthetic. Open **Quality policy** to configure thresholds and mapping; **Execution artifacts** generates downloadable BigQuery SQL and Spark Scala, even without uploading a local file. API documentation: http://127.0.0.1:8787/docs.

## Implemented rules

| Identifier | Distinct association | Default high-runner threshold |
|---|---|---|
| MAID | IPs | > 1,000 |
| IP | MAIDs | > 20,000 |
| Email | MAIDs | > 30,000 |

Thresholds are strict `>` comparisons. Counts default to the **entire supplied source**, with an optional UTC calendar-day window. To profile a week or month as one window, select that source upstream. Counts use normalized identifiers before filtering or deduplication; null associations do not contribute. Every row associated with a high runner is quarantined. A clean result is not guaranteed to retain all source relationships.

- Exact cardinality, null counts/rates, maximum frequency (local), and normalized cardinality (BigQuery).
- Alias-based schema mapping; explicit override for ambiguous columns.
- Trim text, normalize null placeholders, lowercase emails and MAIDs, validate nonzero UUID MAIDs, IPv4/IPv6 and email syntax, and normalize device taxonomies and timestamps.
- Detect cross-field device inconsistencies before deciding whether to quarantine or repair. `iPad` maps to `ipados`; `mobile + ipados` conflicts. Optional repair makes it `tablet`. Ambiguous conflicts remain quarantined.
- Quarantine the entire row for invalid non-null values. Missing optional values stay null; timestamps are required. Require at least one identifier by default.
- Deduplicate canonical rows only after excluding invalid and high-runner records. Duplicate removals appear in quarantine.
- Re-profile accepted output, check device consistency, and assert `accepted + quarantined = source`.

Default policies and generated examples are in `examples/default`. MAID, IP, and email samples never appear in profile reports. Quarantine intentionally contains source values and must be handled like the original source.

## Artifacts and execution

### Local

```sh
python -m source_agent analyze /absolute/path/events.parquet --output /absolute/path/new-run --policy examples/default/policy.json
```

Outputs are `clean.parquet`, `quarantine.parquet`, and `report.json`. Clean columns are `maid`, `ip`, `email`, `device_type`, `device_os`, and UTC `timestamp`. Quarantine retains original columns and adds zero-based `_source_row` and JSON `_rejection_reasons`. Additional source columns are omitted from the canonical clean output. The report includes source SHA-256, mapping, thresholds and execution time. The CLI writes outputs directly; the browser stages them until you approve download.

### BigQuery analysis

```sh
python -m source_agent generate --table project.dataset.events --output /absolute/path/new-artifacts --policy examples/default/policy.json
bq query --use_legacy_sql=false --dry_run < /absolute/path/new-artifacts/analysis.sql
bq query --use_legacy_sql=false --maximum_bytes_billed=10000000000 < /absolute/path/new-artifacts/analysis.sql
```

Use a BigQuery table or external table over Parquet. SQL does not ingest a local file automatically. It returns one report row with arrays of per-field and per-rule metrics and does not modify source tables. Review the dry-run estimate and choose your own billing cap before execution. The query uses exact distinct counts and can be expensive for large sources. Valid rows are counted before deduplication; that number can exceed Spark's final accepted count.

The SQL uses the same policy as Scala but cloud-engine parsing and timestamp semantics must be validated on your source before rollout. Relevant primary references: [BigQuery network functions](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/net_functions), [safe function calls](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference).

### Spark Scala

A build scaffold is in `spark/`; it compiles the canonical-schema example under `examples/default`. Replace that example with the generated Scala for your source mapping, or include `SourceOnboarding.scala` in your existing Scala project.

```sh
cd spark
sbt package
spark-submit --class SourceOnboarding target/scala-2.13/source-onboarding_2.13-0.1.0.jar gs://bucket/input/ gs://bucket/new-run/
```

The build defaults to Spark 3.5.3 / Scala 2.13 for GCP Serverless runtime 2.2. Match dependency versions to your cluster. Storage connectors and credentials are supplied by your deployment. The job uses distributed group-bys and joins, not `collect()` of source records. It caches normalized records, writes clean/quarantine Parquet, metrics, and a final `_ONBOARDING_COMPLETE` marker. Readers must ignore a destination until that marker exists. Failed jobs can leave partial output; retry with a new destination. Quarantine uses `_raw_json`, `_source_row`, `_rejection_reasons`; the row identifier is unique within this execution, not a persistent source record ID.

The Scala runner has been compiled successfully using Java 17, Scala 2.13 and Spark 3.5.3 dependencies. It has not been executed against a real cloud cluster. The timestamp functions were checked against the [Spark 3.5 documentation](https://spark.apache.org/docs/3.5.8/sql-ref-functions-builtin.html). Validate representative input on the target engines before production use, especially IPv4-mapped IPv6, timestamp precision and malformed date formats. Spark's IPv6 presentation may differ from BigQuery/Python while representing the same address. BigQuery/Spark use microseconds; local Arrow output uses nanoseconds.

## Integrate with the existing product

The UI calls these API routes; your product can use the same flow:

1. `POST /api/sources`: raw Parquet bytes with `X-File-Name` header; returns a source ID and columns.
2. `POST /api/analyze`: `{source_id, policy, mapping}`; returns a job ID immediately.
3. `GET /api/jobs/{id}`: queued → running → review or failed. Review includes metrics and reasons.
4. `POST /api/jobs/{id}/approve`: exposes completed output downloads, idempotently.
5. `GET /api/jobs/{id}/artifacts/{name}`: download clean, quarantine or report.
6. `POST /api/generate`: `{source_id: "", source_uri: "project.dataset.table", policy, mapping}` returns SQL and Scala. Omit mapping to use canonical field names.

`source_agent/adapters.py` defines a customer-owned execution adapter (`submit(plan)` / `status(job_id)`). Pass adapters to `create_app(adapters={"spark": your_adapter, "bigquery": your_adapter})` to connect an existing internal job system through `/api/remote/submit` and `/api/remote/{backend}/{id}`. The generic plan is a contract, not executable SQL. Built-in GCP connectors are available under `/api/cloud`; credentials are resolved on the host through ADC. The older `/api/remote` routes remain extension hooks for custom adapters. Add authentication, authorization, persistent jobs and a durable queue as part of integrating with your production product; this server deliberately accepts localhost only.

## Limits and assumptions

- Local: <=500,000 rows, <=128 MiB upload, <=256 MiB decoded Parquet; two concurrent analyses. This in-memory development path is intended for small files. Nested/binary columns are rejected.
- Local jobs and source indexes live in process memory. Restart requires re-upload; files remain under ignored `.runtime/`. Cloud configuration and job plans persist under `.runtime/cloud/`. There is no retention scheduler. Delete unneeded run files according to your data retention policy.
- Numeric timestamp units can be explicitly selected; auto inference uses magnitude. Naive timestamps are UTC. Email validation is syntactic, not deliverability; this schema expects plaintext email, not hashed email. Private IPs are accepted. Device taxonomies are deliberately conservative and should be customized in `engine.py` to match your product.
- OS versions such as `Android 14` require extending the taxonomy; they are currently quarantined rather than guessed.
- Whole-source windows need a bounded input snapshot for reproducible results. Production orchestration should pin a snapshot, record SQL/Scala code version, policy hash and source identity, and run both stages over the same population.

## Verification

```sh
python -m unittest discover -s tests -v
node --check source_agent/static/app.js
```

Tests exercise strict threshold boundaries, all three association directions, UTC-day versus source windows, duplicate accounting, device repairs, invalid values, empty input, code generation, API approval/download gating and cross-origin protection. Browser checks cover demo loading, analysis, report display, approval/export and artifact generation.


## UI cloud connectors — BigQuery and serverless Spark

Open **Cloud connectors** in the sidebar. These are live REST connectors, not placeholders. No cloud execution has been performed during development; tests simulate GCP responses.

### Authentication and access

Install `requirements-cloud.txt`. For local development, run `gcloud auth application-default login` on the backend host. On GCP use workload identity or an attached service account. The connector uses server-side ADC; no credential files, tokens or keys are accepted by the UI. Project IDs, resource URIs, policy snapshots and job records are saved locally with restrictive file permissions. Auth credentials remain managed by Google tooling.

Enable BigQuery, Dataproc and Cloud Storage APIs in your project. The submitting identity needs BigQuery job creation and read access to the source table (and the external data source if applicable). For Spark 2.x, it needs batch creation/get/list and operation access, `iam.serviceAccounts.actAs` on the workload account, and access to upload generated SQL to staging and inspect the JAR object. The Spark workload account needs Dataproc Worker plus appropriate input/JAR/staging read and output write access; configure its network/subnet for your environment. The connectors do not grant IAM permissions. See [GCP serverless permissions](https://docs.cloud.google.com/managed-spark/docs/concepts/iam-serverless).

### BigQuery from the UI

1. Enter project, BigQuery location and maximum bytes billed; save and test BigQuery access.
2. Select BigQuery, enter `project.dataset.table`, and map the cloud source columns.
3. Set thresholds under Quality policy, then **Prepare & validate**. This performs a real dry run and displays estimated bytes. It refuses estimates over your cap.
4. **Run prepared job** submits the fixed policy/mapping snapshot with its billing cap. Use **Refresh status** to view results or open the job in Google Cloud.

BigQuery dry-run and billing-limit fields follow the [Jobs API](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job). Dry-run estimates are not an invoice; reservation billing and external-service costs have their own rules.

### Serverless Spark from the UI

Download the compiled runner from the connector panel, or build it yourself (Java 17 and sbt required), then upload it to GCS once:

```sh
cd spark
sbt package
gcloud storage cp target/scala-2.13/source-onboarding_2.13-0.1.0.jar gs://YOUR_BUCKET/jars/source-onboarding.jar
```

The build defaults match the Scala binary version in [GCP runtime 2.2](https://docs.cloud.google.com/managed-spark/docs/concepts/versions/spark-runtime-2.2). If you select another runtime, match Spark and Scala via the build's `sourcework.sparkVersion` / `sourcework.scalaVersion` system properties. The UI currently supports 2.x runtime identifiers; runtime availability is validated by GCP at submission. The updated runner accepts four arguments: input URI, output URI, generated SQL URI, and deduplication flag. Older two-argument-only JARs are not compatible with this connector.

1. Configure project, region, runtime, workload service account, runner JAR URI, staging prefix, batch timeout and optional subnet. Save and test access.
2. Select Serverless Spark, enter GCS Parquet input and an output prefix, and set source mappings and quality policy.
3. Prepare to inspect the generated SQL and batch payload. This prepares a request locally; it does not validate all IAM or network requirements.
4. Run to upload policy SQL and create a batch through the [serverless batch API](https://docs.cloud.google.com/managed-spark/docs/quickstarts/spark-batch). Every batch gets a unique output subdirectory. Cloud charges begin when the workload runs.
5. Refresh to see state, failures, runtime information and output URI. Consume the output only after success and the `_ONBOARDING_COMPLETE` marker.

Policy changes generate new SQL at submission; recompilation is unnecessary for thresholds, schema mapping, repair policy, timestamp settings or deduplication. Runner code changes still require a new JAR. Only the small generated SQL file is collected on the Spark driver; source records remain distributed.

Prepared plans and submitted IDs survive service restarts. Double submission of the same prepared plan returns the saved job; stable BigQuery job IDs and Dataproc request IDs protect retries after uncertain responses. Configuration changes apply to newly prepared plans. The UI uses manual status refresh and links to GCP for further job management. Jobs submitted outside this app are not imported. There is no built-in cloud cancellation, IAM setup, on-demand JAR compiler or cloud credential provisioning. The connector panel offers the precompiled JAR when the build output is present.

Cloud API: `GET/POST /api/cloud/connection`, `POST /api/cloud/test/{bigquery|spark}`, `POST /api/cloud/prepare`, `POST /api/cloud/submit`, `GET /api/cloud/jobs`, `GET /api/cloud/jobs/{plan_id}`.


### Automatic BigQuery source tables

Configure **Cloud connectors** once with your GCP project, dataset (default `sourcework_onboarding`), BigQuery location and GCS staging prefix. The dataset is created automatically if it does not exist. Its location must match the configured query location, and the source/staging buckets must be colocated as required by BigQuery.

- **GCS path:** choose Google Cloud path, enter a Parquet object, folder prefix or single-wildcard path, then choose **Create table & prepare analysis**. A folder is expanded to `/*.parquet`.
- **Uploaded sample:** upload a local Parquet file, then choose **Create BigQuery table from uploaded sample**. The file is uploaded to the configured GCS staging prefix before the external table is created. The sample alone is analyzed; it is not treated as the whole production source.

The agent creates a Parquet external table with schema autodetection, infers canonical mappings from column aliases, and performs the existing BigQuery analysis dry run. Ambiguous or missing required mappings are shown for correction. You no longer need to provide an existing BigQuery table. **Run prepared job** starts the query after you review its estimate. The local-only analysis option remains available.

Source tables have deterministic `source_<hash>` names. Repeated registration reuses a matching table; an existing different definition is never overwritten. Uploaded objects use content hashes and create-only generation preconditions. Failed creation can leave the uploaded sample or a dataset/table behind; these are reused on retry and are not automatically deleted. Tables are external references to GCS, not managed copies of the data. Avoid deleting staged objects while their tables are in use.

The submitting identity additionally needs `bigquery.datasets.create` when creating a dataset, dataset metadata access, table create/get access in that dataset, and read access to the referenced GCS objects. Uploads require object creation in the configured staging prefix. Existing datasets can be used when dataset-creation permission is unavailable. The app does not modify IAM. Dataset/GCS location or incompatible Parquet schema errors are surfaced from GCP. See [BigQuery external table creation](https://docs.cloud.google.com/bigquery/docs/external-data-cloud-storage).

API: `POST /api/cloud/sources` with either `{ "uri": "gs://bucket/events/*.parquet" }` or `{ "source_id": "uploaded-source-id" }`. Returns the table name, source URI, detected schema, mapping, warnings, and whether the table was created or reused. This operation creates resources but does not run the billable analysis query.

## Deploy on Cloud Run

Cloud Run serves the UI and API; BigQuery performs analysis and serverless Spark performs cleaning. The service uses its attached service account through Application Default Credentials: no credential files or interactive connector login are needed. Configuration is read-only in the hosted UI. Local mode is unchanged.

Cloud mode saves sample files and source/job records in GCS, so another instance or a restarted service can continue a job. Local background analysis is disabled. Uploads are limited to 24 MiB and 500,000 sample rows; use a GCS path for full sources. Temporary sample files are removed after staging. GCS record writes use generation preconditions; job submissions retain stable IDs across retries. Click Refresh to retrieve remote job status.

This is a **shared, trusted-team workspace**: every authorized caller can view and operate its jobs. It does not implement per-user or per-tenant isolation. Keep Cloud Run IAM authentication enabled and grant `roles/run.invoker` only to the intended team. The environment setting `SOURCEWORK_AUTH_MODE` declares this deployment requirement; it does not itself authenticate HTTP requests. Do not expose this container directly to the public internet.

1. Enable Cloud Run, Cloud Build, Artifact Registry, BigQuery, Cloud Storage and Dataproc APIs in your project. Create a Cloud Run service account and a separate Spark workload service account. The deployer needs source-deployment/build permissions and permission to act as the Cloud Run service account.
2. Create state, staging and output buckets (they may be the same bucket with separate prefixes). Choose locations compatible with the BigQuery dataset. Pre-create the dataset to keep dataset-creation privileges out of the runtime account if desired.
3. Grant the Cloud Run account BigQuery Job User on the query project, BigQuery Data Editor on the target dataset, Storage Object Admin on the state/staging buckets, and Storage Object Viewer on source/JAR buckets. If the agent must create the dataset, it also needs `bigquery.datasets.create` on the project. Give it permissions to create/get/list Dataproc batches (for example Dataproc Editor) and Service Account User on the Spark account. Scope these grants to your project/resources.
4. Give the Spark workload account the Dataproc Worker role for runtime 2.2, read access to source/JAR/staging objects, and write access to output and runtime staging objects. Configure a serverless-compatible subnet and its network permissions if using a custom network. Cross-project resources need corresponding grants in their owning projects.
5. Build the Scala runner using the existing Spark instructions above and upload its JAR to GCS. The web container does not compile or run Spark. Set `jar_uri` to this object.
6. Copy `deploy/cloud-run.env.example.yaml` to `deploy/cloud-run.env.yaml` and replace every placeholder. Set project, dataset/location, Spark region/account, staging/JAR paths and state bucket. Keep the file out of source control. No service-account key is needed.
7. From the project directory deploy:

```sh
GCP_PROJECT=your-project \
RUN_SERVICE_ACCOUNT=sourcework-web@your-project.iam.gserviceaccount.com \
REGION=us-central1 ./deploy/deploy.sh
```

The script deploys privately with 2 GiB memory, two CPUs, concurrency four, up to three instances, and a 300-second request timeout. It does not create buckets or grant application IAM roles. For an existing service, verify that no `allUsers`/`allAuthenticatedUsers` invoker binding or disabled invoker check remains. The image runs as a non-root user, binds to `$PORT`, and uses `/tmp` only for temporary data. `/healthz` is the process health endpoint.

For browser access, authenticate your local gcloud account, ensure it has Cloud Run Invoker, then run:

```sh
gcloud run services proxy sourcework --project your-project --region us-central1 --port 8080
```

Open http://localhost:8080. The proxy authenticates requests to the private service. For a normal team-facing browser URL, configure an authenticated entry point such as IAP separately. Custom domains require `SOURCEWORK_ALLOWED_HOSTS` and, if applicable, `SOURCEWORK_ALLOWED_ORIGINS` to be updated; keep these explicit. Default allowed hosts are Cloud Run `.run.app` hosts and localhost.

After deployment, use Test BigQuery/Test Spark access, upload a sample or select a GCS source, create its table, review the dry-run estimate, and submit analysis. Submit a small Spark batch to verify workload IAM and networking before processing a full source. Set bucket retention/lifecycle policies for samples, job records and outputs according to your data policy; do not expire records for active jobs.

References: [Cloud Run service identity](https://docs.cloud.google.com/run/docs/securing/service-identity), [authenticated developer access](https://docs.cloud.google.com/run/docs/authenticating/developers), [Spark service accounts](https://docs.cloud.google.com/managed-spark/docs/concepts/service-accounts-serverless).
