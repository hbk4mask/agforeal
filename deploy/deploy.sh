#!/bin/sh
set -eu
: "${GCP_PROJECT:?Set GCP_PROJECT}"
: "${RUN_SERVICE_ACCOUNT:?Set RUN_SERVICE_ACCOUNT to the Cloud Run service account email}"
REGION=${REGION:-us-central1}
SERVICE=${SERVICE:-sourcework}
ENV_FILE=${ENV_FILE:-deploy/cloud-run.env.yaml}
if [ ! -f "$ENV_FILE" ]; then echo "Copy deploy/cloud-run.env.example.yaml to $ENV_FILE and fill in your settings." >&2; exit 1; fi
if grep -q 'YOUR_' "$ENV_FILE"; then echo 'Replace all YOUR_ placeholders first.' >&2; exit 1; fi
gcloud run deploy "$SERVICE" --source . --project "$GCP_PROJECT" --region "$REGION" \
  --service-account "$RUN_SERVICE_ACCOUNT" --env-vars-file "$ENV_FILE" \
  --no-allow-unauthenticated --port 8080 --memory 2Gi --cpu 2 \
  --concurrency 4 --max-instances 3 --timeout 300
