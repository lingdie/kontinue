#!/usr/bin/env bash
# Run after creating kontinue-system. Existing credentials survive redeployment.
set -euo pipefail
namespace=kontinue-system
name=kontinue-api-credentials
if [[ -n "$(kubectl -n "$namespace" get secret "$name" --ignore-not-found -o name)" ]]; then
  exit 0
fi
python3 - "$@" <<'PY' | kubectl -n "$namespace" create -f -
import json, secrets, sys
tenants = sys.argv[1:] or ["local"]
keys = {name: secrets.token_urlsafe(32) for name in tenants}
print(json.dumps({"apiVersion": "v1", "kind": "Secret",
                  "metadata": {"name": "kontinue-api-credentials"},
                  "stringData": {"tenants.json": json.dumps(keys), "trace-token": secrets.token_urlsafe(32)}}))
PY
