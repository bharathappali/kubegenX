#!/usr/bin/env bash
# Create a dataset, then export it with the name create printed.
#
#   export KUBE_GENX_URL=https://kubegenx-kube-gen-x.apps.example.com
#   scripts/create-and-export.sh
#
# Optional environment:
#   NAME DAYS NAMESPACES PER_NAMESPACE WORKLOADS SHAPE GPU_PERCENT STEP EFFORT
#   SEED EPOCH ANCHOR EXPORT_TIME EXPORT_EFFORT EXPORT_TYPE INTERVAL
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
kubegenx=(python3 "$here/kubegenx")

: "${KUBE_GENX_URL:?Set KUBE_GENX_URL to the controller URL}"

create_args=(
  --shape "${SHAPE:-namespace}"
  --namespaces "${NAMESPACES:-3}"
  --per-namespace "${PER_NAMESPACE:-5}"
  --workloads "${WORKLOADS:-15}"
  --gpu-percent "${GPU_PERCENT:-7}"
  --days "${DAYS:-1}"
  --step "${STEP:-15s}"
  --effort "${EFFORT:-xhigh}"
  --seed "${SEED:-424242}"
)
if [[ -n "${NAME:-}" ]]; then
  create_args+=(--name "$NAME")
fi
if [[ -n "${EPOCH:-}" ]]; then
  create_args+=(--epoch "$EPOCH")
fi

details=$("${kubegenx[@]}" create "${create_args[@]}")
name=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])' <<<"$details")

export_args=(
  --dataset "$name"
  --type "${EXPORT_TYPE:-thanos}"
  --anchor "${ANCHOR:-start}"
  --effort "${EXPORT_EFFORT:-xhigh}"
)
if [[ -n "${EXPORT_TIME:-}" ]]; then
  export_args+=(--time "$EXPORT_TIME")
fi
if [[ "${EXPORT_TYPE:-thanos}" == "generic" ]]; then
  export_args+=(--interval "${INTERVAL:-15m}")
fi

"${kubegenx[@]}" export "${export_args[@]}"
