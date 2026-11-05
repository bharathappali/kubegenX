#!/usr/bin/env bash
# Remove only KubeGenX objects. Clears finalizers on those objects when a delete stalls.
# Uses the current oc context (or CONTEXT / --context if set). Do not hard-code a cluster.
set -euo pipefail
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
NS="kube-gen-x"

if [[ -n "${CONTEXT:-}" ]]; then
  oc config use-context "$CONTEXT" >/dev/null
fi

echo "undeploying from context: $(oc config current-context)"

strip_ns() {
  oc get cm,secret,svc,deploy,job,route,pvc,pod,sa,role,rolebinding -n "$NS" -l app.kubernetes.io/part-of=kubegenx -o name 2>/dev/null \
    | while read -r obj; do
        oc patch -n "$NS" "$obj" --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]' >/dev/null 2>&1 || true
      done
}

if oc get ns "$NS" >/dev/null 2>&1; then
  oc delete ns "$NS" --wait=false >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do
    if ! oc get ns "$NS" >/dev/null 2>&1; then
      break
    fi
    strip_ns
    sleep 2
  done
  if oc get ns "$NS" >/dev/null 2>&1; then
    strip_ns
    oc delete ns "$NS" --wait=true --timeout=120s || true
  fi
fi

oc get pv -l app.kubernetes.io/part-of=kubegenx -o name 2>/dev/null | while read -r pv; do
  oc delete "$pv" --wait=false >/dev/null 2>&1 || true
  phase="$(oc get "$pv" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [[ "$phase" == "Released" || "$phase" == "Failed" || -z "$phase" ]]; then
    oc patch "$pv" --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]' >/dev/null 2>&1 || true
  fi
  if oc get "$pv" >/dev/null 2>&1; then
    oc patch "$pv" --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
    oc delete "$pv" --wait=true --timeout=60s >/dev/null 2>&1 || true
  fi
done
echo "undeployed"
