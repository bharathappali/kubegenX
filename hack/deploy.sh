#!/usr/bin/env bash
# Idempotent install of KubeGenX onto the only reachable OpenShift cluster.
# Existing datasets and exports are kept. Pass --clean-on-start to delete them.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLEAN=0
for arg in "$@"; do
  case "$arg" in
    --clean-on-start) CLEAN=1 ;;
    -h|--help)
      echo "usage: hack/deploy.sh [--clean-on-start]"
      exit 0
      ;;
    *)
      echo "unknown argument: $arg" >&2
      echo "usage: hack/deploy.sh [--clean-on-start]" >&2
      exit 1
      ;;
  esac
done
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
CTX="default/api-cluster-6x4ft-6x4ft-sandbox1763-opentlc-com:6443/kube:admin"
NS="kube-gen-x"
oc config use-context "$CTX"
oc get sc gp3-csi >/dev/null

oc apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: kube-gen-x
  labels:
    app.kubernetes.io/part-of: kubegenx
    pod-security.kubernetes.io/enforce: restricted
EOF

UID_RANGE="$(oc get ns "$NS" -o jsonpath='{.metadata.annotations.openshift\.io/sa\.scc\.uid-range}')"
FS_RANGE="$(oc get ns "$NS" -o jsonpath='{.metadata.annotations.openshift\.io/sa\.scc\.supplemental-groups}')"
RUN_UID="${UID_RANGE%%/*}"
FS_GID="${FS_RANGE%%/*}"
if [[ -z "$RUN_UID" || -z "$FS_GID" ]]; then
  echo "namespace uid range missing" >&2
  exit 1
fi

BUCKET="${BUCKET:-}"
if [[ -z "$BUCKET" ]]; then
  BUCKET="$(oc get pv s3-volume-pv -o jsonpath='{.spec.csi.volumeAttributes.bucketName}' 2>/dev/null || true)"
fi
if [[ -z "$BUCKET" ]]; then
  BUCKET="s3-bucket-6x4ft-for-rwx"
fi

if ! oc get pv kubegenx-data >/dev/null 2>&1; then
  oc apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: kubegenx-data
  labels:
    app.kubernetes.io/part-of: kubegenx
spec:
  capacity:
    storage: 1500Gi
  accessModes: ["ReadWriteMany"]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  mountOptions:
    - allow-delete
    - region us-east-2
    - prefix kubegenx2/
    - allow-other
    - file-mode 0666
    - dir-mode 0777
  csi:
    driver: s3.csi.aws.com
    volumeHandle: kubegenx-data-6x4ft-2
    volumeAttributes:
      bucketName: ${BUCKET}
      prefix: kubegenx2/
EOF
fi

oc apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kubegenx-data
  namespace: ${NS}
  labels:
    app.kubernetes.io/part-of: kubegenx
spec:
  accessModes: ["ReadWriteMany"]
  storageClassName: ""
  volumeName: kubegenx-data
  resources:
    requests:
      storage: 1500Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kubegenx-state
  namespace: ${NS}
  labels:
    app.kubernetes.io/part-of: kubegenx
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: gp3-csi
  resources:
    requests:
      storage: 10Gi
EOF

TMP="$(mktemp -d)"
cp -R "$ROOT/deploy/base/." "$TMP/"
sed -i '' "s/555000555/${RUN_UID}/" "$TMP/manifests.yaml"
# fsGroup is the second 555000555, already replaced with RUN_UID. Put the supplemental gid back on fsGroup only.
python3 - "$TMP/manifests.yaml" "$RUN_UID" "$FS_GID" <<'PY'
import pathlib, sys
path, uid, gid = sys.argv[1:]
text = pathlib.Path(path).read_text()
text = text.replace(f"runAsUser: {uid}", f"runAsUser: {uid}", 1)
text = text.replace(f"fsGroup: {uid}", f"fsGroup: {gid}", 1)
pathlib.Path(path).write_text(text)
PY
oc apply -k "$TMP"
rm -rf "$TMP"

oc rollout status deployment/kubegenx-controller -n "$NS" --timeout=600s

if [[ "$CLEAN" -eq 1 ]]; then
  # Mountpoint returns EPERM on O_TRUNC of an existing object. The prefix survives
  # PV deletion (reclaim Retain, objects stay in the bucket).
  echo "clean-on-start: removing /data/datasets and /data/exports"
  oc delete pod -n "$NS" kgx-wipe --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
  oc apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: kgx-wipe
  namespace: ${NS}
  labels:
    app.kubernetes.io/part-of: kubegenx
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: ${RUN_UID}
    fsGroup: ${FS_GID}
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: datasets
      image: quay.io/bharathappali/kube-gen-x:tools-v0.1.0
      imagePullPolicy: Always
      command: ["/tools", "rm", "/data/datasets"]
      securityContext:
        runAsNonRoot: true
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      volumeMounts:
        - name: data
          mountPath: /data
    - name: exports
      image: quay.io/bharathappali/kube-gen-x:tools-v0.1.0
      imagePullPolicy: Always
      command: ["/tools", "rm", "/data/exports"]
      securityContext:
        runAsNonRoot: true
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: kubegenx-data
EOF
  if ! oc wait -n "$NS" --for=jsonpath='{.status.phase}'=Succeeded pod/kgx-wipe --timeout=300s; then
    oc logs -n "$NS" pod/kgx-wipe --all-containers || true
    exit 1
  fi
  oc delete pod -n "$NS" kgx-wipe --wait=false >/dev/null 2>&1 || true
else
  echo "keeping existing datasets and exports"
fi
echo "deployed uid=${RUN_UID} fsgroup=${FS_GID} bucket=${BUCKET}"
