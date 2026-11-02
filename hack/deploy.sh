#!/usr/bin/env bash
# Install KubeGenX.
#   hack/deploy.sh                         shared S3 volume (default)
#   hack/deploy.sh --volume node-local --node <worker>
# Existing datasets and exports are kept. Pass --clean-on-start to delete them.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VOLUME="s3"
NODE=""
CLEAN=0
usage() {
  echo "usage: hack/deploy.sh [--volume s3|node-local] [--node <worker>] [--clean-on-start]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean-on-start) CLEAN=1; shift ;;
    --volume)
      VOLUME="${2:-}"
      if [[ -z "$VOLUME" ]]; then
        echo "missing value for --volume" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    --node)
      NODE="${2:-}"
      if [[ -z "$NODE" ]]; then
        echo "missing value for --node" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done
case "$VOLUME" in
  s3|node-local) ;;
  *)
    echo "volume must be s3 or node-local" >&2
    exit 1
    ;;
esac
if [[ "$VOLUME" == "node-local" && -z "$NODE" ]]; then
  echo "node-local requires --node <worker>" >&2
  exit 1
fi
if [[ "$VOLUME" == "s3" && -n "$NODE" ]]; then
  echo "--node is only used with --volume node-local" >&2
  exit 1
fi

export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
NS="kube-gen-x"
if [[ "$VOLUME" == "s3" ]]; then
  CTX="default/api-cluster-6x4ft-6x4ft-sandbox1763-opentlc-com:6443/kube:admin"
  oc config use-context "$CTX"
  oc get sc gp3-csi >/dev/null
fi

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

if [[ "$VOLUME" == "s3" ]]; then
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
else
  if oc get pv kubegenx-data >/dev/null 2>&1; then
    driver="$(oc get pv kubegenx-data -o jsonpath='{.spec.csi.driver}' 2>/dev/null || true)"
    if [[ "$driver" == "s3.csi.aws.com" ]]; then
      echo "kubegenx-data is the S3 volume on this cluster" >&2
      exit 1
    fi
  else
    echo "node-local: preparing /mnt/kubegenx on ${NODE}"
    oc debug "node/${NODE}" -- chroot /host mkdir -p /mnt/kubegenx /mnt/kubegenx-state
    oc apply -f - <<EOF
apiVersion: v1
kind: PersistentVolume
metadata:
  name: kubegenx-data
  labels:
    app.kubernetes.io/part-of: kubegenx
spec:
  capacity:
    storage: 300Gi
  accessModes: ["ReadWriteOnce"]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: manual
  local:
    path: /mnt/kubegenx
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: ["${NODE}"]
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: kubegenx-state
  labels:
    app.kubernetes.io/part-of: kubegenx
spec:
  capacity:
    storage: 10Gi
  accessModes: ["ReadWriteOnce"]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: manual
  local:
    path: /mnt/kubegenx-state
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: ["${NODE}"]
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
  accessModes: ["ReadWriteOnce"]
  storageClassName: manual
  volumeName: kubegenx-data
  resources:
    requests:
      storage: 300Gi
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
  storageClassName: manual
  volumeName: kubegenx-state
  resources:
    requests:
      storage: 10Gi
EOF
fi

if [[ "$VOLUME" == "node-local" ]]; then
  MCS="$(oc get ns "$NS" -o jsonpath='{.metadata.annotations.openshift\.io/sa\.scc\.mcs}')"
  if [[ -z "$MCS" ]]; then
    echo "namespace selinux level missing" >&2
    exit 1
  fi
  echo "node-local: shared selinux level ${MCS}"
  sed "s|__MCS_LEVEL__|${MCS}|" "$ROOT/deploy/node-local/scc.yaml" | oc apply -f -
fi

TMP="$(mktemp -d)"
cp -R "$ROOT/deploy/${VOLUME}/." "$TMP/"
python3 - "$TMP/manifests.yaml" "$RUN_UID" "$FS_GID" "$NODE" <<'PY'
import pathlib, sys
path, uid, gid, node = sys.argv[1:]
text = pathlib.Path(path).read_text()
text = text.replace("555000555", uid)
text = text.replace(f"fsGroup: {uid}", f"fsGroup: {gid}", 1)
if node:
    text = text.replace("kubegenx-node-name", node)
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
  NODE_SELECTOR_YAML=""
  if [[ -n "$NODE" ]]; then
    NODE_SELECTOR_YAML="  nodeSelector:
    kubernetes.io/hostname: ${NODE}"
  fi
  oc apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: kgx-wipe
  namespace: ${NS}
  labels:
    app.kubernetes.io/part-of: kubegenx
spec:
${NODE_SELECTOR_YAML}
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
if [[ "$VOLUME" == "s3" ]]; then
  echo "deployed volume=s3 uid=${RUN_UID} fsgroup=${FS_GID} bucket=${BUCKET}"
else
  echo "deployed volume=node-local node=${NODE} uid=${RUN_UID} fsgroup=${FS_GID}"
fi
