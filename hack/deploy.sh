#!/usr/bin/env bash
# Idempotent install of KubeGenX onto the only reachable OpenShift cluster.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
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

BUCKET="$(oc get pv s3-volume-pv -o jsonpath='{.spec.csi.volumeAttributes.bucketName}')"
if [[ -z "$BUCKET" ]]; then
  echo "s3-volume-pv bucketName missing" >&2
  exit 1
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
    storage: 750Gi
  accessModes: ["ReadWriteMany"]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  mountOptions:
    - allow-delete
    - region us-east-2
    - prefix kubegenx/
    - allow-other
    - file-mode 0666
    - dir-mode 0777
  csi:
    driver: s3.csi.aws.com
    volumeHandle: kubegenx-data-6x4ft
    volumeAttributes:
      bucketName: ${BUCKET}
      prefix: kubegenx/
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
      storage: 750Gi
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

# Mountpoint returns EPERM on O_TRUNC of an existing object. The prefix survives
# PV deletion (reclaim Retain, objects stay in the bucket), so each install
# removes the previous datasets and exports before anything writes again.
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
      imagePullPolicy: IfNotPresent
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
      imagePullPolicy: IfNotPresent
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
echo "deployed uid=${RUN_UID} fsgroup=${FS_GID} bucket=${BUCKET}"
