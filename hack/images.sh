#!/usr/bin/env bash
# Multi-arch images to quay.io/bharathappali/kube-gen-x.
# Uses docker buildx when it exists, otherwise podman manifests.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
REG="${REG:-quay.io/bharathappali/kube-gen-x}"
VER="${VERSION:-v0.1.0}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"

push_one() {
  local name="$1" file="$2"
  if docker buildx version >/dev/null 2>&1; then
    docker buildx build --platform "$PLATFORMS" -f "$file" \
      -t "${REG}:${name}-${VER}" -t "${REG}:${name}-latest" --push .
    return
  fi
  local manifest="kgx-${name}"
  podman manifest rm "$manifest" >/dev/null 2>&1 || true
  local platform
  IFS=',' read -r -a archs <<< "$PLATFORMS"
  for platform in "${archs[@]}"; do
    case "$platform" in
      linux/amd64) arch=amd64 ;;
      linux/arm64) arch=arm64 ;;
      *) echo "unsupported platform $platform" >&2; exit 1 ;;
    esac
    podman build --platform "$platform" --build-arg "TARGETARCH=${arch}" --manifest "$manifest" -f "$file" .
  done
  if [[ "${SKIP_PUSH:-}" == "1" ]]; then
    echo "built ${manifest} (${PLATFORMS})"
    return
  fi
  podman manifest push --all "$manifest" "docker://${REG}:${name}-${VER}"
  podman manifest push --all "$manifest" "docker://${REG}:${name}-latest"
}

push_one controller docker/controller.Dockerfile
push_one converter docker/converter.Dockerfile
push_one worker docker/worker.Dockerfile
push_one tools docker/tools.Dockerfile
if [[ "${SKIP_PUSH:-}" == "1" ]]; then
  echo "built ${REG} controller/worker/tools ${VER} locally"
else
  echo "pushed ${REG} controller/worker/tools ${VER} and latest"
fi
