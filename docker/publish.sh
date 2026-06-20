#!/usr/bin/env bash
# Build and push the SciELO downloader image to EPFL RCP Harbor.

set -euo pipefail

REGISTRY="${REGISTRY:-registry.rcp.epfl.ch}"
IMAGE_PATH="${IMAGE_PATH:-scielo-fulltext/downloader}"
TAG="${TAG:-1}"
PLATFORM="${PLATFORM:-linux/amd64}"
PUSH="${PUSH:-1}"
BASE_IMAGE="${BASE_IMAGE:-ic-registry.epfl.ch/mlo/mlo-base:uv1}"
FULL_IMAGE="${REGISTRY}/${IMAGE_PATH}:${TAG}"

HERE="$(cd "$(dirname "$0")" && pwd)"

echo "Building ${FULL_IMAGE}"
echo "  PLATFORM=${PLATFORM}"
echo "  BASE_IMAGE=${BASE_IMAGE}"
echo "  PUSH=${PUSH}"

if docker buildx version >/dev/null 2>&1; then
  build_cmd=(docker buildx build --platform "${PLATFORM}")
  if [[ "${PUSH}" == "1" ]]; then
    build_cmd+=(--push)
  else
    build_cmd+=(--load)
  fi
else
  echo "WARN: docker buildx not available; using plain 'docker build'." >&2
  build_cmd=(docker build)
fi

"${build_cmd[@]}" \
  --build-arg BASE_IMAGE="${BASE_IMAGE}" \
  -t "${FULL_IMAGE}" \
  -f "${HERE}/Dockerfile" \
  "${HERE}"

if ! docker buildx version >/dev/null 2>&1 && [[ "${PUSH}" == "1" ]]; then
  docker push "${FULL_IMAGE}"
fi

echo
echo "Image: ${FULL_IMAGE}"
