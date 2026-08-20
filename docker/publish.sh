#!/usr/bin/env bash
# Build and push the SciELO downloader image to EPFL RCP Harbor.

set -euo pipefail

REGISTRY="${REGISTRY:-registry.rcp.epfl.ch}"
IMAGE_PATH="${IMAGE_PATH:-scielo-fulltext/downloader}"
TAG="${TAG:-}"
PLATFORM="${PLATFORM:-linux/amd64}"
PUSH="${PUSH:-1}"
BASE_IMAGE="${BASE_IMAGE:-}"

: "${TAG:?TAG is required; use a release or commit tag}"
: "${BASE_IMAGE:?BASE_IMAGE is required and must be pinned by sha256 digest}"
: "${SOURCE_COMMIT:?SOURCE_COMMIT is required and must be the full downloader commit}"
if [[ "${TAG}" == "latest" || "${TAG}" == "1" || "${TAG}" == "v1" ]]; then
  echo "ERROR: mutable/generic image tag '${TAG}' is not allowed" >&2
  exit 1
fi
if [[ "${BASE_IMAGE}" != *@sha256:* ]]; then
  echo "ERROR: BASE_IMAGE must be pinned by sha256 digest; mutable tags are not allowed." >&2
  exit 1
fi
if [[ ! "${SOURCE_COMMIT}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "ERROR: SOURCE_COMMIT must be a full 40-character lowercase Git SHA" >&2
  exit 1
fi

FULL_IMAGE="${REGISTRY}/${IMAGE_PATH}:${TAG}"

HERE="$(cd "$(dirname "$0")" && pwd)"

echo "Building ${FULL_IMAGE}"
echo "  PLATFORM=${PLATFORM}"
echo "  BASE_IMAGE=${BASE_IMAGE}"
echo "  SOURCE_COMMIT=${SOURCE_COMMIT}"
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
  --build-arg SOURCE_COMMIT="${SOURCE_COMMIT}" \
  -t "${FULL_IMAGE}" \
  -f "${HERE}/Dockerfile" \
  "${HERE}"

if ! docker buildx version >/dev/null 2>&1 && [[ "${PUSH}" == "1" ]]; then
  docker push "${FULL_IMAGE}"
fi

echo
echo "Image: ${FULL_IMAGE}"
