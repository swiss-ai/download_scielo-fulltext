# SciELO downloader runtime image for EPFL RCP.
#
# Extends mlo-base with Pillow so figure workers can render large SciELO TIFF
# originals to bounded JPEG derivatives before tar packing.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG SOURCE_COMMIT

LABEL org.opencontainers.image.authors="Swiss AI"
LABEL org.opencontainers.image.source="https://github.com/swiss-ai/download_scielo-fulltext"
LABEL org.opencontainers.image.description="mlo-base + Pillow for swiss-ai/download_scielo-fulltext"
LABEL org.opencontainers.image.revision="${SOURCE_COMMIT}"

RUN test -n "${SOURCE_COMMIT}" \
      && /usr/local/bin/uv pip install --system --no-cache-dir \
      'Pillow==12.3.0'
