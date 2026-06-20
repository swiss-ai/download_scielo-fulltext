# SciELO downloader runtime image for EPFL RCP.
#
# Extends mlo-base with Pillow so figure workers can render large SciELO TIFF
# originals to bounded JPEG derivatives before tar packing.
ARG BASE_IMAGE=ic-registry.epfl.ch/mlo/mlo-base:uv1
FROM ${BASE_IMAGE}

LABEL org.opencontainers.image.authors="Swiss AI"
LABEL org.opencontainers.image.source="https://github.com/swiss-ai/download_scielo-fulltext"
LABEL org.opencontainers.image.description="mlo-base + Pillow for swiss-ai/download_scielo-fulltext"

RUN /usr/local/bin/uv pip install --system --no-cache-dir \
      'Pillow>=10,<13'
