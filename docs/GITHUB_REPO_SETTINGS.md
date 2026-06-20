# GitHub Repository Settings

Suggested public repository metadata:

- Name: `download_scielo-fulltext`
- Description: `Resumable downloader for license-filtered SciELO structured XML and figure media.`
- Homepage: `https://www.swiss-ai.org/`
- Topics: `apertus`, `dataset`, `scielo`, `jats`, `open-access`, `data-pipeline`
- Default branch: `main`
- Visibility: public, after operator review
- License: Apache-2.0 for downloader code

Do not publish generated corpus outputs, proxy files, `.env` files, or scratch
state. The `.gitignore`, `config/.gitignore`, and `docker/.dockerignore` files
are intentionally strict.

Open questions before first push:

- Fill `.github/CODEOWNERS` with the real Swiss AI owning team.
- Confirm Apache-2.0 is the desired code license for this downloader.
