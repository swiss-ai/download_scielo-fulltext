# Caveats

## Proxy Requirement

All network scripts require a proxy configuration. Do not bulk-fetch SciELO from
university or cluster shared IPs.

## XML Availability

ArticleMeta is the index, not the full-text XML dump. Some collections expose
canonical OPAC XML with `?format=xml&lang=...`; others may return HTML, block,
or lack full-text links.

ArticleMeta `format=xmlrsps` returns JATS-like XML, but early spot checks showed
it can be front/back metadata rather than full body. Treat it as a possible
fallback artifact, not as primary full text.

## Licenses

The downloader is strict. Only CC BY, CC0, and public-domain-like licenses pass.
Ambiguous, missing, NC, ND, SA, or other licenses are recorded but not packaged
as accepted content.

Third-party figure/caption ambiguity is rejected by default. Rows with caption
terms such as `permission`, `copyright`, `adapted from`, or `reproduced from`
are recorded as `license_figure_ambiguous` and are not packaged.

## Figures

The worker downloads figure/media URLs referenced by the accepted XML. Relative
URLs are resolved against the XML URL. Articles with unreachable figures are
kept as `partial_figures` only when the article license passes; missing figure
URLs are listed in the manifest and should be retried before final clean-corpus
publication.

TIFF originals from SciELO's object store can be tens of MB per panel, and some
PNG originals are also multi-MB. TIFFs and oversized rasters are rendered
immediately to bounded JPEG derivatives by default (`--render-tiff`,
`--normalize-large-raster`, `--max-passthrough-raster-bytes`,
`--render-max-side`, `--render-max-pixels`, `--render-jpeg-quality`). The
original TIFF bytes are not stored unless `--keep-original-tiff-on-render-failure`
is explicitly passed and rendering fails. The manifest records original bytes and
hashes for audit.
