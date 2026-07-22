from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def test_all_rcp_submitters_require_immutable_images() -> None:
    submitters = (
        "scripts/submit_shards.sh",
        "scripts/submit_postprocess.sh",
        "scripts/submit_articlemeta_shards.sh",
    )
    for relative in submitters:
        script = (REPO / relative).read_text()
        assert 'IMAGE="${IMAGE:-}"' in script
        assert '${IMAGE:?IMAGE is required and must be pinned by sha256 digest}' in script
        assert '[[ "${IMAGE}" != *@sha256:* ]]' in script
        assert "mlo-base:uv1" not in script
        assert "downloader:1" not in script


def test_image_build_requires_explicit_tag_and_immutable_base() -> None:
    publish = (REPO / "docker/publish.sh").read_text()
    dockerfile = (REPO / "docker/Dockerfile").read_text()
    assert 'TAG="${TAG:-}"' in publish
    assert 'BASE_IMAGE="${BASE_IMAGE:-}"' in publish
    assert "${TAG:?TAG is required; use a release or commit tag}" in publish
    assert "${BASE_IMAGE:?BASE_IMAGE is required and must be pinned by sha256 digest}" in publish
    assert '[[ "${BASE_IMAGE}" != *@sha256:* ]]' in publish
    assert "ARG BASE_IMAGE\n" in dockerfile
    assert "Pillow==12.3.0" in dockerfile
    assert "mlo-base:uv1" not in dockerfile
