"""Unit tests for the creative_files stream (image/thumbnail volume uploads).

These tests run fully offline: nothing here downloads a file or talks to a
volume, they cover which files the stream decides to upload, under what name,
and how the parent feeds it.
"""

from __future__ import annotations

import pytest

from tap_facebook.streams.creative import (
    ADVANCED_FIELDS,
    CreativeStream,
    selected_creative_file_sources,
)
from tap_facebook.streams.creative_files import CreativeFilesStream, _file_extension
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2024-01-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
}

ENABLED_CONFIG = {
    "enable_creative_files_stream": True,
    "nekt_volume_to_upload_creative_files": "volume-1",
}

IMAGE_URL = "https://scontent.xx.fbcdn.net/v/t45.1600-4/123456_n.jpg?_nc_cat=1&oh=abc"
THUMBNAIL_URL = "https://external.xx.fbcdn.net/emg1/v/t13/9876?url=https%3A%2F%2Fx.com%2Fa.png&fb_obo=1"


def _tap(config: dict | None = None) -> TapFacebook:
    return TapFacebook(config={**SAMPLE_CONFIG, **(config or {})}, validate_config=False)


def _creative_stream(config: dict | None = None) -> CreativeStream:
    return _tap(config).streams["creatives"]


def _files_stream(config: dict | None = None) -> CreativeFilesStream:
    return _tap({**ENABLED_CONFIG, **(config or {})}).streams["creative_files"]


def _child_context(creative: dict, config: dict | None = None) -> dict | None:
    stream = _creative_stream({**ENABLED_CONFIG, **(config or {})})
    return stream.get_child_context({**creative, "ad_id": "ad-1", "ad_updated_time": "2026-08-01T12:00:00+0000"})


# --- file naming ----------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (IMAGE_URL, ".jpg"),
        ("https://scontent.xx.fbcdn.net/v/t45/a_n.PNG?oh=1", ".png"),
        ("https://scontent.xx.fbcdn.net/v/t45/a_n.gif", ".gif"),
        # Thumbnail URLs carry no extension, and the embedded `url` param must not
        # be mistaken for one -- Facebook serves these previews as JPEG.
        (THUMBNAIL_URL, ".jpg"),
        ("https://external.xx.fbcdn.net/emg1/v/t13/9876", ".jpg"),
    ],
)
def test_file_extension(url: str, expected: str) -> None:
    assert _file_extension(url) == expected


# --- which files to upload ------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ["image", "thumbnail"]),
        ("", ["image", "thumbnail"]),
        ("image,thumbnail", ["image", "thumbnail"]),
        # Order always follows CREATIVE_FILE_SOURCE_FIELDS, not the config.
        ("thumbnail,image", ["image", "thumbnail"]),
        (" Thumbnail ", ["thumbnail"]),
        ("image", ["image"]),
        ("video", []),
    ],
)
def test_selected_creative_file_sources(value: str | None, expected: list[str]) -> None:
    config = {} if value is None else {"creative_files_to_upload": value}
    assert selected_creative_file_sources(config) == expected


# --- requesting the URLs from the Graph API -------------------------------


def test_image_urls_requested_in_basic_mode_when_upload_enabled() -> None:
    columns = _creative_stream({**ENABLED_CONFIG, "creative_fields_mode": "basic"}).columns
    assert "image_url" in columns
    assert "thumbnail_url" in columns


def test_only_the_selected_url_is_requested() -> None:
    columns = _creative_stream(
        {**ENABLED_CONFIG, "creative_fields_mode": "basic", "creative_files_to_upload": "thumbnail"},
    ).columns
    assert "thumbnail_url" in columns
    assert "image_url" not in columns


def test_basic_mode_unchanged_when_upload_disabled() -> None:
    columns = _creative_stream({"creative_fields_mode": "basic"}).columns
    assert "image_url" not in columns
    assert "thumbnail_url" not in columns


def test_advanced_mode_columns_are_not_duplicated() -> None:
    """Facebook rejects a field repeated in the `fields` expansion."""
    columns = _creative_stream({**ENABLED_CONFIG, "creative_fields_mode": "advanced"}).columns
    assert len(columns) == len(set(columns))
    assert set(ADVANCED_FIELDS).issubset(columns)


# --- parent -> child context ----------------------------------------------


def test_child_context_carries_both_urls() -> None:
    context = _child_context({"id": "creative-1", "image_url": IMAGE_URL, "thumbnail_url": THUMBNAIL_URL})
    assert context == {
        "creative_id": "creative-1",
        "ad_id": "ad-1",
        "ad_updated_time": "2026-08-01T12:00:00+0000",
        "image_url": IMAGE_URL,
        "thumbnail_url": THUMBNAIL_URL,
    }


def test_no_child_context_when_upload_disabled() -> None:
    stream = _creative_stream()
    assert stream.get_child_context({"id": "creative-1", "image_url": IMAGE_URL}) is None


@pytest.mark.parametrize(
    "creative",
    [
        {"id": "creative-1"},
        {"id": "creative-1", "image_url": None, "thumbnail_url": ""},
        {"image_url": IMAGE_URL},
    ],
)
def test_no_child_context_without_id_or_urls(creative: dict) -> None:
    assert _child_context(creative) is None


def test_creative_shared_by_several_ads_is_uploaded_once() -> None:
    stream = _creative_stream(ENABLED_CONFIG)
    creative = {"id": "creative-1", "image_url": IMAGE_URL}

    assert stream.get_child_context({**creative, "ad_id": "ad-1"}) is not None
    assert stream.get_child_context({**creative, "ad_id": "ad-2"}) is None
    assert stream.get_child_context({"id": "creative-2", "image_url": IMAGE_URL, "ad_id": "ad-3"}) is not None


# --- the stream itself ----------------------------------------------------


def test_stream_only_registered_when_enabled() -> None:
    assert "creative_files" not in _tap().streams
    assert "creative_files" in _tap(ENABLED_CONFIG).streams


def test_iter_attachments_yields_one_file_per_url() -> None:
    stream = _files_stream()
    context = {
        "creative_id": "creative-1",
        "ad_id": "ad-1",
        "ad_updated_time": "2026-08-01T12:00:00+0000",
        "image_url": IMAGE_URL,
        "thumbnail_url": THUMBNAIL_URL,
    }

    image, thumbnail = list(stream.iter_attachments(context))

    assert image.file_name == "creative-1_image.jpg"
    assert image.source == {"url": IMAGE_URL}
    assert image.record == {
        "creative_id": "creative-1",
        "ad_id": "ad-1",
        "ad_updated_time": "2026-08-01T12:00:00+0000",
        "file_source": "image",
        "source_url": IMAGE_URL,
    }
    assert thumbnail.file_name == "creative-1_thumbnail.jpg"
    assert thumbnail.record["file_source"] == "thumbnail"
    assert thumbnail.record["source_url"] == THUMBNAIL_URL


def test_iter_attachments_skips_the_unselected_and_the_missing() -> None:
    stream = _files_stream({"creative_files_to_upload": "image"})
    context = {"creative_id": "creative-1", "image_url": IMAGE_URL, "thumbnail_url": THUMBNAIL_URL}

    (attachment,) = list(stream.iter_attachments(context))
    assert attachment.record["file_source"] == "image"

    assert list(stream.iter_attachments({"creative_id": "creative-1"})) == []


@pytest.mark.parametrize("context", [None, {}, {"image_url": IMAGE_URL}])
def test_iter_attachments_without_a_creative_yields_nothing(context: dict | None) -> None:
    assert list(_files_stream().iter_attachments(context)) == []


def test_unrecognized_file_selection_uploads_nothing() -> None:
    stream = _files_stream({"creative_files_to_upload": "video"})
    context = {"creative_id": "creative-1", "image_url": IMAGE_URL, "thumbnail_url": THUMBNAIL_URL}
    assert list(stream.iter_attachments(context)) == []


def test_schema_carries_the_uploaded_file_metadata() -> None:
    properties = _files_stream().schema["properties"]
    expected = ("creative_id", "file_source", "source_url", "_nekt_file_id", "_nekt_file_size")
    for field in expected:
        assert field in properties
    assert all(properties[field].get("description") for field in properties)


# --- download + upload, with the network and the volume faked ---------------


class _FakeResponse:
    """Minimal stand-in for a streamed `requests` response."""

    status_code = 200
    headers = {"Content-Type": "image/jpeg"}

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int) -> list[bytes]:  # noqa: ARG002
        return [b"\xff\xd8\xff", b"body"]


class _FakeSession:
    """Records the URLs it was asked to download."""

    def __init__(self) -> None:
        self.urls: list[str] = []

    def get(self, url: str, **_kwargs: object) -> _FakeResponse:
        self.urls.append(url)
        return _FakeResponse()


class _FakeNektAPI:
    """Records what was uploaded, and what bytes reached the volume."""

    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, bytes]] = []

    def upload_file_by_volume_id(self, volume_identifier: str, file_path: str, file_name: str) -> dict:
        with open(file_path, "rb") as fp:  # noqa: PTH123
            content = fp.read()
        self.uploads.append((volume_identifier, file_name, content))
        return {"id": f"file-{len(self.uploads)}", "file_size": len(content), "file_type": "image/jpeg"}


def test_get_records_downloads_uploads_and_emits_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEKT_DATA_ACCESS_TOKEN", "test-token")
    stream = _files_stream()
    session, api = _FakeSession(), _FakeNektAPI()
    stream.__dict__["_download_session"] = session
    stream.__dict__["_nekt_api"] = api

    records = list(
        stream.get_records(
            {
                "creative_id": "creative-1",
                "ad_id": "ad-1",
                "ad_updated_time": "2026-08-01T12:00:00+0000",
                "image_url": IMAGE_URL,
                "thumbnail_url": THUMBNAIL_URL,
            },
        ),
    )

    assert session.urls == [IMAGE_URL, THUMBNAIL_URL]
    assert api.uploads == [
        ("volume-1", "creative-1_image.jpg", b"\xff\xd8\xffbody"),
        ("volume-1", "creative-1_thumbnail.jpg", b"\xff\xd8\xffbody"),
    ]
    assert [record["file_source"] for record in records] == ["image", "thumbnail"]
    assert records[0]["_nekt_file_id"] == "file-1"
    assert records[0]["_nekt_file_name"] == "creative-1_image.jpg"
    assert records[0]["_nekt_file_size"] == len(b"\xff\xd8\xffbody")
    assert records[0]["_nekt_mime_type"] == "image/jpeg"
    assert records[0]["_nekt_uploaded_at"]


def test_get_records_without_a_volume_uploads_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NEKT_DATA_ACCESS_TOKEN", raising=False)
    stream = _files_stream()
    api = _FakeNektAPI()
    stream.__dict__["_nekt_api"] = api

    assert list(stream.get_records({"creative_id": "creative-1", "image_url": IMAGE_URL})) == []
    assert api.uploads == []


def _one_attachment():
    context = {"creative_id": "creative-1", "ad_id": "ad-1", "image_url": IMAGE_URL}
    stream = _files_stream()
    return stream, next(iter(stream.iter_attachments(context)))


def test_a_download_dropped_mid_transfer_is_fetched_again(tmp_path) -> None:
    """facebook-ads-n6Rj lost a thumbnail to one IncompleteRead (2026-10-06)."""
    import requests
    from unittest import mock

    stream, attachment = _one_attachment()
    dropped = requests.exceptions.ChunkedEncodingError("Connection broken: IncompleteRead(512 bytes read)")
    with mock.patch.object(stream, "_download", side_effect=[dropped, None]) as download:
        stream.fetch_attachment(attachment, str(tmp_path / "f.jpg"))
    assert download.call_count == 2


def test_a_download_that_keeps_dropping_is_left_to_the_sdk(tmp_path) -> None:
    """The SDK logs the file and skips it, as before; only one more request is spent."""
    import requests
    from unittest import mock

    stream, attachment = _one_attachment()
    dropped = requests.exceptions.ConnectionError("reset")
    with mock.patch.object(stream, "_download", side_effect=dropped) as download, pytest.raises(
        requests.exceptions.ConnectionError
    ):
        stream.fetch_attachment(attachment, str(tmp_path / "f.jpg"))
    assert download.call_count == 2


def test_a_refused_download_is_not_asked_for_again(tmp_path) -> None:
    """A 403/404 from fbcdn is an answer, not a dropped connection."""
    import requests
    from unittest import mock

    stream, attachment = _one_attachment()
    refused = requests.exceptions.HTTPError("403 Forbidden")
    with mock.patch.object(stream, "_download", side_effect=refused) as download, pytest.raises(
        requests.exceptions.HTTPError
    ):
        stream.fetch_attachment(attachment, str(tmp_path / "f.jpg"))
    assert download.call_count == 1
