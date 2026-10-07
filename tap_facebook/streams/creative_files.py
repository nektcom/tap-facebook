"""Creative image files uploaded to a Nekt volume.

Child stream of `creatives`. The parent injects each creative's `image_url` and
`thumbnail_url` into the child context, so this stream issues no Graph API calls
of its own: both are public, tokenized fbcdn links that serve the image bytes
directly (no access token, and sending ours to a CDN host would be wrong). Each
file is downloaded and uploaded to the configured Nekt volume, emitting one
metadata record per uploaded file.

The shared download/upload mechanics live in `VolumeAttachmentMixin`
(nekt_singer_sdk); this stream only supplies the Facebook-specific bits — which
files exist for a given creative (`iter_attachments`) and how to fetch their
bytes from the public URL (`fetch_attachment`). The mixin is also what validates
the destination volume and `NEKT_DATA_ACCESS_TOKEN` at sync time, warning and
yielding nothing when either is missing.

Because the files belong to their creative, replication is driven by the parent:
`ad_updated_time` is the bookmark and `is_sorted = False`. A creative's files are
therefore re-downloaded and re-uploaded — overwriting the same volume keys, since
the key is derived from the creative id — whenever one of its ads is updated.
"""

from __future__ import annotations

import os
import typing as t
from functools import cached_property
from urllib.parse import urlparse

import requests
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.streams import Attachment, VolumeAttachmentMixin
from nekt_singer_sdk.streams.core import Stream

from tap_facebook.streams.creative import (
    CREATIVE_FILE_SOURCE_FIELDS,
    CREATIVE_FILES_TO_UPLOAD_CONFIG_KEY,
    CREATIVE_FILES_VOLUME_CONFIG_KEY,
    CreativeStream,
    selected_creative_file_sources,
)

if t.TYPE_CHECKING:
    from nekt_singer_sdk.helpers.types import Context

# Extensions fbcdn actually serves for creative assets. A `thumbnail_url` path
# carries no extension at all (the size is a query param), and Facebook encodes
# those previews as JPEG, so that is the fallback.
KNOWN_IMAGE_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"})
DEFAULT_IMAGE_EXTENSION = ".jpg"

DOWNLOAD_TIMEOUT_SECONDS = 60

# A file whose connection drops mid-transfer is fetched once more before it is
# given up. The fbcdn URL stays valid for the run, and the SDK skips a file that
# still fails: facebook-ads-n6Rj lost a thumbnail to one IncompleteRead
# (2026-10-06) that a second request would have fetched.
DOWNLOAD_ATTEMPTS = 2
TRANSIENT_DOWNLOAD_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


def _file_extension(url: str) -> str:
    """Return the image extension to store the file under, from the URL path."""
    suffix = os.path.splitext(urlparse(url).path)[1].lower()  # noqa: PTH122
    return suffix if suffix in KNOWN_IMAGE_EXTENSIONS else DEFAULT_IMAGE_EXTENSION


class CreativeFilesStream(VolumeAttachmentMixin, Stream):
    """Download each creative's image/thumbnail and upload them to a volume."""

    name = "creative_files"
    parent_stream_type = CreativeStream
    # One row per file of a creative; the parent dedupes child contexts by
    # creative id, so (creative_id, file_source) is unique per record.
    primary_keys: t.ClassVar[list[str]] = ["creative_id", "file_source"]
    replication_key = "ad_updated_time"
    is_sorted = False
    state_partitioning_keys: t.ClassVar[list[str]] = []
    volume_config_key = CREATIVE_FILES_VOLUME_CONFIG_KEY

    @property
    def schema(self) -> dict:
        """Stream schema: creative identifiers plus uploaded-file metadata."""
        return th.PropertiesList(
            th.Property(
                "creative_id",
                th.StringType,
                description="ID of the ad creative the file belongs to.",
            ),
            th.Property(
                "ad_id",
                th.StringType,
                description=(
                    "ID of the ad whose extraction produced this upload. The same "
                    "creative can be attached to several ads; the file is downloaded "
                    "only once per extraction, so this is one of those ads and not "
                    "the complete list — join through the creatives table for that."
                ),
            ),
            th.Property(
                "ad_updated_time",
                th.DateTimeType,
                description="When the ad that produced this upload was last updated.",
            ),
            th.Property(
                "file_source",
                th.StringType,
                description=(
                    "Which creative field the file came from: 'image' (image_url, the "
                    "full-resolution asset) or 'thumbnail' (thumbnail_url, Facebook's "
                    "downscaled preview, sized by the thumbnail width/height settings)."
                ),
            ),
            th.Property(
                "source_url",
                th.StringType,
                description=(
                    "Facebook CDN URL the file was downloaded from. These links are "
                    "tokenized and expire, so they are only usable at extraction time."
                ),
            ),
            *self.attachment_schema_properties(),
        ).to_dict()

    @cached_property
    def _file_sources(self) -> list[str]:
        """Which file kinds to upload, from the `creative_files_to_upload` setting."""
        sources = selected_creative_file_sources(self.config)
        if not sources:
            user_logger.warning(
                "No creative files will be uploaded: the 'Creative files to upload' "
                "setting holds no recognized value. Accepted values are 'image', "
                "'thumbnail', or both separated by a comma.",
            )
            internal_logger.warning(
                f"[{self.name}] {CREATIVE_FILES_TO_UPLOAD_CONFIG_KEY}="
                f"{self.config.get(CREATIVE_FILES_TO_UPLOAD_CONFIG_KEY)!r} matched no "
                f"known file source (known: {sorted(CREATIVE_FILE_SOURCE_FIELDS)}); "
                "the stream yields no records.",
            )
        return sources

    @cached_property
    def _download_session(self) -> requests.Session:
        """One session for all creative file downloads.

        A large account has tens of thousands of creatives; per-call
        ``requests.get`` would build and tear down a session (TLS pool and all)
        for each file. One session reuses connections, which is both faster and
        avoids thousands of native teardowns.
        """
        return requests.Session()

    def iter_attachments(self, context: Context | None) -> t.Iterable[Attachment]:
        """Yield one Attachment per selected file of the creative in the context."""
        if context is None:
            return
        creative_id = context.get("creative_id")
        if not creative_id:
            return

        for file_source in self._file_sources:
            url = context.get(CREATIVE_FILE_SOURCE_FIELDS[file_source])
            if not url:
                continue
            # The volume keys files by name, and the creative id is unique across
            # Facebook, so `{creative_id}_{file_source}` never collides — and a
            # re-synced creative deliberately overwrites its own file.
            yield Attachment(
                file_name=f"{creative_id}_{file_source}{_file_extension(url)}",
                record={
                    "creative_id": creative_id,
                    "ad_id": context.get("ad_id"),
                    "ad_updated_time": context.get("ad_updated_time"),
                    "file_source": file_source,
                    "source_url": url,
                },
                source={"url": url},
            )

    def fetch_attachment(self, attachment: Attachment, local_path: str) -> None:
        """Stream the file's public fbcdn URL to `local_path`, once more if the connection drops."""
        for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
            try:
                self._download(attachment, local_path)
            except TRANSIENT_DOWNLOAD_ERRORS as err:
                if attempt == DOWNLOAD_ATTEMPTS:
                    raise
                internal_logger.warning(
                    f"[{self.name}] Download of {attachment.file_name} dropped ({err!r}); fetching it again."
                )
            else:
                return

    def _download(self, attachment: Attachment, local_path: str) -> None:
        url = attachment.source["url"]
        with self._download_session.get(
            url,
            stream=True,
            timeout=DOWNLOAD_TIMEOUT_SECONDS,
        ) as resp:
            internal_logger.debug(
                f"[{self.name}] GET {urlparse(url).netloc}{urlparse(url).path} -> "
                f"{resp.status_code} content-type={resp.headers.get('Content-Type')} "
                f"file={attachment.file_name}",
            )
            resp.raise_for_status()
            with open(local_path, "wb") as fp:  # noqa: PTH123
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fp.write(chunk)
