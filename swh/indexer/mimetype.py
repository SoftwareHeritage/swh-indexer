# Copyright (C) 2016-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, List, Optional

import magic

from swh.core.config import merge_configs
from swh.indexer.storage.interface import IndexerStorageInterface
from swh.indexer.storage.model import ContentMimetypeRow
from swh.objstorage.interface import CompositeObjId

from .indexer import ContentIndexer

if not hasattr(magic.Magic, "from_buffer"):
    raise ImportError(
        'Expected "import magic" to import python-magic, but file_magic '
        "was imported instead."
    )


def compute_mimetype_encoding(raw_content: bytes) -> Dict[str, str]:
    """Determine mimetype and encoding from the raw content.

    Args:
        raw_content: content's raw data

    Returns:
        dict: mimetype and encoding key and corresponding values.

    """
    m = magic.Magic(mime=True, mime_encoding=True)
    res = m.from_buffer(raw_content)
    try:
        mimetype, encoding = res.split("; charset=")
    except ValueError:
        mimetype, encoding = res, ""
    return {
        "mimetype": mimetype,
        "encoding": encoding,
    }


DEFAULT_CONFIG: Dict[str, Any] = {
    "tools": {
        "name": "file",
        "version": "1:5.30-1+deb9u1",
        "configuration": {"type": "library", "debian-package": "python3-magic"},
    },
    "write_batch_size": 1000,
}


class MixinMimetypeIndexer:
    """Mixin mimetype indexer.

    See :class:`MimetypeIndexer`

    """

    tool: Any
    idx_storage: IndexerStorageInterface

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = merge_configs(DEFAULT_CONFIG, self.config)

    def index(
        self, id: CompositeObjId, data: Optional[bytes] = None, **kwargs
    ) -> List[ContentMimetypeRow]:
        """Index sha1s' content and store result.

        Args:
            id: content's identifier
            data: raw content in bytes

        Returns:
            dict: content's mimetype; dict keys being

            - id: content's identifier (sha1)
            - mimetype: mimetype in bytes
            - encoding: encoding in bytes

        """
        assert data is not None
        properties = compute_mimetype_encoding(data)
        return [
            ContentMimetypeRow(
                id=id["sha1"],
                indexer_configuration_id=self.tool["id"],
                mimetype=properties["mimetype"],
                encoding=properties["encoding"],
            )
        ]

    def persist_index_computations(
        self, results: List[ContentMimetypeRow]
    ) -> Dict[str, int]:
        """Persist the results in storage.

        Args:
            results: list of content's mimetype dicts
              (see :meth:`.index`)

        """
        return self.idx_storage.content_mimetype_add(results)


class MimetypeIndexer(MixinMimetypeIndexer, ContentIndexer[ContentMimetypeRow]):
    """Mimetype Indexer working on list of content identifiers.

    It:

    - (optionally) filters out content already indexed (cf.
      :meth:`.filter`)
    - reads content from objstorage per the content's id (sha1)
    - computes {mimetype, encoding} from that content
    - stores result in storage

    """

    def filter(self, ids: List[CompositeObjId]):
        """Filter out known sha1s and return only missing ones."""
        yield from self.idx_storage.content_mimetype_missing(
            (
                {
                    "id": id["sha1"],
                    "indexer_configuration_id": self.tool["id"],
                }
                for id in ids
            )
        )
