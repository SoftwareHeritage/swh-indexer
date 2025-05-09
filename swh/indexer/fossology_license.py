# Copyright (C) 2016-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import subprocess
from typing import Any, Dict, List, Optional

import sentry_sdk

from swh.core.config import merge_configs
from swh.indexer.storage.interface import IndexerStorageInterface
from swh.indexer.storage.model import ContentLicenseRow
from swh.model import hashutil
from swh.objstorage.interface import CompositeObjId

from .indexer import ContentIndexer, write_to_temp

logger = logging.getLogger(__name__)


def compute_license(path) -> Dict:
    """Determine license from file at path.

    Args:
        path: filepath to determine the license

    Returns:
        dict: A dict with the following keys:

        - licenses ([str]): associated detected licenses to path
        - path (bytes): content filepath

    """
    try:
        properties = subprocess.check_output(["nomossa", path], universal_newlines=True)
        if properties:
            res = properties.rstrip().split(" contains license(s) ")
            licenses = res[1].split(",")
        else:
            licenses = []

        return {
            "licenses": licenses,
            "path": path,
        }
    except subprocess.CalledProcessError:
        from os import path as __path

        logger.exception(
            "Problem during license detection for sha1 %s" % __path.basename(path)
        )
        sentry_sdk.capture_exception()
        return {
            "licenses": [],
            "path": path,
        }


DEFAULT_CONFIG: Dict[str, Any] = {
    "workdir": "/tmp/swh/indexer.fossology.license",
    "tools": {
        "name": "nomos",
        "version": "3.1.0rc2-31-ga2cbb8c",
        "configuration": {
            "command_line": "nomossa <filepath>",
        },
    },
    "write_batch_size": 1000,
}


class MixinFossologyLicenseIndexer:
    """Mixin fossology license indexer.

    See :class:`FossologyLicenseIndexer`

    """

    tool: Any
    idx_storage: IndexerStorageInterface

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = merge_configs(DEFAULT_CONFIG, self.config)
        self.working_directory = self.config["workdir"]

    def index(
        self, id: CompositeObjId, data: Optional[bytes] = None, **kwargs
    ) -> List[ContentLicenseRow]:
        """Index sha1s' content and store result.

        Args:
            id (bytes): content's identifier
            raw_content (bytes): associated raw content to content id

        Returns:
            dict: A dict, representing a content_license, with keys:

            - id (bytes): content's identifier (sha1)
            - license (bytes): license in bytes
            - path (bytes): path
            - indexer_configuration_id (int): tool used to compute the output

        """
        assert data is not None
        with write_to_temp(
            filename=hashutil.hash_to_hex(id["sha1"]),  # use the id as pathname
            data=data,
            working_directory=self.working_directory,
        ) as content_path:
            properties = compute_license(path=content_path)
        return [
            ContentLicenseRow(
                id=id["sha1"],
                indexer_configuration_id=self.tool["id"],
                license=license,
            )
            for license in properties["licenses"]
        ]

    def persist_index_computations(
        self, results: List[ContentLicenseRow]
    ) -> Dict[str, int]:
        """Persist the results in storage.

        Args:
            results: list of content_license dict with the
              following keys:

              - id (bytes): content's identifier (sha1)
              - license (bytes): license in bytes
              - path (bytes): path

        """
        return self.idx_storage.content_fossology_license_add(results)


class FossologyLicenseIndexer(
    MixinFossologyLicenseIndexer, ContentIndexer[ContentLicenseRow]
):
    """Indexer in charge of:

    - filtering out content already indexed
    - reading content from objstorage per the content's id (sha1)
    - computing {license, encoding} from that content
    - store result in storage

    """

    pass
