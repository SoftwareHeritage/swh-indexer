# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import json
from typing import List, Tuple

from swh.indexer.codemeta import SCHEMA_URI
from swh.indexer.storage.interface import Sha1

from .base import DirectoryLsEntry, JsonMapping


def _prettyprint(d):
    print(json.dumps(d, indent=4))


class GitHubMapping(JsonMapping):
    name = "github"
    mapping = {
        "name": SCHEMA_URI + "name",
        "license": SCHEMA_URI + "license",
    }
    string_fields = ["name"]

    @classmethod
    def detect_metadata_files(cls, file_entries: List[DirectoryLsEntry]) -> List[Sha1]:
        return []

    @classmethod
    def extrinsic_metadata_formats(cls) -> Tuple[str, ...]:
        return ("application/vnd.github.v3+json",)

    def normalize_license(self, d):
        """

        >>> GitHubMapping().normalize_license({'spdx_id': 'MIT'})
        {'@id': 'https://spdx.org/licenses/MIT'}
        """
        if isinstance(d, dict) and isinstance(d.get("spdx_id"), str):
            return {"@id": "https://spdx.org/licenses/" + d["spdx_id"]}
