# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os.path
import re
from typing import Any, Dict, List, Optional

import xmltodict

from swh.indexer.codemeta import _DATA_DIR, _read_crosstable
from swh.indexer.namespaces import SCHEMA
from swh.indexer.storage.interface import Sha1

from .base import DictMapping, DirectoryLsEntry, SingleFileIntrinsicMapping

NUGET_TABLE_PATH = os.path.join(_DATA_DIR, "nuget.csv")

with open(NUGET_TABLE_PATH) as fd:
    (CODEMETA_TERMS, NUGET_TABLE) = _read_crosstable(fd)


class NuGetMapping(DictMapping, SingleFileIntrinsicMapping):
    """
    dedicated class for NuGet (.nuspec) mapping and translation
    """

    name = "nuget"
    mapping = NUGET_TABLE["NuGet"]
    mapping["copyright"] = "http://schema.org/copyrightNotice"
    mapping["language"] = "http://schema.org/inLanguage"
    string_fields = [
        "description",
        "version",
        "projectUrl",
        "name",
        "tags",
        "license",
        "licenseUrl",
        "summary",
        "copyright",
        "language",
    ]

    @classmethod
    def detect_metadata_files(cls, file_entries: List[DirectoryLsEntry]) -> List[Sha1]:
        for entry in file_entries:
            if entry["name"].endswith(b".nuspec"):
                return [entry["sha1"]]
        return []

    def translate(self, content: bytes) -> Optional[Dict[str, Any]]:
        d = (
            xmltodict.parse(content.strip(b" \n "))
            .get("package", {})
            .get("metadata", {})
        )
        if not isinstance(d, dict):
            self.log.warning("Skipping ill-formed XML content: %s", content)
            return None

        return self._translate_dict(d)

    def normalize_projectUrl(self, s):
        if isinstance(s, str):
            return {"@id": s}

    def translate_repository(self, translated_metadata, v):
        if isinstance(v, dict) and isinstance(v["@url"], str):
            codemeta_key = self.mapping["repository.url"]
            translated_metadata[codemeta_key] = {"@id": v["@url"]}

    def normalize_license(self, v):
        if isinstance(v, dict) and v["@type"] == "expression":
            license_string = v["#text"]
            if not bool(
                re.search(r" with |\(|\)| and ", license_string, re.IGNORECASE)
            ):
                return [
                    {"@id": "https://spdx.org/licenses/" + license_type.strip()}
                    for license_type in re.split(
                        r" or ", license_string, flags=re.IGNORECASE
                    )
                ]
            else:
                return None

    def normalize_licenseUrl(self, s):
        if isinstance(s, str):
            return {"@id": s}

    def normalize_authors(self, s):
        if isinstance(s, str):
            author_names = [a.strip() for a in s.split(",")]
            authors = [
                {"@type": SCHEMA.Person, SCHEMA.name: name} for name in author_names
            ]
            return {"@list": authors}

    def translate_releaseNotes(self, translated_metadata, s):
        if isinstance(s, str):
            translated_metadata.setdefault("http://schema.org/releaseNotes", []).append(
                s
            )

    def normalize_tags(self, s):
        if isinstance(s, str):
            return s.split(" ")
