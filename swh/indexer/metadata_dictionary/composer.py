# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os.path

from swh.indexer.codemeta import _DATA_DIR, SCHEMA_URI, _read_crosstable

from .base import JsonMapping, SingleFileIntrinsicMapping

COMPOSER_TABLE_PATH = os.path.join(_DATA_DIR, "composer.csv")

with open(COMPOSER_TABLE_PATH) as fd:
    (CODEMETA_TERMS, COMPOSER_TABLE) = _read_crosstable(fd)


class ComposerMapping(JsonMapping, SingleFileIntrinsicMapping):
    """Dedicated class for Packagist(composer.json) mapping and translation"""

    name = "composer"
    mapping = COMPOSER_TABLE["Composer"]
    filename = b"composer.json"
    string_fields = [
        "name",
        "description",
        "version",
        "keywords",
        "homepage",
        "license",
        "author",
        "authors",
    ]

    def normalize_homepage(self, s):
        if isinstance(s, str):
            return {"@id": s}

    def normalize_license(self, s):
        if isinstance(s, str):
            return {"@id": "https://spdx.org/licenses/" + s}

    def normalize_authors(self, author_list):
        authors = []
        for author in author_list:
            author_obj = {"@type": SCHEMA_URI + "Person"}

            if isinstance(author, dict):
                if isinstance(author.get("name", None), str):
                    author_obj[SCHEMA_URI + "name"] = author.get("name", None)
                if isinstance(author.get("email", None), str):
                    author_obj[SCHEMA_URI + "email"] = author.get("email", None)

                authors.append(author_obj)

        return {"@list": authors}
