# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os.path
import re

from swh.indexer.codemeta import _DATA_DIR, SCHEMA_URI, _read_crosstable

from .base import YamlMapping

PUB_TABLE_PATH = os.path.join(_DATA_DIR, "pubspec.csv")

with open(PUB_TABLE_PATH) as fd:
    (CODEMETA_TERMS, PUB_TABLE) = _read_crosstable(fd)


def name_to_person(name):
    return {
        "@type": SCHEMA_URI + "Person",
        SCHEMA_URI + "name": name,
    }


class PubspecMapping(YamlMapping):

    name = "pubspec"
    filename = b"pubspec.yaml"
    mapping = PUB_TABLE["Pubspec"]
    string_fields = [
        "repository",
        "keywords",
        "description",
        "name",
        "homepage",
        "issue_tracker",
        "platforms",
        "license"
        # license will only be used with the SPDX Identifier
    ]

    def normalize_license(self, s):
        if isinstance(s, str):
            return {"@id": "https://spdx.org/licenses/" + s}

    def normalize_homepage(self, s):
        if isinstance(s, str):
            return {"@id": s}

    def normalize_author(self, s):
        name_email_regex = "(?P<name>.*?)( <(?P<email>.*)>)"
        author = {"@type": SCHEMA_URI + "Person"}
        if isinstance(s, str):
            match = re.search(name_email_regex, s)
            if match:
                name = match.group("name")
                email = match.group("email")
                author[SCHEMA_URI + "email"] = email
            else:
                name = s

            author[SCHEMA_URI + "name"] = name

            return {"@list": [author]}

    def normalize_authors(self, authors_list):
        authors = {"@list": []}

        if isinstance(authors_list, list):
            for s in authors_list:
                author = self.normalize_author(s)["@list"]
                authors["@list"] += author
            return authors
