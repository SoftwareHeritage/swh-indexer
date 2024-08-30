# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os.path
import re

from rdflib import RDF, BNode, Graph, Literal, URIRef

from swh.indexer.codemeta import _DATA_DIR, read_crosstable
from swh.indexer.namespaces import SCHEMA

from .base import SingleFileIntrinsicMapping, YamlMapping
from .utils import add_map

SPDX = URIRef("https://spdx.org/licenses/")

PUB_TABLE_PATH = os.path.join(_DATA_DIR, "pubspec.csv")

with open(PUB_TABLE_PATH) as fd:
    (CODEMETA_TERMS, PUB_TABLE) = read_crosstable(fd)


def name_to_person(name):
    return {
        "@type": SCHEMA.Person,
        SCHEMA.name: name,
    }


class PubspecMapping(YamlMapping, SingleFileIntrinsicMapping):
    name = "pubspec"
    filename = b"pubspec.yaml"
    mapping = PUB_TABLE["Pubspec"]
    string_fields = [
        "repository",
        "keywords",
        "description",
        "name",
        "issue_tracker",
        "platforms",
        "license",
        # license will only be used with the SPDX Identifier
    ]
    uri_fields = ["homepage"]

    def normalize_license(self, s):
        if isinstance(s, str):
            return SPDX + s

    def _translate_author(self, graph, s):
        name_email_re = re.compile("(?P<name>.*?)( <(?P<email>.*)>)")
        if isinstance(s, str):
            author = BNode()
            graph.add((author, RDF.type, SCHEMA.Person))
            match = name_email_re.search(s)
            if match:
                name = match.group("name")
                email = match.group("email")
                graph.add((author, SCHEMA.email, Literal(email)))
            else:
                name = s

            graph.add((author, SCHEMA.name, Literal(name)))

            return author

    def translate_author(self, graph: Graph, root, s) -> None:
        add_map(graph, root, SCHEMA.author, self._translate_author, [s])

    def translate_authors(self, graph: Graph, root, authors) -> None:
        if isinstance(authors, list):
            add_map(graph, root, SCHEMA.author, self._translate_author, authors)
