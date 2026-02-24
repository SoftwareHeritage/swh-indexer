# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os.path
from typing import Optional

from rdflib import BNode, Graph, Literal, URIRef

from swh.indexer.codemeta import _DATA_DIR, read_crosstable
from swh.indexer.namespaces import RDF, SCHEMA

from .base import JsonMapping, SingleFileIntrinsicMapping
from .utils import add_map

SPDX = URIRef("https://spdx.org/licenses/")


COMPOSER_TABLE_PATH = os.path.join(_DATA_DIR, "composer.csv")

with open(COMPOSER_TABLE_PATH) as fd:
    (CODEMETA_TERMS, COMPOSER_TABLE) = read_crosstable(fd)


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
        "license",
        "author",
        "authors",
    ]
    uri_fields = ["homepage"]

    def normalize_license(self, s):
        if isinstance(s, str):
            return SPDX + s

    def _translate_author(self, graph: Graph, author) -> Optional[BNode]:
        if not isinstance(author, dict):
            return None
        node = BNode()
        graph.add((node, RDF.type, SCHEMA.Person))

        if isinstance(author.get("name"), str):
            graph.add((node, SCHEMA.name, Literal(author["name"])))
        if isinstance(author.get("email"), str):
            graph.add((node, SCHEMA.email, Literal(author["email"])))

        return node

    def translate_authors(self, graph: Graph, root: URIRef, authors) -> None:
        add_map(graph, root, SCHEMA.author, self._translate_author, authors)
