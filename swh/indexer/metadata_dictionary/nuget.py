# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os.path
import re
from typing import Any, Dict

from rdflib import RDF, BNode, Graph, Literal, URIRef

from swh.indexer.codemeta import _DATA_DIR, read_crosstable
from swh.indexer.namespaces import SCHEMA

from .base import SingleFileIntrinsicMapping, XmlMapping
from .utils import add_list, add_url_if_valid

NUGET_TABLE_PATH = os.path.join(_DATA_DIR, "nuget.csv")

with open(NUGET_TABLE_PATH) as fd:
    (CODEMETA_TERMS, NUGET_TABLE) = read_crosstable(fd)

SPDX = URIRef("https://spdx.org/licenses/")


class NuGetMapping(XmlMapping, SingleFileIntrinsicMapping):
    """
    dedicated class for NuGet (.nuspec) mapping and translation
    """

    name = "nuget"
    filename = re.compile(rb".*\.nuspec")
    mapping = NUGET_TABLE["NuGet"]
    mapping["copyright"] = URIRef("http://schema.org/copyrightNotice")
    mapping["language"] = URIRef("http://schema.org/inLanguage")
    string_fields = [
        "description",
        "version",
        "name",
        "tags",
        "license",
        "summary",
        "copyright",
        "language",
    ]
    uri_fields = ["projectUrl", "licenseUrl"]

    def _translate_dict(self, d: Dict[str, Any]) -> Dict[str, Any]:
        return super()._translate_dict(d.get("package", {}).get("metadata", {}))

    def translate_repository(self, graph, root, v):
        if isinstance(v, dict) and isinstance(v["@url"], str):
            codemeta_key = URIRef(self.mapping["repository.url"])
            add_url_if_valid(graph, root, codemeta_key, v["@url"])

    def normalize_license(self, v):
        if isinstance(v, dict) and v["@type"] == "expression":
            license_string = v["#text"]
            if not bool(
                re.search(r" with |\(|\)| and ", license_string, re.IGNORECASE)
            ):
                return [
                    SPDX + license_type.strip()
                    for license_type in re.split(
                        r" or ", license_string, flags=re.IGNORECASE
                    )
                ]
            else:
                return None

    def translate_authors(self, graph: Graph, root, s):
        if isinstance(s, str):
            authors = []
            for author_name in s.split(","):
                author_name = author_name.strip()
                author = BNode()
                graph.add((author, RDF.type, SCHEMA.Person))
                graph.add((author, SCHEMA.name, Literal(author_name)))
                authors.append(author)
            add_list(graph, root, SCHEMA.author, authors)

    def translate_releaseNotes(self, graph: Graph, root, s):
        if isinstance(s, str):
            graph.add((root, SCHEMA.releaseNotes, Literal(s)))

    def normalize_tags(self, s):
        if isinstance(s, str):
            return [Literal(tag) for tag in s.split(" ")]
