# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, List
import urllib.parse

from rdflib import BNode, Graph, Literal, URIRef
import rdflib.term

from swh.indexer.codemeta import CROSSWALK_TABLE
from swh.indexer.namespaces import RDF, SCHEMA

from .base import SingleFileIntrinsicMapping, YamlMapping
from .utils import add_map

DOI = URIRef("https://doi.org/")
SPDX = URIRef("https://spdx.org/licenses/")


class CffMapping(YamlMapping, SingleFileIntrinsicMapping):
    """Dedicated class for Citation (CITATION.cff) mapping and translation"""

    name = "cff"
    filename = b"CITATION.cff"
    mapping = CROSSWALK_TABLE["Citation File Format Core (CFF-Core) 1.0.2"]
    string_fields = ["title", "keywords", "license", "abstract", "version", "doi"]
    date_fields = ["date-released"]
    uri_fields = ["url", "repository-code"]

    def _translate_author(self, graph: Graph, author: dict) -> rdflib.term.Node:
        node: rdflib.term.Node
        if (
            "orcid" in author
            and isinstance(author["orcid"], str)
            and urllib.parse.urlparse(author["orcid"]).netloc
        ):
            node = URIRef(author["orcid"])
        else:
            node = BNode()
        graph.add((node, RDF.type, SCHEMA.Person))
        if "affiliation" in author and isinstance(author["affiliation"], str):
            affiliation = BNode()
            graph.add((node, SCHEMA.affiliation, affiliation))
            graph.add((affiliation, RDF.type, SCHEMA.Organization))
            graph.add((affiliation, SCHEMA.name, Literal(author["affiliation"])))
        if "family-names" in author and isinstance(author["family-names"], str):
            graph.add((node, SCHEMA.familyName, Literal(author["family-names"])))
        if "given-names" in author and isinstance(author["given-names"], str):
            graph.add((node, SCHEMA.givenName, Literal(author["given-names"])))
        return node

    def translate_authors(
        self, graph: Graph, root: URIRef, authors: List[dict]
    ) -> None:
        add_map(graph, root, SCHEMA.author, self._translate_author, authors)

    def normalize_doi(self, s: str) -> URIRef:
        if isinstance(s, str):
            return DOI + s

    def normalize_license(self, s: str) -> URIRef:
        if isinstance(s, str):
            return SPDX + s

    def _translate_dict(self, content_dict: Dict) -> Dict[str, Any]:
        # https://github.com/citation-file-format/citation-file-format/blob/main/schema-guide.md#credit-redirection
        return super()._translate_dict(
            content_dict.get("preferred-citation", content_dict)
        )
