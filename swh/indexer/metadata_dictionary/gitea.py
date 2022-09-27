# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Any, Tuple

from rdflib import RDF, BNode, Graph, Literal, URIRef

from swh.indexer.codemeta import _DATA_DIR, read_crosstable
from swh.indexer.namespaces import ACTIVITYSTREAMS, FORGEFED, SCHEMA

from .base import BaseExtrinsicMapping, JsonMapping, produce_terms
from .utils import prettyprint_graph  # noqa

SPDX = URIRef("https://spdx.org/licenses/")


GITEA_TABLE_PATH = os.path.join(_DATA_DIR, "Gitea.csv")

with open(GITEA_TABLE_PATH) as fd:
    (CODEMETA_TERMS, GITEA_TABLE) = read_crosstable(fd)


class GiteaMapping(BaseExtrinsicMapping, JsonMapping):
    name = "gitea"
    mapping = GITEA_TABLE["Gitea"]
    uri_fields = [
        "website",
        "clone_url",
    ]
    date_fields = [
        "created_at",
        "updated_at",
    ]
    string_fields = [
        "name",
        "full_name",
        "languages",
        "description",
    ]

    @classmethod
    def extrinsic_metadata_formats(cls) -> Tuple[str, ...]:
        return ("gitea-project-json", "gogs-project-json")

    def extra_translation(self, graph, root, content_dict):
        graph.remove((root, RDF.type, SCHEMA.SoftwareSourceCode))
        graph.add((root, RDF.type, FORGEFED.Repository))

    def get_root_uri(self, content_dict: dict) -> URIRef:
        if isinstance(content_dict.get("html_url"), str):
            return URIRef(content_dict["html_url"])
        else:
            raise ValueError(
                f"Gitea/Gogs metadata has invalid/missing html_url: {content_dict}"
            )

    @produce_terms(FORGEFED.forks, ACTIVITYSTREAMS.totalItems)
    def translate_forks_count(self, graph: Graph, root: BNode, v: Any) -> None:
        """

        >>> graph = Graph()
        >>> root = URIRef("http://example.org/test-software")
        >>> GiteaMapping().translate_forks_count(graph, root, 42)
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "https://forgefed.org/ns#forks": {
                "@type": "https://www.w3.org/ns/activitystreams#OrderedCollection",
                "https://www.w3.org/ns/activitystreams#totalItems": 42
            }
        }
        """
        if isinstance(v, int):
            collection = BNode()
            graph.add((root, FORGEFED.forks, collection))
            graph.add((collection, RDF.type, ACTIVITYSTREAMS.OrderedCollection))
            graph.add((collection, ACTIVITYSTREAMS.totalItems, Literal(v)))

    @produce_terms(ACTIVITYSTREAMS.likes, ACTIVITYSTREAMS.totalItems)
    def translate_stars_count(self, graph: Graph, root: BNode, v: Any) -> None:
        """

        >>> graph = Graph()
        >>> root = URIRef("http://example.org/test-software")
        >>> GiteaMapping().translate_stars_count(graph, root, 42)
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "https://www.w3.org/ns/activitystreams#likes": {
                "@type": "https://www.w3.org/ns/activitystreams#Collection",
                "https://www.w3.org/ns/activitystreams#totalItems": 42
            }
        }
        """
        if isinstance(v, int):
            collection = BNode()
            graph.add((root, ACTIVITYSTREAMS.likes, collection))
            graph.add((collection, RDF.type, ACTIVITYSTREAMS.Collection))
            graph.add((collection, ACTIVITYSTREAMS.totalItems, Literal(v)))

    @produce_terms(ACTIVITYSTREAMS.followers, ACTIVITYSTREAMS.totalItems)
    def translate_watchers_count(self, graph: Graph, root: BNode, v: Any) -> None:
        """

        >>> graph = Graph()
        >>> root = URIRef("http://example.org/test-software")
        >>> GiteaMapping().translate_watchers_count(graph, root, 42)
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "https://www.w3.org/ns/activitystreams#followers": {
                "@type": "https://www.w3.org/ns/activitystreams#Collection",
                "https://www.w3.org/ns/activitystreams#totalItems": 42
            }
        }
        """
        if isinstance(v, int):
            collection = BNode()
            graph.add((root, ACTIVITYSTREAMS.followers, collection))
            graph.add((collection, RDF.type, ACTIVITYSTREAMS.Collection))
            graph.add((collection, ACTIVITYSTREAMS.totalItems, Literal(v)))
