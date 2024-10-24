# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Tuple

from rdflib import RDF, BNode, Graph, Literal, URIRef

from swh.indexer.codemeta import CROSSWALK_TABLE
from swh.indexer.namespaces import ACTIVITYSTREAMS, CODEMETA, FORGEFED, SCHEMA, XSD

from .base import BaseExtrinsicMapping, JsonMapping, produce_terms
from .utils import add_url_if_valid, prettyprint_graph  # noqa

SPDX = URIRef("https://spdx.org/licenses/")


class GitHubMapping(BaseExtrinsicMapping, JsonMapping):
    name = "github"
    mapping = {
        **CROSSWALK_TABLE["GitHub"],
        "topics": SCHEMA.keywords,  # TODO: submit this to the official crosswalk
        "clone_url": SCHEMA.codeRepository,
        "language": SCHEMA.programmingLanguage,
    }
    uri_fields = [
        "clone_url",
    ]
    date_fields = [
        "created_at",
        "updated_at",
    ]
    string_fields = [
        "description",
        "full_name",
        "language",
        "topics",
    ]

    @classmethod
    def extrinsic_metadata_formats(cls) -> Tuple[str, ...]:
        return ("application/vnd.github.v3+json",)

    def extra_translation(self, graph, root, content_dict):
        graph.remove((root, RDF.type, SCHEMA.SoftwareSourceCode))
        graph.add((root, RDF.type, FORGEFED.Repository))

        if content_dict.get("has_issues"):
            add_url_if_valid(
                graph,
                root,
                CODEMETA.issueTracker,
                URIRef(content_dict["html_url"] + "/issues"),
            )

    def get_root_uri(self, content_dict: dict) -> URIRef:
        if isinstance(content_dict.get("html_url"), str):
            return URIRef(content_dict["html_url"])
        else:
            raise ValueError(
                f"GitHub metadata has missing/invalid html_url: {content_dict}"
            )

    @produce_terms(FORGEFED.forkedFrom)
    def translate_parent(self, graph: Graph, root: BNode, v: Any) -> None:
        """

        >>> graph = Graph()
        >>> root = URIRef("http://example.org/test-fork")
        >>> GitHubMapping().translate_parent(
        ...     graph, root, {"html_url": "http://example.org/test-software"})
        >>> prettyprint_graph(graph, root)
        {
            "@id": "http://example.org/test-fork",
            "https://forgefed.org/ns#forkedFrom": {
                "@id": "http://example.org/test-software"
            }
        }
        """
        if isinstance(v, dict) and isinstance(v.get("html_url"), str):
            repository = URIRef(v["html_url"])
            graph.add((root, FORGEFED.forkedFrom, repository))
            # TODO: uncomment this line to also translate the parent's metadata:
            # self._translate_to_graph(graph, repository, v)
            #
            # But let's not do it yet, because it would double the number of occurrences
            # in the description, causing the current implementation of swh-search
            # to give higher scores to forks than to original repositories, when
            # searching for keywords in the description; whereas forks are usually
            # of less interest than original repositories.

    @produce_terms(FORGEFED.forks, ACTIVITYSTREAMS.totalItems)
    def translate_forks_count(self, graph: Graph, root: BNode, v: Any) -> None:
        """

        >>> graph = Graph()
        >>> root = URIRef("http://example.org/test-software")
        >>> GitHubMapping().translate_forks_count(graph, root, 42)
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "https://forgefed.org/ns#forks": {
                "@type": "https://www.w3.org/ns/activitystreams#OrderedCollection",
                "https://www.w3.org/ns/activitystreams#totalItems": {
                    "@type": "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
                    "@value": "42"
                }
            }
        }
        """
        if isinstance(v, int):
            collection = BNode()
            graph.add((root, FORGEFED.forks, collection))
            graph.add((collection, RDF.type, ACTIVITYSTREAMS.OrderedCollection))
            graph.add(
                (
                    collection,
                    ACTIVITYSTREAMS.totalItems,
                    Literal(v, datatype=XSD.nonNegativeInteger),
                )
            )

    @produce_terms(ACTIVITYSTREAMS.likes, ACTIVITYSTREAMS.totalItems)
    def translate_stargazers_count(self, graph: Graph, root: BNode, v: Any) -> None:
        """

        >>> graph = Graph()
        >>> root = URIRef("http://example.org/test-software")
        >>> GitHubMapping().translate_stargazers_count(graph, root, 42)
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "https://www.w3.org/ns/activitystreams#likes": {
                "@type": "https://www.w3.org/ns/activitystreams#Collection",
                "https://www.w3.org/ns/activitystreams#totalItems": {
                    "@type": "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
                    "@value": "42"
                }
            }
        }
        """
        if isinstance(v, int):
            collection = BNode()
            graph.add((root, ACTIVITYSTREAMS.likes, collection))
            graph.add((collection, RDF.type, ACTIVITYSTREAMS.Collection))
            graph.add(
                (
                    collection,
                    ACTIVITYSTREAMS.totalItems,
                    Literal(v, datatype=XSD.nonNegativeInteger),
                )
            )

    @produce_terms(ACTIVITYSTREAMS.followers, ACTIVITYSTREAMS.totalItems)
    def translate_watchers_count(self, graph: Graph, root: BNode, v: Any) -> None:
        """

        >>> graph = Graph()
        >>> root = URIRef("http://example.org/test-software")
        >>> GitHubMapping().translate_watchers_count(graph, root, 42)
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "https://www.w3.org/ns/activitystreams#followers": {
                "@type": "https://www.w3.org/ns/activitystreams#Collection",
                "https://www.w3.org/ns/activitystreams#totalItems": {
                    "@type": "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
                    "@value": "42"
                }
            }
        }
        """
        if isinstance(v, int):
            collection = BNode()
            graph.add((root, ACTIVITYSTREAMS.followers, collection))
            graph.add((collection, RDF.type, ACTIVITYSTREAMS.Collection))
            graph.add(
                (
                    collection,
                    ACTIVITYSTREAMS.totalItems,
                    Literal(v, datatype=XSD.nonNegativeInteger),
                )
            )

    def normalize_license(self, d):
        """

        >>> GitHubMapping().normalize_license({'spdx_id': 'MIT'})
        rdflib.term.URIRef('https://spdx.org/licenses/MIT')
        """
        if isinstance(d, dict) and isinstance(d.get("spdx_id"), str):
            return SPDX + d["spdx_id"]
