# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
from typing import Any, Callable, Iterable, Optional, Sequence, TypeVar
import urllib.parse

from pyld import jsonld
from rdflib import RDF, Graph, URIRef
import rdflib.term

from swh.indexer.codemeta import _document_loader


def prettyprint_graph(graph: Graph, root: URIRef):
    s = graph.serialize(format="application/ld+json")
    jsonld_graph = json.loads(s)
    translated_metadata = jsonld.frame(
        jsonld_graph,
        {"@id": str(root)},
        options={
            "documentLoader": _document_loader,
            "processingMode": "json-ld-1.1",
        },
    )
    print(json.dumps(translated_metadata, indent=4))


def add_list(
    graph: Graph,
    subject: rdflib.term.Node,
    predicate: rdflib.term.Identifier,
    objects: Sequence[rdflib.term.Node],
) -> None:
    """Adds triples to the ``graph`` so that they are equivalent to this
    JSON-LD object::

        {
            "@id": subject,
            predicate: {"@list": objects}
        }

    This is a naive implementation of
    https://json-ld.org/spec/latest/json-ld-api/#list-to-rdf-conversion
    """
    # JSON-LD's @list is syntactic sugar for a linked list / chain in the RDF graph,
    # which is what we are going to construct, starting from the end:
    last_link: rdflib.term.Node
    last_link = RDF.nil
    for item in reversed(objects):
        link = rdflib.BNode()
        graph.add((link, RDF.first, item))
        graph.add((link, RDF.rest, last_link))
        last_link = link
    graph.add((subject, predicate, last_link))


TValue = TypeVar("TValue")


def add_map(
    graph: Graph,
    subject: rdflib.term.Node,
    predicate: rdflib.term.Identifier,
    f: Callable[[Graph, TValue], Optional[rdflib.term.Node]],
    values: Iterable[TValue],
) -> None:
    """Helper for :func:`add_list` that takes a mapper function ``f``."""
    nodes = [f(graph, value) for value in values]
    add_list(graph, subject, predicate, [node for node in nodes if node])


def add_url_if_valid(
    graph: Graph,
    subject: rdflib.term.Node,
    predicate: rdflib.term.Identifier,
    url: Any,
) -> None:
    """Adds ``(subject, predicate, url)`` to the graph if ``url`` is well-formed.

    This is meant as a workaround for https://github.com/digitalbazaar/pyld/issues/91
    to drop URLs that are blatantly invalid early, so PyLD does not crash.

    >>> from pprint import pprint
    >>> graph = Graph()
    >>> subject = rdflib.term.URIRef("http://example.org/test-software")
    >>> predicate = rdflib.term.URIRef("http://schema.org/license")
    >>> add_url_if_valid(
    ...     graph, subject, predicate, "https//www.apache.org/licenses/LICENSE-2.0.txt"
    ... )
    >>> add_url_if_valid(
    ...     graph, subject, predicate, "http:s//www.apache.org/licenses/LICENSE-2.0.txt"
    ... )
    >>> add_url_if_valid(
    ...     graph, subject, predicate, "https://www.apache.org/licenses/LICENSE-2.0.txt"
    ... )
    >>> add_url_if_valid(
    ...     graph, subject, predicate, 42
    ... )
    >>> pprint(set(graph.triples((subject, predicate, None))))
    {(rdflib.term.URIRef('http://example.org/test-software'),
      rdflib.term.URIRef('http://schema.org/license'),
      rdflib.term.URIRef('https://www.apache.org/licenses/LICENSE-2.0.txt'))}
    """
    if not isinstance(url, str):
        return
    try:
        parsed_url = urllib.parse.urlparse(url)
    except Exception:
        return
    if " " in url or not parsed_url.netloc:
        return
    graph.add((subject, predicate, rdflib.term.URIRef(url)))
