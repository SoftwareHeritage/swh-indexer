# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import email.parser
import email.policy

from rdflib import BNode, Literal, URIRef

from swh.indexer.codemeta import CROSSWALK_TABLE
from swh.indexer.namespaces import RDF, SCHEMA

from .base import DictMapping, SingleFileIntrinsicMapping
from .utils import add_list

_normalize_pkginfo_key = str.lower


class LinebreakPreservingEmailPolicy(email.policy.EmailPolicy):
    def header_fetch_parse(self, name, value):
        if hasattr(value, "name"):
            return value
        value = value.replace("\n        ", "\n")
        return self.header_factory(name, value)


class PythonPkginfoMapping(DictMapping, SingleFileIntrinsicMapping):
    """Dedicated class for Python's PKG-INFO mapping and translation.

    https://www.python.org/dev/peps/pep-0314/"""

    name = "pkg-info"
    filename = b"PKG-INFO"
    mapping = {
        _normalize_pkginfo_key(k): v
        for (k, v) in CROSSWALK_TABLE["Python PKG-INFO"].items()
    }
    string_fields = [
        "name",
        "version",
        "description",
        "summary",
        "author",
        "author-email",
    ]

    _parser = email.parser.BytesHeaderParser(policy=LinebreakPreservingEmailPolicy())

    def translate(self, content):
        msg = self._parser.parsebytes(content)
        d = {}
        for key, value in msg.items():
            key = _normalize_pkginfo_key(key)
            if value != "UNKNOWN":
                d.setdefault(key, []).append(value)
        return self._translate_dict(d)

    def extra_translation(self, graph, root, d):
        author_names = list(graph.triples((root, SCHEMA.author, None)))
        author_emails = list(graph.triples((root, SCHEMA.email, None)))
        graph.remove((root, SCHEMA.author, None))
        graph.remove((root, SCHEMA.email, None))
        if author_names or author_emails:
            author = BNode()
            graph.add((author, RDF.type, SCHEMA.Person))
            for _, _, author_name in author_names:
                graph.add((author, SCHEMA.name, author_name))
            for _, _, author_email in author_emails:
                graph.add((author, SCHEMA.email, author_email))
            add_list(graph, root, SCHEMA.author, [author])

    def normalize_home_page(self, urls):
        return [URIRef(url) for url in urls]

    def normalize_keywords(self, keywords):
        return [Literal(keyword) for s in keywords for keyword in s.split(" ")]

    def normalize_license(self, licenses):
        return [URIRef("https://spdx.org/licenses/" + license) for license in licenses]
