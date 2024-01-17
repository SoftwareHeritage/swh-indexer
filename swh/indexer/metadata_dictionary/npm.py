# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re

from rdflib import RDF, BNode, Graph, Literal, URIRef

from swh.indexer.codemeta import CROSSWALK_TABLE
from swh.indexer.namespaces import SCHEMA

from .base import JsonMapping, SingleFileIntrinsicMapping
from .utils import add_list, add_url_if_valid, prettyprint_graph  # noqa

SPDX = URIRef("https://spdx.org/licenses/")


class NpmMapping(JsonMapping, SingleFileIntrinsicMapping):
    """
    dedicated class for NPM (package.json) mapping and translation
    """

    name = "npm"
    mapping = CROSSWALK_TABLE["NodeJS"]
    filename = b"package.json"
    string_fields = ["name", "version", "description", "email"]
    uri_fields = ["homepage"]

    _schema_shortcuts = {
        "github": "git+https://github.com/%s.git",
        "gist": "git+https://gist.github.com/%s.git",
        "gitlab": "git+https://gitlab.com/%s.git",
        # Bitbucket supports both hg and git, and the shortcut does not
        # tell which one to use.
        # 'bitbucket': 'https://bitbucket.org/',
    }

    def normalize_repository(self, d):
        """https://docs.npmjs.com/files/package.json#repository

        >>> NpmMapping().normalize_repository({
        ...     'type': 'git',
        ...     'url': 'https://example.org/foo.git'
        ... })
        rdflib.term.URIRef('git+https://example.org/foo.git')
        >>> NpmMapping().normalize_repository(
        ...     'gitlab:foo/bar')
        rdflib.term.URIRef('git+https://gitlab.com/foo/bar.git')
        >>> NpmMapping().normalize_repository(
        ...     'foo/bar')
        rdflib.term.URIRef('git+https://github.com/foo/bar.git')
        """
        if (
            isinstance(d, dict)
            and isinstance(d.get("type"), str)
            and isinstance(d.get("url"), str)
        ):
            url = "{type}+{url}".format(**d)
        elif isinstance(d, str):
            if "://" in d:
                url = d
            elif ":" in d:
                (schema, rest) = d.split(":", 1)
                if schema in self._schema_shortcuts:
                    url = self._schema_shortcuts[schema] % rest
                else:
                    return None
            else:
                url = self._schema_shortcuts["github"] % d

        else:
            return None

        return URIRef(url)

    def normalize_bugs(self, d):
        """https://docs.npmjs.com/files/package.json#bugs

        >>> NpmMapping().normalize_bugs({
        ...     'url': 'https://example.org/bugs/',
        ...     'email': 'bugs@example.org'
        ... })
        rdflib.term.URIRef('https://example.org/bugs/')
        >>> NpmMapping().normalize_bugs(
        ...     'https://example.org/bugs/')
        rdflib.term.URIRef('https://example.org/bugs/')
        """
        if isinstance(d, dict) and isinstance(d.get("url"), str):
            url = d["url"]
        elif isinstance(d, str):
            url = d
        else:
            url = ""

        return URIRef(url)

    _parse_author = re.compile(
        r"^ *" r"(?P<name>.*?)" r"( +<(?P<email>.*)>)?" r"( +\((?P<url>.*)\))?" r" *$"
    )

    def translate_author(self, graph: Graph, root, d):
        r"""https://docs.npmjs.com/files/package.json#people-fields-author-contributors'

        >>> from pprint import pprint
        >>> root = URIRef("http://example.org/test-software")
        >>> graph = Graph()
        >>> NpmMapping().translate_author(graph, root, {
        ...     'name': 'John Doe',
        ...     'email': 'john.doe@example.org',
        ...     'url': 'https://example.org/~john.doe',
        ... })
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "http://schema.org/author": {
                "@list": [
                    {
                        "@type": "http://schema.org/Person",
                        "http://schema.org/email": "john.doe@example.org",
                        "http://schema.org/name": "John Doe",
                        "http://schema.org/url": {
                            "@id": "https://example.org/~john.doe"
                        }
                    }
                ]
            }
        }
        >>> graph = Graph()
        >>> NpmMapping().translate_author(graph, root,
        ...     'John Doe <john.doe@example.org> (https://example.org/~john.doe)'
        ... )
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "http://schema.org/author": {
                "@list": [
                    {
                        "@type": "http://schema.org/Person",
                        "http://schema.org/email": "john.doe@example.org",
                        "http://schema.org/name": "John Doe",
                        "http://schema.org/url": {
                            "@id": "https://example.org/~john.doe"
                        }
                    }
                ]
            }
        }
        >>> graph = Graph()
        >>> NpmMapping().translate_author(graph, root, {
        ...     'name': 'John Doe',
        ...     'email': 'john.doe@example.org',
        ...     'url': 'https:\\\\example.invalid/~john.doe',
        ... })
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "http://schema.org/author": {
                "@list": [
                    {
                        "@type": "http://schema.org/Person",
                        "http://schema.org/email": "john.doe@example.org",
                        "http://schema.org/name": "John Doe"
                    }
                ]
            }
        }
        """  # noqa
        author = BNode()
        graph.add((author, RDF.type, SCHEMA.Person))
        if isinstance(d, dict):
            name = d.get("name", None)
            email = d.get("email", None)
            url = d.get("url", None)
        elif isinstance(d, str):
            match = self._parse_author.match(d)
            if not match:
                return None
            name = match.group("name")
            email = match.group("email")
            url = match.group("url")
        else:
            return None

        if name and isinstance(name, str):
            graph.add((author, SCHEMA.name, Literal(name)))
        if email and isinstance(email, str):
            graph.add((author, SCHEMA.email, Literal(email)))
        add_url_if_valid(graph, author, SCHEMA.url, url)

        add_list(graph, root, SCHEMA.author, [author])

    def normalize_description(self, description):
        r"""Try to re-decode ``description`` as UTF-16, as this is a somewhat common
        mistake that causes issues in the database because of null bytes in JSON.

        >>> NpmMapping().normalize_description("foo bar")
        rdflib.term.Literal('foo bar')
        >>> NpmMapping().normalize_description(
        ...     "\ufffd\ufffd#\x00 \x00f\x00o\x00o\x00 \x00b\x00a\x00r\x00\r\x00 \x00"
        ... )
        rdflib.term.Literal('foo bar')
        >>> NpmMapping().normalize_description(
        ...     "\ufffd\ufffd\x00#\x00 \x00f\x00o\x00o\x00 \x00b\x00a\x00r\x00\r\x00 "
        ... )
        rdflib.term.Literal('foo bar')
        >>> NpmMapping().normalize_description(
        ...     # invalid UTF-16 and meaningless UTF-8:
        ...     "\ufffd\ufffd\x00#\x00\x00\x00 \x00\x00\x00\x00f\x00\x00\x00\x00"
        ... ) is None
        True
        >>> NpmMapping().normalize_description(
        ...     # ditto (ut looks like little-endian at first)
        ...     "\ufffd\ufffd#\x00\x00\x00 \x00\x00\x00\x00f\x00\x00\x00\x00\x00"
        ... ) is None
        True
        >>> NpmMapping().normalize_description(None) is None
        True
        """
        if not isinstance(description, str):
            return None
        # XXX: if this function ever need to support more cases, consider
        # switching to https://pypi.org/project/ftfy/ instead of adding more hacks
        if description.startswith("\ufffd\ufffd") and "\x00" in description:
            # 2 unicode replacement characters followed by '# ' encoded as UTF-16
            # is a common mistake, which indicates a README.md was saved as UTF-16,
            # and some NPM tool opened it as UTF-8 and used the first line as
            # description.

            description_bytes = description.encode()

            # Strip the the two unicode replacement characters
            assert description_bytes.startswith(b"\xef\xbf\xbd\xef\xbf\xbd")
            description_bytes = description_bytes[6:]

            # If the following attempts fail to recover the description, discard it
            # entirely because the current indexer storage backend (postgresql) cannot
            # store zero bytes in JSON columns.
            description = None

            if not description_bytes.startswith(b"\x00"):
                # try UTF-16 little-endian (the most common) first
                try:
                    description = description_bytes.decode("utf-16le")
                except UnicodeDecodeError:
                    pass
            if description is None:
                # if it fails, try UTF-16 big-endian
                try:
                    description = description_bytes.decode("utf-16be")
                except UnicodeDecodeError:
                    pass

            if description:
                if description.startswith("# "):
                    description = description[2:]
                return Literal(description.rstrip())
            else:
                return None
        return Literal(description)

    def normalize_license(self, s):
        """https://docs.npmjs.com/files/package.json#license

        >>> NpmMapping().normalize_license('MIT')
        rdflib.term.URIRef('https://spdx.org/licenses/MIT')
        """
        if isinstance(s, str):
            if s.startswith("SEE LICENSE IN "):
                # Very common pattern, because it is an example in the specification.
                # It is followed by the filename; and the indexer architecture currently
                # does not allow accessing that from metadata mappings.
                # (Plus, an hypothetical license mapping would eventually pick it up)
                return

            # Remove parentheses from the string
            s = s.replace("(", "").replace(")", "")

            if " " in s:
                # Either an SPDX expression, or unusable data
                # Check for SPDX expression first
                # Extract the SPDX expression if it contains OR,
                # ignore licenses with AND or WITH operator.
                if " OR " in s and " AND " not in s and " WITH " not in s:
                    # Multiple licenses, or a license exception
                    # return multiple licenses in a list
                    return [self.normalize_license(x) for x in s.split(" OR ")]

                return
            return SPDX + s

    def normalize_keywords(self, lst):
        """https://docs.npmjs.com/files/package.json#homepage

        >>> NpmMapping().normalize_keywords(['foo', 'bar'])
        [rdflib.term.Literal('foo'), rdflib.term.Literal('bar')]
        """
        if isinstance(lst, list):
            return [Literal(x) for x in lst if isinstance(x, str)]
