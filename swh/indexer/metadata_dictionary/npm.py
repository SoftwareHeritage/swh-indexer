# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re

from swh.indexer.codemeta import CROSSWALK_TABLE, SCHEMA_URI
from .base import JsonMapping


class NpmMapping(JsonMapping):
    """
    dedicated class for NPM (package.json) mapping and translation
    """

    name = "npm"
    mapping = CROSSWALK_TABLE["NodeJS"]
    filename = b"package.json"
    string_fields = ["name", "version", "homepage", "description", "email"]

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
        {'@id': 'git+https://example.org/foo.git'}
        >>> NpmMapping().normalize_repository(
        ...     'gitlab:foo/bar')
        {'@id': 'git+https://gitlab.com/foo/bar.git'}
        >>> NpmMapping().normalize_repository(
        ...     'foo/bar')
        {'@id': 'git+https://github.com/foo/bar.git'}
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

        return {"@id": url}

    def normalize_bugs(self, d):
        """https://docs.npmjs.com/files/package.json#bugs

        >>> NpmMapping().normalize_bugs({
        ...     'url': 'https://example.org/bugs/',
        ...     'email': 'bugs@example.org'
        ... })
        {'@id': 'https://example.org/bugs/'}
        >>> NpmMapping().normalize_bugs(
        ...     'https://example.org/bugs/')
        {'@id': 'https://example.org/bugs/'}
        """
        if isinstance(d, dict) and isinstance(d.get("url"), str):
            return {"@id": d["url"]}
        elif isinstance(d, str):
            return {"@id": d}
        else:
            return None

    _parse_author = re.compile(
        r"^ *" r"(?P<name>.*?)" r"( +<(?P<email>.*)>)?" r"( +\((?P<url>.*)\))?" r" *$"
    )

    def normalize_author(self, d):
        """https://docs.npmjs.com/files/package.json#people-fields-author-contributors'

        >>> from pprint import pprint
        >>> pprint(NpmMapping().normalize_author({
        ...     'name': 'John Doe',
        ...     'email': 'john.doe@example.org',
        ...     'url': 'https://example.org/~john.doe',
        ... }))
        {'@list': [{'@type': 'http://schema.org/Person',
                    'http://schema.org/email': 'john.doe@example.org',
                    'http://schema.org/name': 'John Doe',
                    'http://schema.org/url': {'@id': 'https://example.org/~john.doe'}}]}
        >>> pprint(NpmMapping().normalize_author(
        ...     'John Doe <john.doe@example.org> (https://example.org/~john.doe)'
        ... ))
        {'@list': [{'@type': 'http://schema.org/Person',
                    'http://schema.org/email': 'john.doe@example.org',
                    'http://schema.org/name': 'John Doe',
                    'http://schema.org/url': {'@id': 'https://example.org/~john.doe'}}]}
        """  # noqa
        author = {"@type": SCHEMA_URI + "Person"}
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
            author[SCHEMA_URI + "name"] = name
        if email and isinstance(email, str):
            author[SCHEMA_URI + "email"] = email
        if url and isinstance(url, str):
            author[SCHEMA_URI + "url"] = {"@id": url}
        return {"@list": [author]}

    def normalize_license(self, s):
        """https://docs.npmjs.com/files/package.json#license

        >>> NpmMapping().normalize_license('MIT')
        {'@id': 'https://spdx.org/licenses/MIT'}
        """
        if isinstance(s, str):
            return {"@id": "https://spdx.org/licenses/" + s}

    def normalize_homepage(self, s):
        """https://docs.npmjs.com/files/package.json#homepage

        >>> NpmMapping().normalize_homepage('https://example.org/~john.doe')
        {'@id': 'https://example.org/~john.doe'}
        """
        if isinstance(s, str):
            return {"@id": s}

    def normalize_keywords(self, lst):
        """https://docs.npmjs.com/files/package.json#homepage

        >>> NpmMapping().normalize_keywords(['foo', 'bar'])
        ['foo', 'bar']
        """
        if isinstance(lst, list):
            return [x for x in lst if isinstance(x, str)]
