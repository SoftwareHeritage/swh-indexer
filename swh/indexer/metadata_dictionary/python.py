# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import email.parser
import email.policy
import itertools

from swh.indexer.codemeta import CROSSWALK_TABLE
from swh.indexer.namespaces import SCHEMA

from .base import DictMapping, SingleFileIntrinsicMapping

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
        for (key, value) in msg.items():
            key = _normalize_pkginfo_key(key)
            if value != "UNKNOWN":
                d.setdefault(key, []).append(value)
        return self._translate_dict(d)

    def extra_translation(self, translated_metadata, d):
        author_name = translated_metadata.pop(SCHEMA.author, None)
        author_email = translated_metadata.pop(SCHEMA.email, None)
        if author_name or author_email:
            translated_metadata[SCHEMA.author] = {
                "@list": [
                    {
                        "@type": SCHEMA.Person,
                        SCHEMA.name: author_name,
                        SCHEMA.email: author_email,
                    }
                ]
            }

    def normalize_home_page(self, urls):
        return [{"@id": url} for url in urls]

    def normalize_keywords(self, keywords):
        return list(itertools.chain.from_iterable(s.split(" ") for s in keywords))

    def normalize_license(self, licenses):
        return [{"@id": license} for license in licenses]
