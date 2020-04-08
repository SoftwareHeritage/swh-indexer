# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

from swh.indexer.codemeta import CODEMETA_TERMS
from swh.indexer.codemeta import expand
from .base import SingleFileMapping


class CodemetaMapping(SingleFileMapping):
    """
    dedicated class for CodeMeta (codemeta.json) mapping and translation
    """

    name = "codemeta"
    filename = b"codemeta.json"
    string_fields = None

    @classmethod
    def supported_terms(cls):
        return [term for term in CODEMETA_TERMS if not term.startswith("@")]

    def translate(self, content):
        try:
            return self.normalize_translation(expand(json.loads(content.decode())))
        except Exception:
            return None
