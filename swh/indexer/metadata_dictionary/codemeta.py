# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
from typing import Any, Dict, List, Optional

from swh.indexer.codemeta import CODEMETA_TERMS, expand

from .base import SingleFileIntrinsicMapping


class CodemetaMapping(SingleFileIntrinsicMapping):
    """
    dedicated class for CodeMeta (codemeta.json) mapping and translation
    """

    name = "codemeta"
    filename = b"codemeta.json"
    string_fields = None

    @classmethod
    def supported_terms(cls) -> List[str]:
        return [term for term in CODEMETA_TERMS if not term.startswith("@")]

    def translate(self, content: bytes) -> Optional[Dict[str, Any]]:
        try:
            return self.normalize_translation(expand(json.loads(content.decode())))
        except Exception:
            return None
