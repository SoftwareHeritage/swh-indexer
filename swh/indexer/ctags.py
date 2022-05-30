# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import subprocess
from typing import Any, Dict, Iterator, List, Optional

from swh.core.config import merge_configs
from swh.indexer.storage import Sha1
from swh.indexer.storage.model import ContentCtagsRow
from swh.model import hashutil

from .indexer import ContentIndexer, write_to_temp

# Options used to compute tags
__FLAGS = [
    "--fields=+lnz",  # +l: language
    # +n: line number of tag definition
    # +z: include the symbol's kind (function, variable, ...)
    "--sort=no",  # sort output on tag name
    "--links=no",  # do not follow symlinks
    "--output-format=json",  # outputs in json
]


def compute_language(content, log=None):
    raise NotImplementedError(
        "Language detection was unreliable, so it is currently disabled. "
        "See https://forge.softwareheritage.org/D1455"
    )


def run_ctags(path, lang=None, ctags_command="ctags") -> Iterator[Dict[str, Any]]:
    """Run ctags on file path with optional language.

    Args:
        path: path to the file
        lang: language for that path (optional)

    Yields:
        dict: ctags' output

    """
    optional = []
    if lang:
        optional = ["--language-force=%s" % lang]

    cmd = [ctags_command] + __FLAGS + optional + [path]
    output = subprocess.check_output(cmd, universal_newlines=True)

    for symbol in output.split("\n"):
        if not symbol:
            continue
        js_symbol = json.loads(symbol)
        yield {
            "name": js_symbol["name"],
            "kind": js_symbol["kind"],
            "line": js_symbol["line"],
            "lang": js_symbol["language"],
        }


DEFAULT_CONFIG: Dict[str, Any] = {
    "workdir": "/tmp/swh/indexer.ctags",
    "tools": {
        "name": "universal-ctags",
        "version": "~git7859817b",
        "configuration": {
            "command_line": """ctags --fields=+lnz --sort=no --links=no """
            """--output-format=json <filepath>"""
        },
    },
    "languages": {},
}


class CtagsIndexer(ContentIndexer[ContentCtagsRow]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = merge_configs(DEFAULT_CONFIG, self.config)
        self.working_directory = self.config["workdir"]
        self.language_map = self.config["languages"]

    def filter(self, ids):
        """Filter out known sha1s and return only missing ones."""
        yield from self.idx_storage.content_ctags_missing(
            (
                {
                    "id": sha1,
                    "indexer_configuration_id": self.tool["id"],
                }
                for sha1 in ids
            )
        )

    def index(
        self, id: Sha1, data: Optional[bytes] = None, **kwargs
    ) -> List[ContentCtagsRow]:
        """Index sha1s' content and store result.

        Args:
            id (bytes): content's identifier
            data (bytes): raw content in bytes

        Returns:
            dict: a dict representing a content_mimetype with keys:

            - **id** (bytes): content's identifier (sha1)
            - **ctags** ([dict]): ctags list of symbols

        """
        assert isinstance(id, bytes)
        assert data is not None

        lang = compute_language(data, log=self.log)["lang"]

        if not lang:
            return []

        ctags_lang = self.language_map.get(lang)

        if not ctags_lang:
            return []

        ctags = []

        filename = hashutil.hash_to_hex(id)
        with write_to_temp(
            filename=filename, data=data, working_directory=self.working_directory
        ) as content_path:
            for ctag_kwargs in run_ctags(content_path, lang=ctags_lang):
                ctags.append(
                    ContentCtagsRow(
                        id=id,
                        indexer_configuration_id=self.tool["id"],
                        **ctag_kwargs,
                    )
                )

        return ctags

    def persist_index_computations(
        self, results: List[ContentCtagsRow]
    ) -> Dict[str, int]:
        """Persist the results in storage.

        Args:
            results: list of ctags returned by index()

        """
        return self.idx_storage.content_ctags_add(results)
