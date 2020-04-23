# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import subprocess
import json

from typing import Dict, List

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


def run_ctags(path, lang=None, ctags_command="ctags"):
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


class CtagsIndexer(ContentIndexer):
    CONFIG_BASE_FILENAME = "indexer/ctags"

    ADDITIONAL_CONFIG = {
        "workdir": ("str", "/tmp/swh/indexer.ctags"),
        "tools": (
            "dict",
            {
                "name": "universal-ctags",
                "version": "~git7859817b",
                "configuration": {
                    "command_line": """ctags --fields=+lnz --sort=no --links=no """
                    """--output-format=json <filepath>"""
                },
            },
        ),
        "languages": (
            "dict",
            {
                "ada": "Ada",
                "adl": None,
                "agda": None,
                # ...
            },
        ),
    }

    def prepare(self):
        super().prepare()
        self.working_directory = self.config["workdir"]
        self.language_map = self.config["languages"]

    def filter(self, ids):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.idx_storage.content_ctags_missing(
            ({"id": sha1, "indexer_configuration_id": self.tool["id"],} for sha1 in ids)
        )

    def index(self, id, data):
        """Index sha1s' content and store result.

        Args:
            id (bytes): content's identifier
            data (bytes): raw content in bytes

        Returns:
            dict: a dict representing a content_mimetype with keys:

            - **id** (bytes): content's identifier (sha1)
            - **ctags** ([dict]): ctags list of symbols

        """
        lang = compute_language(data, log=self.log)["lang"]

        if not lang:
            return None

        ctags_lang = self.language_map.get(lang)

        if not ctags_lang:
            return None

        ctags = {
            "id": id,
        }

        filename = hashutil.hash_to_hex(id)
        with write_to_temp(
            filename=filename, data=data, working_directory=self.working_directory
        ) as content_path:
            result = run_ctags(content_path, lang=ctags_lang)
            ctags.update(
                {"ctags": list(result), "indexer_configuration_id": self.tool["id"],}
            )

        return ctags

    def persist_index_computations(
        self, results: List[Dict], policy_update: str
    ) -> Dict[str, int]:
        """Persist the results in storage.

        Args:
            results: list of content_mimetype, dict with the
              following keys:
              - id (bytes): content's identifier (sha1)
              - ctags ([dict]): ctags list of symbols
            policy_update: either 'update-dups' or 'ignore-dups' to
              respectively update duplicates or ignore them

        """
        return self.idx_storage.content_ctags_add(
            results, conflict_update=(policy_update == "update-dups")
        )
