# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import subprocess
import json

from swh.core import hashutil

from .language import compute_language
from .indexer import BaseIndexer, DiskIndexer


# Options used to compute tags
__FLAGS = [
    '--fields=+lnz',  # +l: language
                      # +n: line number of tag definition
                      # +z: include the symbol's kind (function, variable, ...)
    '--sort=no',      # sort output on tag name
    '--links=no',     # do not follow symlinks
    '--output-format=json',  # outputs in json
]


def run_ctags(path, lang=None, ctags_binary='ctags'):
    """Run ctags on file path with optional language.

    Args:
        path: path to the file
        lang: language for that path (optional)

    Returns:
        ctags' output

    """
    optional = []
    if lang:
        optional = ['--language-force=%s' % lang]

    cmd = [ctags_binary] + __FLAGS + optional + [path]
    output = subprocess.check_output(cmd, universal_newlines=True)

    for symbol in output.split('\n'):
        if not symbol:
            continue
        js_symbol = json.loads(symbol)
        yield {
            'name': js_symbol['name'],
            'kind': js_symbol['kind'],
            'line': js_symbol['line'],
            'lang': js_symbol['language'],
        }


class CtagsIndexer(BaseIndexer, DiskIndexer):
    CONFIG_BASE_FILENAME = 'indexer/ctags'

    ADDITIONAL_CONFIG = {
        'ctags': ('str', '/usr/bin/ctags'),
        'workdir': ('str', '/tmp/swh/indexer.ctags'),
        'languages': ('dict', {
            'ada': 'Ada',
            'adl': None,
            'agda': None,
            # ...
        })
    }

    def __init__(self):
        super().__init__()
        self.working_directory = self.config['workdir']
        self.language_map = self.config['languages']
        self.ctags_binary = self.config['ctags']

    def filter_contents(self, sha1s):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.storage.content_ctags_missing(sha1s)

    def index_content(self, sha1, raw_content):
        """Index sha1s' content and store result.

        Args:
            sha1 (bytes): content's identifier
            raw_content (bytes): raw content in bytes

        Returns:
            A dict, representing a content_mimetype, with keys:
              - id (bytes): content's identifier (sha1)
              - ctags ([dict]): ctags list of symbols

        """
        lang = compute_language(raw_content)['lang']

        if not lang:
            return None

        ctags_lang = self.language_map.get(lang)

        if not ctags_lang:
            return None

        ctags = {
            'id': sha1,
        }

        filename = hashutil.hash_to_hex(sha1)
        content_path = self.write_to_temp(
            filename=filename,
            data=raw_content)

        result = run_ctags(content_path,
                           lang=ctags_lang,
                           ctags_binary=self.ctags_binary)
        ctags.update({
            'ctags': list(result),
        })

        self.cleanup(content_path)

        return ctags

    def persist_index_computations(self, results, policy_update):
        """Persist the results in storage.

        Args:
            results ([dict]): list of content_mimetype, dict with the
            following keys:
              - id (bytes): content's identifier (sha1)
              - ctags ([dict]): ctags list of symbols
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
            respectively update duplicates or ignore them

        """
        self.storage.content_ctags_add(
            results, conflict_update=(policy_update == 'update-dups'))


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    r = list(run_ctags(path))
    print(r)


if __name__ == '__main__':
    main()
