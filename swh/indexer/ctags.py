# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import subprocess
import json


# Options used to compute tags
__FLAGS = [
    '--fields=+lnz',  # +l: language of source file containing tag
                      # +n: line number of tag definition
                      # +z: include the symbol's kind (function, variable, ...)
    '--sort=no',      # sort output on tag name
    '--links=no',     # do not follow symlinks
    '--output-format=json',  # outputs in json
]


def run_ctags(path, lang=None):
    """Run ctags on file path with optional language.

    Args:
        path: path to the file
        lang: language for that path (optional)

    Returns:
        ctags' output

    """
    optional = []
    # if lang:
    #     optional = ['--language-force', lang]

    cmd = ['ctags'] + __FLAGS + optional + [path]
    output = subprocess.check_output(cmd, universal_newlines=True)

    for symbol in output.split('\n'):
        if not symbol:
            continue
        js_symbol = json.loads(symbol)
        yield {
            k: v for k, v in js_symbol.items() if k != '_type' and k != 'path'
        }


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    r = list(run_ctags(path))
    print(r)


if __name__ == '__main__':
    main()
