# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import click

from pygments.lexers import guess_lexer
from pygments.util import ClassNotFound


def run_language(path):
    """Determine mime-type from file at path.

    Args:
        path: filepath to determine the mime type

    Returns:
        The possible language

    """
    with open(path, 'r') as f:
        try:
            return guess_lexer(f.read())
        except ClassNotFound as e:
            return None


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    r = run_language(path)
    print(r)


if __name__ == '__main__':
    main()
