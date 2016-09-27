# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import subprocess


def run_mimetype(path):
    """Determine mime-type from file at path.

    Args:
        path: filepath to determine the mime type

    Returns:
        The mime type.

    """
    cmd = ['file', '--mime-type', path]
    r = subprocess.check_output(cmd, universal_newlines=True)
    if r:
        r = r.split(':')[1].strip()
        return r


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    r = run_mimetype(path)
    print(r)


if __name__ == '__main__':
    main()
