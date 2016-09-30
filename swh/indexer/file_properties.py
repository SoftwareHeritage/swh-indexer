# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import subprocess


def run_file_properties(path):
    """Determine mimetype and encoding from file at path.

    Args:
        path: filepath to determine the mime type

    Returns:
        A dict with mimetype and encoding key and corresponding values.

    """
    cmd = ['file', '--mime-type', '--mime-encoding', path]
    properties = subprocess.check_output(cmd, universal_newlines=True)
    if properties:
        res = properties.split(': ')[1].strip().split('; ')
        mimetype = res[0]
        encoding = res[1].split('=')[1]
        return {
            'mimetype': mimetype,
            'encoding': encoding
        }


@click.command()
@click.option('--path', help="Path to execute index on")
def main(path):
    print(run_file_properties(path))


if __name__ == '__main__':
    main()
