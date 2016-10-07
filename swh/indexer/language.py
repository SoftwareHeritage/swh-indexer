# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
import os
import math
import sys

from pygments.lexers import guess_lexer
from pygments.util import ClassNotFound


def cleanup_classname(classname):
    """Determine the language from the pygments' lexer names.

    """
    return classname.lower().replace(' ', '-')


def run_language(path, encoding=None):
    """Determine mime-type from file at path.

    Args:
        path (str): filepath to determine the mime type
        encoding (str): optional file's encoding

    Returns:
        Dict with keys:
        - lang: None if nothing found or the possible language
        - decoding_failure: True if a decoding failure happened

    """
    try:
        with open(path, 'r', encoding=encoding) as f:
            try:
                raw_content = f.read()
                lang = cleanup_classname(
                    guess_lexer(raw_content).name)
                return {
                    'lang': lang
                }
            except ClassNotFound as e:
                return {
                    'lang': None
                }
    except LookupError as e:  # Unknown encoding
        return {
            'decoding_failure': True,
            'lang': None
        }
