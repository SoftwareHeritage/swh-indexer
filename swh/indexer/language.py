# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from pygments.lexers import guess_lexer
from pygments.util import ClassNotFound
from chardet import detect


def cleanup_classname(classname):
    """Determine the language from the pygments' lexer names.

    """
    return classname.lower().replace(' ', '-')


def run_language(raw_content):
    """Determine the raw content's language.

    Args:
        raw_content (bytes): content to determine raw content

    Returns:
        Dict with keys:
        - lang: None if nothing found or the possible language
        - decoding_failure: True if a decoding failure happened

    """
    try:
        encoding = detect(raw_content)['encoding']
        content = raw_content.decode(encoding)
        lang = cleanup_classname(
            guess_lexer(content).name)

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
