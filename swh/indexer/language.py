# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from pygments.lexers import guess_lexer
from chardet import detect

from .indexer import BaseIndexer


def _cleanup_classname(classname):
    """Determine the language from the pygments' lexer names.

    """
    return classname.lower().replace(' ', '-')


def compute_language(raw_content):
    """Determine the raw content's language.

    Args:
        raw_content (bytes): content to determine raw content

    Returns:
        Dict with keys:
        - lang: None if nothing found or the possible language
        - decoding_failure: True if a decoding failure happened

    """
    try:
        stats = detect(raw_content)
        encoding = stats['encoding']
        content = raw_content.decode(encoding)
        lang = _cleanup_classname(
            guess_lexer(content).name)
        return {
            'lang': lang
        }
    except Exception:
        return {
            'lang': None
        }


class ContentLanguageIndexer(BaseIndexer):
    """Indexer in charge of:
    - filtering out content already indexed
    - reading content from objstorage per the content's id (sha1)
    - computing {mimetype, encoding} from that content
    - store result in storage

    """
    CONFIG_BASE_FILENAME = 'indexer/language'

    ADDITIONAL_CONFIG = {
        'tool': ('dict', {
            'name': 'pygments',
            'version': '2.0.1+dfsg-1.1+deb8u1',
        }),
    }

    def __init__(self):
        super().__init__()
        self.tool_name = self.config['tool']['name']
        self.tool_version = self.config['tool']['version']

    def filter_contents(self, sha1s):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.storage.content_language_missing((
            {
                'id': sha1,
                'tool_name': self.tool_name,
                'tool_version': self.tool_version
            } for sha1 in sha1s
        ))

    def index_content(self, sha1, raw_content):
        """Index sha1s' content and store result.

        Args:
            sha1 (bytes): content's identifier
            raw_content (bytes): raw content in bytes

        Returns:
            A dict, representing a content_mimetype, with keys:
              - id (bytes): content's identifier (sha1)
              - lang (bytes): detected language

        """
        result = compute_language(raw_content)
        result.update({
            'id': sha1,
            'tool_name': self.tool_name,
            'tool_version': self.tool_version,
        })

        return result

    def persist_index_computations(self, results, policy_update):
        """Persist the results in storage.

        Args:
            results ([dict]): list of content_mimetype, dict with the
            following keys:
              - id (bytes): content's identifier (sha1)
              - lang (bytes): detected language
            policy_update ([str]): either 'update-dups' or 'ignore-dups' to
            respectively update duplicates or ignore them

        """
        self.storage.content_language_add(
            results, conflict_update=(policy_update == 'update-dups'))
