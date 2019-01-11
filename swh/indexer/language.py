# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import io

from pygments.lexers import guess_lexer
from pygments.util import ClassNotFound
from chardet.universaldetector import UniversalDetector

from .indexer import ContentIndexer


def _cleanup_classname(classname):
    """Determine the language from the pygments' lexer names.

    """
    return classname.lower().replace(' ', '-')


def _read_raw(raw_content, size=2048):
    """Read raw content in chunk.

    """
    bs = io.BytesIO(raw_content)
    while True:
        chunk = bs.read(size)
        if not chunk:
            break
        yield chunk


def _detect_encoding(raw_content):
    """Given a raw content, try and detect its encoding.

    """
    detector = UniversalDetector()
    for chunk in _read_raw(raw_content):
        detector.feed(chunk)
        if detector.done:
            break
    detector.close()
    return detector.result['encoding']


def compute_language_from_chunk(encoding, length, raw_content, max_size,
                                log=None):
    """Determine the raw content's language.

    Args:
        encoding (str): Encoding to use to decode the content
        length (int): raw_content's length
        raw_content (bytes): raw content to work with
        max_size (int): max size to split the raw content at

    Returns:
        dict: Dict with keys:
        - **lang**: None if nothing found or the possible language

    """
    try:
        if max_size <= length:
            raw_content = raw_content[0:max_size]

        content = raw_content.decode(encoding)
        lang = _cleanup_classname(
            guess_lexer(content).name)
    except ClassNotFound:
        lang = None
    except UnicodeDecodeError:
        raise
    except Exception:
        if log:
            log.exception('Problem during language detection, skipping')
        lang = None
    return {
        'lang': lang
    }


def compute_language(raw_content, encoding=None, log=None):
    """Determine the raw content's language.

    Args:
        raw_content (bytes): raw content to work with

    Returns:
        dict: Dict with keys:
        - **lang**: None if nothing found or the possible language

    """
    try:
        encoding = _detect_encoding(raw_content)
        content = raw_content.decode(encoding)
        lang = _cleanup_classname(
            guess_lexer(content).name)
    except ClassNotFound:
        lang = None
    except Exception:
        if log:
            log.exception('Problem during language detection, skipping')
        lang = None
    return {
        'lang': lang
    }


class LanguageIndexer(ContentIndexer):
    """Indexer in charge of:

    - filtering out content already indexed
    - reading content from objstorage per the content's id (sha1)
    - computing {mimetype, encoding} from that content
    - store result in storage

    """
    CONFIG_BASE_FILENAME = 'indexer/language'

    ADDITIONAL_CONFIG = {
        'tools': ('dict', {
            'name': 'pygments',
            'version': '2.0.1+dfsg-1.1+deb8u1',
            'configuration': {
                'type': 'library',
                'debian-package': 'python3-pygments',
                'max_content_size': 10240,
            },
        }),
    }

    def prepare(self):
        super().prepare()
        c = self.config
        self.max_content_size = c['tools']['configuration']['max_content_size']
        self.tool = self.tools[0]

    def filter(self, ids):
        """Filter out known sha1s and return only missing ones.

        """
        yield from self.idx_storage.content_language_missing((
            {
                'id': sha1,
                'indexer_configuration_id': self.tool['id']
            } for sha1 in ids
        ))

    def index(self, id, data):
        """Index sha1s' content and store result.

        Args:
            id (bytes): content's identifier
            data (bytes): raw content in bytes

        Returns:
            dict: Dict that represents a content_mimetype, with keys:
            - id (bytes): content's identifier (sha1)
            - lang (bytes): detected language

        """
        result = {
            'id': id,
            'indexer_configuration_id': self.tool['id'],
            'lang': None,
        }

        encoding = _detect_encoding(data)

        if not encoding:
            return result

        _len = len(data)
        for i in range(0, 9):
            max_size = self.max_content_size + i

            try:
                result = compute_language_from_chunk(
                    encoding, _len, data, max_size, log=self.log)
            except UnicodeDecodeError:
                self.log.warning(
                    'Decoding failed on wrong byte chunk at [0-%s]'
                    ', trying again at next ending byte.' % max_size)
                continue

            # we found something, so we return it
            result.update({
                'id': id,
                'indexer_configuration_id': self.tool['id'],
            })
            break

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
        self.idx_storage.content_language_add(
            results, conflict_update=(policy_update == 'update-dups'))
