# Copyright (C) 2017-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import json
import logging

from swh.indexer.codemeta import SCHEMA_URI
from swh.indexer.codemeta import compact


def merge_values(v1, v2):
    """If v1 and v2 are of the form `{"@list": l1}` and `{"@list": l2}`,
    returns `{"@list": l1 + l2}`.
    Otherwise, make them lists (if they are not already) and concatenate
    them.

    >>> merge_values('a', 'b')
    ['a', 'b']
    >>> merge_values(['a', 'b'], 'c')
    ['a', 'b', 'c']
    >>> merge_values({'@list': ['a', 'b']}, {'@list': ['c']})
    {'@list': ['a', 'b', 'c']}
    """
    if v1 is None:
        return v2
    elif v2 is None:
        return v1
    elif isinstance(v1, dict) and set(v1) == {'@list'}:
        assert isinstance(v1['@list'], list)
        if isinstance(v2, dict) and set(v2) == {'@list'}:
            assert isinstance(v2['@list'], list)
            return {'@list': v1['@list'] + v2['@list']}
        else:
            raise ValueError('Cannot merge %r and %r' % (v1, v2))
    else:
        if isinstance(v2, dict) and '@list' in v2:
            raise ValueError('Cannot merge %r and %r' % (v1, v2))
        if not isinstance(v1, list):
            v1 = [v1]
        if not isinstance(v2, list):
            v2 = [v2]
        return v1 + v2


class BaseMapping(metaclass=abc.ABCMeta):
    """Base class for mappings to inherit from

    To implement a new mapping:

    - inherit this class
    - override translate function
    """
    def __init__(self, log_suffix=''):
        self.log_suffix = log_suffix
        self.log = logging.getLogger('%s.%s' % (
            self.__class__.__module__,
            self.__class__.__name__))

    @property
    @abc.abstractmethod
    def name(self):
        """A name of this mapping, used as an identifier in the
        indexer storage."""
        pass

    @classmethod
    @abc.abstractmethod
    def detect_metadata_files(cls, files):
        """
        Detects files potentially containing metadata

        Args:
            file_entries (list): list of files

        Returns:
            list: list of sha1 (possibly empty)
        """
        pass

    @abc.abstractmethod
    def translate(self, file_content):
        pass

    def normalize_translation(self, metadata):
        return compact(metadata)


class SingleFileMapping(BaseMapping):
    """Base class for all mappings that use a single file as input."""

    @property
    @abc.abstractmethod
    def filename(self):
        """The .json file to extract metadata from."""
        pass

    @classmethod
    def detect_metadata_files(cls, file_entries):
        for entry in file_entries:
            if entry['name'] == cls.filename:
                return [entry['sha1']]
        return []


class DictMapping(BaseMapping):
    """Base class for mappings that take as input a file that is mostly
    a key-value store (eg. a shallow JSON dict)."""

    string_fields = []
    '''List of fields that are simple strings, and don't need any
    normalization.'''

    @property
    @abc.abstractmethod
    def mapping(self):
        """A translation dict to map dict keys into a canonical name."""
        pass

    @staticmethod
    def _normalize_method_name(name):
        return name.replace('-', '_')

    @classmethod
    def supported_terms(cls):
        return {
            term for (key, term) in cls.mapping.items()
            if key in cls.string_fields
            or hasattr(cls, 'translate_' + cls._normalize_method_name(key))
            or hasattr(cls, 'normalize_' + cls._normalize_method_name(key))}

    def _translate_dict(self, content_dict, *, normalize=True):
        """
        Translates content  by parsing content from a dict object
        and translating with the appropriate mapping

        Args:
            content_dict (dict): content dict to translate

        Returns:
            dict: translated metadata in json-friendly form needed for
            the indexer

        """
        translated_metadata = {'@type': SCHEMA_URI + 'SoftwareSourceCode'}
        for k, v in content_dict.items():
            # First, check if there is a specific translation
            # method for this key
            translation_method = getattr(
                self, 'translate_' + self._normalize_method_name(k), None)
            if translation_method:
                translation_method(translated_metadata, v)
            elif k in self.mapping:
                # if there is no method, but the key is known from the
                # crosswalk table
                codemeta_key = self.mapping[k]

                # if there is a normalization method, use it on the value
                normalization_method = getattr(
                    self, 'normalize_' + self._normalize_method_name(k), None)
                if normalization_method:
                    v = normalization_method(v)
                elif k in self.string_fields and isinstance(v, str):
                    pass
                elif k in self.string_fields and isinstance(v, list):
                    v = [x for x in v if isinstance(x, str)]
                else:
                    continue

                # set the translation metadata with the normalized value
                if codemeta_key in translated_metadata:
                    translated_metadata[codemeta_key] = merge_values(
                        translated_metadata[codemeta_key], v)
                else:
                    translated_metadata[codemeta_key] = v
        if normalize:
            return self.normalize_translation(translated_metadata)
        else:
            return translated_metadata


class JsonMapping(DictMapping, SingleFileMapping):
    """Base class for all mappings that use a JSON file as input."""

    def translate(self, raw_content):
        """
        Translates content by parsing content from a bytestring containing
        json data and translating with the appropriate mapping

        Args:
            raw_content (bytes): raw content to translate

        Returns:
            dict: translated metadata in json-friendly form needed for
            the indexer

        """
        try:
            raw_content = raw_content.decode()
        except UnicodeDecodeError:
            self.log.warning('Error unidecoding from %s', self.log_suffix)
            return
        try:
            content_dict = json.loads(raw_content)
        except json.JSONDecodeError:
            self.log.warning('Error unjsoning from %s', self.log_suffix)
            return
        if isinstance(content_dict, dict):
            return self._translate_dict(content_dict)
