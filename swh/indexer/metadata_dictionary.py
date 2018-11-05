# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import json
import logging

from swh.indexer.codemeta import CROSSWALK_TABLE, compact


MAPPINGS = {}


def register_mapping(cls):
    MAPPINGS[cls.__name__] = cls()
    return cls


class BaseMapping(metaclass=abc.ABCMeta):
    """Base class for mappings to inherit from

    To implement a new mapping:

    - inherit this class
    - override translate function
    """
    def __init__(self):
        self.log = logging.getLogger('%s.%s' % (
            self.__class__.__module__,
            self.__class__.__name__))

    @abc.abstractmethod
    def detect_metadata_files(self, files):
        """
        Detects files potentially containing metadata
        Args:
            - file_entries (list): list of files

        Returns:
            - empty list if nothing was found
            - list of sha1 otherwise
        """
        pass

    @abc.abstractmethod
    def translate(self, file_content):
        pass

    def normalize_translation(self, metadata):
        return compact(metadata)


class DictMapping(BaseMapping):
    """Base class for mappings that take as input a file that is mostly
    a key-value store (eg. a shallow JSON dict)."""

    @property
    @abc.abstractmethod
    def mapping(self):
        """A translation dict to map dict keys into a canonical name."""
        pass

    def translate_dict(self, content_dict):
        """
        Translates content  by parsing content from a dict object
        and translating with the appropriate mapping

        Args:
            content_dict (dict)

        Returns:
            dict: translated metadata in json-friendly form needed for
                  the indexer

        """
        translated_metadata = {}
        for k, v in content_dict.items():
            # First, check if there is a specific translation
            # method for this key
            translation_method = getattr(self, 'translate_' + k, None)
            if translation_method:
                translation_method(translated_metadata, v)
            elif k in self.mapping:
                # if there is no method, but the key is known from the
                # crosswalk table

                # if there is a normalization method, use it on the value
                normalization_method = getattr(self, 'normalize_' + k, None)
                if normalization_method:
                    v = normalization_method(v)

                # set the translation metadata with the normalized value
                translated_metadata[self.mapping[k]] = v
        return self.normalize_translation(translated_metadata)


class JsonMapping(DictMapping):
    """Base class for all mappings that use a JSON file as input."""

    @property
    @abc.abstractmethod
    def filename(self):
        """The .json file to extract metadata from."""
        pass

    def detect_metadata_files(self, file_entries):
        for entry in file_entries:
            if entry['name'] == self.filename:
                return [entry['sha1']]
        return []

    def translate(self, raw_content):
        """
        Translates content by parsing content from a bytestring containing
        json data and translating with the appropriate mapping

        Args:
            raw_content: bytes

        Returns:
            dict: translated metadata in json-friendly form needed for
                  the indexer

        """
        try:
            raw_content = raw_content.decode()
        except UnicodeDecodeError:
            self.log.warning('Error unidecoding %r', raw_content)
            return
        try:
            content_dict = json.loads(raw_content)
        except json.JSONDecodeError:
            self.log.warning('Error unjsoning %r' % raw_content)
            return
        return self.translate_dict(content_dict)


@register_mapping
class NpmMapping(JsonMapping):
    """
    dedicated class for NPM (package.json) mapping and translation
    """
    mapping = CROSSWALK_TABLE['NodeJS']
    filename = b'package.json'

    def normalize_repository(self, d):
        return '{type}+{url}'.format(**d)

    def normalize_bugs(self, d):
        return '{url}'.format(**d)


@register_mapping
class CodemetaMapping(JsonMapping):
    """
    dedicated class for CodeMeta (codemeta.json) mapping and translation
    """
    mapping = CROSSWALK_TABLE['codemeta-V1']
    filename = b'codemeta.json'


def main():
    raw_content = """{"name": "test_name", "unknown_term": "ut"}"""
    raw_content1 = b"""{"name": "test_name",
                        "unknown_term": "ut",
                        "prerequisites" :"packageXYZ"}"""
    result = MAPPINGS["NpmMapping"].translate(raw_content)
    result1 = MAPPINGS["MavenMapping"].translate(raw_content1)

    print(result)
    print(result1)


if __name__ == "__main__":
    main()
