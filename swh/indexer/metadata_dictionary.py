# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import csv
import json
import os.path
import logging

import swh.indexer

CROSSWALK_TABLE_PATH = os.path.join(os.path.dirname(swh.indexer.__file__),
                                    'data', 'codemeta', 'crosswalk.csv')


def read_crosstable(fd):
    reader = csv.reader(fd)
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError('empty file')

    data_sources = set(header) - {'Parent Type', 'Property',
                                  'Type', 'Description'}
    assert 'codemeta-V1' in data_sources

    codemeta_translation = {data_source: {} for data_source in data_sources}

    for line in reader:  # For each canonical name
        canonical_name = dict(zip(header, line))['Property']
        for (col, value) in zip(header, line):  # For each cell in the row
            if col in data_sources:
                # If that's not the parentType/property/type/description
                for local_name in value.split('/'):
                    # For each of the data source's properties that maps
                    # to this canonical name
                    if local_name.strip():
                        codemeta_translation[col][local_name.strip()] = \
                                canonical_name

    return codemeta_translation


with open(CROSSWALK_TABLE_PATH) as fd:
    CROSSWALK_TABLE = read_crosstable(fd)


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
        default = 'other'
        translated_metadata['other'] = {}
        try:
            for k, v in content_dict.items():
                try:
                    term = self.mapping.get(k, default)
                    if term not in translated_metadata:
                        translated_metadata[term] = v
                        continue
                    if isinstance(translated_metadata[term], str):
                        in_value = translated_metadata[term]
                        translated_metadata[term] = [in_value, v]
                        continue
                    if isinstance(translated_metadata[term], list):
                        translated_metadata[term].append(v)
                        continue
                    if isinstance(translated_metadata[term], dict):
                        translated_metadata[term][k] = v
                        continue
                except KeyError:
                    self.log.exception(
                        "Problem during item mapping")
                    continue
        except Exception:
            raise
            return None
        return translated_metadata


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
