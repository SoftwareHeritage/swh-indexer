# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import re
import abc
import json
import logging
import xmltodict

from swh.indexer.codemeta import CROSSWALK_TABLE, SCHEMA_URI
from swh.indexer.codemeta import compact, expand


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

    def detect_metadata_files(self, file_entries):
        for entry in file_entries:
            if entry['name'] == self.filename:
                return [entry['sha1']]
        return []


class DictMapping(BaseMapping):
    """Base class for mappings that take as input a file that is mostly
    a key-value store (eg. a shallow JSON dict)."""

    @property
    @abc.abstractmethod
    def mapping(self):
        """A translation dict to map dict keys into a canonical name."""
        pass

    def translate_dict(self, content_dict, *, normalize=True):
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

    _schema_shortcuts = {
            'github': 'https://github.com/',
            'gist': 'https://gist.github.com/',
            'bitbucket': 'https://bitbucket.org/',
            'gitlab': 'https://gitlab.com/',
            }

    def normalize_repository(self, d):
        """https://docs.npmjs.com/files/package.json#repository"""
        if isinstance(d, dict):
            url = '{type}+{url}'.format(**d)
        elif isinstance(d, str):
            if '://' in d:
                url = d
            elif ':' in d:
                (schema, rest) = d.split(':', 1)
                if schema in self._schema_shortcuts:
                    url = self._schema_shortcuts[schema] + rest
                else:
                    return None
            else:
                url = self._schema_shortcuts['github'] + d

        else:
            return None

        return {'@id': url}

    def normalize_bugs(self, d):
        return {'@id': '{url}'.format(**d)}

    _parse_author = re.compile(r'^ *'
                               r'(?P<name>.*?)'
                               r'( +<(?P<email>.*)>)?'
                               r'( +\((?P<url>.*)\))?'
                               r' *$')

    def normalize_author(self, d):
        'https://docs.npmjs.com/files/package.json' \
                '#people-fields-author-contributors'
        author = {'@type': SCHEMA_URI+'Person'}
        if isinstance(d, dict):
            name = d.get('name', None)
            email = d.get('email', None)
            url = d.get('url', None)
        elif isinstance(d, str):
            match = self._parse_author.match(d)
            name = match.group('name')
            email = match.group('email')
            url = match.group('url')
        else:
            return None
        if name:
            author[SCHEMA_URI+'name'] = name
        if email:
            author[SCHEMA_URI+'email'] = email
        if url:
            author[SCHEMA_URI+'url'] = {'@id': url}
        return {"@list": [author]}

    def normalize_license(self, s):
        return {"@id": "https://spdx.org/licenses/" + s}

    def normalize_homepage(self, s):
        return {"@id": s}


@register_mapping
class CodemetaMapping(SingleFileMapping):
    """
    dedicated class for CodeMeta (codemeta.json) mapping and translation
    """
    filename = b'codemeta.json'

    def translate(self, content):
        return self.normalize_translation(expand(json.loads(content.decode())))


@register_mapping
class MavenMapping(DictMapping, SingleFileMapping):
    """
    dedicated class for Maven (pom.xml) mapping and translation
    """
    filename = b'pom.xml'
    mapping = CROSSWALK_TABLE['Java (Maven)']

    def translate(self, content):
        d = xmltodict.parse(content)['project']
        metadata = self.translate_dict(d, normalize=False)
        metadata[SCHEMA_URI+'codeRepository'] = self.parse_repositories(d)
        metadata[SCHEMA_URI+'license'] = self.parse_licenses(d)
        return self.normalize_translation(metadata)

    _default_repository = {'url': 'https://repo.maven.apache.org/maven2/'}

    def parse_repositories(self, d):
        """https://maven.apache.org/pom.html#Repositories"""
        if 'repositories' not in d:
            return [self.parse_repository(d, self._default_repository)]
        else:
            repositories = d['repositories'].get('repository', [])
            if not isinstance(repositories, list):
                repositories = [repositories]
            results = []
            for repo in repositories:
                res = self.parse_repository(d, repo)
                if res:
                    results.append(res)
            return results

    def parse_repository(self, d, repo):
        if repo.get('layout', 'default') != 'default':
            return  # TODO ?
        url = repo['url']
        if d['groupId']:
            url = os.path.join(url, *d['groupId'].split('.'))
            if d['artifactId']:
                url = os.path.join(url, d['artifactId'])
        return {"@id": url}

    def normalize_groupId(self, id_):
        return {"@id": id_}

    def parse_licenses(self, d):
        """https://maven.apache.org/pom.html#Licenses

        The origin XML has the form:

            <licenses>
              <license>
                <name>Apache License, Version 2.0</name>
                <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
              </license>
            </licenses>

        Which was translated to a dict by xmltodict and is given as `d`:

        >>> d = {
        ...     # ...
        ...     "licenses": {
        ...         "license": {
        ...             "name": "Apache License, Version 2.0",
        ...             "url":
        ...             "https://www.apache.org/licenses/LICENSE-2.0.txt"
        ...         }
        ...     }
        ... }
        >>> MavenMapping().parse_licenses(d)
        [{'@id': 'https://www.apache.org/licenses/LICENSE-2.0.txt'}]

        or, if there are more than one license:

        >>> from pprint import pprint
        >>> d = {
        ...     # ...
        ...     "licenses": {
        ...         "license": [
        ...             {
        ...                 "name": "Apache License, Version 2.0",
        ...                 "url":
        ...                 "https://www.apache.org/licenses/LICENSE-2.0.txt"
        ...             },
        ...             {
        ...                 "name": "MIT License, ",
        ...                 "url": "https://opensource.org/licenses/MIT"
        ...             }
        ...         ]
        ...     }
        ... }
        >>> pprint(MavenMapping().parse_licenses(d))
        [{'@id': 'https://www.apache.org/licenses/LICENSE-2.0.txt'},
         {'@id': 'https://opensource.org/licenses/MIT'}]
        """

        licenses = d.get('licenses', {}).get('license', [])
        if isinstance(licenses, dict):
            licenses = [licenses]
        return [{"@id": license['url']} for license in licenses]


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
