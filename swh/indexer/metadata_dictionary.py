# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import re
import abc
import ast
import json
import logging
import itertools
import email.parser
import xml.parsers.expat
import email.policy

import xmltodict

from swh.indexer.codemeta import CROSSWALK_TABLE, SCHEMA_URI
from swh.indexer.codemeta import compact, expand


MAPPINGS = {}


def register_mapping(cls):
    MAPPINGS[cls.__name__] = cls
    return cls


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
            translation_method = getattr(
                self, 'translate_' + k.replace('-', '_'), None)
            if translation_method:
                translation_method(translated_metadata, v)
            elif k in self.mapping:
                # if there is no method, but the key is known from the
                # crosswalk table
                codemeta_key = self.mapping[k]

                # if there is a normalization method, use it on the value
                normalization_method = getattr(
                    self, 'normalize_' + k.replace('-', '_'), None)
                if normalization_method:
                    v = normalization_method(v)

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
        return self.translate_dict(content_dict)


@register_mapping
class NpmMapping(JsonMapping):
    """
    dedicated class for NPM (package.json) mapping and translation
    """
    name = 'npm'
    mapping = CROSSWALK_TABLE['NodeJS']
    filename = b'package.json'

    _schema_shortcuts = {
            'github': 'git+https://github.com/%s.git',
            'gist': 'git+https://gist.github.com/%s.git',
            'gitlab': 'git+https://gitlab.com/%s.git',
            # Bitbucket supports both hg and git, and the shortcut does not
            # tell which one to use.
            # 'bitbucket': 'https://bitbucket.org/',
            }

    def normalize_repository(self, d):
        """https://docs.npmjs.com/files/package.json#repository

        >>> NpmMapping().normalize_repository({
        ...     'type': 'git',
        ...     'url': 'https://example.org/foo.git'
        ... })
        {'@id': 'git+https://example.org/foo.git'}
        >>> NpmMapping().normalize_repository(
        ...     'gitlab:foo/bar')
        {'@id': 'git+https://gitlab.com/foo/bar.git'}
        >>> NpmMapping().normalize_repository(
        ...     'foo/bar')
        {'@id': 'git+https://github.com/foo/bar.git'}
        """
        if isinstance(d, dict) and {'type', 'url'} <= set(d):
            url = '{type}+{url}'.format(**d)
        elif isinstance(d, str):
            if '://' in d:
                url = d
            elif ':' in d:
                (schema, rest) = d.split(':', 1)
                if schema in self._schema_shortcuts:
                    url = self._schema_shortcuts[schema] % rest
                else:
                    return None
            else:
                url = self._schema_shortcuts['github'] % d

        else:
            return None

        return {'@id': url}

    def normalize_bugs(self, d):
        """https://docs.npmjs.com/files/package.json#bugs

        >>> NpmMapping().normalize_bugs({
        ...     'url': 'https://example.org/bugs/',
        ...     'email': 'bugs@example.org'
        ... })
        {'@id': 'https://example.org/bugs/'}
        >>> NpmMapping().normalize_bugs(
        ...     'https://example.org/bugs/')
        {'@id': 'https://example.org/bugs/'}
        """
        if isinstance(d, dict) and 'url' in d:
            return {'@id': '{url}'.format(**d)}
        elif isinstance(d, str):
            return {'@id': d}
        else:
            return None

    _parse_author = re.compile(r'^ *'
                               r'(?P<name>.*?)'
                               r'( +<(?P<email>.*)>)?'
                               r'( +\((?P<url>.*)\))?'
                               r' *$')

    def normalize_author(self, d):
        """https://docs.npmjs.com/files/package.json#people-fields-author-contributors'

        >>> from pprint import pprint
        >>> pprint(NpmMapping().normalize_author({
        ...     'name': 'John Doe',
        ...     'email': 'john.doe@example.org',
        ...     'url': 'https://example.org/~john.doe',
        ... }))
        {'@list': [{'@type': 'http://schema.org/Person',
                    'http://schema.org/email': 'john.doe@example.org',
                    'http://schema.org/name': 'John Doe',
                    'http://schema.org/url': {'@id': 'https://example.org/~john.doe'}}]}
        >>> pprint(NpmMapping().normalize_author(
        ...     'John Doe <john.doe@example.org> (https://example.org/~john.doe)'
        ... ))
        {'@list': [{'@type': 'http://schema.org/Person',
                    'http://schema.org/email': 'john.doe@example.org',
                    'http://schema.org/name': 'John Doe',
                    'http://schema.org/url': {'@id': 'https://example.org/~john.doe'}}]}
        """ # noqa
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
        """https://docs.npmjs.com/files/package.json#license

        >>> NpmMapping().normalize_license('MIT')
        {'@id': 'https://spdx.org/licenses/MIT'}
        """
        if isinstance(s, str):
            return {"@id": "https://spdx.org/licenses/" + s}
        else:
            return None

    def normalize_homepage(self, s):
        """https://docs.npmjs.com/files/package.json#homepage

        >>> NpmMapping().normalize_homepage('https://example.org/~john.doe')
        {'@id': 'https://example.org/~john.doe'}
        """
        if isinstance(s, str):
            return {"@id": s}


@register_mapping
class CodemetaMapping(SingleFileMapping):
    """
    dedicated class for CodeMeta (codemeta.json) mapping and translation
    """
    name = 'codemeta'
    filename = b'codemeta.json'

    def translate(self, content):
        return self.normalize_translation(expand(json.loads(content.decode())))


@register_mapping
class MavenMapping(DictMapping, SingleFileMapping):
    """
    dedicated class for Maven (pom.xml) mapping and translation
    """
    name = 'maven'
    filename = b'pom.xml'
    mapping = CROSSWALK_TABLE['Java (Maven)']

    def translate(self, content):
        try:
            d = xmltodict.parse(content).get('project') or {}
        except xml.parsers.expat.ExpatError:
            self.log.warning('Error parsing XML from %s', self.log_suffix)
            return None
        metadata = self.translate_dict(d, normalize=False)
        metadata[SCHEMA_URI+'codeRepository'] = self.parse_repositories(d)
        metadata[SCHEMA_URI+'license'] = self.parse_licenses(d)
        return self.normalize_translation(metadata)

    _default_repository = {'url': 'https://repo.maven.apache.org/maven2/'}

    def parse_repositories(self, d):
        """https://maven.apache.org/pom.html#Repositories

        >>> import xmltodict
        >>> from pprint import pprint
        >>> d = xmltodict.parse('''
        ... <repositories>
        ...   <repository>
        ...     <id>codehausSnapshots</id>
        ...     <name>Codehaus Snapshots</name>
        ...     <url>http://snapshots.maven.codehaus.org/maven2</url>
        ...     <layout>default</layout>
        ...   </repository>
        ... </repositories>
        ... ''')
        >>> MavenMapping().parse_repositories(d)
        """
        repositories = d.get('repositories')
        if not repositories:
            results = [self.parse_repository(d, self._default_repository)]
        else:
            repositories = repositories.get('repository') or []
            if not isinstance(repositories, list):
                repositories = [repositories]
            results = [self.parse_repository(d, repo)
                       for repo in repositories]
        return [res for res in results if res] or None

    def parse_repository(self, d, repo):
        if repo.get('layout', 'default') != 'default':
            return  # TODO ?
        url = repo.get('url')
        group_id = d.get('groupId')
        artifact_id = d.get('artifactId')
        if (isinstance(url, str) and isinstance(group_id, str)
                and isinstance(artifact_id, str)):
            repo = os.path.join(url, *group_id.split('.'), artifact_id)
            return {"@id": repo}

    def normalize_groupId(self, id_):
        """https://maven.apache.org/pom.html#Maven_Coordinates

        >>> MavenMapping().normalize_groupId('org.example')
        {'@id': 'org.example'}
        """
        return {"@id": id_}

    def parse_licenses(self, d):
        """https://maven.apache.org/pom.html#Licenses

        >>> import xmltodict
        >>> import json
        >>> d = xmltodict.parse('''
        ... <licenses>
        ...   <license>
        ...     <name>Apache License, Version 2.0</name>
        ...     <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
        ...   </license>
        ... </licenses>
        ... ''')
        >>> print(json.dumps(d, indent=4))
        {
            "licenses": {
                "license": {
                    "name": "Apache License, Version 2.0",
                    "url": "https://www.apache.org/licenses/LICENSE-2.0.txt"
                }
            }
        }
        >>> MavenMapping().parse_licenses(d)
        [{'@id': 'https://www.apache.org/licenses/LICENSE-2.0.txt'}]

        or, if there are more than one license:

        >>> import xmltodict
        >>> from pprint import pprint
        >>> d = xmltodict.parse('''
        ... <licenses>
        ...   <license>
        ...     <name>Apache License, Version 2.0</name>
        ...     <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
        ...   </license>
        ...   <license>
        ...     <name>MIT License</name>
        ...     <url>https://opensource.org/licenses/MIT</url>
        ...   </license>
        ... </licenses>
        ... ''')
        >>> pprint(MavenMapping().parse_licenses(d))
        [{'@id': 'https://www.apache.org/licenses/LICENSE-2.0.txt'},
         {'@id': 'https://opensource.org/licenses/MIT'}]
        """

        licenses = d.get('licenses', {}).get('license', [])
        if isinstance(licenses, dict):
            licenses = [licenses]
        return [{"@id": license['url']}
                for license in licenses
                if 'url' in license] or None


_normalize_pkginfo_key = str.lower


class LinebreakPreservingEmailPolicy(email.policy.EmailPolicy):
    def header_fetch_parse(self, name, value):
        if hasattr(value, 'name'):
            return value
        value = value.replace('\n        ', '\n')
        return self.header_factory(name, value)


@register_mapping
class PythonPkginfoMapping(DictMapping, SingleFileMapping):
    """Dedicated class for Python's PKG-INFO mapping and translation.

    https://www.python.org/dev/peps/pep-0314/"""
    name = 'pkg-info'
    filename = b'PKG-INFO'
    mapping = {_normalize_pkginfo_key(k): v
               for (k, v) in CROSSWALK_TABLE['Python PKG-INFO'].items()}

    _parser = email.parser.BytesHeaderParser(
        policy=LinebreakPreservingEmailPolicy())

    def translate(self, content):
        msg = self._parser.parsebytes(content)
        d = {}
        for (key, value) in msg.items():
            key = _normalize_pkginfo_key(key)
            if value != 'UNKNOWN':
                d.setdefault(key, []).append(value)
        metadata = self.translate_dict(d, normalize=False)
        if SCHEMA_URI+'author' in metadata or SCHEMA_URI+'email' in metadata:
            metadata[SCHEMA_URI+'author'] = {
                '@list': [{
                    '@type': SCHEMA_URI+'Person',
                    SCHEMA_URI+'name':
                        metadata.pop(SCHEMA_URI+'author', [None])[0],
                    SCHEMA_URI+'email':
                        metadata.pop(SCHEMA_URI+'email', [None])[0],
                }]
            }
        return self.normalize_translation(metadata)

    def normalize_home_page(self, urls):
        return [{'@id': url} for url in urls]

    def normalize_license(self, licenses):
        return [{'@id': license} for license in licenses]


@register_mapping
class GemspecMapping(DictMapping):
    name = 'gemspec'
    mapping = CROSSWALK_TABLE['Ruby Gem']

    _re_spec_new = re.compile(r'.*Gem::Specification.new +(do|\{) +\|.*\|.*')
    _re_spec_entry = re.compile(r'\s*\w+\.(?P<key>\w+)\s*=\s*(?P<expr>.*)')

    @classmethod
    def detect_metadata_files(cls, file_entries):
        for entry in file_entries:
            if entry['name'].endswith(b'.gemspec'):
                return [entry['sha1']]
        return []

    def translate(self, raw_content):
        try:
            raw_content = raw_content.decode()
        except UnicodeDecodeError:
            self.log.warning('Error unidecoding from %s', self.log_suffix)
            return

        # Skip lines before 'Gem::Specification.new'
        lines = itertools.dropwhile(
            lambda x: not self._re_spec_new.match(x),
            raw_content.split('\n'))

        try:
            next(lines)  # Consume 'Gem::Specification.new'
        except StopIteration:
            self.log.warning('Could not find Gem::Specification in %s',
                             self.log_suffix)
            return

        content_dict = {}
        for line in lines:
            match = self._re_spec_entry.match(line)
            if match:
                value = self.eval_ruby_expression(match.group('expr'))
                if value:
                    content_dict[match.group('key')] = value
        return self.translate_dict(content_dict)

    def eval_ruby_expression(self, expr):
        """Very simple evaluator of Ruby expressions.

        >>> GemspecMapping().eval_ruby_expression('"Foo bar"')
        'Foo bar'
        >>> GemspecMapping().eval_ruby_expression("'Foo bar'")
        'Foo bar'
        >>> GemspecMapping().eval_ruby_expression("['Foo', 'bar']")
        ['Foo', 'bar']
        >>> GemspecMapping().eval_ruby_expression("'Foo bar'.freeze")
        'Foo bar'
        >>> GemspecMapping().eval_ruby_expression( \
                "['Foo'.freeze, 'bar'.freeze]")
        ['Foo', 'bar']
        """
        def evaluator(node):
            if isinstance(node, ast.Str):
                return node.s
            elif isinstance(node, ast.List):
                res = []
                for element in node.elts:
                    val = evaluator(element)
                    if not val:
                        return
                    res.append(val)
                return res

        expr = expr.replace('.freeze', '')
        try:
            # We're parsing Ruby expressions here, but Python's
            # ast.parse works for very simple Ruby expressions
            # (mainly strings delimited with " or ', and lists
            # of such strings).
            tree = ast.parse(expr, mode='eval')
        except (SyntaxError, ValueError):
            return
        if isinstance(tree, ast.Expression):
            return evaluator(tree.body)

    def normalize_homepage(self, s):
        if isinstance(s, str):
            return {"@id": s}

    def normalize_license(self, s):
        if isinstance(s, str):
            return [{"@id": "https://spdx.org/licenses/" + s}]

    def normalize_licenses(self, licenses):
        if isinstance(licenses, list):
            return [{"@id": "https://spdx.org/licenses/" + license}
                    for license in licenses
                    if isinstance(license, str)]

    def normalize_author(self, author):
        if isinstance(author, str):
            return {"@list": [author]}

    def normalize_authors(self, authors):
        if isinstance(authors, list):
            return {"@list": [author for author in authors
                              if isinstance(author, str)]}
