# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import json


def convert(raw_content):
    """
    convert raw_content recursively:

    - from bytes to string
    - from string to dict

    Args:
        raw_content (bytes / string / dict)

    Returns:
        dict: content (if string was json, otherwise returns string)

    """
    if isinstance(raw_content, bytes):
        return convert(raw_content.decode())
    if isinstance(raw_content, str):
        try:
            content = json.loads(raw_content)
            if content:
                return content
            else:
                return raw_content
        except json.decoder.JSONDecodeError:
            return raw_content
    if isinstance(raw_content, dict):
        return raw_content


class BaseMapping():
    """Base class for mappings to inherit from

    To implement a new mapping:

    - inherit this class
    - add a local property self.mapping
    - override translate function
    """

    def translate(self, content_dict):
        """
        Tranlsates content  by parsing content to a json object
        and translating with the npm mapping (for now hard_coded mapping)

        Args:
            context_text (text): should be json

        Returns:
            dict: translated metadata in jsonb form needed for the indexer

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
            return None
        return translated_metadata


class NpmMapping(BaseMapping):
    """
    dedicated class for NPM (package.json) mapping and translation
    """
    mapping = {
        'repository': 'codeRepository',
        'os': 'operatingSystem',
        'cpu': 'processorRequirements',
        'engines': 'processorRequirements',
        'dependencies': 'softwareRequirements',
        'bundleDependencies': 'softwareRequirements',
        'peerDependencies': 'softwareRequirements',
        'author': 'author',
        'contributor': 'contributor',
        'keywords': 'keywords',
        'license': 'license',
        'version': 'version',
        'description': 'description',
        'name': 'name',
        'devDependencies': 'softwareSuggestions',
        'optionalDependencies': 'softwareSuggestions',
        'bugs': 'issueTracker',
        'homepage': 'url'
    }

    def translate(self, raw_content):
        content_dict = convert(raw_content)
        return super().translate(content_dict)


class MavenMapping(BaseMapping):
    """
    dedicated class for Maven (pom.xml) mapping and translation
    """
    mapping = {
        'license': 'license',
        'version': 'version',
        'description': 'description',
        'name': 'name',
        'prerequisites': 'softwareRequirements',
        'repositories': 'codeRepository',
        'groupId': 'identifier',
        'ciManagement': 'contIntegration',
        'issuesManagement': 'issueTracker',
    }

    def translate(self, raw_content):
        content = convert(raw_content)
        # parse content from xml to dict
        return super().translate(content)


class DoapMapping(BaseMapping):
    mapping = {

    }

    def translate(self, raw_content):
        content = convert(raw_content)
        # parse content from xml to dict
        return super().translate(content)


def parse_xml(content):
    """
    Parses content from xml to a python dict
    Args:
        - content (text): the string form of the raw_content ( in xml)

    Returns:
        - parsed_xml (dict): a python dict of the content after parsing
    """
    # check if xml
    # use xml parser to dict
    return content


mapping_tool_fn = {
    "npm": NpmMapping(),
    "maven": MavenMapping(),
    "doap_xml": DoapMapping()
}


def compute_metadata(context, raw_content):
    """
    first landing method: a dispatcher that sends content
    to the right function to carry out the real parsing of syntax
    and translation of terms

    Args:
        context (text): defines to which function/tool the content is sent
        content (text): the string form of the raw_content

    Returns:
        dict: translated metadata jsonb dictionary needed for the indexer to
          store in storage

    """
    if raw_content is None or raw_content is b"":
        return None

    # TODO: keep mapping not in code (maybe fetch crosswalk from storage?)
    # if fetched from storage should be done once for batch of sha1s
    dictionary = mapping_tool_fn[context]
    translated_metadata = dictionary.translate(raw_content)
    return translated_metadata


def main():
    raw_content = """{"name": "test_name", "unknown_term": "ut"}"""
    raw_content1 = b"""{"name": "test_name",
                        "unknown_term": "ut",
                        "prerequisites" :"packageXYZ"}"""
    result = compute_metadata("npm", raw_content)
    result1 = compute_metadata("maven", raw_content1)

    print(result)
    print(result1)


if __name__ == "__main__":
    main()
