# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Any, Dict

from swh.indexer.codemeta import CROSSWALK_TABLE
from swh.indexer.namespaces import SCHEMA

from .base import SingleFileIntrinsicMapping, XmlMapping


class MavenMapping(XmlMapping, SingleFileIntrinsicMapping):
    """
    dedicated class for Maven (pom.xml) mapping and translation
    """

    name = "maven"
    filename = b"pom.xml"
    mapping = CROSSWALK_TABLE["Java (Maven)"]
    string_fields = ["name", "version", "description", "email"]

    _default_repository = {"url": "https://repo.maven.apache.org/maven2/"}

    def _translate_dict(self, d: Dict[str, Any]) -> Dict[str, Any]:
        return super()._translate_dict(d.get("project") or {})

    def extra_translation(self, translated_metadata, d):
        repositories = self.parse_repositories(d)
        if repositories:
            translated_metadata[SCHEMA.codeRepository] = repositories

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
        repositories = d.get("repositories")
        if not repositories:
            results = [self.parse_repository(d, self._default_repository)]
        elif isinstance(repositories, dict):
            repositories = repositories.get("repository") or []
            if not isinstance(repositories, list):
                repositories = [repositories]
            results = [self.parse_repository(d, repo) for repo in repositories]
        else:
            results = []
        return [res for res in results if res] or None

    def parse_repository(self, d, repo):
        if not isinstance(repo, dict):
            return
        if repo.get("layout", "default") != "default":
            return  # TODO ?
        url = repo.get("url")
        group_id = d.get("groupId")
        artifact_id = d.get("artifactId")
        if (
            isinstance(url, str)
            and isinstance(group_id, str)
            and isinstance(artifact_id, str)
        ):
            repo = os.path.join(url, *group_id.split("."), artifact_id)
            return {"@id": repo}

    def normalize_groupId(self, id_):
        """https://maven.apache.org/pom.html#Maven_Coordinates

        >>> MavenMapping().normalize_groupId('org.example')
        {'@id': 'org.example'}
        """
        if isinstance(id_, str):
            return {"@id": id_}

    def translate_licenses(self, translated_metadata, d):
        licenses = self.parse_licenses(d)
        if licenses:
            translated_metadata[SCHEMA.license] = licenses

    def parse_licenses(self, licenses):
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
        >>> MavenMapping().parse_licenses(d["licenses"])
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
        >>> pprint(MavenMapping().parse_licenses(d["licenses"]))
        [{'@id': 'https://www.apache.org/licenses/LICENSE-2.0.txt'},
         {'@id': 'https://opensource.org/licenses/MIT'}]
        """

        if not isinstance(licenses, dict):
            return
        licenses = licenses.get("license")
        if isinstance(licenses, dict):
            licenses = [licenses]
        elif not isinstance(licenses, list):
            return
        return [
            {"@id": license["url"]}
            for license in licenses
            if isinstance(license, dict) and isinstance(license.get("url"), str)
        ] or None
