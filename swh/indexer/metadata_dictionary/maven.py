# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Any, Dict

from rdflib import Graph, Literal

from swh.indexer.codemeta import CROSSWALK_TABLE
from swh.indexer.namespaces import SCHEMA

from .base import SingleFileIntrinsicMapping, XmlMapping
from .utils import add_url_if_valid, prettyprint_graph  # noqa


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

    def extra_translation(self, graph: Graph, root, d):
        self.parse_repositories(graph, root, d)

    def parse_repositories(self, graph: Graph, root, d):
        """https://maven.apache.org/pom.html#Repositories

        >>> import rdflib
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
        >>> MavenMapping().parse_repositories(rdflib.Graph(), rdflib.BNode(), d)
        """
        repositories = d.get("repositories")
        if not repositories:
            self.parse_repository(graph, root, d, self._default_repository)
        elif isinstance(repositories, dict):
            repositories = repositories.get("repository") or []
            if not isinstance(repositories, list):
                repositories = [repositories]
            for repo in repositories:
                self.parse_repository(graph, root, d, repo)

    def parse_repository(self, graph: Graph, root, d, repo):
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
            if "${" in repo:
                # Often use as templating in pom.xml files collected from VCSs
                return
            add_url_if_valid(graph, root, SCHEMA.codeRepository, repo)

    def normalize_groupId(self, id_):
        """https://maven.apache.org/pom.html#Maven_Coordinates

        >>> MavenMapping().normalize_groupId('org.example')
        rdflib.term.Literal('org.example')
        """
        if isinstance(id_, str):
            return Literal(id_)

    def translate_licenses(self, graph, root, licenses):
        """https://maven.apache.org/pom.html#Licenses

        >>> import xmltodict
        >>> import json
        >>> from rdflib import URIRef
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
        >>> graph = Graph()
        >>> root = URIRef("http://example.org/test-software")
        >>> MavenMapping().translate_licenses(graph, root, d["licenses"])
        >>> prettyprint_graph(graph, root)
        {
            "@id": ...,
            "http://schema.org/license": {
                "@id": "https://www.apache.org/licenses/LICENSE-2.0.txt"
            }
        }

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
        >>> graph = Graph()
        >>> root = URIRef("http://example.org/test-software")
        >>> MavenMapping().translate_licenses(graph, root, d["licenses"])
        >>> pprint(set(graph.triples((root, URIRef("http://schema.org/license"), None))))
        {(rdflib.term.URIRef('http://example.org/test-software'),
          rdflib.term.URIRef('http://schema.org/license'),
          rdflib.term.URIRef('https://opensource.org/licenses/MIT')),
         (rdflib.term.URIRef('http://example.org/test-software'),
          rdflib.term.URIRef('http://schema.org/license'),
          rdflib.term.URIRef('https://www.apache.org/licenses/LICENSE-2.0.txt'))}
        """

        if not isinstance(licenses, dict):
            return
        licenses = licenses.get("license")
        if isinstance(licenses, dict):
            licenses = [licenses]
        elif not isinstance(licenses, list):
            return
        for license in licenses:
            if isinstance(license, dict):
                add_url_if_valid(graph, root, SCHEMA.license, license.get("url"))
