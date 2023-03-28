# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from hypothesis import HealthCheck, given, settings

from swh.indexer.metadata_dictionary import MAPPINGS

from ..utils import xml_document_strategy


def test_compute_metadata_maven():
    raw_content = b"""
    <project>
      <name>Maven Default Project</name>
      <modelVersion>4.0.0</modelVersion>
      <groupId>com.mycompany.app</groupId>
      <artifactId>my-app</artifactId>
      <version>1.2.3</version>
      <repositories>
        <repository>
          <id>central</id>
          <name>Maven Repository Switchboard</name>
          <layout>default</layout>
          <url>http://repo1.maven.org/maven2</url>
          <snapshots>
            <enabled>false</enabled>
          </snapshots>
        </repository>
      </repositories>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
          <comments>A business-friendly OSS license</comments>
        </license>
      </licenses>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "Maven Default Project",
        "schema:identifier": "com.mycompany.app",
        "version": "1.2.3",
        "license": "https://www.apache.org/licenses/LICENSE-2.0.txt",
        "codeRepository": ("http://repo1.maven.org/maven2/com/mycompany/app/my-app"),
    }


def test_compute_metadata_maven_empty():
    raw_content = b"""
    <project>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }


def test_compute_metadata_maven_almost_empty():
    raw_content = b"""
    <project>
      <foo/>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }


def test_compute_metadata_maven_invalid_xml(caplog):
    expected_warning = (
        "swh.indexer.metadata_dictionary.maven.MavenMapping",
        logging.WARNING,
        "Error parsing XML from foo",
    )
    caplog.at_level(logging.WARNING, logger="swh.indexer.metadata_dictionary")

    raw_content = b"""
    <project>"""
    caplog.clear()
    result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
    assert caplog.record_tuples == [expected_warning], result
    assert result is None

    raw_content = b"""
    """
    caplog.clear()
    result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
    assert caplog.record_tuples == [expected_warning], result
    assert result is None


def test_compute_metadata_maven_unknown_encoding(caplog):
    expected_warning = (
        "swh.indexer.metadata_dictionary.maven.MavenMapping",
        logging.WARNING,
        "Error detecting XML encoding from foo",
    )
    caplog.at_level(logging.WARNING, logger="swh.indexer.metadata_dictionary")

    raw_content = b"""<?xml version="1.0" encoding="foo"?>
    <project>
    </project>"""
    caplog.clear()
    result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
    assert caplog.record_tuples == [expected_warning], result
    assert result is None

    raw_content = b"""<?xml version="1.0" encoding="UTF-7"?>
    <project>
    </project>"""
    caplog.clear()
    result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
    assert caplog.record_tuples == [expected_warning], result
    assert result is None


def test_compute_metadata_maven_invalid_encoding(caplog):
    expected_warning = [
        # libexpat1 <= 2.2.10-2+deb11u1
        [
            (
                "swh.indexer.metadata_dictionary.maven.MavenMapping",
                logging.WARNING,
                "Error unidecoding XML from foo",
            )
        ],
        # libexpat1 >= 2.2.10-2+deb11u2
        [
            (
                "swh.indexer.metadata_dictionary.maven.MavenMapping",
                logging.WARNING,
                "Error parsing XML from foo",
            )
        ],
    ]
    caplog.at_level(logging.WARNING, logger="swh.indexer.metadata_dictionary")

    raw_content = b"""<?xml version="1.0" encoding="UTF-8"?>
    <foo\xe5ct>
    </foo>"""
    caplog.clear()
    result = MAPPINGS["MavenMapping"]("foo").translate(raw_content)
    assert caplog.record_tuples in expected_warning, result
    assert result is None


def test_compute_metadata_maven_minimal():
    raw_content = b"""
    <project>
      <name>Maven Default Project</name>
      <modelVersion>4.0.0</modelVersion>
      <groupId>com.mycompany.app</groupId>
      <artifactId>my-app</artifactId>
      <version>1.2.3</version>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "Maven Default Project",
        "schema:identifier": "com.mycompany.app",
        "version": "1.2.3",
        "codeRepository": (
            "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
        ),
    }


def test_compute_metadata_maven_empty_nodes():
    raw_content = b"""
    <project>
      <name>Maven Default Project</name>
      <modelVersion>4.0.0</modelVersion>
      <groupId>com.mycompany.app</groupId>
      <artifactId>my-app</artifactId>
      <version>1.2.3</version>
      <repositories>
      </repositories>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "Maven Default Project",
        "schema:identifier": "com.mycompany.app",
        "version": "1.2.3",
        "codeRepository": (
            "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
        ),
    }

    raw_content = b"""
    <project>
      <name>Maven Default Project</name>
      <modelVersion>4.0.0</modelVersion>
      <groupId>com.mycompany.app</groupId>
      <artifactId>my-app</artifactId>
      <version></version>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "Maven Default Project",
        "schema:identifier": "com.mycompany.app",
        "codeRepository": (
            "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
        ),
    }

    raw_content = b"""
    <project>
      <name></name>
      <modelVersion>4.0.0</modelVersion>
      <groupId>com.mycompany.app</groupId>
      <artifactId>my-app</artifactId>
      <version>1.2.3</version>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "schema:identifier": "com.mycompany.app",
        "version": "1.2.3",
        "codeRepository": (
            "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
        ),
    }

    raw_content = b"""
    <project>
      <name>Maven Default Project</name>
      <modelVersion>4.0.0</modelVersion>
      <groupId>com.mycompany.app</groupId>
      <artifactId>my-app</artifactId>
      <version>1.2.3</version>
      <licenses>
      </licenses>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "Maven Default Project",
        "schema:identifier": "com.mycompany.app",
        "version": "1.2.3",
        "codeRepository": (
            "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
        ),
    }

    raw_content = b"""
    <project>
      <groupId></groupId>
      <version>1.2.3</version>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "version": "1.2.3",
    }


def test_compute_metadata_maven_invalid_licenses():
    raw_content = b"""
    <project>
      <name>Maven Default Project</name>
      <modelVersion>4.0.0</modelVersion>
      <groupId>com.mycompany.app</groupId>
      <artifactId>my-app</artifactId>
      <version>1.2.3</version>
      <licenses>
        foo
      </licenses>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "Maven Default Project",
        "schema:identifier": "com.mycompany.app",
        "version": "1.2.3",
        "codeRepository": (
            "https://repo.maven.apache.org/maven2/com/mycompany/app/my-app"
        ),
    }


def test_compute_metadata_maven_multiple():
    """Tests when there are multiple code repos and licenses."""
    raw_content = b"""
    <project>
      <name>Maven Default Project</name>
      <modelVersion>4.0.0</modelVersion>
      <groupId>com.mycompany.app</groupId>
      <artifactId>my-app</artifactId>
      <version>1.2.3</version>
      <repositories>
        <repository>
          <id>central</id>
          <name>Maven Repository Switchboard</name>
          <layout>default</layout>
          <url>http://repo1.maven.org/maven2</url>
          <snapshots>
            <enabled>false</enabled>
          </snapshots>
        </repository>
        <repository>
          <id>example</id>
          <name>Example Maven Repo</name>
          <layout>default</layout>
          <url>http://example.org/maven2</url>
        </repository>
      </repositories>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
          <comments>A business-friendly OSS license</comments>
        </license>
        <license>
          <name>MIT license</name>
          <url>https://opensource.org/licenses/MIT</url>
        </license>
      </licenses>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert set(result.pop("license")) == {
        "https://www.apache.org/licenses/LICENSE-2.0.txt",
        "https://opensource.org/licenses/MIT",
    }, result
    assert set(result.pop("codeRepository")) == {
        "http://repo1.maven.org/maven2/com/mycompany/app/my-app",
        "http://example.org/maven2/com/mycompany/app/my-app",
    }, result
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "Maven Default Project",
        "schema:identifier": "com.mycompany.app",
        "version": "1.2.3",
    }


def test_compute_metadata_maven_invalid_repository():
    raw_content = b"""
    <project>
      <name>Maven Default Project</name>
      <modelVersion>4.0.0</modelVersion>
      <groupId>com.mycompany.app</groupId>
      <artifactId>my-app</artifactId>
      <version>1.2.3</version>
      <repositories>
        <repository>
          <id>tcc-transaction-internal-releases</id>
          <name>internal repository for released artifacts</name>
          <url>${repo.internal.releases.url}</url>
          <snapshots>
              <enabled>false</enabled>
          </snapshots>
          <releases>
              <enabled>true</enabled>
          </releases>
        </repository>
      </repositories>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
          <comments>A business-friendly OSS license</comments>
        </license>
      </licenses>
    </project>"""
    result = MAPPINGS["MavenMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "Maven Default Project",
        "schema:identifier": "com.mycompany.app",
        "version": "1.2.3",
        "license": "https://www.apache.org/licenses/LICENSE-2.0.txt",
    }


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(
    xml_document_strategy(
        keys=list(MAPPINGS["MavenMapping"].mapping),  # type: ignore
        root="project",
        xmlns="http://maven.apache.org/POM/4.0.0",
    )
)
def test_maven_adversarial(doc):
    MAPPINGS["MavenMapping"]().translate(doc)
