# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

from hypothesis import HealthCheck, given, settings

from swh.indexer.codemeta import CODEMETA_TERMS
from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata_dictionary import MAPPINGS

from ..utils import json_document_strategy


def test_compute_metadata_valid_codemeta():
    raw_content = b"""{
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "@type": "SoftwareSourceCode",
        "identifier": "CodeMeta",
        "description": "CodeMeta is a concept vocabulary that can be used to standardize the exchange of software metadata across repositories and organizations.",
        "name": "CodeMeta: Minimal metadata schemas for science software and code, in JSON-LD",
        "codeRepository": "https://github.com/codemeta/codemeta",
        "issueTracker": "https://github.com/codemeta/codemeta/issues",
        "license": "https://spdx.org/licenses/Apache-2.0",
        "version": "2.0",
        "author": [
          {
            "@type": "Person",
            "givenName": "Carl",
            "familyName": "Boettiger",
            "email": "cboettig@gmail.com",
            "@id": "http://orcid.org/0000-0002-1642-628X"
          },
          {
            "@type": "Person",
            "givenName": "Matthew B.",
            "familyName": "Jones",
            "email": "jones@nceas.ucsb.edu",
            "@id": "http://orcid.org/0000-0003-0077-4738"
          }
        ],
        "maintainer": {
          "@type": "Person",
          "givenName": "Carl",
          "familyName": "Boettiger",
          "email": "cboettig@gmail.com",
          "@id": "http://orcid.org/0000-0002-1642-628X"
        },
        "contIntegration": "https://travis-ci.org/codemeta/codemeta",
        "developmentStatus": "active",
        "downloadUrl": "https://github.com/codemeta/codemeta/archive/2.0.zip",
        "funder": {
            "@id": "https://doi.org/10.13039/100000001",
            "@type": "Organization",
            "name": "National Science Foundation"
        },
        "funding":"1549758; Codemeta: A Rosetta Stone for Metadata in Scientific Software",
        "keywords": [
          "metadata",
          "software"
        ],
        "version":"2.0",
        "dateCreated":"2017-06-05",
        "datePublished":"2017-06-05",
        "programmingLanguage": "JSON-LD"
      }"""  # noqa
    expected_result = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "identifier": "CodeMeta",
        "description": "CodeMeta is a concept vocabulary that can "
        "be used to standardize the exchange of software metadata "
        "across repositories and organizations.",
        "name": "CodeMeta: Minimal metadata schemas for science "
        "software and code, in JSON-LD",
        "codeRepository": "https://github.com/codemeta/codemeta",
        "issueTracker": "https://github.com/codemeta/codemeta/issues",
        "license": "https://spdx.org/licenses/Apache-2.0",
        "version": "2.0",
        "author": [
            {
                "type": "Person",
                "givenName": "Carl",
                "familyName": "Boettiger",
                "email": "cboettig@gmail.com",
                "id": "http://orcid.org/0000-0002-1642-628X",
            },
            {
                "type": "Person",
                "givenName": "Matthew B.",
                "familyName": "Jones",
                "email": "jones@nceas.ucsb.edu",
                "id": "http://orcid.org/0000-0003-0077-4738",
            },
        ],
        "maintainer": {
            "type": "Person",
            "givenName": "Carl",
            "familyName": "Boettiger",
            "email": "cboettig@gmail.com",
            "id": "http://orcid.org/0000-0002-1642-628X",
        },
        "contIntegration": "https://travis-ci.org/codemeta/codemeta",
        "developmentStatus": "active",
        "downloadUrl": "https://github.com/codemeta/codemeta/archive/2.0.zip",
        "funder": {
            "id": "https://doi.org/10.13039/100000001",
            "type": "Organization",
            "name": "National Science Foundation",
        },
        "funding": "1549758; Codemeta: A Rosetta Stone for Metadata "
        "in Scientific Software",
        "keywords": ["metadata", "software"],
        "version": "2.0",
        "dateCreated": "2017-06-05",
        "datePublished": "2017-06-05",
        "programmingLanguage": "JSON-LD",
    }
    result = MAPPINGS["CodemetaMapping"]().translate(raw_content)
    assert result == expected_result


def test_compute_metadata_codemeta_alternate_context():
    raw_content = b"""{
        "@context": "https://raw.githubusercontent.com/codemeta/codemeta/master/codemeta.jsonld",
        "@type": "SoftwareSourceCode",
        "identifier": "CodeMeta"
    }"""  # noqa
    expected_result = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "identifier": "CodeMeta",
    }
    result = MAPPINGS["CodemetaMapping"]().translate(raw_content)
    assert result == expected_result


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(json_document_strategy(keys=CODEMETA_TERMS))
def test_codemeta_adversarial(doc):
    raw = json.dumps(doc).encode()
    MAPPINGS["CodemetaMapping"]().translate(raw)


def test_detect_metadata_codemeta_json_uppercase():
    df = [
        {
            "sha1_git": b"abc",
            "name": b"index.html",
            "target": b"abc",
            "length": 897,
            "status": "visible",
            "type": "file",
            "perms": 33188,
            "dir_id": b"dir_a",
            "sha1": b"bcd",
        },
        {
            "sha1_git": b"aab",
            "name": b"CODEMETA.json",
            "target": b"aab",
            "length": 712,
            "status": "visible",
            "type": "file",
            "perms": 33188,
            "dir_id": b"dir_a",
            "sha1": b"bcd",
        },
    ]
    results = detect_metadata(df)

    expected_results = {"CodemetaMapping": [b"bcd"]}
    assert expected_results == results


def test_sword_default_xmlns():
    content = """<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0">
      <name>My Software</name>
      <author>
        <name>Author 1</name>
        <email>foo@example.org</email>
      </author>
      <author>
        <name>Author 2</name>
      </author>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
        "author": [
            {"name": "Author 1", "email": "foo@example.org"},
            {"name": "Author 2"},
        ],
    }


def test_sword_basics():
    content = """<?xml version="1.0"?>
    <entry xmlns="http://www.w3.org/2005/Atom"
           xmlns:codemeta="https://doi.org/10.5063/schema/codemeta-2.0">
      <codemeta:name>My Software</codemeta:name>
      <codemeta:author>
        <codemeta:name>Author 1</codemeta:name>
        <codemeta:email>foo@example.org</codemeta:email>
      </codemeta:author>
      <codemeta:author>
        <codemeta:name>Author 2</codemeta:name>
      </codemeta:author>
    </entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
        "author": [
            {"name": "Author 1", "email": "foo@example.org"},
            {"name": "Author 2"},
        ],
    }


def test_sword_mixed():
    content = """<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      blah
      <schema:version>1.2.3</schema:version>
      blih
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
        "version": "1.2.3",
    }


def test_sword_schemaorg_in_codemeta():
    content = """<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      <schema:version>1.2.3</schema:version>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
        "version": "1.2.3",
    }


def test_sword_schemaorg_in_codemeta_constrained():
    """Resulting property has the compact URI 'schema:url' instead of just
    the term 'url', because term 'url' is defined by the Codemeta schema
    has having type '@id'."""
    content = """<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      <schema:url>http://example.org/my-software</schema:url>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
        "schema:url": "http://example.org/my-software",
    }


def test_sword_schemaorg_not_in_codemeta():
    content = """<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      <schema:sameAs>http://example.org/my-software</schema:sameAs>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
        "schema:sameAs": "http://example.org/my-software",
    }


def test_sword_atom_name():
    content = """<?xml version="1.0"?>
    <entry xmlns="http://www.w3.org/2005/Atom"
           xmlns:codemeta="https://doi.org/10.5063/schema/codemeta-2.0">
      <name>My Software</name>
    </entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
    }


def test_sword_multiple_names():
    content = """<?xml version="1.0"?>
    <entry xmlns="http://www.w3.org/2005/Atom"
           xmlns:codemeta="https://doi.org/10.5063/schema/codemeta-2.0">
      <name>Atom Name 1</name>
      <name>Atom Name 2</name>
      <title>Atom Title 1</title>
      <title>Atom Title 2</title>
      <codemeta:name>Codemeta Name 1</codemeta:name>
      <codemeta:name>Codemeta Name 2</codemeta:name>
    </entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": [
            "Atom Name 1",
            "Atom Name 2",
            "Atom Title 1",
            "Atom Title 2",
            "Codemeta Name 1",
            "Codemeta Name 2",
        ],
    }
