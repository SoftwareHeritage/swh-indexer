# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging

from hypothesis import HealthCheck, given, settings
import pytest

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
      <codemeta:dateCreated>2022-10-26</codemeta:dateCreated>
      <author>
        <name>Author 3</name>
        <email>bar@example.org</email>
      </author>
    </entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
        "author": [
            {"name": "Author 1", "email": "foo@example.org"},
            {"name": "Author 2"},
            {"name": "Author 3", "email": "bar@example.org"},
        ],
        "dateCreated": "2022-10-26",
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


@pytest.mark.parametrize("id_", ["", " ", "\n"])
def test_sword_invalid_id(id_):
    content = f"""<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      <id>{id_}</id>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
    }


@pytest.mark.parametrize(
    "id_",
    [
        "foo",
        "42",
        "http://example.org/",
        "http://example.org/foo",
        "https://example.org/",
        "https://example.org/foo",
    ],
)
def test_sword_id(id_):
    content = f"""<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      <id>{id_}</id>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "id": id_,
        "name": "My Software",
    }


def test_sword_multiple_ids():
    """JSON-LD only allows a single id, so we ignore all but the first one."""
    content = """<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      <id>http://example.org/foo</id>
      <id>http://example.org/bar</id>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "id": "http://example.org/foo",
        "name": "My Software",
    }


def test_sword_type():
    content = """<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      <type>http://schema.org/WebSite</type>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "schema:WebSite",
        "name": "My Software",
    }


def test_sword_multiple_type():
    content = """<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      <type>http://schema.org/WebSite</type>
      <type>http://schema.org/SoftwareSourceCode</type>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result in (
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "type": ["schema:WebSite", "SoftwareSourceCode"],
            "name": "My Software",
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "type": ["SoftwareSourceCode", "schema:WebSite"],
            "name": "My Software",
        },
    )


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
    has having type '@id'.
    Ditto for dates (with type http://schema.org/Date)."""
    content = """<?xml version="1.0"?>
    <atom:entry xmlns:atom="http://www.w3.org/2005/Atom"
                xmlns="https://doi.org/10.5063/schema/codemeta-2.0"
                xmlns:schema="http://schema.org/">
      <name>My Software</name>
      <schema:url>http://example.org/my-software</schema:url>
      <schema:dateCreated>foo</schema:dateCreated>
      <schema:dateModified>2022-10-26</schema:dateModified>
    </atom:entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "My Software",
        "schema:url": "http://example.org/my-software",
        "schema:dateCreated": "foo",
        "schema:dateModified": "2022-10-26",
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


def test_sword_propertyvalue():
    content = """<?xml version="1.0"?>
    <entry xmlns="http://www.w3.org/2005/Atom"
           xmlns:codemeta="https://doi.org/10.5063/schema/codemeta-2.0"
           xmlns:schema="http://schema.org/">
      <name>Name</name>
      <schema:identifier>
          <codemeta:type>schema:PropertyValue</codemeta:type>
          <schema:propertyID>HAL-ID</schema:propertyID>
          <schema:value>hal-03780423</schema:value>
      </schema:identifier>
    </entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "Name",
        "identifier": {
            "schema:propertyID": "HAL-ID",
            "schema:value": "hal-03780423",
            "type": "schema:PropertyValue",
        },
    }


def test_sword_fix_date():
    content = """<?xml version="1.0"?>
    <entry xmlns="http://www.w3.org/2005/Atom"
           xmlns:codemeta="https://doi.org/10.5063/schema/codemeta-2.0"
           xmlns:schema="http://schema.org/">
      <name>Name</name>
      <codemeta:dateModified>2020-12-1</codemeta:dateModified>
      <codemeta:dateCreated>2020-12-2</codemeta:dateCreated>
      <codemeta:datePublished>2020-12-3</codemeta:datePublished>
    </entry>
    """

    result = MAPPINGS["SwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "Name",
        "dateModified": "2020-12-01",
        "dateCreated": "2020-12-02",
        "datePublished": "2020-12-03",
    }


def test_sword_codemeta_parsing_error(caplog):
    caplog.set_level(logging.ERROR)
    assert MAPPINGS["SwordCodemetaMapping"]().translate(b"123") is None
    assert caplog.text.endswith("Failed to parse XML document: b'123'\n")


def test_json_sword():
    content = """{"id": "hal-01243573", "@xmlns": "http://www.w3.org/2005/Atom", "author": {"name": "Author 1", "email": "foo@example.org"}, "client": "hal", "codemeta:url": "http://example.org/", "codemeta:name": "The assignment problem", "@xmlns:codemeta": "https://doi.org/10.5063/SCHEMA/CODEMETA-2.0", "codemeta:author": {"codemeta:name": "Author 2"}, "codemeta:license": {"codemeta:name": "GNU General Public License v3.0 or later"}}"""  # noqa
    result = MAPPINGS["JsonSwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "author": [
            {"name": "Author 1", "email": "foo@example.org"},
            {"name": "Author 2"},
        ],
        "license": {"name": "GNU General Public License v3.0 or later"},
        "name": "The assignment problem",
        "url": "http://example.org/",
        "name": "The assignment problem",
    }


def test_json_sword_no_xmlns():
    content = """{"title": "Example Software", "codemeta:url": "http://example.org/", "codemeta:author": [{"codemeta:name": "Author 1"}], "codemeta:version": "1.0"}"""  # noqa
    result = MAPPINGS["JsonSwordCodemetaMapping"]().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "author": [
            {"name": "Author 1"},
        ],
        "name": "Example Software",
        "url": "http://example.org/",
        "version": "1.0",
    }


def test_json_sword_codemeta_parsing_error(caplog):
    caplog.set_level(logging.ERROR)
    assert MAPPINGS["JsonSwordCodemetaMapping"]().translate(b"{123}") is None
    assert caplog.text.endswith("Failed to parse JSON document: b'{123}'\n")
