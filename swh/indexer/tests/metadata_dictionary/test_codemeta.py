# Copyright (C) 2017-2026  The Software Heritage developers
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
from swh.model.hashutil import HashDict

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
            "givenName": "Gname",
            "familyName": "Doe",
            "email": "gdoe@example.com",
            "@id": "http://orcid.org/0000-0002-0000-001X"
          },
          {
            "@type": "Person",
            "givenName": "Given N.",
            "familyName": "Doe",
            "email": "g.n.doe@edu.example.com",
            "@id": "http://orcid.org/0000-0003-0000-002X"
          }
        ],
        "maintainer": {
          "@type": "Person",
          "givenName": "Gname",
          "familyName": "Doe",
          "email": "gdoe@example.com",
          "@id": "http://orcid.org/0000-0002-0000-001X"
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
                "givenName": "Gname",
                "familyName": "Doe",
                "email": "gdoe@example.com",
                "id": "http://orcid.org/0000-0002-0000-001X",
            },
            {
                "type": "Person",
                "givenName": "Given N.",
                "familyName": "Doe",
                "email": "g.n.doe@edu.example.com",
                "id": "http://orcid.org/0000-0003-0000-002X",
            },
        ],
        "maintainer": {
            "type": "Person",
            "givenName": "Gname",
            "familyName": "Doe",
            "email": "gdoe@example.com",
            "id": "http://orcid.org/0000-0002-0000-001X",
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

    expected_results = {"CodemetaMapping": [HashDict(sha1=b"bcd", sha1_git=b"aab")]}
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


@pytest.fixture
def raw_mention() -> bytes:
    """The expanded version of the notification found in swh-coarnotify docs.

    https://docs.softwareheritage.org/user/coarnotify/howto/mention.html
    """
    return json.dumps(
        [
            {
                "https://www.w3.org/ns/activitystreams#actor": [
                    {
                        "@id": "https://your-organization.tld",
                        "https://www.w3.org/ns/activitystreams#name": [
                            {"@value": "Your Organization"}
                        ],
                        "@type": ["https://www.w3.org/ns/activitystreams#Organization"],
                    }
                ],
                "https://www.w3.org/ns/activitystreams#object": [
                    {
                        "@id": "urn:uuid:74FFB356-0632-44D9-B176-888DA85758DC",
                        "https://www.w3.org/ns/activitystreams#object": [
                            {"@id": "https://github.com/rdicosmo/parmap"}
                        ],
                        "https://www.w3.org/ns/activitystreams#relationship": [
                            {"@id": "http://schema.org/mentions"}
                        ],
                        "https://www.w3.org/ns/activitystreams#subject": [
                            {"@id": "https://your-organization.tld/item/12345/"}
                        ],
                        "@type": ["https://www.w3.org/ns/activitystreams#Relationship"],
                    }
                ],
                "https://www.w3.org/ns/activitystreams#context": [
                    {
                        "@id": "https://your-organization.tld/item/12345/",
                        "http://www.iana.org/assignments/relation/cite-as": [
                            {"@id": "https://doi.org/XXX/YYY"}
                        ],
                        "http://www.iana.org/assignments/relation/item": [
                            {
                                "@id": "https://your-organization.tld/item/12345/document.pdf",
                                "https://www.w3.org/ns/activitystreams#mediaType": [
                                    {"@value": "application/pdf"}
                                ],
                                "@type": [
                                    "https://www.w3.org/ns/activitystreams#Object",
                                    "http://schema.org/ScholarlyArticle",
                                ],
                            }
                        ],
                        "http://schema.org/author": [
                            {
                                "@type": [
                                    "https://www.w3.org/ns/activitystreams#Person"
                                ],
                                "http://schema.org/email": [
                                    {"@value": "author@example.com"}
                                ],
                                "http://schema.org/givenName": [
                                    {"@value": "Author Name"}
                                ],
                            }
                        ],
                        "http://schema.org/name": [{"@value": "My paper title"}],
                        "@type": [
                            "https://www.w3.org/ns/activitystreams#Page",
                            "http://schema.org/AboutPage",
                        ],
                    }
                ],
                "@id": "urn:uuid:6908e2d0-ab41-4fbf-8b27-e6d6cf1f7b95",
                "https://www.w3.org/ns/activitystreams#origin": [
                    {
                        "@id": "https://your-organization.tld/repository",
                        "http://www.w3.org/ns/ldp#inbox": [
                            {"@id": "https://inbox.your-organization.tld"}
                        ],
                        "@type": ["https://www.w3.org/ns/activitystreams#Service"],
                    }
                ],
                "https://www.w3.org/ns/activitystreams#target": [
                    {
                        "@id": "https://www.softwareheritage.org",
                        "http://www.w3.org/ns/ldp#inbox": [
                            {"@id": "https://inbox.staging.swh.network"}
                        ],
                        "@type": ["https://www.w3.org/ns/activitystreams#Service"],
                    }
                ],
                "@type": [
                    "https://www.w3.org/ns/activitystreams#Announce",
                    "http://coar-notify.net/specification/vocabulary/RelationshipAction",
                ],
            }
        ]
    ).encode()


def test_coarnotify_mention(raw_mention):
    result = MAPPINGS["CoarNotifyMentionMapping"]().translate(raw_mention)
    assert result == {
        "@context": [
            "https://doi.org/10.5063/schema/codemeta-2.0",
            {
                "as": "https://www.w3.org/ns/activitystreams#",
                "forge": "https://forgefed.org/ns#",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
        ],
        "id": "https://github.com/rdicosmo/parmap",
        "type": "https://schema.org/SoftwareSourceCode",
        "http://purl.org/spar/datacite/IsCitedBy": {
            "id": "https://your-organization.tld/item/12345/",
            "type": ["as:Page", "schema:AboutPage"],
            "schema:author": {
                "type": "as:Person",
                "email": "author@example.com",
                "givenName": "Author Name",
            },
            "name": "My paper title",
            "http://www.iana.org/assignments/relation/cite-as": {
                "id": "https://doi.org/XXX/YYY"
            },
            "http://www.iana.org/assignments/relation/item": {
                "id": "https://your-organization.tld/item/12345/document.pdf",
                "type": ["as:Object", "schema:ScholarlyArticle"],
                "as:mediaType": "application/pdf",
            },
        },
    }


def test_coarnotify_mention_invalid_json(caplog):
    caplog.set_level(logging.ERROR)
    result = MAPPINGS["CoarNotifyMentionMapping"]().translate("{{}}".encode())
    assert result is None
    assert "Failed to parse the notification document" in caplog.records[0].message


def test_coarnotify_mention_missing_id(raw_mention, caplog):
    caplog.set_level(logging.ERROR)
    altered = json.loads(raw_mention)
    del altered[0]["@id"]
    result = MAPPINGS["CoarNotifyMentionMapping"]().translate(
        json.dumps(altered).encode()
    )
    assert result is None
    assert "Unable to find the notification @id" in caplog.records[0].message


@pytest.mark.parametrize(
    "key,expected",
    [
        ("https://www.w3.org/ns/activitystreams#object", "as:object"),
        ("https://www.w3.org/ns/activitystreams#context", "as:context"),
    ],
)
def test_coarnotify_mention_missing_root_elements(raw_mention, caplog, key, expected):
    caplog.set_level(logging.ERROR)
    altered = json.loads(raw_mention)
    altered[0].pop(key)
    result = MAPPINGS["CoarNotifyMentionMapping"]().translate(
        json.dumps(altered).encode()
    )
    assert result is None
    assert f"Unable to find {expected}" in caplog.records[0].message


@pytest.mark.parametrize(
    "key,expected",
    [
        ("https://www.w3.org/ns/activitystreams#object", "as:object/object"),
        ("https://www.w3.org/ns/activitystreams#subject", "as:object/subject"),
        (
            "https://www.w3.org/ns/activitystreams#relationship",
            "as:object/relationship",
        ),
    ],
)
def test_coarnotify_mention_missing_object_elements(raw_mention, caplog, key, expected):
    caplog.set_level(logging.ERROR)
    altered = json.loads(raw_mention)
    altered[0]["https://www.w3.org/ns/activitystreams#object"][0].pop(key)
    result = MAPPINGS["CoarNotifyMentionMapping"]().translate(
        json.dumps(altered).encode()
    )
    assert result is None
    assert f"Unable to find {expected}" in caplog.records[0].message


@pytest.mark.parametrize(
    "relationship,success,expected_reverse",
    [
        (
            "https://schema.org/mentions",
            True,
            "http://purl.org/spar/datacite/IsCitedBy",
        ),
        ("http://schema.org/mentions", True, "http://purl.org/spar/datacite/IsCitedBy"),
        (
            "http://purl.org/spar/datacite/Cites",
            True,
            "http://purl.org/spar/datacite/IsCitedBy",
        ),
        ("http://purl.org/vocab/frbr/core#supplement", False, None),
    ],
)
def test_coarnotify_mention_relationships(
    raw_mention, caplog, relationship, success, expected_reverse
):
    caplog.set_level(logging.ERROR)
    altered = json.loads(raw_mention)
    altered[0]["https://www.w3.org/ns/activitystreams#object"][0][
        "https://www.w3.org/ns/activitystreams#relationship"
    ][0]["@id"] = relationship
    result = MAPPINGS["CoarNotifyMentionMapping"]().translate(
        json.dumps(altered).encode()
    )
    if success:
        assert result[expected_reverse]
    else:
        assert result is None
        assert (
            f"Unable to find a reverse relationship for {relationship}"
            in caplog.records[0].message
        )
