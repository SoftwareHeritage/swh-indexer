# Copyright (C) 2017-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import pytest

from swh.indexer.metadata_dictionary import get_mapping


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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
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

    result = get_mapping("SwordCodemetaMapping")().translate(content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "Name",
        "dateModified": "2020-12-01",
        "dateCreated": "2020-12-02",
        "datePublished": "2020-12-03",
    }


def test_sword_codemeta_parsing_error(caplog):
    caplog.set_level(logging.ERROR)
    assert get_mapping("SwordCodemetaMapping")().translate(b"123") is None
    assert caplog.text.endswith("Failed to parse XML document: b'123'\n")


def test_json_sword():
    content = """{"id": "hal-01243573", "@xmlns": "http://www.w3.org/2005/Atom", "author": {"name": "Author 1", "email": "foo@example.org"}, "client": "hal", "codemeta:url": "http://example.org/", "codemeta:name": "The assignment problem", "@xmlns:codemeta": "https://doi.org/10.5063/SCHEMA/CODEMETA-2.0", "codemeta:author": {"codemeta:name": "Author 2"}, "codemeta:license": {"codemeta:name": "GNU General Public License v3.0 or later"}}"""  # noqa
    result = get_mapping("JsonSwordCodemetaMapping")().translate(content)
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
    result = get_mapping("JsonSwordCodemetaMapping")().translate(content)
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
    assert get_mapping("JsonSwordCodemetaMapping")().translate(b"{123}") is None
    assert caplog.text.endswith("Failed to parse JSON document: b'{123}'\n")
