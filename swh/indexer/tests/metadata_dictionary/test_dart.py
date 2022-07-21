# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.indexer.metadata_dictionary import MAPPINGS


def test_compute_metadata_pubspec():
    raw_content = """
---
name: newtify
description: >-
  Have you been turned into a newt?  Would you like to be?
  This package can help. It has all of the
  newt-transmogrification functionality you have been looking
  for.
keywords:
  - polyfill
  - shim
  - compatibility
  - portable
  - mbstring
version: 1.2.3
license: MIT
homepage: https://example-pet-store.com/newtify
documentation: https://example-pet-store.com/newtify/docs

environment:
  sdk: '>=2.10.0 <3.0.0'

dependencies:
  efts: ^2.0.4
  transmogrify: ^0.4.0

dev_dependencies:
  test: '>=1.15.0 <2.0.0'
    """.encode(
        "utf-8"
    )

    result = MAPPINGS["PubMapping"]().translate(raw_content)

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "newtify",
        "keywords": [
            "polyfill",
            "shim",
            "compatibility",
            "portable",
            "mbstring",
        ],
        "description": """Have you been turned into a newt?  Would you like to be? \
This package can help. It has all of the \
newt-transmogrification functionality you have been looking \
for.""",
        "url": "https://example-pet-store.com/newtify",
        "license": "https://spdx.org/licenses/MIT",
    }

    assert result == expected


def test_normalize_author_pubspec():
    raw_content = """
    author: Atlee Pine <atlee@example.org>
    """.encode(
        "utf-8"
    )

    result = MAPPINGS["PubMapping"]().translate(raw_content)

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [
            {"type": "Person", "name": "Atlee Pine", "email": "atlee@example.org"},
        ],
    }

    assert result == expected


def test_normalize_authors_pubspec():
    raw_content = """
    authors:
      - Vicky Merzown <vmz@example.org>
      - Ron Bilius Weasley
    """.encode(
        "utf-8"
    )

    result = MAPPINGS["PubMapping"]().translate(raw_content)

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [
            {"type": "Person", "name": "Vicky Merzown", "email": "vmz@example.org"},
            {
                "type": "Person",
                "name": "Ron Bilius Weasley",
            },
        ],
    }

    assert result == expected


def test_normalize_author_authors_pubspec():
    raw_content = """
    authors:
      - Vicky Merzown <vmz@example.org>
      - Ron Bilius Weasley
    author: Hermione Granger
    """.encode(
        "utf-8"
    )

    result = MAPPINGS["PubMapping"]().translate(raw_content)

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [
            {"type": "Person", "name": "Vicky Merzown", "email": "vmz@example.org"},
            {
                "type": "Person",
                "name": "Ron Bilius Weasley",
            },
            {
                "type": "Person",
                "name": "Hermione Granger",
            },
        ],
    }

    assert result == expected


def test_normalize_empty_authors():
    raw_content = """
    authors:
    """.encode(
        "utf-8"
    )

    result = MAPPINGS["PubMapping"]().translate(raw_content)

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    assert result == expected
