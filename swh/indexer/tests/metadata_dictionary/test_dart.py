# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.indexer.metadata_dictionary import MAPPINGS


def test_compute_metadata_pubspec():
    raw_content = b"""
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
    """

    result = MAPPINGS["PubMapping"]().translate(raw_content)

    assert set(result.pop("keywords")) == {
        "polyfill",
        "shim",
        "compatibility",
        "portable",
        "mbstring",
    }, result
    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "newtify",
        "description": """Have you been turned into a newt?  Would you like to be? \
This package can help. It has all of the \
newt-transmogrification functionality you have been looking \
for.""",
        "url": "https://example-pet-store.com/newtify",
        "license": "https://spdx.org/licenses/MIT",
    }

    assert result == expected


def test_normalize_author_pubspec():
    raw_content = b"""
    author: Atlee Pine <atlee@example.org>
    """

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
    raw_content = b"""
    authors:
      - Vicky Merzown <vmz@example.org>
      - Ron Bilius Weasley
    """

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


@pytest.mark.xfail(reason="https://github.com/w3c/json-ld-api/issues/547")
def test_normalize_author_authors_pubspec():
    raw_content = b"""
    authors:
      - Vicky Merzown <vmz@example.org>
      - Ron Bilius Weasley
    author: Hermione Granger
    """

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
    raw_content = b"""
    authors:
    """

    result = MAPPINGS["PubMapping"]().translate(raw_content)

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    assert result == expected


def test_invalid_yaml():
    raw_content = b"""
    name: smartech_push
    license: { :type => "Commercial", :file => "LICENSE" }
    """

    result = MAPPINGS["PubMapping"]().translate(raw_content)

    assert result is None


def test_invalid_tag():
    raw_content = b"""
    name: translatron
    description: !BETA VERSION - NOT FOR LIVE OR PROD USAGE!
    """

    result = MAPPINGS["PubMapping"]().translate(raw_content)

    assert result is None
