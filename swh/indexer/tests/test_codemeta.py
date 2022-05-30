# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.indexer.codemeta import CROSSWALK_TABLE, merge_documents, merge_values


def test_crosstable():
    assert CROSSWALK_TABLE["NodeJS"] == {
        "repository": "http://schema.org/codeRepository",
        "os": "http://schema.org/operatingSystem",
        "cpu": "http://schema.org/processorRequirements",
        "engines": "http://schema.org/runtimePlatform",
        "author": "http://schema.org/author",
        "author.email": "http://schema.org/email",
        "author.name": "http://schema.org/name",
        "contributors": "http://schema.org/contributor",
        "keywords": "http://schema.org/keywords",
        "license": "http://schema.org/license",
        "version": "http://schema.org/version",
        "description": "http://schema.org/description",
        "name": "http://schema.org/name",
        "bugs": "https://codemeta.github.io/terms/issueTracker",
        "homepage": "http://schema.org/url",
    }


def test_merge_values():
    assert merge_values("a", "b") == ["a", "b"]
    assert merge_values(["a", "b"], "c") == ["a", "b", "c"]
    assert merge_values("a", ["b", "c"]) == ["a", "b", "c"]

    assert merge_values({"@list": ["a"]}, {"@list": ["b"]}) == {"@list": ["a", "b"]}
    assert merge_values({"@list": ["a", "b"]}, {"@list": ["c"]}) == {
        "@list": ["a", "b", "c"]
    }

    with pytest.raises(ValueError):
        merge_values({"@list": ["a"]}, "b")
    with pytest.raises(ValueError):
        merge_values("a", {"@list": ["b"]})
    with pytest.raises(ValueError):
        merge_values({"@list": ["a"]}, ["b"])
    with pytest.raises(ValueError):
        merge_values(["a"], {"@list": ["b"]})

    assert merge_values("a", None) == "a"
    assert merge_values(["a", "b"], None) == ["a", "b"]
    assert merge_values(None, ["b", "c"]) == ["b", "c"]
    assert merge_values({"@list": ["a"]}, None) == {"@list": ["a"]}
    assert merge_values(None, {"@list": ["a"]}) == {"@list": ["a"]}


def test_merge_documents():
    """
    Test the creation of a coherent minimal metadata set
    """
    # given
    metadata_list = [
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "name": "test_1",
            "version": "0.0.2",
            "description": "Simple package.json test for indexer",
            "codeRepository": "git+https://github.com/moranegg/metadata_test",
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "name": "test_0_1",
            "version": "0.0.2",
            "description": "Simple package.json test for indexer",
            "codeRepository": "git+https://github.com/moranegg/metadata_test",
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "name": "test_metadata",
            "version": "0.0.2",
            "author": {
                "type": "Person",
                "name": "moranegg",
            },
        },
    ]

    # when
    results = merge_documents(metadata_list)

    # then
    expected_results = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "version": "0.0.2",
        "description": "Simple package.json test for indexer",
        "name": ["test_1", "test_0_1", "test_metadata"],
        "author": [{"type": "Person", "name": "moranegg"}],
        "codeRepository": "git+https://github.com/moranegg/metadata_test",
    }
    assert results == expected_results


def test_merge_documents_ids():
    # given
    metadata_list = [
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "id": "http://example.org/test1",
            "name": "test_1",
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "id": "http://example.org/test2",
            "name": "test_2",
        },
    ]

    # when
    results = merge_documents(metadata_list)

    # then
    expected_results = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "id": "http://example.org/test1",
        "schema:sameAs": "http://example.org/test2",
        "name": ["test_1", "test_2"],
    }
    assert results == expected_results


def test_merge_documents_duplicate_ids():
    # given
    metadata_list = [
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "id": "http://example.org/test1",
            "name": "test_1",
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "id": "http://example.org/test1",
            "name": "test_1b",
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "id": "http://example.org/test2",
            "name": "test_2",
        },
    ]

    # when
    results = merge_documents(metadata_list)

    # then
    expected_results = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "id": "http://example.org/test1",
        "schema:sameAs": "http://example.org/test2",
        "name": ["test_1", "test_1b", "test_2"],
    }
    assert results == expected_results


def test_merge_documents_lists():
    """Tests merging two @list elements."""
    # given
    metadata_list = [
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "@list": [
                    {"name": "test_1"},
                ]
            },
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "@list": [
                    {"name": "test_2"},
                ]
            },
        },
    ]

    # when
    results = merge_documents(metadata_list)

    # then
    expected_results = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "author": [
            {"name": "test_1"},
            {"name": "test_2"},
        ],
    }
    assert results == expected_results


def test_merge_documents_lists_duplicates():
    """Tests merging two @list elements with a duplicate subelement."""
    # given
    metadata_list = [
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "@list": [
                    {"name": "test_1"},
                ]
            },
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "@list": [
                    {"name": "test_2"},
                    {"name": "test_1"},
                ]
            },
        },
    ]

    # when
    results = merge_documents(metadata_list)

    # then
    expected_results = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "author": [
            {"name": "test_1"},
            {"name": "test_2"},
        ],
    }
    assert results == expected_results


def test_merge_documents_list_left():
    """Tests merging a singleton with an @list."""
    # given
    metadata_list = [
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "test_1"},
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "@list": [
                    {"name": "test_2"},
                ]
            },
        },
    ]

    # when
    results = merge_documents(metadata_list)

    # then
    expected_results = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "author": [
            {"name": "test_1"},
            {"name": "test_2"},
        ],
    }
    assert results == expected_results


def test_merge_documents_list_right():
    """Tests merging an @list with a singleton."""
    # given
    metadata_list = [
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "@list": [
                    {"name": "test_1"},
                ]
            },
        },
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "test_2"},
        },
    ]

    # when
    results = merge_documents(metadata_list)

    # then
    expected_results = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "author": [
            {"name": "test_1"},
            {"name": "test_2"},
        ],
    }
    assert results == expected_results
