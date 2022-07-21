# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

from hypothesis import HealthCheck, given, settings
import pytest

from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata_dictionary import MAPPINGS
from swh.indexer.storage.model import ContentMetadataRow
from swh.model.hashutil import hash_to_bytes

from ..test_metadata import TRANSLATOR_TOOL, ContentMetadataTestIndexer
from ..utils import (
    BASE_TEST_CONFIG,
    fill_obj_storage,
    fill_storage,
    json_document_strategy,
)


def test_compute_metadata_none():
    """
    testing content empty content is empty
    should return None
    """
    content = b""

    # None if no metadata was found or an error occurred
    declared_metadata = None
    result = MAPPINGS["NpmMapping"]().translate(content)
    assert declared_metadata == result


def test_compute_metadata_npm():
    """
    testing only computation of metadata with hard_mapping_npm
    """
    content = b"""
        {
            "name": "test_metadata",
            "version": "0.0.2",
            "description": "Simple package.json test for indexer",
              "repository": {
                "type": "git",
                "url": "https://github.com/moranegg/metadata_test"
            },
            "author": {
                "email": "moranegg@example.com",
                "name": "Morane G"
            }
        }
    """
    declared_metadata = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "test_metadata",
        "version": "0.0.2",
        "description": "Simple package.json test for indexer",
        "codeRepository": "git+https://github.com/moranegg/metadata_test",
        "author": [
            {
                "type": "Person",
                "name": "Morane G",
                "email": "moranegg@example.com",
            }
        ],
    }

    result = MAPPINGS["NpmMapping"]().translate(content)
    assert declared_metadata == result


def test_compute_metadata_invalid_description_npm():
    """
    testing only computation of metadata with hard_mapping_npm
    """
    content = b"""
        {
            "name": "test_metadata",
            "version": "0.0.2",
            "description": 1234
    }
    """
    declared_metadata = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "test_metadata",
        "version": "0.0.2",
    }

    result = MAPPINGS["NpmMapping"]().translate(content)
    assert declared_metadata == result


def test_index_content_metadata_npm():
    """
    testing NPM with package.json
    - one sha1 uses a file that can't be translated to metadata and
      should return None in the translated metadata
    """
    sha1s = [
        hash_to_bytes("26a9f72a7c87cc9205725cfd879f514ff4f3d8d5"),
        hash_to_bytes("d4c647f0fc257591cc9ba1722484229780d1c607"),
        hash_to_bytes("02fb2c89e14f7fab46701478c83779c7beb7b069"),
    ]
    # this metadata indexer computes only metadata for package.json
    # in npm context with a hard mapping
    config = BASE_TEST_CONFIG.copy()
    config["tools"] = [TRANSLATOR_TOOL]
    metadata_indexer = ContentMetadataTestIndexer(config=config)
    fill_obj_storage(metadata_indexer.objstorage)
    fill_storage(metadata_indexer.storage)

    metadata_indexer.run(sha1s)
    results = list(metadata_indexer.idx_storage.content_metadata_get(sha1s))

    expected_results = [
        ContentMetadataRow(
            id=hash_to_bytes("26a9f72a7c87cc9205725cfd879f514ff4f3d8d5"),
            tool=TRANSLATOR_TOOL,
            metadata={
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "codeRepository": "git+https://github.com/moranegg/metadata_test",
                "description": "Simple package.json test for indexer",
                "name": "test_metadata",
                "version": "0.0.1",
            },
        ),
        ContentMetadataRow(
            id=hash_to_bytes("d4c647f0fc257591cc9ba1722484229780d1c607"),
            tool=TRANSLATOR_TOOL,
            metadata={
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "type": "SoftwareSourceCode",
                "issueTracker": "https://github.com/npm/npm/issues",
                "author": [
                    {
                        "type": "Person",
                        "name": "Isaac Z. Schlueter",
                        "email": "i@izs.me",
                        "url": "http://blog.izs.me",
                    }
                ],
                "codeRepository": "git+https://github.com/npm/npm",
                "description": "a package manager for JavaScript",
                "license": "https://spdx.org/licenses/Artistic-2.0",
                "version": "5.0.3",
                "name": "npm",
                "keywords": [
                    "install",
                    "modules",
                    "package manager",
                    "package.json",
                ],
                "url": "https://docs.npmjs.com/",
            },
        ),
    ]

    for result in results:
        del result.tool["id"]

    # The assertion below returns False sometimes because of nested lists
    assert expected_results == results


def test_npm_bugs_normalization():
    # valid dictionary
    package_json = b"""{
        "name": "foo",
        "bugs": {
            "url": "https://github.com/owner/project/issues",
            "email": "foo@example.com"
        }
    }"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "foo",
        "issueTracker": "https://github.com/owner/project/issues",
        "type": "SoftwareSourceCode",
    }

    # "invalid" dictionary
    package_json = b"""{
        "name": "foo",
        "bugs": {
            "email": "foo@example.com"
        }
    }"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "foo",
        "type": "SoftwareSourceCode",
    }

    # string
    package_json = b"""{
        "name": "foo",
        "bugs": "https://github.com/owner/project/issues"
    }"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "foo",
        "issueTracker": "https://github.com/owner/project/issues",
        "type": "SoftwareSourceCode",
    }


def test_npm_repository_normalization():
    # normal
    package_json = b"""{
        "name": "foo",
        "repository": {
            "type" : "git",
            "url" : "https://github.com/npm/cli.git"
        }
    }"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "foo",
        "codeRepository": "git+https://github.com/npm/cli.git",
        "type": "SoftwareSourceCode",
    }

    # missing url
    package_json = b"""{
        "name": "foo",
        "repository": {
            "type" : "git"
        }
    }"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "foo",
        "type": "SoftwareSourceCode",
    }

    # github shortcut
    package_json = b"""{
        "name": "foo",
        "repository": "github:npm/cli"
    }"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    expected_result = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "foo",
        "codeRepository": "git+https://github.com/npm/cli.git",
        "type": "SoftwareSourceCode",
    }
    assert result == expected_result

    # github shortshortcut
    package_json = b"""{
        "name": "foo",
        "repository": "npm/cli"
    }"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == expected_result

    # gitlab shortcut
    package_json = b"""{
        "name": "foo",
        "repository": "gitlab:user/repo"
    }"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "foo",
        "codeRepository": "git+https://gitlab.com/user/repo.git",
        "type": "SoftwareSourceCode",
    }


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(json_document_strategy(keys=list(MAPPINGS["NpmMapping"].mapping)))  # type: ignore
def test_npm_adversarial(doc):
    raw = json.dumps(doc).encode()
    MAPPINGS["NpmMapping"]().translate(raw)


@pytest.mark.parametrize(
    "filename", [b"package.json", b"Package.json", b"PACKAGE.json", b"PACKAGE.JSON"]
)
def test_detect_metadata_package_json(filename):
    df = [
        {
            "sha1_git": b"abc",
            "name": b"index.js",
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
            "name": filename,
            "target": b"aab",
            "length": 712,
            "status": "visible",
            "type": "file",
            "perms": 33188,
            "dir_id": b"dir_a",
            "sha1": b"cde",
        },
    ]
    results = detect_metadata(df)

    expected_results = {"NpmMapping": [b"cde"]}
    assert expected_results == results
