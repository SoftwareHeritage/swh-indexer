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

from ..test_metadata import TRANSLATOR_TOOL, ContentMetadataTestIndexer
from ..utils import (
    BASE_TEST_CONFIG,
    MAPPING_DESCRIPTION_CONTENT_SHA1,
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


def test_index_content_metadata_npm(storage, obj_storage):
    """
    testing NPM with package.json
    - one sha1 uses a file that can't be translated to metadata and
      should return None in the translated metadata
    """
    sha1s = [
        MAPPING_DESCRIPTION_CONTENT_SHA1["json:test-metadata-package.json"],
        MAPPING_DESCRIPTION_CONTENT_SHA1["json:npm-package.json"],
        MAPPING_DESCRIPTION_CONTENT_SHA1["python:code"],
    ]

    # this metadata indexer computes only metadata for package.json
    # in npm context with a hard mapping
    config = BASE_TEST_CONFIG.copy()
    config["tools"] = [TRANSLATOR_TOOL]
    metadata_indexer = ContentMetadataTestIndexer(config=config)
    metadata_indexer.run(sha1s, log_suffix="unknown content")
    results = list(metadata_indexer.idx_storage.content_metadata_get(sha1s))

    expected_results = [
        ContentMetadataRow(
            id=sha1s[0],
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
            id=sha1s[1],
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
                "url": "https://docs.npmjs.com/",
            },
        ),
    ]

    for result in results:
        del result.tool["id"]
        result.metadata.pop("keywords", None)

    # The assertion below returns False sometimes because of nested lists
    assert expected_results == results


def test_npm_null_list_item_normalization():
    package_json = b"""{
        "name": "foo",
        "keywords": [
            "foo",
            null
        ],
        "homepage": [
            "http://example.org/",
            null
        ]
    }"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "name": "foo",
        "type": "SoftwareSourceCode",
        "url": "http://example.org/",
        "keywords": "foo",
    }


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


def test_npm_author():
    package_json = rb"""{
  "version": "1.0.0",
  "author": "Foo Bar (@example)"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [{"name": "Foo Bar", "type": "Person"}],
        "version": "1.0.0",
    }


def test_npm_invalid_uris():
    package_json = rb"""{
  "version": "1.0.0",
  "homepage": "",
  "author": {
    "name": "foo",
    "url": "http://example.org"
  }
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [{"name": "foo", "type": "Person", "url": "http://example.org"}],
        "version": "1.0.0",
    }

    package_json = rb"""{
  "version": "1.0.0",
  "homepage": "http://example.org",
  "author": {
    "name": "foo",
    "url": ""
  }
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [{"name": "foo", "type": "Person"}],
        "url": "http://example.org",
        "version": "1.0.0",
    }

    package_json = rb"""{
  "version": "1.0.0",
  "homepage": "",
  "author": {
    "name": "foo",
    "url": ""
  },
  "bugs": ""
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [{"name": "foo", "type": "Person"}],
        "version": "1.0.0",
    }

    package_json = rb"""{
  "version": "1.0.0",
  "homepage": "http:example.org",
  "author": {
    "name": "foo",
    "url": "http:example.com"
  },
  "bugs": {
    "url": "http:example.com"
  }
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [{"name": "foo", "type": "Person"}],
        "version": "1.0.0",
    }

    package_json = rb"""{
  "version": "1.0.0",
  "repository": "git+https://g ithub.com/foo/bar.git"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "version": "1.0.0",
    }

    package_json = rb"""{
  "version": "1.0.0",
  "repository": "git+http://\\u001b[D\\u001b[D\\u001b[Ds\\u001b[C\\u001b[C\\u001b[D\\u001b://github.com/dearzoe/array-combination"
}"""  # noqa
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "version": "1.0.0",
    }


def test_npm_invalid_licenses():
    package_json = rb"""{
  "version": "1.0.0",
  "license": "SEE LICENSE IN LICENSE.md",
  "author": {
    "name": "foo",
    "url": "http://example.org"
  }
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [{"name": "foo", "type": "Person", "url": "http://example.org"}],
        "version": "1.0.0",
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


def test_valid_spdx_expressions():
    package_json = rb"""{
  "license": "Apache-2.0"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "license": "https://spdx.org/licenses/Apache-2.0",
    }

    package_json = rb"""{
  "license": "(Apache-2.0)"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "license": "https://spdx.org/licenses/Apache-2.0",
    }

    package_json = rb"""{
  "license": "MIT OR Apache-2.0"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "license": [
            "https://spdx.org/licenses/Apache-2.0",
            "https://spdx.org/licenses/MIT",
        ],
    }

    package_json = rb"""{
  "license": "(MIT OR Apache-2.0)"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "license": [
            "https://spdx.org/licenses/Apache-2.0",
            "https://spdx.org/licenses/MIT",
        ],
    }

    package_json = rb"""{
  "license": "MIT OR (LGPL-2.1-only OR BSD-3-Clause)"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "license": [
            "https://spdx.org/licenses/BSD-3-Clause",
            "https://spdx.org/licenses/LGPL-2.1-only",
            "https://spdx.org/licenses/MIT",
        ],
    }

    package_json = rb"""{
  "license": "MIT OR LGPL-2.1-only OR BSD-3-Clause"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "license": [
            "https://spdx.org/licenses/BSD-3-Clause",
            "https://spdx.org/licenses/LGPL-2.1-only",
            "https://spdx.org/licenses/MIT",
        ],
    }


def test_unsupported_spdx_expressions():
    package_json = rb"""{
  "license": "MIT AND Apache-2.0"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    package_json = rb"""{
  "license": "(MIT AND Apache-2.0)"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    package_json = rb"""{
  "license": "MIT AND Apache-2.0 OR GPL-3.0-only"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    package_json = rb"""{
  "license": "MIT AND (LGPL-2.1-only OR BSD-3-Clause)"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    package_json = rb"""{
  "license": "MIT AND Apache-2.0 WITH Bison-exception-2.2"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    package_json = rb"""{
  "license": "MIT OR Apache-2.0 WITH Bison-exception-2.2"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    package_json = rb"""{
  "license": "MIT OR (LGPL-2.1-only WITH Bison-exception-2.2)"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    package_json = rb"""{
  "license": "MIT AND (LGPL-2.1-only WITH Bison-exception-2.2)"
}"""
    result = MAPPINGS["NpmMapping"]().translate(package_json)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }
