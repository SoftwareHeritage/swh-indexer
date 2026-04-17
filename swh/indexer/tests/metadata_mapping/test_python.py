# Copyright (C) 2017-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata_mapping import get_mapping

from ..utils import convert_dict_to_directory_entries, filter_directory_entries_by_name

PythonPkginfoMapping = get_mapping("PythonPkginfoMapping")


def test_compute_metadata_pkginfo():
    raw_content = b"""\
Metadata-Version: 2.1
Name: swh.core
Version: 0.0.49
Summary: Software Heritage core utilities
Home-page: https://forge.softwareheritage.org/diffusion/DCORE/
Author: Software Heritage developers
Author-email: swh-devel@inria.fr
License: UNKNOWN
Project-URL: Bug Reports, https://forge.softwareheritage.org/maniphest
Project-URL: Funding, https://www.softwareheritage.org/donate
Project-URL: Source, https://forge.softwareheritage.org/source/swh-core
Description: swh-core
        ========
       \x20
        core library for swh's modules:
        - config parser
        - hash computations
        - serialization
        - logging mechanism
       \x20
Platform: UNKNOWN
Classifier: Programming Language :: Python :: 3
Classifier: Intended Audience :: Developers
Classifier: License :: OSI Approved :: GNU General Public License v3 (GPLv3)
Classifier: Operating System :: OS Independent
Classifier: Development Status :: 5 - Production/Stable
Description-Content-Type: text/markdown
Provides-Extra: testing
"""  # noqa
    result = PythonPkginfoMapping().translate(raw_content)
    assert set(result.pop("description")) == {
        "Software Heritage core utilities",  # note the comma here
        "swh-core\n"
        "========\n"
        "\n"
        "core library for swh's modules:\n"
        "- config parser\n"
        "- hash computations\n"
        "- serialization\n"
        "- logging mechanism\n"
        "",
    }, result
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "url": "https://forge.softwareheritage.org/diffusion/DCORE/",
        "name": "swh.core",
        "author": [
            {
                "type": "Person",
                "name": "Software Heritage developers",
                "email": "swh-devel@inria.fr",
            }
        ],
        "version": "0.0.49",
    }


def test_compute_metadata_pkginfo_utf8():
    raw_content = b"""\
Metadata-Version: 1.1
Name: snowpyt
Description-Content-Type: UNKNOWN
Description: foo
        Hydrology N\xc2\xb083
"""  # noqa
    result = PythonPkginfoMapping().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "snowpyt",
        "description": "foo\nHydrology N°83",
    }


def test_compute_metadata_pkginfo_keywords():
    raw_content = b"""\
Metadata-Version: 2.1
Name: foo
Keywords: foo bar baz
"""  # noqa
    result = PythonPkginfoMapping().translate(raw_content)
    assert set(result.pop("keywords")) == {"foo", "bar", "baz"}, result
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "foo",
    }


def test_compute_metadata_pkginfo_license():
    raw_content = b"""\
Metadata-Version: 2.1
Name: foo
License: MIT
"""  # noqa
    result = PythonPkginfoMapping().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "foo",
        "license": "https://spdx.org/licenses/MIT",
    }


def test_detect_metadata_files():
    directory_entries = convert_dict_to_directory_entries(
        [
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
                "name": b"PKG-INFO",
                "target": b"aab",
                "length": 712,
                "status": "visible",
                "type": "file",
                "perms": 33188,
                "dir_id": b"dir_a",
                "sha1": b"cde",
            },
        ]
    )

    result = detect_metadata(directory_entries)

    expected_result = filter_directory_entries_by_name(b"PKG-INFO", directory_entries)

    assert {"PythonPkginfoMapping": expected_result} == result
