# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.indexer.metadata_detector import detect_metadata
from swh.indexer.metadata_dictionary import MAPPINGS


def test_compute_metadata_nuget():
    raw_content = b"""<?xml version="1.0" encoding="utf-8"?>
    <package xmlns="http://schemas.microsoft.com/packaging/2010/07/nuspec.xsd">
        <metadata>
            <id>sample</id>
            <version>1.2.3</version>
            <authors>Kim Abercrombie, Franck Halmaert</authors>
            <description>Sample exists only to show a sample .nuspec file.</description>
            <summary>Summary is being deprecated. Use description instead.</summary>
            <projectUrl>http://example.org/</projectUrl>
            <repository type="git" url="https://github.com/NuGet/NuGet.Client.git"/>
            <license type="expression">MIT</license>
            <licenseUrl>https://raw.github.com/timrwood/moment/master/LICENSE</licenseUrl>
            <dependencies>
                <dependency id="another-package" version="3.0.0" />
                <dependency id="yet-another-package" version="1.0.0" />
            </dependencies>
            <releaseNotes>
                See the [changelog](https://github.com/httpie/httpie/releases/tag/3.2.0).
            </releaseNotes>
            <tags>python3 java cpp search-tag</tags>
        </metadata>
        <files>
            <file src="bin\\Debug\\*.dll" target="lib" />
        </files>
    </package>"""

    result = MAPPINGS["NuGetMapping"]().translate(raw_content)

    assert set(result.pop("keywords")) == {
        "python3",
        "java",
        "cpp",
        "search-tag",
    }, result

    assert set(result.pop("license")) == {
        "https://spdx.org/licenses/MIT",
        "https://raw.github.com/timrwood/moment/master/LICENSE",
    }, result

    assert set(result.pop("description")) == {
        "Sample exists only to show a sample .nuspec file.",
        "Summary is being deprecated. Use description instead.",
    }, result

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [
            {"type": "Person", "name": "Kim Abercrombie"},
            {"type": "Person", "name": "Franck Halmaert"},
        ],
        "codeRepository": "https://github.com/NuGet/NuGet.Client.git",
        "url": "http://example.org/",
        "version": "1.2.3",
        "schema:releaseNotes": (
            "See the [changelog](https://github.com/httpie/httpie/releases/tag/3.2.0)."
        ),
    }

    assert result == expected


@pytest.mark.parametrize(
    "filename",
    [b"package_name.nuspec", b"number_5.nuspec", b"CAPS.nuspec", b"\x8anan.nuspec"],
)
def test_detect_metadata_package_nuspec(filename):
    df = [
        {
            "sha1_git": b"abc",
            "name": b"example.json",
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

    expected_results = {"NuGetMapping": [b"cde"]}
    assert expected_results == results


def test_normalize_license_multiple_licenses_or_delimiter():
    raw_content = b"""<?xml version="1.0" encoding="utf-8"?>
    <package xmlns="http://schemas.microsoft.com/packaging/2010/07/nuspec.xsd">
        <metadata>
            <license type="expression">BitTorrent-1.0 or GPL-3.0-with-GCC-exception</license>
        </metadata>
        <files>
            <file src="bin\\Debug\\*.dll" target="lib" />
        </files>
    </package>"""
    result = MAPPINGS["NuGetMapping"]().translate(raw_content)
    assert set(result.pop("license")) == {
        "https://spdx.org/licenses/BitTorrent-1.0",
        "https://spdx.org/licenses/GPL-3.0-with-GCC-exception",
    }
    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    assert result == expected


def test_normalize_license_unsupported_delimiter():
    raw_content = b"""<?xml version="1.0" encoding="utf-8"?>
    <package xmlns="http://schemas.microsoft.com/packaging/2010/07/nuspec.xsd">
        <metadata>
            <license type="expression">(MIT)</license>
        </metadata>
        <files>
            <file src="bin\\Debug\\*.dll" target="lib" />
        </files>
    </package>"""
    result = MAPPINGS["NuGetMapping"]().translate(raw_content)
    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }

    assert result == expected


def test_copyrightNotice_absolute_uri_property():
    raw_content = b"""<?xml version="1.0" encoding="utf-8"?>
    <package xmlns="http://schemas.microsoft.com/packaging/2010/07/nuspec.xsd">
        <metadata>
            <copyright>Copyright 2017-2022</copyright>
            <language>en-us</language>
        </metadata>
        <files>
            <file src="bin\\Debug\\*.dll" target="lib" />
        </files>
    </package>"""
    result = MAPPINGS["NuGetMapping"]().translate(raw_content)
    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "schema:copyrightNotice": "Copyright 2017-2022",
        "schema:inLanguage": "en-us",
    }

    assert result == expected
