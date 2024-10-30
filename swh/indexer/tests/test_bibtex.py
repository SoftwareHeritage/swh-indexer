# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import textwrap

import pytest

from swh.indexer.bibtex import cff_to_bibtex, codemeta_to_bibtex
from swh.model.swhids import QualifiedSWHID


def test_empty():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        }
    ) == textwrap.dedent(
        """\
            @software{REPLACEME
            }
            """
    )


def test_minimal():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
        }
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            author = "Doe, Jane",
            date = "2023-10-10",
            year = "2023",
            month = oct,
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


def test_empty_author_list():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": [],
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
        }
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            date = "2023-10-10",
            year = "2023",
            month = oct,
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


@pytest.mark.parametrize("key", ["version", "softwareVersion"])
def test_version_minimal(key):
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
            key: "1.2.3",
        }
    ) == textwrap.dedent(
        """\
        @softwareversion{REPLACEME,
            author = "Doe, Jane",
            date = "2023-10-10",
            year = "2023",
            month = oct,
            title = "Example Software",
            url = "http://example.org/",
            version = "1.2.3"
        }
        """
    )


def test_id():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
            "identifier": "example-software",
        }
    ) == textwrap.dedent(
        """\
        @software{example-software,
            author = "Doe, Jane",
            date = "2023-10-10",
            year = "2023",
            month = oct,
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


@pytest.mark.parametrize("key", ["@id", "id", "identifier"])
def test_id_url(key):
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
            key: "http://example.org/example-software",
        }
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            author = "Doe, Jane",
            date = "2023-10-10",
            year = "2023",
            month = oct,
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


@pytest.mark.parametrize("key", ["@id", "id", "identifier"])
def test_id_doi(key):
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
            key: "https://doi.org/10.1000/182",
        }
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            author = "Doe, Jane",
            date = "2023-10-10",
            year = "2023",
            month = oct,
            doi = "https://doi.org/10.1000/182",
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


def test_license():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
            "license": "https://spdx.org/licenses/Apache-2.0",
        }
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            author = "Doe, Jane",
            license = "Apache-2.0",
            date = "2023-10-10",
            year = "2023",
            month = oct,
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


def test_licenses():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
            "license": [
                "https://spdx.org/licenses/Apache-2.0",
                "https://spdx.org/licenses/GPL-3.0",
            ],
        }
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            author = "Doe, Jane",
            license = "Apache-2.0 and GPL-3.0",
            date = "2023-10-10",
            year = "2023",
            month = oct,
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


def test_organization_author():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "@type": "Organization",
                "name": "Example University",
            },
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
        }
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            author = "Example University",
            date = "2023-10-10",
            year = "2023",
            month = oct,
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


def test_affiliation():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "name": "Jane Doe",
                "affiliation": {"@type": "Organization", "name": "Example University"},
            },
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "2023-10-10",
        }
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            author = "Doe, Jane",
            organization = "Example University",
            date = "2023-10-10",
            year = "2023",
            month = oct,
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


def test_cff_empty():
    assert cff_to_bibtex("") == textwrap.dedent(
        """\
        @software{REPLACEME
        }
        """
    )


def test_cff_invalid():
    assert cff_to_bibtex("foo") == textwrap.dedent(
        """\
        @software{REPLACEME
        }
        """
    )


def test_cff_minimal():
    assert (
        cff_to_bibtex(
            """
cff-version: 1.2.0
message: "If you use this software, please cite it as below."
authors:
  - family-names: Druskat
    given-names: Stephan
title: "My Research Software"
date-released: 2021-08-11
url: "http://example.org/"
            """
        )
        == textwrap.dedent(
            """\
            @software{REPLACEME,
                author = "Druskat, Stephan",
                date = "2021-08-11",
                year = "2021",
                month = aug,
                title = "My Research Software",
                url = "http://example.org/"
            }
            """
        )
    )


def test_swhid_type_snp():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        },
        QualifiedSWHID.from_string(
            "swh:1:snp:da39a3ee5e6b4b0d3255bfef95601890afd80709"
        ),
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            swhid = "swh:1:snp:da39a3ee5e6b4b0d3255bfef95601890afd80709"
        }
        """
    )


def test_swhid_type_rev():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        },
        QualifiedSWHID.from_string(
            "swh:1:rev:5b909292bcfe6099d726c0b5194165c72f93b767"
        ),
    ) == textwrap.dedent(
        """\
        @softwareversion{REPLACEME,
            swhid = "swh:1:rev:5b909292bcfe6099d726c0b5194165c72f93b767"
        }
        """
    )


def test_swhid_type_cnt():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        },
        QualifiedSWHID.from_string(
            "swh:1:cnt:5b909292bcfe6099d726c0b5194165c72f93b767;lines=5-10"
        ),
    ) == textwrap.dedent(
        """\
        @codefragment{REPLACEME,
            swhid = "swh:1:cnt:5b909292bcfe6099d726c0b5194165c72f93b767;lines=5-10"
        }
        """
    )
