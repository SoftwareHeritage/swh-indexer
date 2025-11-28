# Copyright (C) 2023-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import textwrap

import pytest

from swh.indexer.bibtex import BibTeXCitationError, cff_to_bibtex, codemeta_to_bibtex
from swh.indexer.codemeta import CODEMETA_V3_CONTEXT
from swh.model.swhids import QualifiedSWHID


def test_empty():
    with pytest.raises(
        BibTeXCitationError,
        match="No BibTex fields could be extracted from citation metadata file",
    ):
        codemeta_to_bibtex(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            }
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


def test_author_id():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "name": "Jane Doe",
                "id": "https://orcid.org/0000-0002-0000-000X",
            },
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


def test_author_invalid_id():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {
                "name": "Jane Doe",
                "id": "https://orcid.org/ 0000-0002-0000-000X",
            },
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


def test_invalid_date():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
            "url": "http://example.org/",
            "datePublished": "TBD",
            "license": "https://spdx.org/licenses/Apache-2.0",
        }
    ) == textwrap.dedent(
        """\
        @software{REPLACEME,
            author = "Doe, Jane",
            license = "Apache-2.0",
            date = "TBD",
            title = "Example Software",
            url = "http://example.org/"
        }
        """
    )


def test_context_contains_schema_org():
    assert codemeta_to_bibtex(
        {
            "@context": [
                "https://doi.org/10.5063/schema/codemeta-2.0",
                "http://schema.org",
            ],
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


@pytest.mark.parametrize(
    "author",
    [
        {
            "type": "Role",
            "schema:author": {
                "name": "Jane Doe",
                "id": "https://orcid.org/0000-0002-0000-000X",
            },
        },
        [
            {
                "type": "Role",
                "schema:author": {
                    "name": "Jane Doe",
                    "id": "https://orcid.org/0000-0002-0000-000X",
                },
            }
        ],
        {
            "type": "Role",
            "schema:author": {
                "name": "Jane Doe",
            },
        },
        [
            {
                "type": "Role",
                "schema:author": {
                    "name": "Jane Doe",
                },
            }
        ],
    ],
)
def test_author_role(author):
    assert codemeta_to_bibtex(
        {
            "@context": "https://w3id.org/codemeta/3.0",
            "author": author,
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


def test_cff_empty():
    with pytest.raises(
        BibTeXCitationError,
        match="No BibTex fields could be extracted from citation metadata file",
    ):
        cff_to_bibtex("")


def test_cff_invalid():
    with pytest.raises(
        BibTeXCitationError,
        match="No BibTex fields could be extracted from citation metadata file",
    ):
        cff_to_bibtex("foo")


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


def test_cff_orchid_with_trailing_whitespace():
    assert (
        cff_to_bibtex(
            """
cff-version: 1.2.0
message: "If you use this software, please cite it as below."
authors:
  - family-names: Druskat
    given-names: Stephan
    orcid: "https://orcid.org/0000-0001-0000-000X "
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
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
        },
        QualifiedSWHID.from_string(
            "swh:1:snp:da39a3ee5e6b4b0d3255bfef95601890afd80709"
        ),
    ) == textwrap.dedent(
        """\
        @software{swh-snp-da39a3e,
            author = "Doe, Jane",
            title = "Example Software",
            swhid = "swh:1:snp:da39a3ee5e6b4b0d3255bfef95601890afd80709"
        }
        """
    )


def test_swhid_type_rev():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
        },
        QualifiedSWHID.from_string(
            "swh:1:rev:5b909292bcfe6099d726c0b5194165c72f93b767"
        ),
    ) == textwrap.dedent(
        """\
        @softwareversion{swh-rev-5b90929,
            author = "Doe, Jane",
            title = "Example Software",
            swhid = "swh:1:rev:5b909292bcfe6099d726c0b5194165c72f93b767"
        }
        """
    )


def test_swhid_type_cnt():
    assert codemeta_to_bibtex(
        {
            "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            "author": {"name": "Jane Doe"},
            "name": "Example Software",
        },
        QualifiedSWHID.from_string(
            "swh:1:cnt:5b909292bcfe6099d726c0b5194165c72f93b767;lines=5-10"
        ),
    ) == textwrap.dedent(
        """\
        @codefragment{swh-cnt-5b90929-L5-L10,
            author = "Doe, Jane",
            title = "Example Software",
            swhid = "swh:1:cnt:5b909292bcfe6099d726c0b5194165c72f93b767;lines=5-10"
        }
        """
    )


def test_codemeta_v3_context():
    assert codemeta_to_bibtex(
        {
            "@context": "https://w3id.org/codemeta/3.0",
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


def test_codemeta_context_with_trailing_slash():
    assert codemeta_to_bibtex(
        {
            "@context": "https://w3id.org/codemeta/3.0/",
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


def test_cff_doi_license_full_url():
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
doi: https://doi.org/10.5281/example
license: https://spdx.org/licenses/GPL-3.0-or-later
            """
        )
        == textwrap.dedent(
            """\
            @software{REPLACEME,
                author = "Druskat, Stephan",
                license = "GPL-3.0-or-later",
                date = "2021-08-11",
                year = "2021",
                month = aug,
                doi = "https://doi.org/10.5281/example",
                title = "My Research Software"
            }
            """
        )
    )


def test_codemeta_to_bibtex_resolve_unknown_context_url(requests_mock):
    unknown_context_url = "https://example.org/codemeta/3.0"
    codemeta = {
        "@context": unknown_context_url,
        "author": {"name": "Jane Doe"},
        "name": "Example Software",
        "url": "http://example.org/",
        "datePublished": "2023-10-10",
    }
    requests_mock.get(unknown_context_url, json=CODEMETA_V3_CONTEXT)

    # should raise by default with an unknown context URL
    with pytest.raises(Exception, match=f"Unknown context URL: {unknown_context_url}"):
        codemeta_to_bibtex(codemeta)

    # should generate citation after fetching context with requests
    assert codemeta_to_bibtex(
        codemeta, resolve_unknown_context_url=True
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


def test_codemeta_to_bibtex_force_codemeta_context():
    unknown_context_url = "https://example.org/codemeta/3.0"
    codemeta = {
        "@context": unknown_context_url,
        "author": {"name": "Jane Doe"},
        "name": "Example Software",
        "url": "http://example.org/",
        "datePublished": "2023-10-10",
    }

    # should raise by default with an unknown context URL
    with pytest.raises(Exception, match=f"Unknown context URL: {unknown_context_url}"):
        codemeta_to_bibtex(codemeta)

    # should generate citation after overriding JSON-LD context to CodeMeta v3.0
    assert codemeta_to_bibtex(codemeta, force_codemeta_context=True) == textwrap.dedent(
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
