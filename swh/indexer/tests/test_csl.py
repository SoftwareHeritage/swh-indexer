# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

import pytest

from swh.indexer.citation import CitationError, CitationFormat, codemeta_to_citation
from swh.indexer.citation.codemeta_data import extract_codemeta_data
from swh.indexer.citation.csl import codemeta_data_to_csl
from swh.indexer.codemeta import CODEMETA_V3_CONTEXT
from swh.indexer.metadata_mapping.cff import CffMapping
from swh.model.swhids import QualifiedSWHID


def _parse_csl(csl: str) -> dict:
    return json.loads(csl)


def cff_to_csl(cff_text: str) -> dict:
    codemeta = CffMapping().translate(cff_text.encode()) or {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0"
    }
    codemeta_data = extract_codemeta_data(codemeta)
    return _parse_csl(codemeta_data_to_csl(codemeta_data))


def test_empty():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
            },
            CitationFormat.CSL,
        )
    ) == {"type": "software"}


def test_minimal():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "author": {"name": "Jane Doe"},
                "name": "Example Software",
                "url": "http://example.org/",
                "datePublished": "2023-10-10",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "author": [{"given": "Jane", "family": "Doe"}],
        "issued": {"date-parts": [[2023, 10, 10]]},
        "URL": "http://example.org/",
    }


def test_empty_author_list():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "author": [],
                "name": "Example Software",
                "url": "http://example.org/",
                "datePublished": "2023-10-10",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "issued": {"date-parts": [[2023, 10, 10]]},
        "URL": "http://example.org/",
    }


@pytest.mark.parametrize("key", ["version", "softwareVersion"])
def test_version_minimal(key):
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "author": {"name": "Jane Doe"},
                "name": "Example Software",
                "url": "http://example.org/",
                "datePublished": "2023-10-10",
                key: "1.2.3",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "author": [{"given": "Jane", "family": "Doe"}],
        "issued": {"date-parts": [[2023, 10, 10]]},
        "URL": "http://example.org/",
        "version": "1.2.3",
    }


def test_invalid_date():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "author": {"name": "Jane Doe"},
                "name": "Example Software",
                "url": "http://example.org/",
                "datePublished": "TBD",
                "license": "https://spdx.org/licenses/Apache-2.0",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "author": [{"given": "Jane", "family": "Doe"}],
        "URL": "http://example.org/",
        "license": "Apache-2.0",
    }


def test_date_fallback_to_created():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "author": {"name": "Jane Doe"},
                "name": "Example Software",
                "dateCreated": "2022-01-15",
                "dateModified": "2023-02-01",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "author": [{"given": "Jane", "family": "Doe"}],
        "issued": {"date-parts": [[2022, 1, 15]]},
    }


def test_author_single_name():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "author": {"name": "Plato"},
                "name": "Example Software",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "author": [{"family": "Plato"}],
    }


def test_publisher_organization():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "Example Software",
                "publisher": {
                    "@type": "Organization",
                    "name": "Example University Press",
                },
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "publisher": "Example University Press",
    }


def test_url_fallback_to_related_link():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "Example Software",
                "relatedLink": "http://example.org/related",
                "downloadUrl": "http://example.org/download",
                "installUrl": "http://example.org/install",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "URL": "http://example.org/related",
    }


def test_url_fallback_to_install_url():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "Example Software",
                "installUrl": "http://example.org/install",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "URL": "http://example.org/install",
    }


def test_url_fallback_to_download_url():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "Example Software",
                "downloadUrl": "http://example.org/download",
                "installUrl": "http://example.org/install",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "URL": "http://example.org/download",
    }


def test_cff_empty():
    assert cff_to_csl("") == {"type": "software"}


def test_cff_invalid():
    assert cff_to_csl("foo") == {"type": "software"}


def test_cff_minimal():
    assert (
        cff_to_csl(
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
        == {
            "type": "software",
            "title": "My Research Software",
            "author": [{"given": "Stephan", "family": "Druskat"}],
            "issued": {"date-parts": [[2021, 8, 11]]},
            "URL": "http://example.org/",
        }
    )


def test_cff_orcid_with_trailing_whitespace():
    assert (
        cff_to_csl(
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
        == {
            "type": "software",
            "title": "My Research Software",
            "author": [{"given": "Stephan", "family": "Druskat"}],
            "issued": {"date-parts": [[2021, 8, 11]]},
            "URL": "http://example.org/",
        }
    )


def test_swhid():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "author": {"name": "Jane Doe"},
                "name": "Example Software",
            },
            CitationFormat.CSL,
            QualifiedSWHID.from_string(
                "swh:1:cnt:5b909292bcfe6099d726c0b5194165c72f93b767;lines=5-10"
            ),
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "author": [{"given": "Jane", "family": "Doe"}],
        "id": "swh:1:cnt:5b909292bcfe6099d726c0b5194165c72f93b767;lines=5-10",
    }


def test_cff_doi_license_full_url():
    assert (
        cff_to_csl(
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
        == {
            "type": "software",
            "title": "My Research Software",
            "author": [{"given": "Stephan", "family": "Druskat"}],
            "issued": {"date-parts": [[2021, 8, 11]]},
            "DOI": "https://doi.org/10.5281/example",
            "license": "GPL-3.0-or-later",
        }
    )


def test_organization_author():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "author": {
                    "@type": "Organization",
                    "name": "Example University",
                },
                "name": "Example Software",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "author": [{"family": "Example University"}],
    }


def test_description():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "Example Software",
                "description": "A short description of the software.",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "abstract": "A short description of the software.",
    }


def test_codeRepository():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "Example Software",
                "codeRepository": "https://github.com/example/example-software",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "source": "https://github.com/example/example-software",
    }


@pytest.mark.parametrize("key", ["@id", "id", "identifier"])
def test_doi_identifier(key):
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "Example Software",
                "url": "http://example.org/",
                "datePublished": "2023-10-10",
                key: "https://doi.org/10.1000/182",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "issued": {"date-parts": [[2023, 10, 10]]},
        "URL": "http://example.org/",
        "DOI": "https://doi.org/10.1000/182",
    }


def test_license_full_url():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "Example Software",
                "license": "https://spdx.org/licenses/Apache-2.0",
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "license": "Apache-2.0",
    }


def test_licenses():
    assert _parse_csl(
        codemeta_to_citation(
            {
                "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
                "name": "Example Software",
                "license": [
                    "https://spdx.org/licenses/Apache-2.0",
                    "https://spdx.org/licenses/GPL-3.0",
                ],
            },
            CitationFormat.CSL,
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "license": "Apache-2.0 and GPL-3.0",
    }


def test_codemeta_to_csl_resolve_unknown_context_url(requests_mock):
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
    with pytest.raises(
        CitationError, match=f"Unknown context URL: {unknown_context_url}"
    ):
        codemeta_to_citation(codemeta, CitationFormat.CSL)

    # should generate citation after fetching context with requests
    assert _parse_csl(
        codemeta_to_citation(
            codemeta, CitationFormat.CSL, resolve_unknown_context_url=True
        )
    ) == {
        "type": "software",
        "title": "Example Software",
        "author": [{"given": "Jane", "family": "Doe"}],
        "issued": {"date-parts": [[2023, 10, 10]]},
        "URL": "http://example.org/",
    }


def test_codemeta_to_csl_force_codemeta_context():
    unknown_context_url = "https://example.org/codemeta/3.0"
    codemeta = {
        "@context": unknown_context_url,
        "author": {"name": "Jane Doe"},
        "name": "Example Software",
        "url": "http://example.org/",
        "datePublished": "2023-10-10",
    }

    # should raise by default with an unknown context URL
    with pytest.raises(
        CitationError, match=f"Unknown context URL: {unknown_context_url}"
    ):
        codemeta_to_citation(codemeta, CitationFormat.CSL)

    # should generate citation after overriding JSON-LD context to CodeMeta v3.0
    assert _parse_csl(
        codemeta_to_citation(codemeta, CitationFormat.CSL, force_codemeta_context=True)
    ) == {
        "type": "software",
        "title": "Example Software",
        "author": [{"given": "Jane", "family": "Doe"}],
        "issued": {"date-parts": [[2023, 10, 10]]},
        "URL": "http://example.org/",
    }
