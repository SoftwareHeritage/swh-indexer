# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.indexer.metadata_dictionary import MAPPINGS


def test_compute_metadata_cff():
    """
    testing CITATION.cff translation
    """
    content = """# YAML 1.2
---
abstract: "Command line program to convert from Citation File \
Format to various other formats such as BibTeX, EndNote, RIS, \
schema.org, CodeMeta, and .zenodo.json."
authors:
  -
    affiliation: "Netherlands eScience Center"
    family-names: Klaver
    given-names: Tom
  -
    affiliation: "Humboldt-Universität zu Berlin"
    family-names: Druskat
    given-names: Stephan
    orcid: https://orcid.org/0000-0003-4925-7248
cff-version: "1.0.3"
date-released: 2019-11-12
doi: 10.5281/zenodo.1162057
keywords:
  - "citation"
  - "bibliography"
  - "cff"
  - "CITATION.cff"
license: Apache-2.0
message: "If you use this software, please cite it using these metadata."
license: Apache-2.0
message: "If you use this software, please cite it using these metadata."
repository-code: "https://github.com/citation-file-format/cff-converter-python"
title: cffconvert
version: "1.4.0-alpha0"
    """.encode(
        "utf-8"
    )

    result = MAPPINGS["CffMapping"]().translate(content)
    assert set(result.pop("keywords")) == {
        "citation",
        "bibliography",
        "cff",
        "CITATION.cff",
    }
    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [
            {
                "type": "Person",
                "affiliation": {
                    "type": "Organization",
                    "name": "Netherlands eScience Center",
                },
                "familyName": "Klaver",
                "givenName": "Tom",
            },
            {
                "id": "https://orcid.org/0000-0003-4925-7248",
                "type": "Person",
                "affiliation": {
                    "type": "Organization",
                    "name": "Humboldt-Universität zu Berlin",
                },
                "familyName": "Druskat",
                "givenName": "Stephan",
            },
        ],
        "codeRepository": (
            "https://github.com/citation-file-format/cff-converter-python"
        ),
        "datePublished": "2019-11-12",
        "description": """Command line program to convert from \
Citation File Format to various other formats such as BibTeX, EndNote, \
RIS, schema.org, CodeMeta, and .zenodo.json.""",
        "identifier": "https://doi.org/10.5281/zenodo.1162057",
        "license": "https://spdx.org/licenses/Apache-2.0",
        "version": "1.4.0-alpha0",
        "name": "cffconvert",
    }

    assert expected == result


def test_compute_metadata_cff_invalid_yaml():
    """
    test yaml translation for invalid yaml file
    """
    content = """cff-version: 1.0.3
message: To cite the SigMF specification, please include the following:
authors:
  - name: The GNU Radio Foundation, Inc.
    """.encode(
        "utf-8"
    )

    expected = None

    result = MAPPINGS["CffMapping"]().translate(content)
    assert expected == result


def test_compute_metadata_cff_empty():
    """
    test yaml translation for empty yaml file
    """
    content = """
    """.encode(
        "utf-8"
    )

    expected = None

    result = MAPPINGS["CffMapping"]().translate(content)
    assert expected == result


def test_compute_metadata_cff_list():
    """
    test yaml translation for empty yaml file
    """
    content = """
- Foo
- Bar
    """.encode(
        "utf-8"
    )

    expected = None

    result = MAPPINGS["CffMapping"]().translate(content)
    assert expected == result


def test_cff_empty_fields():
    """
    testing CITATION.cff translation
    """
    content = """# YAML 1.2
  authors:
  -
    affiliation: "Hogwarts"
    family-names:
    given-names: Harry
  -
    affiliation: "Ministry of Magic"
    family-names: Weasley
    orcid:
    given-names: Arthur


    """.encode(
        "utf-8"
    )

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [
            {
                "type": "Person",
                "affiliation": {
                    "type": "Organization",
                    "name": "Hogwarts",
                },
                "givenName": "Harry",
            },
            {
                "type": "Person",
                "affiliation": {
                    "type": "Organization",
                    "name": "Ministry of Magic",
                },
                "familyName": "Weasley",
                "givenName": "Arthur",
            },
        ],
    }

    result = MAPPINGS["CffMapping"]().translate(content)
    assert expected == result


def test_cff_invalid_fields():
    """
    testing CITATION.cff translation
    """
    content = """# YAML 1.2
  authors:
  -
    affiliation: "Hogwarts"
    family-names:
    - Potter
    - James
    given-names: Harry

    """.encode(
        "utf-8"
    )

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [
            {
                "type": "Person",
                "affiliation": {
                    "type": "Organization",
                    "name": "Hogwarts",
                },
                "givenName": "Harry",
            },
        ],
    }

    result = MAPPINGS["CffMapping"]().translate(content)
    assert expected == result


def test_cff_preferred_citation():
    content = """# YAML 1.2
cff-version: 1.2.0
title: scikit-learn
type: software
authors:
  - name: "The scikit-learn developers"
preferred-citation:
  type: article
  title: "Scikit-learn: Machine Learning in Python"
  authors:
  - family-names: "Pedregosa"
    given-names: "Fabian"
  - family-names: "Varoquaux"
    given-names: "Gaël"
  - family-names: "Gramfort"
    given-names: "Alexandre"
  - family-names: "Michel"
    given-names: "Vincent"
  - family-names: "Thirion"
    given-names: "Bertrand"
  - family-names: "Grisel"
    given-names: "Olivier"
  - family-names: "Blondel"
    given-names: "Mathieu"
  - family-names: "Prettenhofer"
    given-names: "Peter"
  - family-names: "Weiss"
    given-names: "Ron"
  - family-names: "Dubourg"
    given-names: "Vincent"
  - family-names: "Vanderplas"
    given-names: "Jake"
  - family-names: "Passos"
    given-names: "Alexandre"
  - family-names: "Cournapeau"
    given-names: "David"
  - family-names: "Brucher"
    given-names: "Matthieu"
  - family-names: "Perrot"
    given-names: "Matthieu"
  - family-names: "Duchesnay"
    given-names: "Édouard"
  journal: "Journal of Machine Learning Research"
  volume: 12
  start: 2825
  end: 2830
  year: 2011
  url: "https://jmlr.csail.mit.edu/papers/v12/pedregosa11a.html"
    """.encode(
        "utf-8"
    )

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [
            {"type": "Person", "familyName": "Pedregosa", "givenName": "Fabian"},
            {"type": "Person", "familyName": "Varoquaux", "givenName": "Gaël"},
            {"type": "Person", "familyName": "Gramfort", "givenName": "Alexandre"},
            {"type": "Person", "familyName": "Michel", "givenName": "Vincent"},
            {"type": "Person", "familyName": "Thirion", "givenName": "Bertrand"},
            {"type": "Person", "familyName": "Grisel", "givenName": "Olivier"},
            {"type": "Person", "familyName": "Blondel", "givenName": "Mathieu"},
            {"type": "Person", "familyName": "Prettenhofer", "givenName": "Peter"},
            {"type": "Person", "familyName": "Weiss", "givenName": "Ron"},
            {"type": "Person", "familyName": "Dubourg", "givenName": "Vincent"},
            {"type": "Person", "familyName": "Vanderplas", "givenName": "Jake"},
            {"type": "Person", "familyName": "Passos", "givenName": "Alexandre"},
            {"type": "Person", "familyName": "Cournapeau", "givenName": "David"},
            {"type": "Person", "familyName": "Brucher", "givenName": "Matthieu"},
            {"type": "Person", "familyName": "Perrot", "givenName": "Matthieu"},
            {"type": "Person", "familyName": "Duchesnay", "givenName": "Édouard"},
        ],
        "name": "Scikit-learn: Machine Learning in Python",
        "url": "https://jmlr.csail.mit.edu/papers/v12/pedregosa11a.html",
    }

    result = MAPPINGS["CffMapping"]().translate(content)
    assert result == expected
