# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
from typing import Dict, Type

import click

from . import cff, codemeta, composer, dart, github, maven, npm, python, ruby
from .base import BaseExtrinsicMapping, BaseIntrinsicMapping, BaseMapping

INTRINSIC_MAPPINGS: Dict[str, Type[BaseIntrinsicMapping]] = {
    "CffMapping": cff.CffMapping,
    "CodemetaMapping": codemeta.CodemetaMapping,
    "GemspecMapping": ruby.GemspecMapping,
    "MavenMapping": maven.MavenMapping,
    "NpmMapping": npm.NpmMapping,
    "PubMapping": dart.PubspecMapping,
    "PythonPkginfoMapping": python.PythonPkginfoMapping,
    "ComposerMapping": composer.ComposerMapping,
}

EXTRINSIC_MAPPINGS: Dict[str, Type[BaseExtrinsicMapping]] = {
    "GitHubMapping": github.GitHubMapping,
}


MAPPINGS: Dict[str, Type[BaseMapping]] = {**INTRINSIC_MAPPINGS, **EXTRINSIC_MAPPINGS}


def list_terms():
    """Returns a dictionary with all supported CodeMeta terms as keys,
    and the mappings that support each of them as values."""
    d = collections.defaultdict(set)
    for mapping in MAPPINGS.values():
        for term in mapping.supported_terms():
            d[term].add(mapping)
    return d


@click.command()
@click.argument("mapping_name")
@click.argument("file_name")
def main(mapping_name: str, file_name: str):
    from pprint import pprint

    with open(file_name, "rb") as fd:
        file_content = fd.read()
    res = MAPPINGS[mapping_name]().translate(file_content)
    pprint(res)


if __name__ == "__main__":
    main()
