import collections
from typing import DefaultDict, Dict, Set, Type

import click
from typing_extensions import Final

from . import cff, codemeta, maven, npm, python, ruby
from .base import BaseMapping

MAPPINGS: Final[Dict[str, Type[BaseMapping]]] = {
    "CodemetaMapping": codemeta.CodemetaMapping,
    "MavenMapping": maven.MavenMapping,
    "NpmMapping": npm.NpmMapping,
    "PythonPkginfoMapping": python.PythonPkginfoMapping,
    "GemspecMapping": ruby.GemspecMapping,
    "CffMapping": cff.CffMapping,
}


def list_terms() -> DefaultDict[str, Set[Type[BaseMapping]]]:
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
def main(mapping_name: str, file_name: str) -> None:
    from pprint import pprint

    with open(file_name, "rb") as fd:
        file_content = fd.read()
    res = MAPPINGS[mapping_name]().translate(file_content)
    pprint(res)


if __name__ == "__main__":
    main()
