import collections

import click

from . import maven, npm, codemeta, python, ruby

MAPPINGS = {
    "CodemetaMapping": codemeta.CodemetaMapping,
    "MavenMapping": maven.MavenMapping,
    "NpmMapping": npm.NpmMapping,
    "PythonPkginfoMapping": python.PythonPkginfoMapping,
    "GemspecMapping": ruby.GemspecMapping,
}


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
def main(mapping_name, file_name):
    from pprint import pprint

    with open(file_name, "rb") as fd:
        file_content = fd.read()
    res = MAPPINGS[mapping_name]().translate(file_content)
    pprint(res)


if __name__ == "__main__":
    main()
