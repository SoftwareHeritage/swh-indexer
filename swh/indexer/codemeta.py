# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import csv
import itertools
import json
import os.path
import re

import swh.indexer
from pyld import jsonld

_DATA_DIR = os.path.join(os.path.dirname(swh.indexer.__file__), "data")

CROSSWALK_TABLE_PATH = os.path.join(_DATA_DIR, "codemeta", "crosswalk.csv")

CODEMETA_CONTEXT_PATH = os.path.join(_DATA_DIR, "codemeta", "codemeta.jsonld")


with open(CODEMETA_CONTEXT_PATH) as fd:
    CODEMETA_CONTEXT = json.load(fd)

CODEMETA_CONTEXT_URL = "https://doi.org/10.5063/schema/codemeta-2.0"
CODEMETA_ALTERNATE_CONTEXT_URLS = {
    ("https://raw.githubusercontent.com/codemeta/codemeta/master/codemeta.jsonld")
}
CODEMETA_URI = "https://codemeta.github.io/terms/"
SCHEMA_URI = "http://schema.org/"


PROPERTY_BLACKLIST = {
    # CodeMeta properties that we cannot properly represent.
    SCHEMA_URI + "softwareRequirements",
    CODEMETA_URI + "softwareSuggestions",
    # Duplicate of 'author'
    SCHEMA_URI + "creator",
}

_codemeta_field_separator = re.compile(r"\s*[,/]\s*")


def make_absolute_uri(local_name):
    definition = CODEMETA_CONTEXT["@context"][local_name]
    if isinstance(definition, str):
        return definition
    elif isinstance(definition, dict):
        prefixed_name = definition["@id"]
        (prefix, local_name) = prefixed_name.split(":")
        if prefix == "schema":
            canonical_name = SCHEMA_URI + local_name
        elif prefix == "codemeta":
            canonical_name = CODEMETA_URI + local_name
        else:
            assert False, prefix
        return canonical_name
    else:
        assert False, definition


def _read_crosstable(fd):
    reader = csv.reader(fd)
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("empty file")

    data_sources = set(header) - {"Parent Type", "Property", "Type", "Description"}
    assert "codemeta-V1" in data_sources

    codemeta_translation = {data_source: {} for data_source in data_sources}
    terms = set()

    for line in reader:  # For each canonical name
        local_name = dict(zip(header, line))["Property"]
        if not local_name:
            continue
        canonical_name = make_absolute_uri(local_name)
        if canonical_name in PROPERTY_BLACKLIST:
            continue
        terms.add(canonical_name)
        for (col, value) in zip(header, line):  # For each cell in the row
            if col in data_sources:
                # If that's not the parentType/property/type/description
                for local_name in _codemeta_field_separator.split(value):
                    # For each of the data source's properties that maps
                    # to this canonical name
                    if local_name.strip():
                        codemeta_translation[col][local_name.strip()] = canonical_name

    return (terms, codemeta_translation)


with open(CROSSWALK_TABLE_PATH) as fd:
    (CODEMETA_TERMS, CROSSWALK_TABLE) = _read_crosstable(fd)


def _document_loader(url, options=None):
    """Document loader for pyld.

    Reads the local codemeta.jsonld file instead of fetching it
    from the Internet every single time."""
    if url == CODEMETA_CONTEXT_URL or url in CODEMETA_ALTERNATE_CONTEXT_URLS:
        return {
            "contextUrl": None,
            "documentUrl": url,
            "document": CODEMETA_CONTEXT,
        }
    elif url == CODEMETA_URI:
        raise Exception(
            "{} is CodeMeta's URI, use {} as context url".format(
                CODEMETA_URI, CODEMETA_CONTEXT_URL
            )
        )
    else:
        raise Exception(url)


def compact(doc):
    """Same as `pyld.jsonld.compact`, but in the context of CodeMeta."""
    return jsonld.compact(
        doc, CODEMETA_CONTEXT_URL, options={"documentLoader": _document_loader}
    )


def expand(doc):
    """Same as `pyld.jsonld.expand`, but in the context of CodeMeta."""
    return jsonld.expand(doc, options={"documentLoader": _document_loader})


def merge_values(v1, v2):
    """If v1 and v2 are of the form `{"@list": l1}` and `{"@list": l2}`,
    returns `{"@list": l1 + l2}`.
    Otherwise, make them lists (if they are not already) and concatenate
    them.

    >>> merge_values('a', 'b')
    ['a', 'b']
    >>> merge_values(['a', 'b'], 'c')
    ['a', 'b', 'c']
    >>> merge_values({'@list': ['a', 'b']}, {'@list': ['c']})
    {'@list': ['a', 'b', 'c']}
    """
    if v1 is None:
        return v2
    elif v2 is None:
        return v1
    elif isinstance(v1, dict) and set(v1) == {"@list"}:
        assert isinstance(v1["@list"], list)
        if isinstance(v2, dict) and set(v2) == {"@list"}:
            assert isinstance(v2["@list"], list)
            return {"@list": v1["@list"] + v2["@list"]}
        else:
            raise ValueError("Cannot merge %r and %r" % (v1, v2))
    else:
        if isinstance(v2, dict) and "@list" in v2:
            raise ValueError("Cannot merge %r and %r" % (v1, v2))
        if not isinstance(v1, list):
            v1 = [v1]
        if not isinstance(v2, list):
            v2 = [v2]
        return v1 + v2


def merge_documents(documents):
    """Takes a list of metadata dicts, each generated from a different
    metadata file, and merges them.

    Removes duplicates, if any."""
    documents = list(itertools.chain.from_iterable(map(expand, documents)))
    merged_document = collections.defaultdict(list)
    for document in documents:
        for (key, values) in document.items():
            if key == "@id":
                # @id does not get expanded to a list
                value = values

                # Only one @id is allowed, move it to sameAs
                if "@id" not in merged_document:
                    merged_document["@id"] = value
                elif value != merged_document["@id"]:
                    if value not in merged_document[SCHEMA_URI + "sameAs"]:
                        merged_document[SCHEMA_URI + "sameAs"].append(value)
            else:
                for value in values:
                    if isinstance(value, dict) and set(value) == {"@list"}:
                        # Value is of the form {'@list': [item1, item2]}
                        # instead of the usual [item1, item2].
                        # We need to merge the inner lists (and mostly
                        # preserve order).
                        merged_value = merged_document.setdefault(key, {"@list": []})
                        for subvalue in value["@list"]:
                            # merged_value must be of the form
                            # {'@list': [item1, item2]}; as it is the same
                            # type as value, which is an @list.
                            if subvalue not in merged_value["@list"]:
                                merged_value["@list"].append(subvalue)
                    elif value not in merged_document[key]:
                        merged_document[key].append(value)

    return compact(merged_document)
