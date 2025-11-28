# Copyright (C) 2018-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import csv
from functools import lru_cache, partial
import itertools
import json
import os.path
import re
from typing import Any, Dict, List, Set, TextIO, Tuple

from pyld import jsonld
from pyld.documentloader.requests import requests_document_loader
import rdflib

import swh.indexer
from swh.indexer.namespaces import ACTIVITYSTREAMS, CODEMETA, FORGEFED, SCHEMA, XSD

_DATA_DIR = os.path.join(os.path.dirname(swh.indexer.__file__), "data")

CROSSWALK_TABLE_PATH = os.path.join(_DATA_DIR, "codemeta", "crosswalk.csv")

CODEMETA_V2_CONTEXT_PATH = os.path.join(_DATA_DIR, "codemeta", "codemeta-2.0.jsonld")
CODEMETA_V3_CONTEXT_PATH = os.path.join(_DATA_DIR, "codemeta", "codemeta-3.0.jsonld")


with open(CODEMETA_V2_CONTEXT_PATH) as fdv2, open(CODEMETA_V3_CONTEXT_PATH) as fdv3:
    CODEMETA_V2_CONTEXT = json.load(fdv2)
    CODEMETA_V3_CONTEXT = json.load(fdv3)

with open(os.path.join(_DATA_DIR, "schema.org", "schemaorgcontext.jsonld")) as fd:
    _SCHEMA_DOT_ORG_CONTEXT = json.load(fd)

_EMPTY_PROCESSED_CONTEXT: Any = {"mappings": {}}
_PROCESSED_CODEMETA_CONTEXT = jsonld.JsonLdProcessor().process_context(
    _EMPTY_PROCESSED_CONTEXT, CODEMETA_V2_CONTEXT, None
)

CODEMETA_V2_CONTEXT_URL = "https://doi.org/10.5063/schema/codemeta-2.0"
CODEMETA_V2_ALTERNATE_CONTEXT_URLS = {
    "https://raw.githubusercontent.com/codemeta/codemeta/master/codemeta.jsonld",
    "https://raw.githubusercontent.com/codemeta/codemeta/2.0/codemeta.jsonld",
    "https://doi.org/doi:10.5063/schema/codemeta-2.0",
    "http://purl.org/codemeta/2.0",
}

CODEMETA_V3_CONTEXT_URL = "https://w3id.org/codemeta/3.0"
CODEMETA_V3_ALTERNATE_CONTEXT_URLS = {
    "https://raw.githubusercontent.com/codemeta/codemeta/3.0/codemeta.jsonld"
}

PROPERTY_BLACKLIST = {
    # CodeMeta properties that we cannot properly represent.
    SCHEMA.softwareRequirements,
    CODEMETA.softwareSuggestions,
    # Duplicate of 'author'
    SCHEMA.creator,
}

_codemeta_field_separator = re.compile(r"\s*[,/]\s*")


@lru_cache
def _requests_document_loader(url):
    return requests_document_loader()(url)


def make_absolute_uri(local_name):
    """Parses codemeta.jsonld, and returns the @id of terms it defines.

    >>> make_absolute_uri("name")
    'http://schema.org/name'
    >>> make_absolute_uri("downloadUrl")
    'http://schema.org/downloadUrl'
    >>> make_absolute_uri("referencePublication")
    'https://codemeta.github.io/terms/referencePublication'
    """
    uri = jsonld.JsonLdProcessor.get_context_value(
        _PROCESSED_CODEMETA_CONTEXT, local_name, "@id"
    )
    assert uri.startswith(("@", CODEMETA, SCHEMA)), (local_name, uri)
    return uri


def read_crosstable(fd: TextIO) -> Tuple[Set[str], Dict[str, Dict[str, rdflib.URIRef]]]:
    """
    Given a file-like object to a `CodeMeta crosswalk table` (either the main
    cross-table with all columns, or an auxiliary table with just the CodeMeta
    column and one ecosystem-specific table); returns a list of all CodeMeta
    terms, and a dictionary ``{ecosystem: {ecosystem_term: codemeta_term}}``

    .. _CodeMeta crosswalk table: <https://codemeta.github.io/crosswalk/
    """
    reader = csv.reader(fd)
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError("empty file")

    data_sources = set(header) - {"Parent Type", "Property", "Type", "Description"}

    codemeta_translation: Dict[str, Dict[str, rdflib.URIRef]] = {
        data_source: {} for data_source in data_sources
    }
    terms = set()

    for line in reader:  # For each canonical name
        local_name = dict(zip(header, line))["Property"]
        if not local_name:
            continue
        canonical_name = make_absolute_uri(local_name)
        if rdflib.URIRef(canonical_name) in PROPERTY_BLACKLIST:
            continue
        terms.add(canonical_name)
        for col, value in zip(header, line):  # For each cell in the row
            if col in data_sources:
                # If that's not the parentType/property/type/description
                for local_name in _codemeta_field_separator.split(value):
                    # For each of the data source's properties that maps
                    # to this canonical name
                    if local_name.strip():
                        codemeta_translation[col][local_name.strip()] = rdflib.URIRef(
                            canonical_name
                        )

    return (terms, codemeta_translation)


with open(CROSSWALK_TABLE_PATH) as fd:
    (CODEMETA_TERMS, CROSSWALK_TABLE) = read_crosstable(fd)


def _document_loader(url, options=None, resolve_unknown_context_url: bool = False):
    """Document loader for pyld.

    Reads the local codemeta.jsonld file instead of fetching it
    from the Internet every single time."""
    if (
        url.lower().rstrip("/") == CODEMETA_V2_CONTEXT_URL.lower()
        or url.lower() in CODEMETA_V2_ALTERNATE_CONTEXT_URLS
    ):
        return {
            "contextUrl": None,
            "documentUrl": url,
            "document": CODEMETA_V2_CONTEXT,
        }
    if (
        url.lower().rstrip("/") == CODEMETA_V3_CONTEXT_URL.lower()
        or url.lower().rstrip("/") in CODEMETA_V3_ALTERNATE_CONTEXT_URLS
    ):
        return {
            "contextUrl": None,
            "documentUrl": url,
            "document": CODEMETA_V3_CONTEXT,
        }
    elif url == CODEMETA:
        raise Exception(
            "{} is CodeMeta's URI, use {} as context url".format(
                CODEMETA, CODEMETA_V2_CONTEXT_URL
            )
        )
    elif url.lower().rstrip("/") in ("http://schema.org", "https://schema.org"):
        return {
            "contextUrl": None,
            "documentUrl": url,
            "document": _SCHEMA_DOT_ORG_CONTEXT,
        }
    else:
        if resolve_unknown_context_url:
            return _requests_document_loader(url)
        else:
            raise Exception(f"Unknown context URL: {url}")


def compact(
    doc: Dict[str, Any], forgefed: bool, resolve_unknown_context_url: bool = False
) -> Dict[str, Any]:
    """Same as `pyld.jsonld.compact`, but in the context of CodeMeta.

    Args:
        doc: parsed ``codemeta.json`` file
        forgefed: Whether to add ForgeFed and ActivityStreams as compact URIs.
            This is typically used for extrinsic metadata documents, which frequently
            use properties from these namespaces.
        resolve_unknown_context_url: if const:`True` unknown JSON-LD context URL
            will be fetched using ``requests`` instead of raising an exception,
            :const:`False` by default as it can lead sending requests to arbitrary
            URLs so use with caution
    Returns:
        A compacted JSON-LD document.
    """
    contexts: List[Any] = [CODEMETA_V2_CONTEXT_URL]
    if forgefed:
        contexts.append(
            {"as": str(ACTIVITYSTREAMS), "forge": str(FORGEFED), "xsd": str(XSD)}
        )
    return jsonld.compact(
        doc,
        contexts,
        options={
            "documentLoader": partial(
                _document_loader,
                resolve_unknown_context_url=resolve_unknown_context_url,
            )
        },
    )


def expand(
    doc: Dict[str, Any], resolve_unknown_context_url: bool = False
) -> Dict[str, Any]:
    """Same as `pyld.jsonld.expand`, but in the context of CodeMeta.

    Args:
        doc: parsed ``codemeta.json`` file
        resolve_unknown_context_url: if const:`True` unknown JSON-LD context URL
            will be fetched using ``requests`` instead of raising an exception,
            :const:`False` by default as it can lead sending requests to arbitrary
            URLs so use with caution
    Returns:
        An expanded JSON-LD document.
    """
    return jsonld.expand(
        doc,
        options={
            "documentLoader": partial(
                _document_loader,
                resolve_unknown_context_url=resolve_unknown_context_url,
            )
        },
    )


def merge_documents(documents):
    """Takes a list of metadata dicts, each generated from a different
    metadata file, and merges them.

    Removes duplicates, if any."""
    documents = list(itertools.chain.from_iterable(map(expand, documents)))
    merged_document = collections.defaultdict(list)
    for document in documents:
        for key, values in document.items():
            if key == "@id":
                # @id does not get expanded to a list
                value = values

                # Only one @id is allowed, move it to sameAs
                if "@id" not in merged_document:
                    merged_document["@id"] = value
                elif value != merged_document["@id"]:
                    if value not in merged_document[SCHEMA.sameAs]:
                        merged_document[SCHEMA.sameAs].append(value)
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

    # XXX: we should set forgefed=True when merging extrinsic_metadata documents.
    # however, this function is only used to merge multiple files of the same
    # directory (which is only for intrinsic-metadata), so it is not an issue for now
    return compact(merged_document, forgefed=False)
