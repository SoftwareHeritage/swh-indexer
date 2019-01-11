# Copyright (C) 2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.indexer.codemeta import compact, expand
from swh.indexer.codemeta import make_absolute_uri
from swh.indexer.metadata_dictionary import MAPPINGS


def detect_metadata(files):
    """
    Detects files potentially containing metadata

    Args:
        file_entries (list): list of files

    Returns:
        dict: {mapping_filenames[name]:f['sha1']} (may be empty)
    """
    results = {}
    for (mapping_name, mapping) in MAPPINGS.items():
        matches = mapping.detect_metadata_files(files)
        if matches:
            results[mapping_name] = matches
    return results


_MINIMAL_PROPERTY_SET = {
    "developmentStatus", "version", "operatingSystem", "description",
    "keywords", "issueTracker", "name", "author", "relatedLink",
    "url", "license", "maintainer", "email", "identifier",
    "codeRepository"}

MINIMAL_METADATA_SET = {make_absolute_uri(prop)
                        for prop in _MINIMAL_PROPERTY_SET}


def extract_minimal_metadata_dict(metadata_list):
    """
    Every item in the metadata_list is a dict of translated_metadata in the
    CodeMeta vocabulary.

    We wish to extract a minimal set of terms and keep all values corresponding
    to this term without duplication.

    Args:
        metadata_list (list): list of dicts of translated_metadata

    Returns:
        dict: minimal_dict; dict with selected values of metadata
    """
    minimal_dict = {}
    for document in metadata_list:
        for metadata_item in expand(document):
            for (term, value) in metadata_item.items():
                if term in MINIMAL_METADATA_SET:
                    if term not in minimal_dict:
                        minimal_dict[term] = [value]
                    elif value not in minimal_dict[term]:
                        minimal_dict[term].append(value)
    return compact(minimal_dict)
