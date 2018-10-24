# Copyright (C) 2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.indexer.metadata_dictionary import MAPPINGS


def detect_metadata(files):
    """
    Detects files potentially containing metadata
    Args:
        - file_entries (list): list of files

    Returns:
        - empty list if nothing was found
        - dictionary {mapping_filenames[name]:f['sha1']}
    """
    results = {}
    for (mapping_name, mapping) in MAPPINGS.items():
        matches = mapping.detect_metadata_files(files)
        if matches:
            results[mapping_name] = matches
    return results


def extract_minimal_metadata_dict(metadata_list):
    """
    Every item in the metadata_list is a dict of translated_metadata in the
    CodeMeta vocabulary
    we wish to extract a minimal set of terms and keep all values corresponding
    to this term without duplication
    Args:
        - metadata_list (list): list of dicts of translated_metadata

    Returns:
        - minimal_dict (dict): one dict with selected values of metadata
    """
    minimal_dict = {
        "developmentStatus": [],
        "version": [],
        "operatingSystem": [],
        "description": [],
        "keywords": [],
        "issueTracker": [],
        "name": [],
        "author": [],
        "relatedLink": [],
        "url": [],
        "license": [],
        "maintainer": [],
        "email": [],
        "softwareRequirements": [],
        "identifier": [],
        "codeRepository": []
    }
    for term in minimal_dict.keys():
        for metadata_item in metadata_list:
            if term in metadata_item:
                if not metadata_item[term] in minimal_dict[term]:
                    minimal_dict[term].append(metadata_item[term])
        if not minimal_dict[term]:
            minimal_dict[term] = None
    return minimal_dict
