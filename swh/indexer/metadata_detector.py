# Copyright (C) 2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


mapping_filenames = {
    b"package.json": "npm",
    b"codemeta.json": "codemeta"
}


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
    for f in files:
        name = f['name'].lower().strip()
        # TODO: possibility to detect extensions
        if name in mapping_filenames:
            tool = mapping_filenames[name]
            if tool in results:
                results[tool].append(f['sha1'])
            else:
                results[tool] = [f['sha1']]
    return results


def extract_minimal_metadata_dict(metadata_list):
    """
    Every item in the metadata_list is a dict of translated_metadata in the
    CodeMeta vocabulary
    we wish to extract a minimal set of terms and keep all values corresponding
    to this term
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
        "type": [],
        "license": [],
        "maintainer": [],
        "email": [],
        "softwareRequirements": [],
        "identifier": [],
        "codeRepository": []
    }
    for term in minimal_dict.keys():
        for metadata_dict in metadata_list:
            if term in metadata_dict:
                minimal_dict[term].append(metadata_dict[term])
        if not minimal_dict[term]:
            minimal_dict[term] = None
    return minimal_dict
