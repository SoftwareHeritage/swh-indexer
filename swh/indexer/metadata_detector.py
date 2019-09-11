# Copyright (C) 2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

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
