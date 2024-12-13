# Copyright (C) 2017-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List

from swh.indexer.metadata_dictionary import INTRINSIC_MAPPINGS
from swh.indexer.metadata_dictionary.base import DirectoryLsEntry
from swh.objstorage.interface import CompositeObjId


def detect_metadata(files: List[DirectoryLsEntry]) -> Dict[str, List[CompositeObjId]]:
    """
    Detects files potentially containing metadata

    Args:
        file_entries (list): list of files

    Returns:
        dict: {mapping_filenames[name]:f['sha1']} (may be empty)
    """
    results = {}
    for mapping_name, mapping in INTRINSIC_MAPPINGS.items():
        matches = mapping.detect_metadata_files(files)
        if matches:
            results[mapping_name] = matches
    return results
