# Copyright (C) 2017-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Dict, List, Set

from swh.indexer.metadata_mapping import get_intrinsic_mappings
from swh.indexer.metadata_mapping.base import DirectoryLsEntry
from swh.model.hashutil import HashDict
from swh.model.model import DirectoryEntry


def detect_metadata(files: List[DirectoryLsEntry]) -> Dict[str, List[HashDict]]:
    """
    Detects files potentially containing metadata

    Args:
        file_entries (list): list of files

    Returns:
        dict: {mapping_filenames[name]:f['sha1']} (may be empty)
    """
    results = {}
    for mapping_name, mapping in get_intrinsic_mappings().items():
        matches = mapping.detect_metadata_files(files)
        if matches:
            results[mapping_name] = matches
    return results


def detect_metadata_from_directory_entries(
    file_entries: List[DirectoryEntry],
) -> Dict[str, Set[DirectoryEntry]]:
    """Detects file entries potentially containing metadata.

    Args:
        file_entries: list of file entries

    Returns:
        dict: {mapping_filenames[name]: Set(DirectoryEntry)} (may be empty)

    """
    results = {}
    for mapping_name, mapping in get_intrinsic_mappings().items():
        matched_entry = mapping.detect_metadata_from_directory_entries(file_entries)
        if matched_entry:
            results[mapping_name] = matched_entry
    return results
