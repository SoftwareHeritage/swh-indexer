# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def db_to_mimetype(mimetype):
    """Convert a mimetype entry into a ready mimetype output."""
    return {
        "id": mimetype["id"],
        "encoding": mimetype["encoding"],
        "mimetype": mimetype["mimetype"],
        "tool": {
            "id": mimetype["tool_id"],
            "name": mimetype["tool_name"],
            "version": mimetype["tool_version"],
            "configuration": mimetype["tool_configuration"],
        },
    }


def db_to_metadata(metadata):
    """Convert a metadata entry into a ready metadata output."""
    metadata["tool"] = {
        "id": metadata["tool_id"],
        "name": metadata["tool_name"],
        "version": metadata["tool_version"],
        "configuration": metadata["tool_configuration"],
    }
    del metadata["tool_id"], metadata["tool_configuration"]
    del metadata["tool_version"], metadata["tool_name"]
    return metadata


def db_to_fossology_license(license):
    return {
        "id": license["id"],
        "license": license["license"],
        "tool": {
            "id": license["tool_id"],
            "name": license["tool_name"],
            "version": license["tool_version"],
            "configuration": license["tool_configuration"],
        },
    }
