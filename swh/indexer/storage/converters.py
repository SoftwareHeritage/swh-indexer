# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def ctags_to_db(ctags):
    """Convert a ctags entry into a ready ctags entry.

    Args:
        ctags (dict): ctags entry with the following keys:

            - id (bytes): content's identifier
            - tool_id (int): tool id used to compute ctags
            - ctags ([dict]): List of dictionary with the following keys:

              - name (str): symbol's name
              - kind (str): symbol's kind
              - line (int): symbol's line in the content
              - language (str): language

    Returns:
        list: list of ctags entries as dicts with the following keys:

        - id (bytes): content's identifier
        - name (str): symbol's name
        - kind (str): symbol's kind
        - language (str): language for that content
        - tool_id (int): tool id used to compute ctags

    """
    id = ctags["id"]
    tool_id = ctags["indexer_configuration_id"]
    for ctag in ctags["ctags"]:
        yield {
            "id": id,
            "name": ctag["name"],
            "kind": ctag["kind"],
            "line": ctag["line"],
            "lang": ctag["lang"],
            "indexer_configuration_id": tool_id,
        }


def db_to_ctags(ctag):
    """Convert a ctags entry into a ready ctags entry.

    Args:
        ctags (dict): ctags entry with the following keys:

          - id (bytes): content's identifier
          - ctags ([dict]): List of dictionary with the following keys:
            - name (str): symbol's name
            - kind (str): symbol's kind
            - line (int): symbol's line in the content
            - language (str): language

    Returns:
        list: list of ctags ready entry (dict with the following keys):

        - id (bytes): content's identifier
        - name (str): symbol's name
        - kind (str): symbol's kind
        - language (str): language for that content
        - tool (dict): tool used to compute the ctags

    """
    return {
        "id": ctag["id"],
        "name": ctag["name"],
        "kind": ctag["kind"],
        "line": ctag["line"],
        "lang": ctag["lang"],
        "tool": {
            "id": ctag["tool_id"],
            "name": ctag["tool_name"],
            "version": ctag["tool_version"],
            "configuration": ctag["tool_configuration"],
        },
    }


def db_to_mimetype(mimetype):
    """Convert a ctags entry into a ready ctags output."""
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


def db_to_language(language):
    """Convert a language entry into a ready language output."""
    return {
        "id": language["id"],
        "lang": language["lang"],
        "tool": {
            "id": language["tool_id"],
            "name": language["tool_name"],
            "version": language["tool_version"],
            "configuration": language["tool_configuration"],
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
