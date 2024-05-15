# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
from typing import List, Optional, Tuple, Union

from swh.model.model import Snapshot, SnapshotBranch, SnapshotTargetType
from swh.model.swhids import CoreSWHID, ObjectType
from swh.storage.algos.origin import origin_get_latest_visit_status
from swh.storage.algos.snapshot import snapshot_get_all_branches
from swh.storage.interface import PartialBranches, StorageInterface


def get_head_swhid(storage: StorageInterface, origin_url: str) -> Optional[CoreSWHID]:
    """Returns the SWHID of the head revision or release of an origin"""
    visit_status = origin_get_latest_visit_status(
        storage, origin_url, allowed_statuses=["full"], require_snapshot=True
    )
    if not visit_status:
        return None
    assert visit_status.snapshot is not None

    if visit_status.type == "ftp":
        # We need to fetch all branches in order to find the largest one
        snapshot = snapshot_get_all_branches(storage, visit_status.snapshot)
        if snapshot is None:
            return None
        return _try_get_ftp_head(storage, snapshot)
    else:
        # Peak into the snapshot, without fetching too many refs.
        # If the snapshot is small, this gets all of it in a single request.
        # If the snapshot is large, we will query specific branches as we need them.
        partial_branches = storage.snapshot_get_branches(
            visit_status.snapshot, branches_count=100
        )
        if partial_branches is None:
            # Snapshot does not exist
            return None
        return _try_get_head_generic(storage, partial_branches)


_archive_filename_re = re.compile(
    rb"^"
    rb"(?P<pkgname>.*)[-_]"
    rb"(?P<version>[0-9]+(\.[0-9])*)"
    rb"(?P<preversion>[-+][a-zA-Z0-9.~]+?)?"
    rb"(?P<extension>(\.[a-zA-Z0-9]+)+)"
    rb"$"
)


def _parse_version(filename: bytes) -> Tuple[Union[float, int, str], ...]:
    """Extracts the release version from an archive filename,
    to get an ordering whose maximum is likely to be the last
    version of the software

    >>> _parse_version(b'foo')
    (-inf,)
    >>> _parse_version(b'foo.tar.gz')
    (-inf,)
    >>> _parse_version(b'gnu-hello-0.0.1.tar.gz')
    (0, 0, 1, 0)
    >>> _parse_version(b'gnu-hello-0.0.1-beta2.tar.gz')
    (0, 0, 1, -1, 'beta2')
    >>> _parse_version(b'gnu-hello-0.0.1+foobar.tar.gz')
    (0, 0, 1, 1, 'foobar')
    """
    res = _archive_filename_re.match(filename)
    if res is None:
        return (float("-infinity"),)
    version: List[Union[float, int, str]] = [
        int(n) for n in res.group("version").decode().split(".")
    ]
    if res.group("preversion") is None:
        version.append(0)
    else:
        preversion = res.group("preversion").decode()
        if preversion.startswith("-"):
            version.append(-1)
            version.append(preversion[1:])
        elif preversion.startswith("+"):
            version.append(1)
            version.append(preversion[1:])
        else:
            assert False, res.group("preversion")
    return tuple(version)


def _try_get_ftp_head(
    storage: StorageInterface, snapshot: Snapshot
) -> Optional[CoreSWHID]:
    archive_names = list(snapshot.branches)
    max_archive_name = max(archive_names, key=_parse_version)
    return _try_resolve_target(
        storage,
        {"id": snapshot.id, "branches": dict(snapshot.branches), "next_branch": None},
        branch_name=max_archive_name,
    )


def _try_get_head_generic(
    storage: StorageInterface, partial_branches: PartialBranches
) -> Optional[CoreSWHID]:
    # Works on 'deposit', 'pypi', and VCSs.
    return _try_resolve_target(
        storage, partial_branches, branch_name=b"HEAD"
    ) or _try_resolve_target(storage, partial_branches, branch_name=b"master")


def _get_branch(
    storage: StorageInterface, partial_branches: PartialBranches, branch_name: bytes
) -> Optional[SnapshotBranch]:
    """Given a ``branch_name``, gets it from ``partial_branches`` if present,
    and fetches it from the storage otherwise."""
    if branch_name in partial_branches["branches"]:
        return partial_branches["branches"][branch_name]
    elif partial_branches["next_branch"] is not None:
        # Branch is not in `partial_branches`, and `partial_branches` indeed partial
        res = storage.snapshot_get_branches(
            partial_branches["id"], branches_from=branch_name, branches_count=1
        )
        assert res is not None, "Snapshot does not exist anymore"
        return res["branches"].get(branch_name)
    else:
        # Branch is not in `partial_branches`, but `partial_branches` is the full
        # list of branches, which means it is a dangling reference.
        return None


def _try_resolve_target(
    storage: StorageInterface, partial_branches: PartialBranches, branch_name: bytes
) -> Optional[CoreSWHID]:
    try:
        branch = _get_branch(storage, partial_branches, branch_name)
        if branch is None:
            return None

        while branch.target_type == SnapshotTargetType.ALIAS:
            branch = _get_branch(storage, partial_branches, branch.target)
            if branch is None:
                return None

        if branch.target_type == SnapshotTargetType.REVISION:
            return CoreSWHID(object_type=ObjectType.REVISION, object_id=branch.target)
        elif branch.target_type == SnapshotTargetType.CONTENT:
            return None  # TODO
        elif branch.target_type == SnapshotTargetType.DIRECTORY:
            return None  # TODO
        elif branch.target_type == SnapshotTargetType.RELEASE:
            return CoreSWHID(object_type=ObjectType.RELEASE, object_id=branch.target)
        else:
            assert False, branch
    except KeyError:
        return None
