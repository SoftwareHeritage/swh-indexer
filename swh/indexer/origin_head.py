# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
from typing import Dict, List, Optional, Tuple, Union

from swh.model.model import SnapshotBranch, TargetType
from swh.model.swhids import CoreSWHID, ObjectType
from swh.storage.algos.origin import origin_get_latest_visit_status
from swh.storage.algos.snapshot import snapshot_get_all_branches


def get_head_swhid(storage, origin_url: str) -> Optional[CoreSWHID]:
    """Returns the SWHID of the head revision or release of an origin"""
    visit_status = origin_get_latest_visit_status(
        storage, origin_url, allowed_statuses=["full"], require_snapshot=True
    )
    if not visit_status:
        return None
    assert visit_status.snapshot is not None
    snapshot = snapshot_get_all_branches(storage, visit_status.snapshot)
    if snapshot is None:
        return None

    if visit_status.type == "ftp":
        return _try_get_ftp_head(dict(snapshot.branches))
    else:
        return _try_get_head_generic(dict(snapshot.branches))


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
    branches: Dict[bytes, Optional[SnapshotBranch]]
) -> Optional[CoreSWHID]:
    archive_names = list(branches)
    max_archive_name = max(archive_names, key=_parse_version)
    return _try_resolve_target(branches, max_archive_name)


def _try_get_head_generic(
    branches: Dict[bytes, Optional[SnapshotBranch]]
) -> Optional[CoreSWHID]:
    # Works on 'deposit', 'pypi', and VCSs.
    return _try_resolve_target(branches, b"HEAD") or _try_resolve_target(
        branches, b"master"
    )


def _try_resolve_target(
    branches: Dict[bytes, Optional[SnapshotBranch]], branch_name: bytes
) -> Optional[CoreSWHID]:
    try:
        branch = branches[branch_name]
        if branch is None:
            return None
        while branch.target_type == TargetType.ALIAS:
            branch = branches[branch.target]
            if branch is None:
                return None

        if branch.target_type == TargetType.REVISION:
            return CoreSWHID(object_type=ObjectType.REVISION, object_id=branch.target)
        elif branch.target_type == TargetType.CONTENT:
            return None  # TODO
        elif branch.target_type == TargetType.DIRECTORY:
            return None  # TODO
        elif branch.target_type == TargetType.RELEASE:
            return CoreSWHID(object_type=ObjectType.RELEASE, object_id=branch.target)
        else:
            assert False, branch
    except KeyError:
        return None
