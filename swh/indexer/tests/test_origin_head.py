# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import itertools

import pytest

from swh.indexer.origin_head import get_head_swhid
from swh.indexer.tests.utils import fill_storage
from swh.model.model import (
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.model.swhids import CoreSWHID
from swh.storage.utils import now


@pytest.fixture
def swh_storage_backend_config():
    """In-memory storage, to make tests go faster."""
    return {"cls": "memory"}


SAMPLE_SNAPSHOT = Snapshot(
    branches={
        b"foo": None,
        b"HEAD": SnapshotBranch(
            target_type=SnapshotTargetType.ALIAS,
            target=b"foo",
        ),
    },
)


def _add_snapshot_to_origin(storage, origin_url, visit_type, snapshot):
    storage.origin_add([Origin(url=origin_url)])
    visit = storage.origin_visit_add(
        [
            OriginVisit(
                origin=origin_url,
                date=datetime(2019, 2, 27, tzinfo=timezone.utc),
                type="pypi",
            )
        ]
    )[0]
    storage.snapshot_add([snapshot])
    visit_status = OriginVisitStatus(
        origin=origin_url,
        visit=visit.visit,
        date=now(),
        status="full",
        snapshot=snapshot.id,
    )
    storage.origin_visit_status_add([visit_status])


@pytest.fixture
def storage(swh_storage):
    fill_storage(swh_storage)
    return swh_storage


def test_git(storage):
    origin_url = "https://github.com/SoftwareHeritage/swh-storage"
    assert get_head_swhid(storage, origin_url) == CoreSWHID.from_string(
        "swh:1:rev:384b12006403cce45d6253e38f7bd77dacef726d"
    )


def test_git_partial_snapshot(storage):
    """Checks partial snapshots are ignored."""
    origin_url = "https://github.com/SoftwareHeritage/swh-core"
    storage.origin_add([Origin(url=origin_url)])
    visit = storage.origin_visit_add(
        [
            OriginVisit(
                origin=origin_url,
                date=datetime(2019, 2, 27, tzinfo=timezone.utc),
                type="git",
            )
        ]
    )[0]
    storage.snapshot_add([SAMPLE_SNAPSHOT])
    visit_status = OriginVisitStatus(
        origin=origin_url,
        visit=visit.visit,
        date=now(),
        status="partial",
        snapshot=SAMPLE_SNAPSHOT.id,
    )
    storage.origin_visit_status_add([visit_status])
    assert get_head_swhid(storage, origin_url) is None


def test_vcs_missing_snapshot(storage):
    origin_url = "https://github.com/SoftwareHeritage/swh-indexer"
    storage.origin_add([Origin(url=origin_url)])
    assert get_head_swhid(storage, origin_url) is None


def test_pypi_missing_branch(storage):
    origin_url = "https://pypi.org/project/abcdef/"
    _add_snapshot_to_origin(storage, origin_url, "pypi", SAMPLE_SNAPSHOT)
    assert get_head_swhid(storage, origin_url) is None


@pytest.mark.parametrize(
    "branches_start,branches_middle,branches_end",
    itertools.product([0, 40, 99, 100, 200], [0, 40, 99, 100, 200], [0, 40, 200]),
)
def test_large_snapshot(storage, branches_start, branches_middle, branches_end):
    rev_id = "8ea98e2fea7d9f6546f49ffdeecc1ab4608c8b79"
    snapshot = Snapshot(
        branches=dict(
            [(f"AAAA{i}".encode(), None) for i in range(branches_start)]
            + [
                (
                    b"HEAD",
                    SnapshotBranch(
                        target_type=SnapshotTargetType.ALIAS, target=b"refs/heads/foo"
                    ),
                )
            ]
            + [(f"aaaa{i}".encode(), None) for i in range(branches_middle)]
            + [
                (
                    b"refs/heads/foo",
                    SnapshotBranch(
                        target_type=SnapshotTargetType.REVISION,
                        target=bytes.fromhex(rev_id),
                    ),
                )
            ]
            + [(f"zzzz{i}".encode(), None) for i in range(branches_end)]
        )
    )

    origin_url = "https://example.org/repo.git"
    _add_snapshot_to_origin(storage, origin_url, "git", snapshot)

    assert get_head_swhid(storage, origin_url) == CoreSWHID.from_string(
        "swh:1:rev:8ea98e2fea7d9f6546f49ffdeecc1ab4608c8b79"
    )


def test_large_snapshot_chained_aliases(storage):
    rev_id = "8ea98e2fea7d9f6546f49ffdeecc1ab4608c8b79"
    snapshot = Snapshot(
        branches=dict(
            [(f"AAAA{i}".encode(), None) for i in range(200)]
            + [
                (
                    b"HEAD",
                    SnapshotBranch(
                        target_type=SnapshotTargetType.ALIAS,
                        target=b"refs/heads/alias2",
                    ),
                )
            ]
            + [(f"aaaa{i}".encode(), None) for i in range(200)]
            + [
                (
                    b"refs/heads/alias2",
                    SnapshotBranch(
                        target_type=SnapshotTargetType.ALIAS,
                        target=b"refs/heads/branch",
                    ),
                )
            ]
            + [(f"refs/heads/bbbb{i}".encode(), None) for i in range(200)]
            + [
                (
                    b"refs/heads/branch",
                    SnapshotBranch(
                        target_type=SnapshotTargetType.REVISION,
                        target=bytes.fromhex(rev_id),
                    ),
                )
            ]
        )
    )

    origin_url = "https://example.org/repo.git"
    _add_snapshot_to_origin(storage, origin_url, "git", snapshot)

    assert get_head_swhid(storage, origin_url) == CoreSWHID.from_string(
        "swh:1:rev:8ea98e2fea7d9f6546f49ffdeecc1ab4608c8b79"
    )


@pytest.mark.parametrize(
    "branches_start,branches_end",
    itertools.product([0, 40, 99, 100, 200], [0, 40, 200]),
)
def test_large_snapshot_dangling_alias(storage, branches_start, branches_end):
    snapshot = Snapshot(
        branches=dict(
            [(f"AAAA{i}".encode(), None) for i in range(branches_start)]
            + [
                (
                    b"HEAD",
                    SnapshotBranch(
                        target_type=SnapshotTargetType.ALIAS, target=b"refs/heads/foo"
                    ),
                )
            ]
            + [(f"zzzz{i}".encode(), None) for i in range(branches_end)]
        )
    )

    origin_url = "https://example.org/repo.git"
    _add_snapshot_to_origin(storage, origin_url, "git", snapshot)

    assert get_head_swhid(storage, origin_url) is None


def test_ftp(storage):
    origin_url = "rsync://ftp.gnu.org/gnu/3dldf"
    assert get_head_swhid(storage, origin_url) == CoreSWHID.from_string(
        "swh:1:rev:8ea98e2fea7d9f6546f49ffdeecc1ab4608c8b79"
    )


def test_ftp_missing_snapshot(storage):
    origin_url = "rsync://ftp.gnu.org/gnu/foobar"
    storage.origin_add([Origin(url=origin_url)])
    assert get_head_swhid(storage, origin_url) is None


def test_deposit(storage):
    origin_url = "https://forge.softwareheritage.org/source/jesuisgpl/"
    storage.origin_add([Origin(url=origin_url)])
    assert get_head_swhid(storage, origin_url) == CoreSWHID.from_string(
        "swh:1:rev:e76ea49c9ffbb7f73611087ba6e999b19e5d71eb"
    )


def test_deposit_missing_snapshot(storage):
    origin_url = "https://forge.softwareheritage.org/source/foobar"
    storage.origin_add(
        [
            Origin(
                url=origin_url,
            )
        ]
    )
    assert get_head_swhid(storage, origin_url) is None


def test_pypi(storage):
    origin_url = "https://old-pypi.example.org/project/limnoria/"
    assert get_head_swhid(storage, origin_url) == CoreSWHID.from_string(
        "swh:1:rev:83b9b6c705b125d0fe6dd86b41109dc5fa32f874"
    )

    origin_url = "https://pypi.org/project/limnoria/"
    assert get_head_swhid(storage, origin_url) == CoreSWHID.from_string(
        "swh:1:rel:83b9b6c705b125d0fe6dd86b41109dc5fa32f874"
    )


def test_svn(storage):
    origin_url = "http://0-512-md.googlecode.com/svn/"
    assert get_head_swhid(storage, origin_url) == CoreSWHID.from_string(
        "swh:1:rev:e43f72e12c88abece79a87b8c9ad232e1b773d18"
    )
