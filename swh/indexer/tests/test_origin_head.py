# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
from datetime import datetime, timezone

import pytest

from swh.indexer.origin_head import OriginHeadIndexer
from swh.indexer.tests.utils import fill_storage
from swh.model.model import (
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.storage.utils import now


@pytest.fixture
def swh_indexer_config(swh_indexer_config):
    config = copy.deepcopy(swh_indexer_config)
    config.update(
        {
            "tools": {
                "name": "origin-metadata",
                "version": "0.0.1",
                "configuration": {},
            },
            "tasks": {
                "revision_intrinsic_metadata": None,
                "origin_intrinsic_metadata": None,
            },
        }
    )
    return config


class OriginHeadTestIndexer(OriginHeadIndexer):
    """Specific indexer whose configuration is enough to satisfy the
    indexing tests.
    """

    def persist_index_computations(self, results):
        self.results = results


SAMPLE_SNAPSHOT = Snapshot(
    branches={
        b"foo": None,
        b"HEAD": SnapshotBranch(
            target_type=TargetType.ALIAS,
            target=b"foo",
        ),
    },
)


@pytest.fixture
def indexer(swh_config):
    indexer = OriginHeadTestIndexer()
    indexer.catch_exceptions = False
    fill_storage(indexer.storage)
    return indexer


def test_git(indexer):
    origin_url = "https://github.com/SoftwareHeritage/swh-storage"
    indexer.run([origin_url])
    rev_id = b"8K\x12\x00d\x03\xcc\xe4]bS\xe3\x8f{\xd7}\xac\xefrm"
    assert indexer.results == (
        [
            {
                "revision_id": rev_id,
                "origin_url": origin_url,
            }
        ]
    )


def test_git_partial_snapshot(indexer):
    """Checks partial snapshots are ignored."""
    origin_url = "https://github.com/SoftwareHeritage/swh-core"
    indexer.storage.origin_add([Origin(url=origin_url)])
    visit = indexer.storage.origin_visit_add(
        [
            OriginVisit(
                origin=origin_url,
                date=datetime(2019, 2, 27, tzinfo=timezone.utc),
                type="git",
            )
        ]
    )[0]
    indexer.storage.snapshot_add([SAMPLE_SNAPSHOT])
    visit_status = OriginVisitStatus(
        origin=origin_url,
        visit=visit.visit,
        date=now(),
        status="partial",
        snapshot=SAMPLE_SNAPSHOT.id,
    )
    indexer.storage.origin_visit_status_add([visit_status])
    indexer.run([origin_url])
    assert indexer.results == []


def test_vcs_missing_snapshot(indexer):
    origin_url = "https://github.com/SoftwareHeritage/swh-indexer"
    indexer.storage.origin_add([Origin(url=origin_url)])
    indexer.run([origin_url])
    assert indexer.results == []


def test_pypi_missing_branch(indexer):
    origin_url = "https://pypi.org/project/abcdef/"
    indexer.storage.origin_add(
        [
            Origin(
                url=origin_url,
            )
        ]
    )
    visit = indexer.storage.origin_visit_add(
        [
            OriginVisit(
                origin=origin_url,
                date=datetime(2019, 2, 27, tzinfo=timezone.utc),
                type="pypi",
            )
        ]
    )[0]
    indexer.storage.snapshot_add([SAMPLE_SNAPSHOT])
    visit_status = OriginVisitStatus(
        origin=origin_url,
        visit=visit.visit,
        date=now(),
        status="full",
        snapshot=SAMPLE_SNAPSHOT.id,
    )
    indexer.storage.origin_visit_status_add([visit_status])
    indexer.run(["https://pypi.org/project/abcdef/"])
    assert indexer.results == []


def test_ftp(indexer):
    origin_url = "rsync://ftp.gnu.org/gnu/3dldf"
    indexer.run([origin_url])
    rev_id = b"\x8e\xa9\x8e/\xea}\x9feF\xf4\x9f\xfd\xee\xcc\x1a\xb4`\x8c\x8by"
    assert indexer.results == [
        {
            "revision_id": rev_id,
            "origin_url": origin_url,
        }
    ]


def test_ftp_missing_snapshot(indexer):
    origin_url = "rsync://ftp.gnu.org/gnu/foobar"
    indexer.storage.origin_add([Origin(url=origin_url)])
    indexer.run([origin_url])
    assert indexer.results == []


def test_deposit(indexer):
    origin_url = "https://forge.softwareheritage.org/source/jesuisgpl/"
    indexer.storage.origin_add([Origin(url=origin_url)])
    indexer.run([origin_url])
    rev_id = b"\xe7n\xa4\x9c\x9f\xfb\xb7\xf76\x11\x08{\xa6\xe9\x99\xb1\x9e]q\xeb"
    assert indexer.results == [
        {
            "revision_id": rev_id,
            "origin_url": origin_url,
        }
    ]


def test_deposit_missing_snapshot(indexer):
    origin_url = "https://forge.softwareheritage.org/source/foobar"
    indexer.storage.origin_add(
        [
            Origin(
                url=origin_url,
            )
        ]
    )
    indexer.run([origin_url])
    assert indexer.results == []


def test_pypi(indexer):
    origin_url = "https://pypi.org/project/limnoria/"
    indexer.run([origin_url])

    rev_id = b"\x83\xb9\xb6\xc7\x05\xb1%\xd0\xfem\xd8kA\x10\x9d\xc5\xfa2\xf8t"
    assert indexer.results == [{"revision_id": rev_id, "origin_url": origin_url}]


def test_svn(indexer):
    origin_url = "http://0-512-md.googlecode.com/svn/"
    indexer.run([origin_url])
    rev_id = b"\xe4?r\xe1,\x88\xab\xec\xe7\x9a\x87\xb8\xc9\xad#.\x1bw=\x18"
    assert indexer.results == [
        {
            "revision_id": rev_id,
            "origin_url": origin_url,
        }
    ]
