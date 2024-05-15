# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import datetime
import functools
from typing import Any, Dict, List, Tuple
import unittest

from hypothesis import strategies

from swh.core.api.classes import stream_results
from swh.indexer.storage import INDEXER_CFG_KEY
from swh.model.hashutil import hash_to_bytes
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    ObjectType,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Person,
    Release,
    Revision,
    RevisionType,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
    TimestampWithTimezone,
)
from swh.storage.utils import now

BASE_TEST_CONFIG: Dict[str, Dict[str, Any]] = {
    "storage": {"cls": "memory"},
    "objstorage": {"cls": "memory"},
    INDEXER_CFG_KEY: {"cls": "memory"},
}

ORIGIN_VISITS = [
    {"type": "git", "origin": "https://github.com/SoftwareHeritage/swh-storage"},
    {"type": "ftp", "origin": "rsync://ftp.gnu.org/gnu/3dldf"},
    {
        "type": "deposit",
        "origin": "https://forge.softwareheritage.org/source/jesuisgpl/",
    },
    {
        "type": "pypi",
        "origin": "https://old-pypi.example.org/project/limnoria/",
    },  # with rev head
    {"type": "pypi", "origin": "https://pypi.org/project/limnoria/"},  # with rel head
    {"type": "svn", "origin": "http://0-512-md.googlecode.com/svn/"},
    {"type": "git", "origin": "https://github.com/librariesio/yarn-parser"},
    {"type": "git", "origin": "https://github.com/librariesio/yarn-parser.git"},
    {"type": "git", "origin": "https://npm.example.org/yarn-parser"},
]

ORIGINS = [Origin(url=visit["origin"]) for visit in ORIGIN_VISITS]

OBJ_STORAGE_RAW_CONTENT: Dict[str, bytes] = {
    "text:some": b"this is some text",
    "text:another": b"another text",
    "text:yet": b"yet another text",
    "python:code": b"""
    import unittest
    import logging
    from swh.indexer.mimetype import MimetypeIndexer
    from swh.indexer.tests.test_utils import MockObjStorage

    class MockStorage():
        def content_mimetype_add(self, mimetypes):
            self.state = mimetypes

        def indexer_configuration_add(self, tools):
            return [{
                'id': 10,
            }]
    """,
    "c:struct": b"""
        #ifndef __AVL__
        #define __AVL__

        typedef struct _avl_tree avl_tree;

        typedef struct _data_t {
          int content;
        } data_t;
    """,
    "lisp:assertion": b"""
    (should 'pygments (recognize 'lisp 'easily))

    """,
    "json:test-metadata-package.json": b"""
    {
        "name": "test_metadata",
        "version": "0.0.1",
        "description": "Simple package.json test for indexer",
        "repository": {
          "type": "git",
          "url": "https://github.com/moranegg/metadata_test"
      }
    }
    """,
    "json:npm-package.json": b"""
    {
      "version": "5.0.3",
      "name": "npm",
      "description": "a package manager for JavaScript",
      "preferGlobal": true,
      "config": {
        "publishtest": false
      },
      "homepage": "https://docs.npmjs.com/",
      "author": "Isaac Z. Schlueter <i@izs.me> (http://blog.izs.me)",
      "repository": {
        "type": "git",
        "url": "https://github.com/npm/npm"
      },
      "bugs": {
        "url": "https://github.com/npm/npm/issues"
      },
      "dependencies": {
        "JSONStream": "~1.3.1",
        "abbrev": "~1.1.0",
        "ansi-regex": "~2.1.1",
        "ansicolors": "~0.3.2",
        "ansistyles": "~0.1.3"
      },
      "devDependencies": {
        "tacks": "~1.2.6",
        "tap": "~10.3.2"
      },
      "license": "Artistic-2.0"
    }

    """,
    "text:carriage-return": b"""
    """,
    "text:empty": b"",
    # was 626364 / b'bcd'
    "text:unimportant": b"unimportant content for bcd",
    # was 636465 / b'cde' now yarn-parser package.json
    "json:yarn-parser-package.json": b"""
    {
      "name": "yarn-parser",
      "version": "1.0.0",
      "description": "Tiny web service for parsing yarn.lock files",
      "main": "index.js",
      "scripts": {
        "start": "node index.js",
        "test": "mocha"
      },
      "engines": {
        "node": "9.8.0"
      },
      "repository": {
        "type": "git",
        "url": "git+https://github.com/librariesio/yarn-parser.git"
      },
      "author": "Andrew Nesbitt",
      "license": "AGPL-3.0",
      "bugs": {
        "url": "https://github.com/librariesio/yarn-parser/issues"
      },
      "homepage": "https://github.com/librariesio/yarn-parser#readme",
      "dependencies": {
        "@yarnpkg/lockfile": "^1.0.0",
        "body-parser": "^1.15.2",
        "express": "^4.14.0"
      },
      "devDependencies": {
        "chai": "^4.1.2",
        "mocha": "^5.2.0",
        "request": "^2.87.0",
        "test": "^0.6.0"
      }
    }

""",
}

MAPPING_DESCRIPTION_CONTENT_SHA1GIT: Dict[str, bytes] = {}
MAPPING_DESCRIPTION_CONTENT_SHA1: Dict[str, bytes] = {}
OBJ_STORAGE_DATA: Dict[bytes, bytes] = {}

for key_description, data in OBJ_STORAGE_RAW_CONTENT.items():
    content = Content.from_data(data)
    MAPPING_DESCRIPTION_CONTENT_SHA1GIT[key_description] = content.sha1_git
    MAPPING_DESCRIPTION_CONTENT_SHA1[key_description] = content.sha1
    OBJ_STORAGE_DATA[content.sha1] = data


RAW_CONTENT_METADATA = [
    (
        "du français".encode(),
        "text/plain",
        "utf-8",
    ),
    (
        b"def __init__(self):",
        ("text/x-python", "text/x-script.python"),
        "us-ascii",
    ),
    (
        b"\xff\xfe\x00\x00\x00\x00\xff\xfe\xff\xff",
        "application/octet-stream",
        "",
    ),
]

RAW_CONTENTS: Dict[bytes, Tuple] = {}
RAW_CONTENT_IDS: List[bytes] = []

for index, raw_content_d in enumerate(RAW_CONTENT_METADATA):
    raw_content = raw_content_d[0]
    content = Content.from_data(raw_content)
    RAW_CONTENTS[content.sha1] = raw_content_d
    RAW_CONTENT_IDS.append(content.sha1)
    # and write it to objstorage data so it's flushed in the objstorage
    OBJ_STORAGE_DATA[content.sha1] = raw_content


SHA1_TO_LICENSES: Dict[bytes, List[str]] = {
    RAW_CONTENT_IDS[0]: ["GPL"],
    RAW_CONTENT_IDS[1]: ["AGPL"],
    RAW_CONTENT_IDS[2]: [],
}


DIRECTORY = Directory(
    entries=(
        DirectoryEntry(
            name=b"index.js",
            type="file",
            target=MAPPING_DESCRIPTION_CONTENT_SHA1GIT["text:some"],
            perms=0o100644,
        ),
        DirectoryEntry(
            name=b"package.json",
            type="file",
            target=MAPPING_DESCRIPTION_CONTENT_SHA1GIT[
                "json:test-metadata-package.json"
            ],
            perms=0o100644,
        ),
        DirectoryEntry(
            name=b".github",
            type="dir",
            target=Directory(entries=()).id,
            perms=0o040000,
        ),
    ),
)

DIRECTORY2 = Directory(
    entries=(
        DirectoryEntry(
            name=b"package.json",
            type="file",
            target=MAPPING_DESCRIPTION_CONTENT_SHA1GIT["json:yarn-parser-package.json"],
            perms=0o100644,
        ),
    ),
)

_utc_plus_2 = datetime.timezone(datetime.timedelta(minutes=120))

REVISION = Revision(
    message=b"Improve search functionality",
    author=Person(
        name=b"Andrew Nesbitt",
        fullname=b"Andrew Nesbitt <andrewnez@gmail.com>",
        email=b"andrewnez@gmail.com",
    ),
    committer=Person(
        name=b"Andrew Nesbitt",
        fullname=b"Andrew Nesbitt <andrewnez@gmail.com>",
        email=b"andrewnez@gmail.com",
    ),
    committer_date=TimestampWithTimezone.from_datetime(
        datetime.datetime(2013, 10, 4, 12, 50, 49, tzinfo=_utc_plus_2)
    ),
    type=RevisionType.GIT,
    synthetic=False,
    date=TimestampWithTimezone.from_datetime(
        datetime.datetime(2017, 2, 20, 16, 14, 16, tzinfo=_utc_plus_2)
    ),
    directory=DIRECTORY2.id,
    parents=(),
)

REVISIONS = [REVISION]

RELEASE = Release(
    name=b"v0.0.0",
    message=None,
    author=Person(
        name=b"Andrew Nesbitt",
        fullname=b"Andrew Nesbitt <andrewnez@gmail.com>",
        email=b"andrewnez@gmail.com",
    ),
    synthetic=False,
    date=TimestampWithTimezone.from_datetime(
        datetime.datetime(2017, 2, 20, 16, 14, 16, tzinfo=_utc_plus_2)
    ),
    target_type=ObjectType.DIRECTORY,
    target=DIRECTORY2.id,
)

RELEASES = [RELEASE]

SNAPSHOTS = [
    # https://github.com/SoftwareHeritage/swh-storage
    Snapshot(
        branches={
            b"refs/heads/add-revision-origin-cache": SnapshotBranch(
                target=b'L[\xce\x1c\x88\x8eF\t\xf1"\x19\x1e\xfb\xc0s\xe7/\xe9l\x1e',
                target_type=SnapshotTargetType.REVISION,
            ),
            b"refs/head/master": SnapshotBranch(
                target=b"8K\x12\x00d\x03\xcc\xe4]bS\xe3\x8f{\xd7}\xac\xefrm",
                target_type=SnapshotTargetType.REVISION,
            ),
            b"HEAD": SnapshotBranch(
                target=b"refs/head/master", target_type=SnapshotTargetType.ALIAS
            ),
            b"refs/tags/v0.0.103": SnapshotBranch(
                target=b'\xb6"Im{\xfdLb\xb0\x94N\xea\x96m\x13x\x88+\x0f\xdd',
                target_type=SnapshotTargetType.RELEASE,
            ),
        },
    ),
    # rsync://ftp.gnu.org/gnu/3dldf
    Snapshot(
        branches={
            b"3DLDF-1.1.4.tar.gz": SnapshotBranch(
                target=b'dJ\xfb\x1c\x91\xf4\x82B%]6\xa2\x90|\xd3\xfc"G\x99\x11',
                target_type=SnapshotTargetType.REVISION,
            ),
            b"3DLDF-2.0.2.tar.gz": SnapshotBranch(
                target=b"\xb6\x0e\xe7\x9e9\xac\xaa\x19\x9e=\xd1\xc5\x00\\\xc6\xfc\xe0\xa6\xb4V",  # noqa
                target_type=SnapshotTargetType.REVISION,
            ),
            b"3DLDF-2.0.3-examples.tar.gz": SnapshotBranch(
                target=b"!H\x19\xc0\xee\x82-\x12F1\xbd\x97\xfe\xadZ\x80\x80\xc1\x83\xff",  # noqa
                target_type=SnapshotTargetType.REVISION,
            ),
            b"3DLDF-2.0.3.tar.gz": SnapshotBranch(
                target=b"\x8e\xa9\x8e/\xea}\x9feF\xf4\x9f\xfd\xee\xcc\x1a\xb4`\x8c\x8by",  # noqa
                target_type=SnapshotTargetType.REVISION,
            ),
            b"3DLDF-2.0.tar.gz": SnapshotBranch(
                target=b"F6*\xff(?\x19a\xef\xb6\xc2\x1fv$S\xe3G\xd3\xd1m",
                target_type=SnapshotTargetType.REVISION,
            ),
        },
    ),
    # https://forge.softwareheritage.org/source/jesuisgpl/",
    Snapshot(
        branches={
            b"master": SnapshotBranch(
                target=b"\xe7n\xa4\x9c\x9f\xfb\xb7\xf76\x11\x08{\xa6\xe9\x99\xb1\x9e]q\xeb",  # noqa
                target_type=SnapshotTargetType.REVISION,
            )
        },
    ),
    # https://old-pypi.example.org/project/limnoria/
    Snapshot(
        branches={
            b"HEAD": SnapshotBranch(
                target=b"releases/2018.09.09", target_type=SnapshotTargetType.ALIAS
            ),
            b"releases/2018.09.01": SnapshotBranch(
                target=b"<\xee1(\xe8\x8d_\xc1\xc9\xa6rT\xf1\x1d\xbb\xdfF\xfdw\xcf",
                target_type=SnapshotTargetType.REVISION,
            ),
            b"releases/2018.09.09": SnapshotBranch(
                target=b"\x83\xb9\xb6\xc7\x05\xb1%\xd0\xfem\xd8kA\x10\x9d\xc5\xfa2\xf8t",  # noqa
                target_type=SnapshotTargetType.REVISION,
            ),
        },
    ),
    # https://pypi.org/project/limnoria/
    Snapshot(
        branches={
            b"HEAD": SnapshotBranch(
                target=b"releases/2018.09.09", target_type=SnapshotTargetType.ALIAS
            ),
            b"releases/2018.09.01": SnapshotBranch(
                target=b"<\xee1(\xe8\x8d_\xc1\xc9\xa6rT\xf1\x1d\xbb\xdfF\xfdw\xcf",
                target_type=SnapshotTargetType.RELEASE,
            ),
            b"releases/2018.09.09": SnapshotBranch(
                target=b"\x83\xb9\xb6\xc7\x05\xb1%\xd0\xfem\xd8kA\x10\x9d\xc5\xfa2\xf8t",  # noqa
                target_type=SnapshotTargetType.RELEASE,
            ),
        },
    ),
    # http://0-512-md.googlecode.com/svn/
    Snapshot(
        branches={
            b"master": SnapshotBranch(
                target=b"\xe4?r\xe1,\x88\xab\xec\xe7\x9a\x87\xb8\xc9\xad#.\x1bw=\x18",
                target_type=SnapshotTargetType.REVISION,
            )
        },
    ),
    # https://github.com/librariesio/yarn-parser
    Snapshot(
        branches={
            b"HEAD": SnapshotBranch(
                target=REVISION.id,
                target_type=SnapshotTargetType.REVISION,
            )
        },
    ),
    # https://github.com/librariesio/yarn-parser.git
    Snapshot(
        branches={
            b"HEAD": SnapshotBranch(
                target=REVISION.id,
                target_type=SnapshotTargetType.REVISION,
            )
        },
    ),
    # https://npm.example.org/yarn-parser
    Snapshot(
        branches={
            b"HEAD": SnapshotBranch(
                target=RELEASE.id,
                target_type=SnapshotTargetType.RELEASE,
            )
        },
    ),
]

assert len(SNAPSHOTS) == len(ORIGIN_VISITS)


YARN_PARSER_METADATA = {
    "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
    "url": "https://github.com/librariesio/yarn-parser#readme",
    "codeRepository": "git+git+https://github.com/librariesio/yarn-parser.git",
    "author": [{"type": "Person", "name": "Andrew Nesbitt"}],
    "license": "https://spdx.org/licenses/AGPL-3.0",
    "version": "1.0.0",
    "description": "Tiny web service for parsing yarn.lock files",
    "issueTracker": "https://github.com/librariesio/yarn-parser/issues",
    "name": "yarn-parser",
    "type": "SoftwareSourceCode",
}


json_dict_keys = strategies.one_of(
    strategies.characters(),
    strategies.just("type"),
    strategies.just("url"),
    strategies.just("name"),
    strategies.just("email"),
    strategies.just("@id"),
    strategies.just("@context"),
    strategies.just("repository"),
    strategies.just("license"),
    strategies.just("repositories"),
    strategies.just("licenses"),
)
"""Hypothesis strategy that generates strings, with an emphasis on those
that are often used as dictionary keys in metadata files."""


generic_json_document = strategies.recursive(
    strategies.none()
    | strategies.booleans()
    | strategies.floats()
    | strategies.characters(),
    lambda children: (
        strategies.lists(children, min_size=1)
        | strategies.dictionaries(json_dict_keys, children, min_size=1)
    ),
)
"""Hypothesis strategy that generates possible values for values of JSON
metadata files."""


def json_document_strategy(keys=None):
    """Generates an hypothesis strategy that generates metadata files
    for a JSON-based format that uses the given keys."""
    if keys is None:
        keys = strategies.characters()
    else:
        keys = strategies.one_of(map(strategies.just, keys))

    return strategies.dictionaries(keys, generic_json_document, min_size=1)


def _tree_to_xml(root, xmlns, data):
    def encode(s):
        "Skips unpaired surrogates generated by json_document_strategy"
        return s.encode("utf8", "replace")

    def to_xml(data, indent=b" "):
        if data is None:
            return b""
        elif isinstance(data, (bool, str, int, float)):
            return indent + encode(str(data))
        elif isinstance(data, list):
            return b"\n".join(to_xml(v, indent=indent) for v in data)
        elif isinstance(data, dict):
            lines = []
            for key, value in data.items():
                lines.append(indent + encode("<{}>".format(key)))
                lines.append(to_xml(value, indent=indent + b" "))
                lines.append(indent + encode("</{}>".format(key)))
            return b"\n".join(lines)
        else:
            raise TypeError(data)

    return b"\n".join(
        [
            '<{} xmlns="{}">'.format(root, xmlns).encode(),
            to_xml(data),
            "</{}>".format(root).encode(),
        ]
    )


class TreeToXmlTest(unittest.TestCase):
    def test_leaves(self):
        self.assertEqual(
            _tree_to_xml("root", "http://example.com", None),
            b'<root xmlns="http://example.com">\n\n</root>',
        )
        self.assertEqual(
            _tree_to_xml("root", "http://example.com", True),
            b'<root xmlns="http://example.com">\n True\n</root>',
        )
        self.assertEqual(
            _tree_to_xml("root", "http://example.com", "abc"),
            b'<root xmlns="http://example.com">\n abc\n</root>',
        )
        self.assertEqual(
            _tree_to_xml("root", "http://example.com", 42),
            b'<root xmlns="http://example.com">\n 42\n</root>',
        )
        self.assertEqual(
            _tree_to_xml("root", "http://example.com", 3.14),
            b'<root xmlns="http://example.com">\n 3.14\n</root>',
        )

    def test_dict(self):
        self.assertIn(
            _tree_to_xml("root", "http://example.com", {"foo": "bar", "baz": "qux"}),
            [
                b'<root xmlns="http://example.com">\n'
                b" <foo>\n  bar\n </foo>\n"
                b" <baz>\n  qux\n </baz>\n"
                b"</root>",
                b'<root xmlns="http://example.com">\n'
                b" <baz>\n  qux\n </baz>\n"
                b" <foo>\n  bar\n </foo>\n"
                b"</root>",
            ],
        )

    def test_list(self):
        self.assertEqual(
            _tree_to_xml(
                "root",
                "http://example.com",
                [
                    {"foo": "bar"},
                    {"foo": "baz"},
                ],
            ),
            b'<root xmlns="http://example.com">\n'
            b" <foo>\n  bar\n </foo>\n"
            b" <foo>\n  baz\n </foo>\n"
            b"</root>",
        )


def xml_document_strategy(keys, root, xmlns):
    """Generates an hypothesis strategy that generates metadata files
    for an XML format that uses the given keys."""

    return strategies.builds(
        functools.partial(_tree_to_xml, root, xmlns), json_document_strategy(keys)
    )


def filter_dict(d, keys):
    "return a copy of the dict with keys deleted"
    if not isinstance(keys, (list, tuple)):
        keys = (keys,)
    return dict((k, v) for (k, v) in d.items() if k not in keys)


def fill_obj_storage(obj_storage):
    """Add some content in an object storage."""
    for obj_id, content in OBJ_STORAGE_DATA.items():
        obj_storage.add(content, obj_id)


def fill_storage(storage):
    """Fill in storage with consistent test dataset."""
    storage.content_add([Content.from_data(data) for data in OBJ_STORAGE_DATA.values()])
    storage.directory_add([DIRECTORY, DIRECTORY2])
    storage.revision_add(REVISIONS)
    storage.release_add(RELEASES)
    storage.snapshot_add(SNAPSHOTS)

    storage.origin_add(ORIGINS)
    for visit, snapshot in zip(ORIGIN_VISITS, SNAPSHOTS):
        assert snapshot.id is not None

        visit = storage.origin_visit_add(
            [OriginVisit(origin=visit["origin"], date=now(), type=visit["type"])]
        )[0]
        visit_status = OriginVisitStatus(
            origin=visit.origin,
            visit=visit.visit,
            date=now(),
            status="full",
            snapshot=snapshot.id,
        )
        storage.origin_visit_status_add([visit_status])


class CommonContentIndexerTest(metaclass=abc.ABCMeta):
    def get_indexer_results(self, ids):
        """Override this for indexers that don't have a mock storage."""
        return self.indexer.idx_storage.state

    def assert_results_ok(self, sha1s, expected_results=None):
        sha1s = [hash_to_bytes(sha1) for sha1 in sha1s]
        actual_results = list(self.get_indexer_results(sha1s))

        if expected_results is None:
            expected_results = self.expected_results

        # expected results may contain slightly duplicated results
        assert 0 < len(actual_results) <= len(expected_results)
        for result in actual_results:
            assert result in expected_results

    def test_index(self):
        """Known sha1 have their data indexed"""
        sha1s = [self.id0, self.id1, self.id2]

        # when
        self.indexer.run(sha1s)

        self.assert_results_ok(sha1s)

        # 2nd pass
        self.indexer.run(sha1s)

        self.assert_results_ok(sha1s)

    def test_index_one_unknown_sha1(self):
        """Unknown sha1s are not indexed"""
        sha1s = [
            self.id1,
            "799a5ef812c53907562fe379d4b3851e69c7cb15",  # unknown
            "800a5ef812c53907562fe379d4b3851e69c7cb15",  # unknown
        ]  # unknown

        # when
        self.indexer.run(sha1s)

        # then
        expected_results = [res for res in self.expected_results if res.id in sha1s]

        self.assert_results_ok(sha1s, expected_results)


class CommonContentIndexerPartitionTest:
    """Allows to factorize tests on range indexer."""

    def setUp(self):
        self.contents = sorted(OBJ_STORAGE_DATA)

    def assert_results_ok(self, partition_id, nb_partitions, actual_results):
        expected_ids = [
            c.sha1
            for c in stream_results(
                self.indexer.storage.content_get_partition,
                partition_id=partition_id,
                nb_partitions=nb_partitions,
            )
        ]

        actual_results = list(actual_results)
        for indexed_data in actual_results:
            _id = indexed_data.id
            assert _id in expected_ids

            _tool_id = indexed_data.indexer_configuration_id
            assert _tool_id == self.indexer.tool["id"]

    def test__index_contents(self):
        """Indexing contents without existing data results in indexed data"""
        partition_id = 0
        nb_partitions = 4

        actual_results = list(
            self.indexer._index_contents(partition_id, nb_partitions, indexed={})
        )

        self.assert_results_ok(partition_id, nb_partitions, actual_results)

    def test__index_contents_with_indexed_data(self):
        """Indexing contents with existing data results in less indexed data"""
        partition_id = 3
        nb_partitions = 4

        # first pass
        actual_results = list(
            self.indexer._index_contents(partition_id, nb_partitions, indexed={}),
        )

        self.assert_results_ok(partition_id, nb_partitions, actual_results)

        indexed_ids = {res.id for res in actual_results}

        actual_results = list(
            self.indexer._index_contents(
                partition_id, nb_partitions, indexed=indexed_ids
            )
        )

        # already indexed, so nothing new
        assert actual_results == []

    def test_generate_content_get(self):
        """Optimal indexing should result in indexed data"""
        partition_id = 0
        nb_partitions = 1

        actual_results = self.indexer.run(
            partition_id, nb_partitions, skip_existing=False
        )

        assert actual_results["status"] == "eventful", actual_results

    def test_generate_content_get_no_result(self):
        """No result indexed returns False"""
        actual_results = self.indexer.run(1, 2**512, incremental=False)

        assert actual_results == {"status": "uneventful"}


def mock_compute_license(path):
    """path is the content identifier"""
    if isinstance(id, bytes):
        path = path.decode("utf-8")
    # path is something like /tmp/tmpXXX/<sha1> so we keep only the sha1 part
    id_ = path.split("/")[-1]
    return {"licenses": SHA1_TO_LICENSES.get(hash_to_bytes(id_), [])}
