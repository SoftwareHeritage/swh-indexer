# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import datetime
import functools
from typing import Any, Dict
import unittest

from hypothesis import strategies

from swh.core.api.classes import stream_results
from swh.indexer.storage import INDEXER_CFG_KEY
from swh.model import hashutil
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
    TargetType,
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


DIRECTORY = Directory(
    id=hash_to_bytes("34f335a750111ca0a8b64d8034faec9eedc396be"),
    entries=(
        DirectoryEntry(
            name=b"index.js",
            type="file",
            target=hash_to_bytes("01c9379dfc33803963d07c1ccc748d3fe4c96bb5"),
            perms=0o100644,
        ),
        DirectoryEntry(
            name=b"package.json",
            type="file",
            target=hash_to_bytes("26a9f72a7c87cc9205725cfd879f514ff4f3d8d5"),
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
    id=b"\xf8zz\xa1\x12`<1$\xfav\xf9\x01\xfd5\x85F`\xf2\xb6",
    entries=(
        DirectoryEntry(
            name=b"package.json",
            type="file",
            target=hash_to_bytes("f5305243b3ce7ef8dc864ebc73794da304025beb"),
            perms=0o100644,
        ),
    ),
)

_utc_plus_2 = datetime.timezone(datetime.timedelta(minutes=120))

REVISION = Revision(
    id=hash_to_bytes("c6201cb1b9b9df9a7542f9665c3b5dfab85e9775"),
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
        id=hash_to_bytes("a50fde72265343b7d28cecf6db20d98a81d21965"),
        branches={
            b"refs/heads/add-revision-origin-cache": SnapshotBranch(
                target=b'L[\xce\x1c\x88\x8eF\t\xf1"\x19\x1e\xfb\xc0s\xe7/\xe9l\x1e',
                target_type=TargetType.REVISION,
            ),
            b"refs/head/master": SnapshotBranch(
                target=b"8K\x12\x00d\x03\xcc\xe4]bS\xe3\x8f{\xd7}\xac\xefrm",
                target_type=TargetType.REVISION,
            ),
            b"HEAD": SnapshotBranch(
                target=b"refs/head/master", target_type=TargetType.ALIAS
            ),
            b"refs/tags/v0.0.103": SnapshotBranch(
                target=b'\xb6"Im{\xfdLb\xb0\x94N\xea\x96m\x13x\x88+\x0f\xdd',
                target_type=TargetType.RELEASE,
            ),
        },
    ),
    # rsync://ftp.gnu.org/gnu/3dldf
    Snapshot(
        id=hash_to_bytes("2c67f69a416bca4e1f3fcd848c588fab88ad0642"),
        branches={
            b"3DLDF-1.1.4.tar.gz": SnapshotBranch(
                target=b'dJ\xfb\x1c\x91\xf4\x82B%]6\xa2\x90|\xd3\xfc"G\x99\x11',
                target_type=TargetType.REVISION,
            ),
            b"3DLDF-2.0.2.tar.gz": SnapshotBranch(
                target=b"\xb6\x0e\xe7\x9e9\xac\xaa\x19\x9e=\xd1\xc5\x00\\\xc6\xfc\xe0\xa6\xb4V",  # noqa
                target_type=TargetType.REVISION,
            ),
            b"3DLDF-2.0.3-examples.tar.gz": SnapshotBranch(
                target=b"!H\x19\xc0\xee\x82-\x12F1\xbd\x97\xfe\xadZ\x80\x80\xc1\x83\xff",  # noqa
                target_type=TargetType.REVISION,
            ),
            b"3DLDF-2.0.3.tar.gz": SnapshotBranch(
                target=b"\x8e\xa9\x8e/\xea}\x9feF\xf4\x9f\xfd\xee\xcc\x1a\xb4`\x8c\x8by",  # noqa
                target_type=TargetType.REVISION,
            ),
            b"3DLDF-2.0.tar.gz": SnapshotBranch(
                target=b"F6*\xff(?\x19a\xef\xb6\xc2\x1fv$S\xe3G\xd3\xd1m",
                target_type=TargetType.REVISION,
            ),
        },
    ),
    # https://forge.softwareheritage.org/source/jesuisgpl/",
    Snapshot(
        id=hash_to_bytes("68c0d26104d47e278dd6be07ed61fafb561d0d20"),
        branches={
            b"master": SnapshotBranch(
                target=b"\xe7n\xa4\x9c\x9f\xfb\xb7\xf76\x11\x08{\xa6\xe9\x99\xb1\x9e]q\xeb",  # noqa
                target_type=TargetType.REVISION,
            )
        },
    ),
    # https://old-pypi.example.org/project/limnoria/
    Snapshot(
        id=hash_to_bytes("f255245269e15fc99d284affd79f766668de0b67"),
        branches={
            b"HEAD": SnapshotBranch(
                target=b"releases/2018.09.09", target_type=TargetType.ALIAS
            ),
            b"releases/2018.09.01": SnapshotBranch(
                target=b"<\xee1(\xe8\x8d_\xc1\xc9\xa6rT\xf1\x1d\xbb\xdfF\xfdw\xcf",
                target_type=TargetType.REVISION,
            ),
            b"releases/2018.09.09": SnapshotBranch(
                target=b"\x83\xb9\xb6\xc7\x05\xb1%\xd0\xfem\xd8kA\x10\x9d\xc5\xfa2\xf8t",  # noqa
                target_type=TargetType.REVISION,
            ),
        },
    ),
    # https://pypi.org/project/limnoria/
    Snapshot(
        branches={
            b"HEAD": SnapshotBranch(
                target=b"releases/2018.09.09", target_type=TargetType.ALIAS
            ),
            b"releases/2018.09.01": SnapshotBranch(
                target=b"<\xee1(\xe8\x8d_\xc1\xc9\xa6rT\xf1\x1d\xbb\xdfF\xfdw\xcf",
                target_type=TargetType.RELEASE,
            ),
            b"releases/2018.09.09": SnapshotBranch(
                target=b"\x83\xb9\xb6\xc7\x05\xb1%\xd0\xfem\xd8kA\x10\x9d\xc5\xfa2\xf8t",  # noqa
                target_type=TargetType.RELEASE,
            ),
        },
    ),
    # http://0-512-md.googlecode.com/svn/
    Snapshot(
        id=hash_to_bytes("a1a28c0ab387a8f9e0618cb705eab81fc448f473"),
        branches={
            b"master": SnapshotBranch(
                target=b"\xe4?r\xe1,\x88\xab\xec\xe7\x9a\x87\xb8\xc9\xad#.\x1bw=\x18",
                target_type=TargetType.REVISION,
            )
        },
    ),
    # https://github.com/librariesio/yarn-parser
    Snapshot(
        id=hash_to_bytes("bb4fd3a836930ce629d912864319637040ff3040"),
        branches={
            b"HEAD": SnapshotBranch(
                target=REVISION.id,
                target_type=TargetType.REVISION,
            )
        },
    ),
    # https://github.com/librariesio/yarn-parser.git
    Snapshot(
        id=hash_to_bytes("bb4fd3a836930ce629d912864319637040ff3040"),
        branches={
            b"HEAD": SnapshotBranch(
                target=REVISION.id,
                target_type=TargetType.REVISION,
            )
        },
    ),
    # https://npm.example.org/yarn-parser
    Snapshot(
        branches={
            b"HEAD": SnapshotBranch(
                target=RELEASE.id,
                target_type=TargetType.RELEASE,
            )
        },
    ),
]

assert len(SNAPSHOTS) == len(ORIGIN_VISITS)


SHA1_TO_LICENSES = {
    "01c9379dfc33803963d07c1ccc748d3fe4c96bb5": ["GPL"],
    "02fb2c89e14f7fab46701478c83779c7beb7b069": ["Apache2.0"],
    "103bc087db1d26afc3a0283f38663d081e9b01e6": ["MIT"],
    "688a5ef812c53907562fe379d4b3851e69c7cb15": ["AGPL"],
    "da39a3ee5e6b4b0d3255bfef95601890afd80709": [],
}


SHA1_TO_CTAGS = {
    "01c9379dfc33803963d07c1ccc748d3fe4c96bb5": [
        {
            "name": "foo",
            "kind": "str",
            "line": 10,
            "lang": "bar",
        }
    ],
    "d4c647f0fc257591cc9ba1722484229780d1c607": [
        {
            "name": "let",
            "kind": "int",
            "line": 100,
            "lang": "haskell",
        }
    ],
    "688a5ef812c53907562fe379d4b3851e69c7cb15": [
        {
            "name": "symbol",
            "kind": "float",
            "line": 99,
            "lang": "python",
        }
    ],
}


OBJ_STORAGE_DATA = {
    "01c9379dfc33803963d07c1ccc748d3fe4c96bb5": b"this is some text",
    "688a5ef812c53907562fe379d4b3851e69c7cb15": b"another text",
    "8986af901dd2043044ce8f0d8fc039153641cf17": b"yet another text",
    "02fb2c89e14f7fab46701478c83779c7beb7b069": b"""
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
    "103bc087db1d26afc3a0283f38663d081e9b01e6": b"""
        #ifndef __AVL__
        #define __AVL__

        typedef struct _avl_tree avl_tree;

        typedef struct _data_t {
          int content;
        } data_t;
    """,
    "93666f74f1cf635c8c8ac118879da6ec5623c410": b"""
    (should 'pygments (recognize 'lisp 'easily))

    """,
    "26a9f72a7c87cc9205725cfd879f514ff4f3d8d5": b"""
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
    "d4c647f0fc257591cc9ba1722484229780d1c607": b"""
    {
      "version": "5.0.3",
      "name": "npm",
      "description": "a package manager for JavaScript",
      "keywords": [
        "install",
        "modules",
        "package manager",
        "package.json"
      ],
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
    "a7ab314d8a11d2c93e3dcf528ca294e7b431c449": b"""
    """,
    "da39a3ee5e6b4b0d3255bfef95601890afd80709": b"",
    # was 626364 / b'bcd'
    "e3e40fee6ff8a52f06c3b428bfe7c0ed2ef56e92": b"unimportant content for bcd",
    # was 636465 / b'cde' now yarn-parser package.json
    "f5305243b3ce7ef8dc864ebc73794da304025beb": b"""
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
      "keywords": [
        "yarn",
        "parse",
        "lock",
        "dependencies"
      ],
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
    "keywords": ["yarn", "parse", "lock", "dependencies"],
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
            for (key, value) in data.items():
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
    for (obj_id, content) in OBJ_STORAGE_DATA.items():
        obj_storage.add(content, obj_id=hash_to_bytes(obj_id))


def fill_storage(storage):
    storage.origin_add(ORIGINS)
    storage.directory_add([DIRECTORY, DIRECTORY2])
    storage.revision_add(REVISIONS)
    storage.release_add(RELEASES)
    storage.snapshot_add(SNAPSHOTS)

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

    contents = []
    for (obj_id, content) in OBJ_STORAGE_DATA.items():
        content_hashes = hashutil.MultiHash.from_data(content).digest()
        contents.append(
            Content(
                data=content,
                length=len(content),
                status="visible",
                sha1=hash_to_bytes(obj_id),
                sha1_git=hash_to_bytes(obj_id),
                sha256=content_hashes["sha256"],
                blake2s256=content_hashes["blake2s256"],
            )
        )
    storage.content_add(contents)


class CommonContentIndexerTest(metaclass=abc.ABCMeta):
    def get_indexer_results(self, ids):
        """Override this for indexers that don't have a mock storage."""
        return self.indexer.idx_storage.state

    def assert_results_ok(self, sha1s, expected_results=None):
        sha1s = [
            sha1 if isinstance(sha1, bytes) else hash_to_bytes(sha1) for sha1 in sha1s
        ]
        actual_results = list(self.get_indexer_results(sha1s))

        if expected_results is None:
            expected_results = self.expected_results

        self.assertEqual(expected_results, actual_results)

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
        """Unknown sha1 are not indexed"""
        sha1s = [
            self.id1,
            "799a5ef812c53907562fe379d4b3851e69c7cb15",  # unknown
            "800a5ef812c53907562fe379d4b3851e69c7cb15",
        ]  # unknown

        # when
        self.indexer.run(sha1s)

        # then
        expected_results = [
            res
            for res in self.expected_results
            if hashutil.hash_to_hex(res.id) in sha1s
        ]

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
