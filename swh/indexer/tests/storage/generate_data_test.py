# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from uuid import uuid1

from hypothesis.strategies import composite, one_of, sampled_from, sets, tuples, uuids

from swh.model.hashutil import MultiHash

MIMETYPES = [
    b"application/json",
    b"application/octet-stream",
    b"application/xml",
    b"text/plain",
]

ENCODINGS = [
    b"iso8859-1",
    b"iso8859-15",
    b"latin1",
    b"utf-8",
]


def gen_mimetype():
    """Generate one mimetype strategy."""
    return one_of(sampled_from(MIMETYPES))


def gen_encoding():
    """Generate one encoding strategy."""
    return one_of(sampled_from(ENCODINGS))


def _init_content(uuid):
    """Given a uuid, initialize a content"""
    return {
        "id": MultiHash.from_data(uuid.bytes, {"sha1"}).digest()["sha1"],
        "indexer_configuration_id": 1,
    }


@composite
def gen_content_mimetypes(draw, *, min_size=0, max_size=100):
    """Generate valid and consistent content_mimetypes.

    Context: Test purposes

    Args:
        **draw** (callable): Used by hypothesis to generate data
        **min_size** (int): Minimal number of elements to generate
                            (default: 0)
        **max_size** (int): Maximal number of elements to generate
                            (default: 100)

    Returns:
        List of content_mimetypes as expected by the
        content_mimetype_add api endpoint.

    """
    _ids = draw(
        sets(
            tuples(uuids(), gen_mimetype(), gen_encoding()),
            min_size=min_size,
            max_size=max_size,
        )
    )

    content_mimetypes = []
    for uuid, mimetype, encoding in _ids:
        content_mimetypes.append(
            {
                **_init_content(uuid),
                "mimetype": mimetype,
                "encoding": encoding,
            }
        )
    return content_mimetypes


TOOLS = [
    {
        "tool_name": "swh-metadata-translator",
        "tool_version": "0.0.1",
        "tool_configuration": {"type": "local", "context": "NpmMapping"},
    },
    {
        "tool_name": "swh-metadata-detector",
        "tool_version": "0.0.1",
        "tool_configuration": {
            "type": "local",
            "context": ["NpmMapping", "CodemetaMapping"],
        },
    },
    {
        "tool_name": "swh-metadata-detector2",
        "tool_version": "0.0.1",
        "tool_configuration": {
            "type": "local",
            "context": ["NpmMapping", "CodemetaMapping"],
        },
    },
    {
        "tool_name": "file",
        "tool_version": "5.22",
        "tool_configuration": {"command_line": "file --mime <filepath>"},
    },
    {
        "tool_name": "pygments",
        "tool_version": "2.0.1+dfsg-1.1+deb8u1",
        "tool_configuration": {"type": "library", "debian-package": "python3-pygments"},
    },
    {
        "tool_name": "pygments2",
        "tool_version": "2.0.1+dfsg-1.1+deb8u1",
        "tool_configuration": {
            "type": "library",
            "debian-package": "python3-pygments",
            "max_content_size": 10240,
        },
    },
    {
        "tool_name": "nomos",
        "tool_version": "3.1.0rc2-31-ga2cbb8c",
        "tool_configuration": {"command_line": "nomossa <filepath>"},
    },
]


MIMETYPE_OBJECTS = [
    {
        "id": MultiHash.from_data(uuid1().bytes, {"sha1"}).digest()["sha1"],
        "mimetype": mt,
        "encoding": enc,
        # 'indexer_configuration_id' will be added after TOOLS get registered
    }
    for mt in MIMETYPES
    for enc in ENCODINGS
]

LICENSES = [
    b"3DFX",
    b"BSD",
    b"GPL",
    b"Apache2",
    b"MIT",
]

FOSSOLOGY_LICENSES = [
    {
        "id": MultiHash.from_data(uuid1().bytes, {"sha1"}).digest()["sha1"],
        "licenses": [
            LICENSES[i % len(LICENSES)],
        ],
        # 'indexer_configuration_id' will be added after TOOLS get registered
    }
    for i in range(10)
]


def gen_license():
    return one_of(sampled_from(LICENSES))


@composite
def gen_content_fossology_licenses(draw, *, min_size=0, max_size=100):
    """Generate valid and consistent content_fossology_licenses.

    Context: Test purposes

    Args:
        **draw** (callable): Used by hypothesis to generate data
        **min_size** (int): Minimal number of elements to generate
                            (default: 0)
        **max_size** (int): Maximal number of elements to generate
                            (default: 100)

    Returns:
        List of content_fossology_licenses as expected by the
        content_fossology_license_add api endpoint.

    """
    _ids = draw(
        sets(
            tuples(
                uuids(),
                gen_license(),
            ),
            min_size=min_size,
            max_size=max_size,
        )
    )

    content_licenses = []
    for uuid, license in _ids:
        content_licenses.append(
            {
                **_init_content(uuid),
                "licenses": [license],
            }
        )
    return content_licenses
