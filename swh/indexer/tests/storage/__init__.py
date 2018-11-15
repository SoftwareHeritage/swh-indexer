# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path
import swh.storage

from swh.model.hashutil import MultiHash
from hypothesis.strategies import (composite, sets, one_of, uuids,
                                   tuples, sampled_from)

SQL_DIR = path.join(path.dirname(swh.indexer.__file__), 'sql')


MIMETYPES = [
    b'application/json',
    b'application/octet-stream',
    b'application/xml',
    b'text/plain',
]

ENCODINGS = [
    b'iso8859-1',
    b'iso8859-15',
    b'latin1',
    b'utf-8',
]


def gen_mimetype():
    """Generate one mimetype strategy.

    """
    return one_of(sampled_from(MIMETYPES))


def gen_encoding():
    """Generate one encoding strategy.

    """
    return one_of(sampled_from(ENCODINGS))


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
            tuples(
                uuids(),
                gen_mimetype(),
                gen_encoding()
            ),
            min_size=min_size, max_size=max_size
        )
    )

    content_mimetypes = []
    for uuid, mimetype, encoding in _ids:
        content_id = MultiHash.from_data(uuid.bytes, {'sha1'}).digest()['sha1']
        content_mimetypes.append({
            'id': content_id,
            'mimetype': mimetype,
            'encoding': encoding,
            'indexer_configuration_id': 1,
        })
    return content_mimetypes
