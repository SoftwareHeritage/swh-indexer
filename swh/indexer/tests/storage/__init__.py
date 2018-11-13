# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path
import swh.storage

from hypothesis.strategies import (binary, composite, sets, one_of, tuples)


SQL_DIR = path.join(path.dirname(swh.indexer.__file__), 'sql')


MIMETYPES = {
    'text/plain',
    'application/xml',
    'application/json',
    'application/octet-stream',
}

ENCODINGS = {
    'utf-8',
    'latin1',
    'iso8859-1',
    'iso8859-15',
}


def gen_content_id():
    """Generate raw id

    """
    return binary(min_size=20, max_size=20)


def gen_mimetype():
    """Generate one mimetype.

    """
    return one_of(MIMETYPES)


def gen_encoding():
    """Generate one encoding.

    """
    return one_of(ENCODINGS)


@composite
def gen_content_mimetypes(draw, *, min_size=0, max_size=100):
    """Generate valid and consistent content_mimetypes.

    Context: Test purposes

    Args:
        **draw**: Used by hypothesis to generate data
        **min_size** (int): Minimal number of elements to generate
                            (default: 0)
        **max_size** (int): Maximal number of elements to generate
                            (default: 100)

    Returns:
        List of content_mimetypes as expected by the
        content_mimetype_add api endpoint.

    """
    # tuple_mimetypes = draw(
    #     sets(
    #         tuples(
    #             gen_content_id(),
    #             gen_mimetype(),
    #             gen_encoding()
    #         )
    #     ),
    #     min_size=min_size, max_size=max_size
    # )

    _mimetypes = draw(sets(gen_content_id(),
                           min_size=min_size, max_size=max_size))
    tuple_mimetypes = []
    for mimetype in _mimetypes:
        tuple_mimetypes.append((mimetype, 'text/plain', 'utf-8'))

    # print('##### %s' % tuple_mimetypes)

    mimetypes = []
    for content_id, mimetype, encoding in tuple_mimetypes:
        mimetypes.append({
            'id': content_id,
            'mimetype': mimetype,
            'encoding': encoding,
            'indexer_configuration_id': 1,
        })

    print('##### mimetypes: %s' % mimetypes)
    return mimetypes
