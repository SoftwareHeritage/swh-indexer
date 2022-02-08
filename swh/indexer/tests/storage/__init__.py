# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path

import swh.indexer

SQL_DIR = path.join(path.dirname(swh.indexer.__file__), "sql")
