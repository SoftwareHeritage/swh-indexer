from os import path

import swh.indexer

__all__ = ["start_worker_thread"]

SQL_DIR = path.join(path.dirname(swh.indexer.__file__), "sql")
