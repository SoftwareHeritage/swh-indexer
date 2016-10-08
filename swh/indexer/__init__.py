# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .file_properties import ContentMimetypeIndexer
from .language import ContentLanguageIndexer


INDEXER_CLASSES = {
    'mimetype': ContentMimetypeIndexer,
    'language': ContentLanguageIndexer,
}


TASK_NAMES = {
    'orchestrator': 'swh.indexer.tasks.SWHOrchestratorTask',
    'mimetype': 'swh.indexer.tasks.SWHContentMimetypeTask',
    'language': 'swh.indexer.tasks.SWHContentLanguageTask',
}


__all__ = [
    'INDEXER_CLASSES', 'TASK_NAMES'
]
