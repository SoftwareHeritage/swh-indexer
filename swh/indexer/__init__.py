# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


INDEXER_CLASSES = {
    'mimetype': 'swh.indexer.mimetype.ContentMimetypeIndexer',
    'language': 'swh.indexer.language.ContentLanguageIndexer',
    'ctags': 'swh.indexer.ctags.CtagsIndexer',
    'license': 'swh.indexer.license.ContentLicenseIndexer',
}


TASK_NAMES = {
    'orchestrator_all': 'swh.indexer.tasks.SWHOrchestratorAllContentsTask',
    'orchestrator_text': 'swh.indexer.tasks.SWHOrchestratorTextContentsTask',
    'mimetype': 'swh.indexer.tasks.SWHContentMimetypeTask',
    'language': 'swh.indexer.tasks.SWHContentLanguageTask',
    'ctags': 'swh.indexer.tasks.SWHCtagsTask',
    'license': 'swh.indexer.tasks.SWHContentLicenseTask',
}


__all__ = [
    'INDEXER_CLASSES', 'TASK_NAMES',
]
