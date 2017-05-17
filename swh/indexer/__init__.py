# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


INDEXER_CLASSES = {
    'mimetype': 'swh.indexer.mimetype.ContentMimetypeIndexer',
    'language': 'swh.indexer.language.ContentLanguageIndexer',
    'ctags': 'swh.indexer.ctags.CtagsIndexer',
    'fossology_license':
    'swh.indexer.fossology_license.ContentFossologyLicenseIndexer',
}


TASK_NAMES = {
    'orchestrator_all': 'swh.indexer.tasks.SWHOrchestratorAllContentsTask',
    'orchestrator_text': 'swh.indexer.tasks.SWHOrchestratorTextContentsTask',
    'mimetype': 'swh.indexer.tasks.SWHContentMimetypeTask',
    'language': 'swh.indexer.tasks.SWHContentLanguageTask',
    'ctags': 'swh.indexer.tasks.SWHCtagsTask',
    'fossology_license': 'swh.indexer.tasks.SWHContentFossologyLicenseTask',
    'rehash': 'swh.indexer.tasks.SWHRecomputeChecksumsTask',
}


__all__ = [
    'INDEXER_CLASSES', 'TASK_NAMES',
]
