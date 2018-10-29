# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


INDEXER_CLASSES = {
    'indexer_mimetype': 'swh.indexer.mimetype.ContentMimetypeIndexer',
    'indexer_language': 'swh.indexer.language.ContentLanguageIndexer',
    'indexer_ctags': 'swh.indexer.ctags.CtagsIndexer',
    'indexer_fossology_license':
    'swh.indexer.fossology_license.ContentFossologyLicenseIndexer',
}


TASK_NAMES = {
    'indexer_orchestrator_all': 'swh.indexer.tasks.OrchestratorAllContents',
    'indexer_orchestrator_text': 'swh.indexer.tasks.OrchestratorTextContents',
    'indexer_mimetype': 'swh.indexer.tasks.ContentMimetype',
    'indexer_language': 'swh.indexer.tasks.ContentLanguage',
    'indexer_ctags': 'swh.indexer.tasks.Ctags',
    'indexer_fossology_license': 'swh.indexer.tasks.ContentFossologyLicense',
    'indexer_rehash': 'swh.indexer.tasks.RecomputeChecksums',
    'indexer_revision_metadata': 'swh.indexer.tasks.RevisionMetadata',
    'indexer_origin_intrinsic_metadata': 'swh.indexer.tasks.OriginMetadata',
}


__all__ = [
    'INDEXER_CLASSES', 'TASK_NAMES',
]
