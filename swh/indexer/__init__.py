# Copyright (C) 2016-2018  The Software Heritage developers
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
    'orchestrator_all': 'swh.indexer.tasks.OrchestratorAllContents',
    'orchestrator_text': 'swh.indexer.tasks.OrchestratorTextContents',
    'mimetype': 'swh.indexer.tasks.ContentMimetype',
    'language': 'swh.indexer.tasks.ContentLanguage',
    'ctags': 'swh.indexer.tasks.Ctags',
    'fossology_license': 'swh.indexer.tasks.ContentFossologyLicense',
    'rehash': 'swh.indexer.tasks.RecomputeChecksums',
    'revision_metadata': 'swh.indexer.tasks.RevisionMetadata',
    'origin_intrinsic_metadata': 'swh.indexer.tasks.OriginMetadata',
}


__all__ = [
    'INDEXER_CLASSES', 'TASK_NAMES',
]
