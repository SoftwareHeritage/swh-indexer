# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.scheduler.task import Task as SchedulerTask

from .orchestrator import OrchestratorAllContentsIndexer
from .orchestrator import OrchestratorTextContentsIndexer
from .mimetype import ContentMimetypeIndexer
from .language import ContentLanguageIndexer
from .ctags import CtagsIndexer
from .fossology_license import ContentFossologyLicenseIndexer
from .rehash import RecomputeChecksums
from .metadata import RevisionMetadataIndexer, OriginMetadataIndexer

logging.basicConfig(level=logging.INFO)


class Task(SchedulerTask):
    def run_task(self, *args, **kwargs):
        indexer = self.Indexer().run(*args, **kwargs)
        if hasattr(indexer, 'results'):  # indexer tasks
            return indexer.results
        return indexer


class OrchestratorAllContents(Task):
    """Main task in charge of reading batch contents (of any type) and
    broadcasting them back to other tasks.

    """
    task_queue = 'swh_indexer_orchestrator_content_all'

    Indexer = OrchestratorAllContentsIndexer


class OrchestratorTextContents(Task):
    """Main task in charge of reading batch contents (of type text) and
    broadcasting them back to other tasks.

    """
    task_queue = 'swh_indexer_orchestrator_content_text'

    Indexer = OrchestratorTextContentsIndexer


class RevisionMetadata(Task):
    task_queue = 'swh_indexer_revision_metadata'

    serializer = 'msgpack'

    Indexer = RevisionMetadataIndexer


class OriginMetadata(Task):
    task_queue = 'swh_indexer_origin_intrinsic_metadata'

    Indexer = OriginMetadataIndexer


class ContentMimetype(Task):
    """Task which computes the mimetype, encoding from the sha1's content.

    """
    task_queue = 'swh_indexer_content_mimetype'

    Indexer = ContentMimetypeIndexer


class ContentLanguage(Task):
    """Task which computes the language from the sha1's content.

    """
    task_queue = 'swh_indexer_content_language'

    def run_task(self, *args, **kwargs):
        ContentLanguageIndexer().run(*args, **kwargs)


class Ctags(Task):
    """Task which computes ctags from the sha1's content.

    """
    task_queue = 'swh_indexer_content_ctags'

    Indexer = CtagsIndexer


class ContentFossologyLicense(Task):
    """Task which computes licenses from the sha1's content.

    """
    task_queue = 'swh_indexer_content_fossology_license'

    Indexer = ContentFossologyLicenseIndexer


class RecomputeChecksums(Task):
    """Task which recomputes hashes and possibly new ones.

    """
    task_queue = 'swh_indexer_content_rehash'

    Indexer = RecomputeChecksums
