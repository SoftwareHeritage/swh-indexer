# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.scheduler.task import Task as SchedulerTask

from .mimetype import ContentMimetypeIndexer, MimetypeRangeIndexer
from .language import ContentLanguageIndexer
from .ctags import CtagsIndexer
from .fossology_license import ContentFossologyLicenseIndexer
from .rehash import RecomputeChecksums
from .metadata import RevisionMetadataIndexer, OriginMetadataIndexer
from .origin_head import OriginHeadIndexer

logging.basicConfig(level=logging.INFO)


class Task(SchedulerTask):
    def run_task(self, *args, **kwargs):
        indexer = self.Indexer().run(*args, **kwargs)
        if hasattr(indexer, 'results'):  # indexer tasks
            return indexer.results
        return indexer


class RevisionMetadata(Task):
    task_queue = 'swh_indexer_revision_metadata'

    serializer = 'msgpack'

    Indexer = RevisionMetadataIndexer


class OriginMetadata(Task):
    task_queue = 'swh_indexer_origin_intrinsic_metadata'

    Indexer = OriginMetadataIndexer


class OriginHead(Task):
    task_queue = 'swh_indexer_origin_head'

    Indexer = OriginHeadIndexer


class ContentMimetype(Task):
    """Task which computes the mimetype, encoding from the sha1's content.

    """
    task_queue = 'swh_indexer_content_mimetype'

    Indexer = ContentMimetypeIndexer


class ContentRangeMimetype(Task):
    """Compute mimetype, encoding on a range of sha1s.

    """
    task_queue = 'swh_indexer_content_mimetype_range'

    Indexer = MimetypeRangeIndexer


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
