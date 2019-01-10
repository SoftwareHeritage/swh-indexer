# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.scheduler.task import Task as SchedulerTask

from .mimetype import MimetypeIndexer, MimetypeRangeIndexer
from .language import LanguageIndexer
from .ctags import CtagsIndexer
from .fossology_license import (
    FossologyLicenseIndexer, FossologyLicenseRangeIndexer
)
from .rehash import RecomputeChecksums
from .metadata import RevisionMetadataIndexer, OriginMetadataIndexer
from .origin_head import OriginHeadIndexer

logging.basicConfig(level=logging.INFO)


class Task(SchedulerTask):
    """Task whose results is needed for other computations.

    """
    def run_task(self, *args, **kwargs):
        indexer = self.Indexer().run(*args, **kwargs)
        if hasattr(indexer, 'results'):  # indexer tasks
            return indexer.results
        return indexer


class StatusTask(SchedulerTask):
    """Task which returns a status either eventful or uneventful.

    """
    def run_task(self, *args, **kwargs):
        results = self.Indexer().run(*args, **kwargs)
        return {'status': 'eventful' if results else 'uneventful'}


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


class ContentMimetype(StatusTask):
    """Compute (mimetype, encoding) on a list of sha1s' content.

    """
    task_queue = 'swh_indexer_content_mimetype'
    Indexer = MimetypeIndexer


class ContentRangeMimetype(StatusTask):
    """Compute (mimetype, encoding) on a range of sha1s.

    """
    task_queue = 'swh_indexer_content_mimetype_range'
    Indexer = MimetypeRangeIndexer


class ContentLanguage(Task):
    """Task which computes the language from the sha1's content.

    """
    task_queue = 'swh_indexer_content_language'

    Indexer = LanguageIndexer


class Ctags(Task):
    """Task which computes ctags from the sha1's content.

    """
    task_queue = 'swh_indexer_content_ctags'

    Indexer = CtagsIndexer


class ContentFossologyLicense(Task):
    """Compute fossology licenses on a list of sha1s' content.

    """
    task_queue = 'swh_indexer_content_fossology_license'
    Indexer = FossologyLicenseIndexer


class ContentRangeFossologyLicense(StatusTask):
    """Compute fossology license on a range of sha1s.

    """
    task_queue = 'swh_indexer_content_fossology_license_range'
    Indexer = FossologyLicenseRangeIndexer


class RecomputeChecksums(Task):
    """Task which recomputes hashes and possibly new ones.

    """
    task_queue = 'swh_indexer_content_rehash'

    Indexer = RecomputeChecksums
