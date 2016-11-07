# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task

from .orchestrator import OrchestratorAllContentsIndexer
from .orchestrator import OrchestratorTextContentsIndexer
from .mimetype import ContentMimetypeIndexer
from .language import ContentLanguageIndexer
from .ctags import CtagsIndexer
from .license import ContentLicenseIndexer


class SWHOrchestratorAllContentsTask(Task):
    """Main task in charge of reading batch contents (of any type) and
    broadcasting them back to other tasks.

    """
    task_queue = 'swh_indexer_orchestrator_content_all'

    def run(self, *args, **kwargs):
        OrchestratorAllContentsIndexer().run(*args, **kwargs)


class SWHOrchestratorTextContentsTask(Task):
    """Main task in charge of reading batch contents (of type text) and
    broadcasting them back to other tasks.

    """
    task_queue = 'swh_indexer_orchestrator_content_text'

    def run(self, *args, **kwargs):
        OrchestratorTextContentsIndexer().run(*args, **kwargs)


class SWHContentMimetypeTask(Task):
    """Task which computes the mimetype, encoding from the sha1's content.

    """
    task_queue = 'swh_indexer_content_mimetype'

    def run(self, *args, **kwargs):
        ContentMimetypeIndexer().run(*args, **kwargs)


class SWHContentLanguageTask(Task):
    """Task which computes the language from the sha1's content.

    """
    task_queue = 'swh_indexer_content_language'

    def run(self, *args, **kwargs):
        ContentLanguageIndexer().run(*args, **kwargs)


class SWHCtagsTask(Task):
    """Task which computes ctags from the sha1's content.

    """
    task_queue = 'swh_indexer_content_ctags'

    def run(self, *args, **kwargs):
        CtagsIndexer().run(*args, **kwargs)


class SWHContentLicenseTask(Task):
    """Task which computes licenses from the sha1's content.

    """
    task_queue = 'swh_indexer_content_license'

    def run(self, *args, **kwargs):
        ContentLicenseIndexer().run(*args, **kwargs)
