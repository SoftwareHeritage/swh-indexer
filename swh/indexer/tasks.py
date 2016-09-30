# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task
from .worker import ReaderWorker, FilePropertiesWorker, LanguageWorker
from .worker import CtagsWorker


class SWHReaderTask(Task):
    """Main task that read from storage the sha1's content.

    """
    task_queue = 'swh_indexer_worker_reader'

    def run(self, *args, **kwargs):
        ReaderWorker().run(*args, **kwargs)


class SWHFilePropertiesTask(Task):
    """Main task which computes the mime type from the sha1's content.

    """
    task_queue = 'swh_indexer_worker_file_properties'

    def run(self, *args, **kwargs):
        FilePropertiesWorker().run(*args, **kwargs)


class SWHLanguageTask(Task):
    """Main task which computes the language from the sha1's content.

    """
    task_queue = 'swh_indexer_worker_language'

    def run(self, *args, **kwargs):
        LanguageWorker().run(*args, **kwargs)


class SWHCtagsTask(Task):
    """Main task which computes the ctags from the sha1's content.

    """
    task_queue = 'swh_indexer_worker_ctags'

    def run(self, *args, **kwargs):
        CtagsWorker().run(*args, **kwargs)
