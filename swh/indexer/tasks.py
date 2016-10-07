# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task
from .file_properties import ContentMimetypeIndexer


class SWHContentMimetypeTask(Task):
    """Main task which computes the mimetype, encoding from the sha1's content.

    """
    task_queue = 'swh_indexer_worker_content_mimetype'

    def run(self, *args, **kwargs):
        ContentMimetypeIndexer().run(*args, **kwargs)
