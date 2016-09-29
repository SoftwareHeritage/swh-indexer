# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import os
import tempfile

from . import mimetype, converters, language, ctags
from .storage import Storage

from swh.scheduler.celery_backend.config import app
from swh.core import hashutil
from swh.core.config import SWHConfig
from swh.core.serializers import msgpack_dumps, msgpack_loads
from swh.objstorage import get_objstorage


class BaseWorker(SWHConfig, metaclass=abc.ABCMeta):
    """Base worker for the indexing computations.

    Inherit from this class and override the following properties:
    - ADDITIONAL_CONFIG: to add a dictionary of extra configuration
    - CONFIG_BASE_FILENAME: the default configuration file to lookup for
    - def compute(self, *args, **kwargs): method in charge of doing
      the actual computation on sha1 or sha1's content.

    """
    DEFAULT_CONFIG = {
        'db': ('dict', {
            'conn': 'mongodb://mongodb0.example.net:27017',
            'name': 'content',
        }),
        'next_task_queue': ('str', 'next.plugged.task.queue'),
    }

    ADDITIONAL_CONFIG = {}

    CONFIG_BASE_FILENAME = 'indexer/worker'

    def __init__(self):
        super().__init__()
        self.config = self.parse_config_file(
            additional_configs=[self.ADDITIONAL_CONFIG])
        # Next task queue to send message too (can be empty for final step)
        next_task_queue = self.config.get('next_task_queue', None)
        if not next_task_queue:
            next_task_queue = None
        self.next_task_queue = next_task_queue

    @abc.abstractmethod
    def compute(self, content):
        """Method in charge of actual computations on the dictionary content.

        Args:
            - content (dict): a content with at least the 'sha1' key filled in.

        Returns:
            The updated content

        """
        pass

    def encode(self, content):
        content_copy = content
        if 'data' in content:
            content_copy = content.copy()
            content_copy['data'] = msgpack_dumps(content['data'])

        return content_copy

    def decode(self, content):
        content_copy = content
        if 'data' in content:
            content_copy = content.copy()
            content_copy['data'] = msgpack_loads(content['data'])

        return content_copy

    def compute_contents(self, contents_packed):
        """Compute what's necessary for each content and yield the updated
           content.

        """
        for content_packed in contents_packed:
            content = self.decode(content_packed)
            content_updated = self.compute(content)
            yield self.encode(content_updated)

    def run(self, contents_packed, task_destination=None, **kwargs):
        """Compute from a sha1 or sha1's content and then propagate the result
        to another queue.

        """
        contents_updated_pack = list(self.compute_contents(contents_packed))
        if task_destination:
            task = app.tasks[task_destination]
            task.delay(contents_updated_pack, self.next_task_queue)


class ReaderWorker(BaseWorker):
    """Class in charge of reading the sha1's content from objstorage and
    flush its contents in another queue.

    Note: The default config below demonstrates a configuration for a
    multiplexer objstorage. One which can only read from multiple
    objstorages based on the sha1's prefix.

    """
    CONFIG_BASE_FILENAME = 'indexer/reader'

    ADDITIONAL_CONFIG = {
        'next_task_queue': ('str', 'swh.indexer.worker.tasks.SWHMimeTypeTask'),
        'objstorage': ('dict', {
            'cls': 'multiplexer',
            'args': {
                'objstorages': [{
                    'cls': 'filtered',
                    'args': {
                        'storage_conf': {
                            'cls': 'azure-storage',
                            'args': {
                                'account_name': '0euwestswh',
                                'api_secret_key': 'secret',
                                'container_name': 'contents'
                            }
                        },
                        'filters_conf': [
                            {'type': 'readonly'},
                            {'type': 'prefix', 'prefix': '0'}
                        ]
                    }
                }, {
                    'cls': 'filtered',
                    'args': {
                        'storage_conf': {
                            'cls': 'azure-storage',
                            'args': {
                                'account_name': '1euwestswh',
                                'api_secret_key': 'secret',
                                'container_name': 'contents'
                            }
                        },
                        'filters_conf': [
                            {'type': 'readonly'},
                            {'type': 'prefix', 'prefix': '1'}
                        ]
                    }
                }]
            },
        }),
    }

    def __init__(self):
        super().__init__()
        objstorage = self.config['objstorage']
        self.objstorage = get_objstorage(objstorage['cls'], objstorage['args'])

    def compute(self, content):
        """Compute from the sha1 its content and returns it.

        """
        content_copy = content.copy()
        sha1 = hashutil.hex_to_hash(content['sha1'])
        data = self.objstorage.get(sha1)
        content_copy.update({'data': data})
        return content_copy


class DiskWorker:
    """Mixin intended to be used with other *Worker Class.

       Worker inheriting from this class are a category of workers
       which needs the disk for their computations.

       Expects:
           Have the self.working_directory defined at runtime.

    """
    def __init__(self):
        super().__init__()

    def write_to_temp(self, filename, data):
        """Write the sha1's content in a temporary file.

        Args:
            sha1 (str): the sha1 name
            filename (str): one of sha1's many filenames
            data (bytes): the sha1's content to write in temporary
            file

        Returns:
            The path to the temporary file created. That file is
            filled in with the raw content's data.

        """
        temp_dir = tempfile.mkdtemp(dir=self.working_directory)
        content_path = os.path.join(temp_dir, filename)

        with open(content_path, 'wb') as f:
            f.write(data)

        return content_path

    def cleanup(self, content_path):
        """Remove content_path from working directory.

        Args:
            content_path (str): the file to remove

        """
        os.unlink(content_path)


class PersistResultWorker:
    """Mixin intended to be used with other *Worker Class.

       Worker inheriting from this class are a category of workers
       which are able to perist the computed data in storage.

       Expects:
           Have the self.storage defined at runtime.

    """
    def save(self, content):
        """Store the content in storage.

        Args:
            content: dict with the following keys:
                - sha1: content id
                - data: raw data for the content
                - mimetype: its newly computed mimetype

        """
        content_to_store = converters.content_to_storage(content)
        self.storage.content_add(content_to_store)


class MimeTypeWorker(BaseWorker, DiskWorker, PersistResultWorker):
    """Worker in charge of computing the mimetype of a content.

    """
    CONFIG_BASE_FILENAME = 'indexer/mimetype'
    ADDITIONAL_CONFIG = {
        'workdir': ('str', '/tmp/swh/worker.mimetype'),
        'next_task_queue': ('str', 'swh.indexer.tasks.SWHLanguageTask'),
    }

    def __init__(self):
        super().__init__()
        db = self.config['db']
        self.storage = Storage(db_conn=db['conn'], db_name=db['name'])
        self.working_directory = self.config['workdir']

    def compute(self, content):
        """Compute the mimetype of the content, updates the content, stores
           the result and return the updated result.

        """
        content_copy = content.copy()
        content_path = self.write_to_temp(
            filename=content['name'],
            data=content['data'])
        typemime = mimetype.run_mimetype(content_path)
        content_copy.update({'mimetype': typemime})
        self.save(content_copy)
        self.cleanup(content_path)
        return content_copy


class LanguageWorker(BaseWorker, DiskWorker, PersistResultWorker):
    """Worker in charge of computing the mimetype of a content.

    """
    CONFIG_BASE_FILENAME = 'indexer/language'
    ADDITIONAL_CONFIG = {
        'workdir': ('str', '/tmp/swh/worker.language/'),
        'next_task_queue': ('str', 'swh.indexer.tasks.SWHCtagsTask'),
    }

    def __init__(self):
        super().__init__()
        db = self.config['db']
        self.storage = Storage(db_conn=db['conn'], db_name=db['name'])
        self.working_directory = self.config['workdir']

    def compute(self, content):
        """Compute the mimetype of the content, updates the content, stores
           the result and return the updated result.

        """
        content_copy = content.copy()
        content_path = self.write_to_temp(
            filename=content['name'],
            data=content['data'])
        lang = language.run_language(content_path)
        # Keep all the information on the resulting data (including errors)
        for key, value in lang.items():
            content_copy[key] = value
        self.save(content_copy)
        self.cleanup(content_path)
        return content_copy


class CtagsWorker(BaseWorker, DiskWorker, PersistResultWorker):
    CONFIG_BASE_FILENAME = 'indexer/ctags'
    ADDITIONAL_CONFIG = {
        'workdir': ('str', '/tmp/swh/worker.ctags/'),
        'next_task_queue': ('str', ''),  # empty for final step
    }

    def __init__(self):
        super().__init__()
        db = self.config['db']
        self.storage = Storage(db_conn=db['conn'], db_name=db['name'])
        self.working_directory = self.config['workdir']

    def compute(self, content):
        """Compute the mimetype of the content, updates the content, stores
           the result and return the updated result.

        """
        # Bypass contents with error (for now)
        if 'decoding_failure' in content:
            return content
        if not content.get('lang'):
            return content

        content_copy = content.copy()
        content_path = self.write_to_temp(
            filename=content['name'],
            data=content['data'])
        ctagsfile = ctags.run_ctags(path=content_path,
                                    lang=content.get('lang'))
        content_copy['ctags'] = list(ctags.parse_ctags(ctagsfile))
        self.save(content_copy)
        self.cleanup(content_path)
        return content_copy
