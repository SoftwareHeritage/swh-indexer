# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import os
import shutil
import tempfile

from swh.core.config import SWHConfig
from swh.objstorage import get_objstorage
from swh.objstorage.exc import ObjNotFoundError
from swh.storage import get_storage


class BaseIndexer(SWHConfig,
                  metaclass=abc.ABCMeta):
    """Base class for indexers to inherit from.
    Indexers can:
    - filter out sha1 whose data has already been indexed.
    - retrieve sha1's content from objstorage, index this content then
      store the result in storage.

    Thus the following interface to implement per inheriting class:
      - def filter: filter out data already indexed (in storage)
      - def index: compute index on data (stored by sha1 in
        objstorage) and store result in storage.

    """
    CONFIG_BASE_FILENAME = 'indexer/base'

    DEFAULT_CONFIG = {
        'storage': ('dict', {
            'host': 'uffizi',
            'cls': 'pathslicing',
            'args': {'root': '/tmp/softwareheritage/objects',
                     'slicing': '0:2/2:4/4:6'}
        }),
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

    ADDITIONAL_CONFIG = {}

    def __init__(self):
        super().__init__()
        self.config = self.parse_config_file(
            additional_configs=[self.ADDITIONAL_CONFIG])
        objstorage = self.config['objstorage']
        self.objstorage = get_objstorage(objstorage['cls'], objstorage['args'])
        storage = self.config['storage']
        self.storage = get_storage(storage['cls'], storage['args'])

    @abc.abstractmethod
    def filter_contents(self, sha1s):
        """Filter missing sha1 for that particular indexer.

        Args:
            sha1s ([bytes]): list of contents' sha1

        Yields:
            iterator of missing sha1

        """
        pass

    def index_contents(self, sha1s):
        """Given a list of sha1s:
        - retrieve the content from the storage
        - execute the indexing computations
        - store the results

        """
        results = []
        for sha1 in sha1s:
            try:
                raw_content = self.objstorage.get(sha1)
            except ObjNotFoundError:
                continue
            res = self.index_content(sha1, raw_content)
            results.append(res)

        self.persist_index_computations(results)

    @abc.abstractmethod
    def index_content(self, sha1, content):
        pass

    @abc.abstractmethod
    def persist_index_computations(self, results):
        """Persist the computation resulting from the index.

        Args:
            results ([result]): List of results. One result is the
            result of the index_content function.

        """
        pass

    def run(self, sha1s):
        """Main entry point for the base indexer.

        """
        self.index_contents(sha1s)


class DiskIndexer:
    """Mixin intended to be used with other *Indexer classes.

       Indexer* inheriting from this class are a category of indexers
       which needs the disk for their computations.

       Expects:
           self.working_directory variable defined at runtime.

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
        os.makedirs(self.working_directory, exist_ok=True)
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
        temp_dir = os.path.dirname(content_path)
        shutil.rmtree(temp_dir)
