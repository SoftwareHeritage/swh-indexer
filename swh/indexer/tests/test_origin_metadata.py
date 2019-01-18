# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery.result import AsyncResult
from unittest import mock

from swh.model.hashutil import hash_to_bytes
from swh.storage.in_memory import Storage


from swh.indexer.storage.in_memory import IndexerStorage
from swh.objstorage.objstorage_in_memory import InMemoryObjStorage

from swh.scheduler.celery_backend.runner import run_ready_tasks

from .utils import fill_storage, fill_obj_storage
from .test_origin_head import OriginHeadTestIndexer
from swh.indexer.tests.tasks import (
    RevisionMetadataTestIndexer, OriginMetadataTestIndexer)


class OriginHeadTestIndexer(OriginHeadTestIndexer):
    def prepare(self):
        super().prepare()
        self.config['tasks'] = {
            'revision_metadata': 'revision_metadata',
            'origin_intrinsic_metadata': 'origin_intrinsic_metadata',
        }


@mock.patch('swh.indexer.storage.in_memory.IndexerStorage')
@mock.patch('swh.storage.in_memory.Storage')
def test_pipeline(storage_mock, idx_storage_mock,
                  swh_app, celery_session_worker, indexer_scheduler):
    scheduler = indexer_scheduler
    # Always returns the same instance of the idx storage, because
    # this function is called by each of the three indexers.
    objstorage = InMemoryObjStorage()
    storage = Storage()
    idx_storage = IndexerStorage()

    storage_mock.return_value = storage
    idx_storage_mock.return_value = idx_storage

    fill_obj_storage(objstorage)
    fill_storage(storage)

    # TODO: find a better way to share the ContentMetadataIndexer use
    # the same objstorage instance.
    import swh.objstorage
    old_inmem_objstorage = swh.objstorage._STORAGE_CLASSES['memory']
    swh.objstorage._STORAGE_CLASSES['memory'] = lambda: objstorage
    try:
        RevisionMetadataTestIndexer.scheduler = scheduler
        OriginMetadataTestIndexer.scheduler = scheduler
        indexer = OriginHeadTestIndexer()
        indexer.scheduler = scheduler
        indexer.run(["git+https://github.com/librariesio/yarn-parser"])
        tasks = []

        tasks.extend(run_ready_tasks(scheduler, swh_app))  # Run the first task
        # Wait for the task to complete and schedule the 2nd one
        task = [x for x in tasks if x['task'] == 1]
        assert len(task) == 1
        promise = AsyncResult(id=task[0]['backend_id'])
        promise.wait()

        tasks.extend(run_ready_tasks(scheduler, swh_app))  # Run the 2nd task
        task = [x for x in tasks if x['task'] == 2]
        assert len(task) == 1
        promise = AsyncResult(id=task[0]['backend_id'])
        promise.wait()
    finally:
        swh.objstorage._STORAGE_CLASSES['memory'] = old_inmem_objstorage
        del RevisionMetadataTestIndexer.scheduler
        del OriginMetadataTestIndexer.scheduler

    origin = storage.origin_get({
        'type': 'git',
        'url': 'https://github.com/librariesio/yarn-parser'})
    rev_id = hash_to_bytes('8dbb6aeb036e7fd80664eb8bfd1507881af1ba9f')

    metadata = {
        '@context': 'https://doi.org/10.5063/schema/codemeta-2.0',
        'url':
            'https://github.com/librariesio/yarn-parser#readme',
        'codeRepository':
            'git+git+https://github.com/librariesio/yarn-parser.git',
        'author': [{
            'type': 'Person',
            'name': 'Andrew Nesbitt'
        }],
        'license': 'https://spdx.org/licenses/AGPL-3.0',
        'version': '1.0.0',
        'description':
            'Tiny web service for parsing yarn.lock files',
        'issueTracker':
            'https://github.com/librariesio/yarn-parser/issues',
        'name': 'yarn-parser',
        'keywords': ['yarn', 'parse', 'lock', 'dependencies'],
    }
    rev_metadata = {
        'id': rev_id,
        'translated_metadata': metadata,
    }
    origin_metadata = {
        'origin_id': origin['id'],
        'from_revision': rev_id,
        'metadata': metadata,
    }

    results = list(indexer.idx_storage.revision_metadata_get([rev_id]))
    for result in results:
        del result['tool']
    assert results == [rev_metadata]

    results = list(indexer.idx_storage.origin_intrinsic_metadata_get([
        origin['id']]))
    for result in results:
        del result['tool']
    assert results == [origin_metadata]
