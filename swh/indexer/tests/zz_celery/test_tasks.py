# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def test_task_origin_metadata(
    mocker, swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_config
):

    mock_indexer = mocker.patch("swh.indexer.tasks.OriginMetadataIndexer.run")
    mock_indexer.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.indexer.tasks.OriginMetadata",
        args=["origin-url"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_task_fossology_license(
    mocker, swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_config
):

    mock_indexer = mocker.patch("swh.indexer.tasks.FossologyLicenseIndexer.run")
    mock_indexer.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.indexer.tasks.ContentFossologyLicense",
        args=["id0"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_task_recompute_checksums(
    mocker, swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_config
):

    mock_indexer = mocker.patch("swh.indexer.tasks.RecomputeChecksums.run")
    mock_indexer.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.indexer.tasks.RecomputeChecksums",
        args=[[{"blake2b256": "id"}]],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}


def test_task_mimetype(
    mocker, swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_config
):

    mock_indexer = mocker.patch("swh.indexer.tasks.MimetypeIndexer.run")
    mock_indexer.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.indexer.tasks.ContentMimetype",
        args=["id0"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
