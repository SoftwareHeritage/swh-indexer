# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch

from swh.indexer.storage.metrics import (
    send_metric,
    OPERATIONS_METRIC,
    OPERATIONS_UNIT_METRIC,
)


def test_send_metric_unknown_unit():
    r = send_metric("content", count=10, method_name="content_add")
    assert r is False
    r = send_metric("sthg:add:bytes:extra", count=10, method_name="sthg_add")
    assert r is False


def test_send_metric_no_value():
    r = send_metric("content_mimetype:add", count=0, method_name="content_mimetype_add")
    assert r is False


@patch("swh.indexer.storage.metrics.statsd.increment")
def test_send_metric_no_unit(mock_statsd):
    r = send_metric(
        "content_mimetype:add", count=10, method_name="content_mimetype_add"
    )

    mock_statsd.assert_called_with(
        OPERATIONS_METRIC,
        10,
        tags={
            "endpoint": "content_mimetype_add",
            "object_type": "content_mimetype",
            "operation": "add",
        },
    )

    assert r


@patch("swh.indexer.storage.metrics.statsd.increment")
def test_send_metric_unit(mock_statsd):
    unit_ = "bytes"
    r = send_metric("c:add:%s" % unit_, count=100, method_name="c_add")

    expected_metric = OPERATIONS_UNIT_METRIC.format(unit=unit_)
    mock_statsd.assert_called_with(
        expected_metric,
        100,
        tags={"endpoint": "c_add", "object_type": "c", "operation": "add",},
    )

    assert r
