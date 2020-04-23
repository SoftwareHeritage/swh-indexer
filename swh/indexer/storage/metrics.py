# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import wraps
import logging

from swh.core.statsd import statsd

OPERATIONS_METRIC = "swh_indexer_storage_operations_total"
OPERATIONS_UNIT_METRIC = "swh_indexer_storage_operations_{unit}_total"
DURATION_METRIC = "swh_indexer_storage_request_duration_seconds"


def timed(f):
    """Time that function!

    """

    @wraps(f)
    def d(*a, **kw):
        with statsd.timed(DURATION_METRIC, tags={"endpoint": f.__name__}):
            return f(*a, **kw)

    return d


def send_metric(metric, count, method_name):
    """Send statsd metric with count for method `method_name`

    If count is 0, the metric is discarded.  If the metric is not
    parseable, the metric is discarded with a log message.

    Args:
        metric (str): Metric's name (e.g content:add, content:add:bytes)
        count (int): Associated value for the metric
        method_name (str): Method's name

    Returns:
        Bool to explicit if metric has been set or not
    """
    if count == 0:
        return False

    metric_type = metric.split(":")
    _length = len(metric_type)
    if _length == 2:
        object_type, operation = metric_type
        metric_name = OPERATIONS_METRIC
    elif _length == 3:
        object_type, operation, unit = metric_type
        metric_name = OPERATIONS_UNIT_METRIC.format(unit=unit)
    else:
        logging.warning("Skipping unknown metric {%s: %s}" % (metric, count))
        return False

    statsd.increment(
        metric_name,
        count,
        tags={
            "endpoint": method_name,
            "object_type": object_type,
            "operation": operation,
        },
    )
    return True


def process_metrics(f):
    """Increment object counters for the decorated function.

    """

    @wraps(f)
    def d(*a, **kw):
        r = f(*a, **kw)
        for metric, count in r.items():
            send_metric(metric=metric, count=count, method_name=f.__name__)

        return r

    return d
