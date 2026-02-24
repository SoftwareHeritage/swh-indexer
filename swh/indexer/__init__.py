# Copyright (C) 2016-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import TYPE_CHECKING, Dict, List, Type

if TYPE_CHECKING:
    from swh.indexer.indexer import BaseIndexer

_INDEXERS: Dict[str, Type["BaseIndexer"]] = {}


# implemented as a function to help lazy loading
def get_datastore(*args, **kw):
    from .indexer import get_indexer_storage

    return get_indexer_storage(*args, **kw)


def get_indexer_names() -> List[str]:
    from backports.entry_points_selectable import entry_points as get_entry_points

    entry_points = get_entry_points(group="swh.indexer.classes")
    return [ep.name for ep in entry_points]


def get_indexer(name: str) -> Type["BaseIndexer"]:
    if not _INDEXERS:
        _INDEXERS.update(**load_indexers())
    return _INDEXERS[name]


def load_indexers() -> Dict[str, Type["BaseIndexer"]]:
    from backports.entry_points_selectable import entry_points as get_entry_points

    entry_points = get_entry_points(group="swh.indexer.classes")
    return {ep.name: ep.load() for ep in entry_points}


default_cfg = {
    "default_interval": "1 day",
    "min_interval": "12 hours",
    "max_interval": "1 day",
    "backoff_factor": 2,
    "max_queue_length": 5000,
}
