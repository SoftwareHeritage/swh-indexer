# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import logging
from threading import Lock
from typing import Dict, List, Type

from .base import BaseExtrinsicMapping, BaseIntrinsicMapping, BaseMapping

LOGGER = logging.getLogger(__name__)

_INTRINSIC_MAPPINGS: Dict[str, Type[BaseIntrinsicMapping]] = {}
_EXTRINSIC_MAPPINGS: Dict[str, Type[BaseExtrinsicMapping]] = {}
_MAPPINGS: Dict[str, Type[BaseMapping]] = {}
_mapping_lock = Lock()


def get_mappings():
    with _mapping_lock:
        if not _MAPPINGS:
            _INTRINSIC_MAPPINGS.clear()
            _EXTRINSIC_MAPPINGS.clear()
            _MAPPINGS.clear()
            for name, map_cls in load_mappings().items():
                if issubclass(map_cls, BaseExtrinsicMapping):
                    _EXTRINSIC_MAPPINGS[name] = map_cls
                elif issubclass(map_cls, BaseIntrinsicMapping):
                    _INTRINSIC_MAPPINGS[name] = map_cls
                else:
                    raise EnvironmentError("Unknown mapping type %s", map_cls.__name__)
            _MAPPINGS.update(**_INTRINSIC_MAPPINGS)
            _MAPPINGS.update(**_EXTRINSIC_MAPPINGS)
        return _MAPPINGS.copy()


def get_intrinsic_mappings() -> Dict[str, Type[BaseIntrinsicMapping]]:
    # make sure mappings have been loaded
    get_mappings()
    return _INTRINSIC_MAPPINGS.copy()


def get_extrinsic_mappings() -> Dict[str, Type[BaseExtrinsicMapping]]:
    # make sure mappings have been loaded
    get_mappings()
    return _EXTRINSIC_MAPPINGS.copy()


def get_mapping(name) -> Type[BaseMapping]:
    return get_mappings()[name]


def get_mapping_names() -> List[str]:
    # we do not use load_mappings() because there is no need for actually
    # loading the modules, we just need the names...
    from backports.entry_points_selectable import entry_points as get_entry_points

    entry_points = get_entry_points(group="swh.indexer.metadata_mappings")
    return [ep.name for ep in entry_points]


def load_mappings() -> Dict[str, Type[BaseExtrinsicMapping]]:
    from backports.entry_points_selectable import entry_points as get_entry_points

    entry_points = get_entry_points(group="swh.indexer.metadata_mappings")
    mappings = {ep.name: ep.load() for ep in entry_points}
    return mappings


def list_terms():
    """Returns a dictionary with all supported CodeMeta terms as keys,
    and the mappings that support each of them as values."""
    d = collections.defaultdict(set)
    for mapping in get_mappings().values():
        for term in mapping.supported_terms():
            d[term].add(mapping)
    return d
