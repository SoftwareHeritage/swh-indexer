# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Classes used internally by the in-memory idx-storage, and will be
used for the interface of the idx-storage in the near future."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar

import attr
from typing_extensions import Final

from swh.model.model import Sha1Git, dictify

TSelf = TypeVar("TSelf")


@attr.s
class BaseRow:
    UNIQUE_KEY_FIELDS: Tuple = ("id", "indexer_configuration_id")

    id = attr.ib(type=Any)
    indexer_configuration_id = attr.ib(type=Optional[int], default=None, kw_only=True)
    tool = attr.ib(type=Optional[Dict], default=None, kw_only=True)

    def __attrs_post_init__(self):
        if self.indexer_configuration_id is None and self.tool is None:
            raise TypeError("Either indexer_configuration_id or tool must be not None.")
        if self.indexer_configuration_id is not None and self.tool is not None:
            raise TypeError(
                "indexer_configuration_id and tool are mutually exclusive; "
                "only one may be not None."
            )

    def anonymize(self: TSelf) -> Optional[TSelf]:
        # Needed to implement swh.journal.writer.ValueProtocol
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Wrapper of `attr.asdict` that can be overridden by subclasses
        that have special handling of some of the fields."""
        d = dictify(attr.asdict(self, recurse=False))
        if d["indexer_configuration_id"] is None:
            del d["indexer_configuration_id"]
        if d["tool"] is None:
            del d["tool"]

        return d

    @classmethod
    def from_dict(cls: Type[TSelf], d) -> TSelf:
        return cls(**d)

    def unique_key(self) -> Dict:
        obj = self

        # tool["id"] and obj.indexer_configuration_id are the same value, but
        # only one of them is set for any given object
        if obj.indexer_configuration_id is None:
            assert obj.tool  # constructors ensures tool XOR indexer_configuration_id
            obj = attr.evolve(obj, indexer_configuration_id=obj.tool["id"], tool=None)

        return {key: getattr(obj, key) for key in self.UNIQUE_KEY_FIELDS}


@attr.s
class ContentMimetypeRow(BaseRow):
    object_type: Final = "content_mimetype"

    id = attr.ib(type=Sha1Git)
    mimetype = attr.ib(type=str)
    encoding = attr.ib(type=str)


@attr.s
class ContentLanguageRow(BaseRow):
    object_type: Final = "content_language"

    id = attr.ib(type=Sha1Git)
    lang = attr.ib(type=str)


@attr.s
class ContentCtagsRow(BaseRow):
    object_type: Final = "content_ctags"
    UNIQUE_KEY_FIELDS = (
        "id",
        "indexer_configuration_id",
        "name",
        "kind",
        "line",
        "lang",
    )

    id = attr.ib(type=Sha1Git)
    name = attr.ib(type=str)
    kind = attr.ib(type=str)
    line = attr.ib(type=int)
    lang = attr.ib(type=str)


@attr.s
class ContentLicenseRow(BaseRow):
    object_type: Final = "content_fossology_license"
    UNIQUE_KEY_FIELDS = ("id", "indexer_configuration_id", "license")

    id = attr.ib(type=Sha1Git)
    license = attr.ib(type=str)


@attr.s
class ContentMetadataRow(BaseRow):
    object_type: Final = "content_metadata"

    id = attr.ib(type=Sha1Git)
    metadata = attr.ib(type=Dict[str, Any])


@attr.s
class DirectoryIntrinsicMetadataRow(BaseRow):
    object_type: Final = "directory_intrinsic_metadata"

    id = attr.ib(type=Sha1Git)
    metadata = attr.ib(type=Dict[str, Any])
    mappings = attr.ib(type=List[str])


@attr.s
class OriginIntrinsicMetadataRow(BaseRow):
    object_type: Final = "origin_intrinsic_metadata"

    id = attr.ib(type=str)
    metadata = attr.ib(type=Dict[str, Any])
    from_directory = attr.ib(type=Sha1Git)
    mappings = attr.ib(type=List[str])


@attr.s
class OriginExtrinsicMetadataRow(BaseRow):
    object_type: Final = "origin_extrinsic_metadata"

    id = attr.ib(type=str)
    """origin URL"""
    metadata = attr.ib(type=Dict[str, Any])
    from_remd_id = attr.ib(type=Sha1Git)
    """id of the RawExtrinsicMetadata object used as source for indexed metadata"""
    mappings = attr.ib(type=List[str])
