# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Classes used internally by the in-memory idx-storage, and will be
used for the interface of the idx-storage in the near future."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, TypeVar

import attr
from typing_extensions import TypedDict

from swh.model.model import Sha1Git, dictify


class KeyDict(TypedDict):
    id: Sha1Git
    indexer_configuration_id: int


TSelf = TypeVar("TSelf")


@attr.s
class BaseRow:
    id = attr.ib(type=Sha1Git)
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
        return cls(**d)  # type: ignore


@attr.s
class ContentMimetypeRow(BaseRow):
    mimetype = attr.ib(type=str)
    encoding = attr.ib(type=str)


@attr.s
class ContentLanguageRow(BaseRow):
    lang = attr.ib(type=str)


@attr.s
class ContentCtagsEntry:
    name = attr.ib(type=str)
    kind = attr.ib(type=str)
    line = attr.ib(type=int)
    lang = attr.ib(type=str)

    def to_dict(self) -> Dict[str, Any]:
        return dictify(attr.asdict(self, recurse=False))

    @classmethod
    def from_dict(cls, d) -> ContentCtagsEntry:
        return cls(**d)


@attr.s
class ContentCtagsRow(BaseRow):
    ctags = attr.ib(type=List[ContentCtagsEntry])

    @classmethod
    def from_dict(cls, d) -> ContentCtagsRow:
        d = d.copy()
        items = d.pop("ctags")
        return cls(ctags=[ContentCtagsEntry.from_dict(item) for item in items], **d,)


@attr.s
class ContentLicensesRow(BaseRow):
    licenses = attr.ib(type=List[str])


@attr.s
class ContentMetadataRow(BaseRow):
    metadata = attr.ib(type=Dict[str, Any])


@attr.s
class RevisionIntrinsicMetadataRow(BaseRow):
    metadata = attr.ib(type=Dict[str, Any])
    mappings = attr.ib(type=List[str])


@attr.s
class OriginIntrinsicMetadataRow(BaseRow):
    metadata = attr.ib(type=Dict[str, Any])
    from_revision = attr.ib(type=Sha1Git)
    mappings = attr.ib(type=List[str])
