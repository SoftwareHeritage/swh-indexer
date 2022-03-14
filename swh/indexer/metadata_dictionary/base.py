# Copyright (C) 2017-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
from typing import Dict, Iterable, List, Optional

from typing_extensions import TypedDict

from swh.indexer.codemeta import SCHEMA_URI, compact, merge_values


class FileEntry(TypedDict):
    name: bytes
    sha1: bytes
    sha1_git: bytes
    target: bytes
    length: int
    status: str
    type: str
    perms: int
    dir_id: bytes


class BaseMapping:
    """Base class for mappings to inherit from

    To implement a new mapping:

    - inherit this class
    - override translate function
    """

    def __init__(self, log_suffix: str = ""):
        self.log_suffix = log_suffix
        self.log = logging.getLogger(
            "%s.%s" % (self.__class__.__module__, self.__class__.__name__)
        )

    @property
    def name(self) -> str:
        """A name of this mapping, used as an identifier in the
        indexer storage."""
        raise NotImplementedError(f"{self.__class__.__name__}.name")

    @classmethod
    def detect_metadata_files(cls, file_entries: List[FileEntry]) -> List[bytes]:
        """
        Detects files potentially containing metadata

        Args:
            file_entries: list of files

        Returns:
            list: list of sha1 (possibly empty)
        """
        raise NotImplementedError(f"{cls.__name__}.detect_metadata_files")

    @classmethod
    def supported_terms(cls) -> Iterable[str]:
        """Returns all CodeMeta terms this mapping supports"""
        raise NotImplementedError(f"{cls.__name__}.supported_terms")

    def translate(self, file_content: bytes) -> Optional[Dict]:
        raise NotImplementedError(f"{self.__class__.__name__}.translate")

    def normalize_translation(self, metadata: Dict) -> Dict:
        return compact(metadata)


class SingleFileMapping(BaseMapping):
    """Base class for all mappings that use a single file as input."""

    @property
    def filename(self) -> bytes:
        """The .json file to extract metadata from."""
        raise NotImplementedError(f"{self.__class__.__name__}.filename")

    @classmethod
    def detect_metadata_files(cls, file_entries: List[FileEntry]) -> List[bytes]:
        for entry in file_entries:
            if entry["name"].lower() == cls.filename.lower():  # type: ignore
                return [entry["sha1"]]
        return []


class DictMapping(BaseMapping):
    """Base class for mappings that take as input a file that is mostly
    a key-value store (eg. a shallow JSON dict)."""

    string_fields: List[str] = []
    """List of fields that are simple strings, and don't need any
    normalization."""

    @property
    def mapping(self) -> Dict[str, str]:
        """A translation dict to map dict keys into a canonical name."""
        raise NotImplementedError(f"{self.__class__.__name__}.mapping")

    @staticmethod
    def _normalize_method_name(name: str) -> str:
        return name.replace("-", "_")

    @classmethod
    def supported_terms(cls) -> Iterable[str]:
        return {
            term
            for (key, term) in cls.mapping.items()  # type: ignore
            if key in cls.string_fields
            or hasattr(cls, "translate_" + cls._normalize_method_name(key))
            or hasattr(cls, "normalize_" + cls._normalize_method_name(key))
        }

    def _translate_dict(self, content_dict: Dict, *, normalize: bool = True) -> Dict:
        """
        Translates content  by parsing content from a dict object
        and translating with the appropriate mapping

        Args:
            content_dict: content dict to translate

        Returns:
            dict: translated metadata in json-friendly form needed for
            the indexer

        """
        translated_metadata = {"@type": SCHEMA_URI + "SoftwareSourceCode"}
        for k, v in content_dict.items():
            # First, check if there is a specific translation
            # method for this key
            translation_method = getattr(
                self, "translate_" + self._normalize_method_name(k), None
            )
            if translation_method:
                translation_method(translated_metadata, v)
            elif k in self.mapping:
                # if there is no method, but the key is known from the
                # crosswalk table
                codemeta_key = self.mapping[k]

                # if there is a normalization method, use it on the value
                normalization_method = getattr(
                    self, "normalize_" + self._normalize_method_name(k), None
                )
                if normalization_method:
                    v = normalization_method(v)
                elif k in self.string_fields and isinstance(v, str):
                    pass
                elif k in self.string_fields and isinstance(v, list):
                    v = [x for x in v if isinstance(x, str)]
                else:
                    continue

                # set the translation metadata with the normalized value
                if codemeta_key in translated_metadata:
                    translated_metadata[codemeta_key] = merge_values(
                        translated_metadata[codemeta_key], v
                    )
                else:
                    translated_metadata[codemeta_key] = v
        if normalize:
            return self.normalize_translation(translated_metadata)
        else:
            return translated_metadata


class JsonMapping(DictMapping, SingleFileMapping):
    """Base class for all mappings that use a JSON file as input."""

    def translate(self, raw_content: bytes) -> Optional[Dict]:
        """
        Translates content by parsing content from a bytestring containing
        json data and translating with the appropriate mapping

        Args:
            raw_content: raw content to translate

        Returns:
            dict: translated metadata in json-friendly form needed for
            the indexer

        """
        try:
            content: str = raw_content.decode()
        except UnicodeDecodeError:
            self.log.warning("Error unidecoding from %s", self.log_suffix)
            return None
        try:
            content_dict = json.loads(content)
        except json.JSONDecodeError:
            self.log.warning("Error unjsoning from %s", self.log_suffix)
            return None
        if isinstance(content_dict, dict):
            return self._translate_dict(content_dict)
        return None
