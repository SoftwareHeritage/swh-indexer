# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

from typing_extensions import TypedDict
import yaml

from swh.indexer.codemeta import SCHEMA_URI, compact, merge_values
from swh.indexer.storage.interface import Sha1


class DirectoryLsEntry(TypedDict):
    target: Sha1
    sha1: Sha1
    name: bytes
    type: str


TTranslateCallable = TypeVar(
    "TTranslateCallable", bound=Callable[[Any, Dict[str, Any], Any], None]
)


def produce_terms(
    namespace: str, terms: List[str]
) -> Callable[[TTranslateCallable], TTranslateCallable]:
    """Returns a decorator that marks the decorated function as adding
    the given terms to the ``translated_metadata`` dict"""

    def decorator(f: TTranslateCallable) -> TTranslateCallable:
        if not hasattr(f, "produced_terms"):
            f.produced_terms = []  # type: ignore
        f.produced_terms.extend(namespace + term for term in terms)  # type: ignore
        return f

    return decorator


class BaseMapping:
    """Base class for :class:`BaseExtrinsicMapping` and :class:`BaseIntrinsicMapping`,
    not to be inherited directly."""

    def __init__(self, log_suffix=""):
        self.log_suffix = log_suffix
        self.log = logging.getLogger(
            "%s.%s" % (self.__class__.__module__, self.__class__.__name__)
        )

    @property
    def name(self):
        """A name of this mapping, used as an identifier in the
        indexer storage."""
        raise NotImplementedError(f"{self.__class__.__name__}.name")

    def translate(self, file_content: bytes) -> Optional[Dict]:
        """Translates metadata, from the content of a file or of a RawExtrinsicMetadata
        object."""
        raise NotImplementedError(f"{self.__class__.__name__}.translate")

    def normalize_translation(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError(f"{self.__class__.__name__}.normalize_translation")


class BaseExtrinsicMapping(BaseMapping):
    """Base class for extrinsic-metadata mappings to inherit from

    To implement a new mapping:

    - inherit this class
    - override translate function
    """

    @classmethod
    def extrinsic_metadata_formats(cls) -> Tuple[str, ...]:
        """
        Returns the list of extrinsic metadata formats which can be translated
        by this mapping
        """
        raise NotImplementedError(f"{cls.__name__}.extrinsic_metadata_formats")

    def normalize_translation(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        return compact(metadata, forgefed=True)


class BaseIntrinsicMapping(BaseMapping):
    """Base class for intrinsic-metadata mappings to inherit from

    To implement a new mapping:

    - inherit this class
    - override translate function
    """

    @classmethod
    def detect_metadata_files(cls, file_entries: List[DirectoryLsEntry]) -> List[Sha1]:
        """
        Returns the sha1 hashes of files which can be translated by this mapping
        """
        raise NotImplementedError(f"{cls.__name__}.detect_metadata_files")

    def normalize_translation(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        return compact(metadata, forgefed=False)


class SingleFileIntrinsicMapping(BaseIntrinsicMapping):
    """Base class for all intrinsic metadata mappings that use a single file as input."""

    @property
    def filename(self):
        """The .json file to extract metadata from."""
        raise NotImplementedError(f"{self.__class__.__name__}.filename")

    @classmethod
    def detect_metadata_files(cls, file_entries: List[DirectoryLsEntry]) -> List[Sha1]:
        for entry in file_entries:
            if entry["name"].lower() == cls.filename:
                return [entry["sha1"]]
        return []


class DictMapping(BaseMapping):
    """Base class for mappings that take as input a file that is mostly
    a key-value store (eg. a shallow JSON dict)."""

    string_fields = []  # type: List[str]
    """List of fields that are simple strings, and don't need any
    normalization."""

    @property
    def mapping(self):
        """A translation dict to map dict keys into a canonical name."""
        raise NotImplementedError(f"{self.__class__.__name__}.mapping")

    @staticmethod
    def _normalize_method_name(name: str) -> str:
        return name.replace("-", "_")

    @classmethod
    def supported_terms(cls):
        # one-to-one mapping from the original key to a CodeMeta term
        simple_terms = {
            term
            for (key, term) in cls.mapping.items()
            if key in cls.string_fields
            or hasattr(cls, "normalize_" + cls._normalize_method_name(key))
        }

        # more complex mapping from the original key to JSON-LD
        complex_terms = {
            term
            for meth_name in dir(cls)
            if meth_name.startswith("translate_")
            for term in getattr(getattr(cls, meth_name), "produced_terms", [])
        }

        return simple_terms | complex_terms

    def _translate_dict(
        self, content_dict: Dict, *, normalize: bool = True
    ) -> Dict[str, str]:
        """
        Translates content  by parsing content from a dict object
        and translating with the appropriate mapping

        Args:
            content_dict (dict): content dict to translate

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


class JsonMapping(DictMapping):
    """Base class for all mappings that use JSON data as input."""

    def translate(self, raw_content: bytes) -> Optional[Dict]:
        """
        Translates content by parsing content from a bytestring containing
        json data and translating with the appropriate mapping

        Args:
            raw_content (bytes): raw content to translate

        Returns:
            dict: translated metadata in json-friendly form needed for
            the indexer

        """
        try:
            raw_content_string: str = raw_content.decode()
        except UnicodeDecodeError:
            self.log.warning("Error unidecoding from %s", self.log_suffix)
            return None
        try:
            content_dict = json.loads(raw_content_string)
        except json.JSONDecodeError:
            self.log.warning("Error unjsoning from %s", self.log_suffix)
            return None
        if isinstance(content_dict, dict):
            return self._translate_dict(content_dict)
        return None


class SafeLoader(yaml.SafeLoader):
    yaml_implicit_resolvers = {
        k: [r for r in v if r[0] != "tag:yaml.org,2002:timestamp"]
        for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }


class YamlMapping(DictMapping, SingleFileIntrinsicMapping):
    """Base class for all mappings that use Yaml data as input."""

    def translate(self, raw_content: bytes) -> Optional[Dict[str, str]]:
        raw_content_string: str = raw_content.decode()
        try:
            content_dict = yaml.load(raw_content_string, Loader=SafeLoader)
        except yaml.scanner.ScannerError:
            return None

        if isinstance(content_dict, dict):
            return self._translate_dict(content_dict)

        return None
