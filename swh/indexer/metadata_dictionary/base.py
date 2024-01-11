# Copyright (C) 2017-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
from typing import Any, Callable, Dict, List, Optional, Pattern, Tuple, TypeVar, Union
import uuid
import xml.parsers.expat

from pyld import jsonld
import rdflib
from typing_extensions import TypedDict
import xmltodict
import yaml

from swh.indexer.codemeta import _document_loader, compact
from swh.indexer.namespaces import RDF, SCHEMA
from swh.indexer.storage.interface import Sha1

from .utils import add_url_if_valid

TMP_ROOT_URI_PREFIX = "https://www.softwareheritage.org/schema/2022/indexer/tmp-node/"
"""Prefix used to generate temporary URIs for root nodes being translated."""


class DirectoryLsEntry(TypedDict):
    target: Sha1
    sha1: Optional[Sha1]
    name: bytes
    type: str


TTranslateCallable = TypeVar(
    "TTranslateCallable",
    bound=Callable[[Any, rdflib.Graph, rdflib.term.BNode, Any], None],
)


def produce_terms(*uris: str) -> Callable[[TTranslateCallable], TTranslateCallable]:
    """Returns a decorator that marks the decorated function as adding
    the given terms to the ``translated_metadata`` dict"""

    def decorator(f: TTranslateCallable) -> TTranslateCallable:
        if not hasattr(f, "produced_terms"):
            f.produced_terms = []  # type: ignore
        f.produced_terms.extend(uris)  # type: ignore
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

    def translate(self, raw_content: bytes) -> Optional[Dict]:
        """
        Translates content by parsing content from a bytestring containing
        mapping-specific data and translating with the appropriate mapping
        to JSON-LD using the Codemeta and ForgeFed vocabularies.

        Args:
            raw_content: raw content to translate

        Returns:
            translated metadata in JSON friendly form needed for the content
            if parseable, :const:`None` otherwise.

        """
        raise NotImplementedError(f"{self.__class__.__name__}.translate")

    def normalize_translation(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError(f"{self.__class__.__name__}.normalize_translation")


class BaseExtrinsicMapping(BaseMapping):
    """Base class for extrinsic_metadata mappings to inherit from

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

    filename: Union[bytes, Pattern[bytes]]

    @classmethod
    def detect_metadata_files(cls, file_entries: List[DirectoryLsEntry]) -> List[Sha1]:
        filename = cls.filename
        # Check if filename is a regex or bytes:
        if isinstance(filename, bytes):
            for entry in file_entries:
                if entry["name"].lower() == filename.lower():
                    if entry["sha1"] is not None:  # ignore skipped_content and dangling
                        return [entry["sha1"]]
        else:
            for entry in file_entries:
                if filename.match(entry["name"]):
                    if entry["sha1"] is not None:  # ignore skipped_content and dangling
                        return [entry["sha1"]]

        return []


class DictMapping(BaseMapping):
    """Base class for mappings that take as input a file that is mostly
    a key-value store (eg. a shallow JSON dict)."""

    string_fields: List[str] = []
    """List of fields that are simple strings, and don't need any
    normalization."""

    date_fields: List[str] = []
    """List of fields that are strings that should be typed as http://schema.org/Date
    """

    uri_fields: List[str] = []
    """List of fields that are simple URIs, and don't need any
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
            str(term)
            for (key, term) in cls.mapping.items()
            if key in cls.string_fields + cls.date_fields + cls.uri_fields
            or hasattr(cls, "normalize_" + cls._normalize_method_name(key))
        }

        # more complex mapping from the original key to JSON-LD
        complex_terms = {
            str(term)
            for meth_name in dir(cls)
            if meth_name.startswith("translate_")
            for term in getattr(getattr(cls, meth_name), "produced_terms", [])
        }

        return simple_terms | complex_terms

    def get_root_uri(self, content_dict: Dict) -> rdflib.URIRef:
        """Returns an URI for the SoftwareSourceCode or Repository being described.

        The default implementation uses a temporary URI that is stripped before
        normalization by :meth:`_translate_dict`.
        """
        # The main object being described (the SoftwareSourceCode) does not necessarily
        # may or may not have an id.
        # If it does, it will need to be set by a subclass.
        # If it doesn't we temporarily use this URI to identify it. Unfortunately,
        # we cannot use a blank node as we need to use it for JSON-LD framing later,
        # and blank nodes cannot be used for framing in JSON-LD >= 1.1
        root_id = TMP_ROOT_URI_PREFIX + str(uuid.uuid4())
        return rdflib.URIRef(root_id)

    def _translate_dict(self, content_dict: Dict) -> Dict[str, Any]:
        """
        Translates content  by parsing content from a dict object
        and translating with the appropriate mapping

        Args:
            content_dict (dict): content dict to translate

        Returns:
            dict: translated metadata in json-friendly form needed for
            the indexer

        """
        graph = rdflib.Graph()

        root = self.get_root_uri(content_dict)

        self._translate_to_graph(graph, root, content_dict)

        self.sanitize(graph)

        # Convert from rdflib's internal graph representation to JSON
        s = graph.serialize(format="application/ld+json")

        # Load from JSON to a list of Python objects
        jsonld_graph = json.loads(s)

        # Use JSON-LD framing to turn the graph into a rooted tree
        # frame = {"@type": str(SCHEMA.SoftwareSourceCode)}
        translated_metadata = jsonld.frame(
            jsonld_graph,
            {"@id": str(root)},
            options={
                "documentLoader": _document_loader,
                "processingMode": "json-ld-1.1",
            },
        )

        # Remove the temporary id we added at the beginning
        assert isinstance(translated_metadata["@id"], str)
        if translated_metadata["@id"].startswith(TMP_ROOT_URI_PREFIX):
            del translated_metadata["@id"]

        return self.normalize_translation(translated_metadata)

    def _translate_to_graph(
        self, graph: rdflib.Graph, root: rdflib.term.Identifier, content_dict: Dict
    ) -> None:
        """
        Translates content  by parsing content from a dict object
        and translating with the appropriate mapping to the graph passed as parameter

        Args:
            content_dict (dict): content dict to translate

        """
        graph.add((root, RDF.type, SCHEMA.SoftwareSourceCode))

        for k, v in content_dict.items():
            # First, check if there is a specific translation
            # method for this key
            translation_method = getattr(
                self, "translate_" + self._normalize_method_name(k), None
            )
            if translation_method:
                translation_method(graph, root, v)
            elif k in self.mapping:
                # if there is no method, but the key is known from the
                # crosswalk table
                codemeta_key = self.mapping[k]

                # if there is a normalization method, use it on the value,
                # and add its results to the triples
                normalization_method = getattr(
                    self, "normalize_" + self._normalize_method_name(k), None
                )
                if normalization_method:
                    v = normalization_method(v)
                    if v is None:
                        pass
                    elif isinstance(v, list):
                        for item in reversed(v):
                            if isinstance(item, rdflib.URIRef):
                                add_url_if_valid(graph, root, codemeta_key, str(item))
                            else:
                                graph.add((root, codemeta_key, item))
                    else:
                        if isinstance(v, rdflib.URIRef):
                            add_url_if_valid(graph, root, codemeta_key, str(v))
                        else:
                            graph.add((root, codemeta_key, v))
                elif k in self.string_fields and isinstance(v, str):
                    graph.add((root, codemeta_key, rdflib.Literal(v)))
                elif k in self.string_fields and isinstance(v, list):
                    for item in v:
                        graph.add((root, codemeta_key, rdflib.Literal(item)))
                elif k in self.date_fields and isinstance(v, str):
                    typed_v = rdflib.Literal(v, datatype=SCHEMA.Date)
                    graph.add((root, codemeta_key, typed_v))
                elif k in self.date_fields and isinstance(v, list):
                    for item in v:
                        if isinstance(item, str):
                            typed_item = rdflib.Literal(item, datatype=SCHEMA.Date)
                            graph.add((root, codemeta_key, typed_item))
                elif k in self.uri_fields and isinstance(v, str):
                    add_url_if_valid(graph, root, codemeta_key, v)
                elif k in self.uri_fields and isinstance(v, list):
                    for item in v:
                        add_url_if_valid(graph, root, codemeta_key, item)
                else:
                    continue

        self.extra_translation(graph, root, content_dict)

    def sanitize(self, graph: rdflib.Graph) -> None:
        # Remove triples that make PyLD crash
        for subject, predicate, _ in graph.triples((None, None, rdflib.URIRef(""))):
            graph.remove((subject, predicate, rdflib.URIRef("")))

        # Should not happen, but we's better check as this may lead to incorrect data
        invalid = False
        for triple in graph.triples((rdflib.URIRef(""), None, None)):
            invalid = True
            logging.error("Empty triple subject URI: %r", triple)
        if invalid:
            raise ValueError("Empty triple subject(s)")

    def extra_translation(
        self, graph: rdflib.Graph, root: rdflib.term.Node, d: Dict[str, Any]
    ) -> None:
        """Called at the end of the translation process, and may add arbitrary triples
        to ``graph`` based on the input dictionary (passed as ``d``).
        """
        pass


class JsonMapping(DictMapping):
    """Base class for all mappings that use JSON data as input."""

    def translate(self, raw_content: bytes) -> Optional[Dict]:
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


class XmlMapping(DictMapping):
    """Base class for all mappings that use XML data as input."""

    def translate(self, raw_content: bytes) -> Optional[Dict]:
        try:
            d = xmltodict.parse(raw_content)
        except xml.parsers.expat.ExpatError:
            self.log.warning("Error parsing XML from %s", self.log_suffix)
            return None
        except UnicodeDecodeError:
            self.log.warning("Error unidecoding XML from %s", self.log_suffix)
            return None
        except (LookupError, ValueError):
            # unknown encoding or multi-byte encoding
            self.log.warning("Error detecting XML encoding from %s", self.log_suffix)
            return None
        if not isinstance(d, dict):
            self.log.warning("Skipping ill-formed XML content: %s", raw_content)
            return None
        return self._translate_dict(d)


class SafeLoader(yaml.SafeLoader):
    yaml_implicit_resolvers = {
        k: [r for r in v if r[0] != "tag:yaml.org,2002:timestamp"]
        for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }


class YamlMapping(DictMapping):
    """Base class for all mappings that use Yaml data as input."""

    def translate(self, raw_content: bytes) -> Optional[Dict[str, str]]:
        raw_content_string: str = raw_content.decode()
        try:
            content_dict = yaml.load(raw_content_string, Loader=SafeLoader)
        except yaml.YAMLError:
            return None

        if isinstance(content_dict, dict):
            return self._translate_dict(content_dict)

        return None
