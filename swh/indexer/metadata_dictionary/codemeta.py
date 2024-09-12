# Copyright (C) 2018-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Union
import xml.etree.ElementTree as ET

import iso8601
import xmltodict

from swh.indexer.codemeta import CODEMETA_CONTEXT_URL, CODEMETA_TERMS, compact, expand

from .base import BaseExtrinsicMapping, SingleFileIntrinsicMapping

ATOM_URI = "http://www.w3.org/2005/Atom"

_TAG_RE = re.compile(r"\{(?P<namespace>.*?)\}(?P<localname>.*)")
_IGNORED_NAMESPACES = ("http://www.w3.org/2005/Atom",)
_DATE_RE = re.compile("^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$")

logger = logging.getLogger(__name__)


class CodemetaMapping(SingleFileIntrinsicMapping):
    """
    dedicated class for CodeMeta (codemeta.json) mapping and translation
    """

    name = "codemeta"
    filename = b"codemeta.json"
    string_fields = None

    @classmethod
    def supported_terms(cls) -> List[str]:
        return [term for term in CODEMETA_TERMS if not term.startswith("@")]

    def translate(self, content: bytes) -> Optional[Dict[str, Any]]:
        try:
            return self.normalize_translation(expand(json.loads(content.decode())))
        except Exception:
            return None


class SwordCodemetaMapping(BaseExtrinsicMapping):
    """
    dedicated class for mapping and translation from JSON-LD statements
    embedded in SWORD documents, optionally using Codemeta contexts,
    as described in the :ref:`deposit-protocol`.
    """

    name = "sword-codemeta"

    @classmethod
    def extrinsic_metadata_formats(cls) -> Tuple[str, ...]:
        return (
            "sword-v2-atom-codemeta",
            "sword-v2-atom-codemeta-v2",
        )

    @classmethod
    def supported_terms(cls) -> List[str]:
        return [term for term in CODEMETA_TERMS if not term.startswith("@")]

    def xml_to_jsonld(self, e: ET.Element) -> Union[str, Dict[str, Any]]:
        # Keys are JSON-LD property names (URIs or terms).
        # Values are either a single string (if key is "type") or list of
        # other dicts with the same type recursively.
        # To simply annotations, we omit the single string case here.
        doc: Dict[str, List[Union[str, Dict[str, Any]]]] = collections.defaultdict(list)

        for child in e:
            m = _TAG_RE.match(child.tag)
            assert m, f"Tag with no namespace: {child}"
            namespace = m.group("namespace")
            localname = m.group("localname")
            if namespace == ATOM_URI and localname in ("title", "name"):
                # Convert Atom to Codemeta name; in case codemeta:name
                # is not provided or different
                doc["name"].append(self.xml_to_jsonld(child))
            elif namespace == ATOM_URI and localname in ("author", "email"):
                # ditto for these author properties (note that author email is also
                # covered by the previous test)
                doc[localname].append(self.xml_to_jsonld(child))
            elif namespace in _IGNORED_NAMESPACES:
                # SWORD-specific namespace that is not interesting to translate
                pass
            elif namespace.lower() == CODEMETA_CONTEXT_URL:
                # It is a term defined by the context; write is as-is and JSON-LD
                # expansion will convert it to a full URI based on
                # "@context": CODEMETA_CONTEXT_URL
                jsonld_child = self.xml_to_jsonld(child)
                if (
                    localname
                    in (
                        "dateCreated",
                        "dateModified",
                        "datePublished",
                    )
                    and isinstance(jsonld_child, str)
                    and _DATE_RE.match(jsonld_child)
                ):
                    # Dates missing a leading zero for their day/month, used
                    # to be allowed by the deposit; so we need to reformat them
                    # to be valid ISO8601.
                    jsonld_child = iso8601.parse_date(jsonld_child).date().isoformat()
                if localname == "id":
                    # JSON-LD only allows a single id, and they have to be strings.
                    if localname in doc:
                        logger.error(
                            "Duplicate <id>s in SWORD document: %r and %r",
                            doc[localname],
                            jsonld_child,
                        )
                        continue
                    elif not jsonld_child:
                        logger.error("Empty <id> value in SWORD document")
                        continue
                    elif not isinstance(jsonld_child, str):
                        logger.error(
                            "Unexpected <id> value in SWORD document: %r", jsonld_child
                        )
                        continue
                    else:
                        doc[localname] = jsonld_child  # type: ignore[assignment]
                else:
                    doc[localname].append(jsonld_child)
            else:
                # Otherwise, we already know the URI
                doc[f"{namespace}{localname}"].append(self.xml_to_jsonld(child))

        # The above needed doc values to be list to work; now we allow any type
        # of value as key "@value" cannot have a list as value.
        doc_: Dict[str, Any] = doc

        text = e.text.strip() if e.text else None
        if text:
            # TODO: check doc is empty, and raise mixed-content error otherwise?
            return text

        return doc_

    def translate(self, content: bytes) -> Optional[Dict[str, Any]]:
        # Parse XML
        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            logger.error("Failed to parse XML document: %s", content)
            return None
        else:
            # Transform to JSON-LD document
            doc = self.xml_to_jsonld(root)

            assert isinstance(doc, dict), f"Root object is not a dict: {doc}"

            # Add @context to JSON-LD expansion replaces the "codemeta:" prefix
            # hash (which uses the context URL as namespace URI for historical
            # reasons) into properties in `http://schema.org/` and
            # `https://codemeta.github.io/terms/` namespaces
            doc["@context"] = CODEMETA_CONTEXT_URL

            # Normalize as a Codemeta document
            return self.normalize_translation(expand(doc))

    def normalize_translation(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        return compact(metadata, forgefed=False)


def iter_keys(d):
    """Recursively iterates on dictionary keys"""
    if isinstance(d, dict):
        yield from d
        for value in d:
            yield from iter_keys(value)
    elif isinstance(d, list):
        for value in d:
            yield from iter_keys(value)
    else:
        pass


class JsonSwordCodemetaMapping(SwordCodemetaMapping):
    """
    Variant of :class:`SwordCodemetaMapping` that reads the legacy
    ``sword-v2-atom-codemeta-v2-in-json`` format and converts it back to
    ``sword-v2-atom-codemeta-v2`` XML
    """

    name = "json-sword-codemeta"

    @classmethod
    def extrinsic_metadata_formats(cls) -> Tuple[str, ...]:
        return ("sword-v2-atom-codemeta-v2-in-json",)

    def translate(self, content: bytes) -> Optional[Dict[str, Any]]:
        # ``content`` was generated by calling ``xmltodict.parse()`` on a XML document,
        # so ``xmltodict.unparse()`` is guaranteed to return a document that is
        # semantically equivalent to the original and pass it to SwordCodemetaMapping.
        try:
            json_doc = json.loads(content)
        except json.JSONDecodeError:
            logger.error("Failed to parse JSON document: %s", content)
            return None
        else:
            if "@xmlns" not in json_doc:
                # Technically invalid, but old versions of the deposit dropped
                # XMLNS information
                json_doc["@xmlns"] = ATOM_URI

            if "@xmlns:codemeta" not in json_doc and any(
                key.startswith("codemeta:") for key in iter_keys(json_doc)
            ):
                # ditto
                json_doc["@xmlns:codemeta"] = CODEMETA_CONTEXT_URL

            if json_doc["@xmlns"] not in (ATOM_URI, [ATOM_URI]):
                # Technically, non-default XMLNS were allowed, but no one used them,
                # and we don't write this format anymore, so they do not need to be
                # implemented here.
                raise NotImplementedError(f"Unexpected XMLNS set: {json_doc}")

            # Root tag was stripped by swh-deposit
            json_doc = {"entry": json_doc}

            return super().translate(xmltodict.unparse(json_doc))
