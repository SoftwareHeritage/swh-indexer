# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import json
import re
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

import xmltodict

from swh.indexer.codemeta import CODEMETA_CONTEXT_URL, CODEMETA_TERMS, compact, expand

from .base import BaseExtrinsicMapping, SingleFileIntrinsicMapping

ATOM_URI = "http://www.w3.org/2005/Atom"

_TAG_RE = re.compile(r"\{(?P<namespace>.*?)\}(?P<localname>.*)")
_IGNORED_NAMESPACES = ("http://www.w3.org/2005/Atom",)


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

    def xml_to_jsonld(self, e: ET.Element) -> Dict[str, Any]:
        doc: Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
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
                doc[localname].append(self.xml_to_jsonld(child))
            else:
                # Otherwise, we already know the URI
                doc[f"{namespace}{localname}"].append(self.xml_to_jsonld(child))

        # The above needed doc values to be list to work; now we allow any type
        # of value as key "@value" cannot have a list as value.
        doc_: Dict[str, Any] = doc

        text = e.text.strip() if e.text else None
        if text:
            # TODO: check doc is empty, and raise mixed-content error otherwise?
            doc_["@value"] = text

        return doc_

    def translate(self, content: bytes) -> Optional[Dict[str, Any]]:
        # Parse XML
        root = ET.fromstring(content)

        # Transform to JSON-LD document
        doc = self.xml_to_jsonld(root)

        # Add @context to JSON-LD expansion replaces the "codemeta:" prefix
        # hash (which uses the context URL as namespace URI for historical
        # reasons) into properties in `http://schema.org/` and
        # `https://codemeta.github.io/terms/` namespaces
        doc["@context"] = CODEMETA_CONTEXT_URL

        # Normalize as a Codemeta document
        return self.normalize_translation(expand(doc))

    def normalize_translation(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        return compact(metadata, forgefed=False)


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
        json_doc = json.loads(content)

        if json_doc.get("@xmlns") != ATOM_URI:
            # Technically, non-default XMLNS were allowed, but it does not seem like
            # anyone used them, so they do not need to be implemented here.
            raise NotImplementedError(f"Unexpected XMLNS set: {json_doc}")

        # Root tag was stripped by swh-deposit
        json_doc = {"entry": json_doc}

        return super().translate(xmltodict.unparse(json_doc))
