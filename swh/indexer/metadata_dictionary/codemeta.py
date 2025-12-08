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
from pyld import jsonld
import xmltodict

from swh.indexer.codemeta import (
    CODEMETA_TERMS,
    CODEMETA_V2_CONTEXT_URL,
    compact,
    expand,
)

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
            elif namespace.lower() == CODEMETA_V2_CONTEXT_URL:
                # It is a term defined by the context; write is as-is and JSON-LD
                # expansion will convert it to a full URI based on
                # "@context": CODEMETA_V2_CONTEXT_URL
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
            doc["@context"] = CODEMETA_V2_CONTEXT_URL

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
                json_doc["@xmlns:codemeta"] = CODEMETA_V2_CONTEXT_URL

            if json_doc["@xmlns"] not in (ATOM_URI, [ATOM_URI]):
                # Technically, non-default XMLNS were allowed, but no one used them,
                # and we don't write this format anymore, so they do not need to be
                # implemented here.
                raise NotImplementedError(f"Unexpected XMLNS set: {json_doc}")

            # Root tag was stripped by swh-deposit
            json_doc = {"entry": json_doc}

            return super().translate(xmltodict.unparse(json_doc).encode())


def load_and_compact_notification(content: bytes | str) -> dict[str, Any] | None:
    """Load and compact a notification from the REMS.

    Errors logs will be written if something went wrong in the process.

    Args:
        content: the expanded COAR Notification

    Returns:
        The compacted form of the COAR Notification or None if we weren't able to
        read it
    """
    try:
        raw_json = json.loads(content)
        notification = jsonld.compact(
            raw_json,
            {
                "@context": [
                    "https://www.w3.org/ns/activitystreams",
                    "https://coar-notify.net",
                ]
            },
        )
    except json.JSONDecodeError:
        logger.error("Failed to parse JSON document: %s", content)
        return None
    except jsonld.JsonLdError:
        logger.error("Failed to compact JSON-LD document: %s", content)
        return None
    return notification


def validate_mention(notification: dict[str, Any]) -> bool:
    """Validate minimal notification's requirements before indexation.

    Args:
        notification: a compact form of a COAR Notification

    Returns:
        False if the we can't find required props in the notification
    """
    object_ = notification.get("object", {}).get("as:object")
    if object_ is None:
        logger.error("Missing object[as:object] key in %s", notification)
        return False
    if not isinstance(object_, str):
        logger.error("object[as:object] value is not a string in %s", notification)
        return False

    paper = notification.get("context", {}).get("id")
    if paper is None:
        logger.error("Missing context[id] key in %s", notification)
        return False
    if not isinstance(paper, str):
        logger.error("context[id] value is not a string in %s", notification)
        return False
    # FIXME: CN specs (1.0.1) are a bit unclear about what should context_data contains,
    # especially the id. It would be more logical to find the paper URI in the id and
    # then some metadata about it, but instead we might find the software URI in the id
    # and then metadata about the paper. We are trying to make some changes on the
    # specs but meanwhile we'll skip verifying that context.id == object.as:subject

    notification_id = notification.get("id")
    if notification_id is None:
        logger.error("missing id key in %s", notification)
        return False
    if not isinstance(notification_id, str):
        logger.error("id value is not a string in %s", notification)
        return False

    return True


class CoarNotifyMentionMapping(BaseExtrinsicMapping):
    """Map & translate a COAR Notify software mention in a CodeMeta format.

    COAR Notify mentions are received by ``swh-coarnotify`` and saved expanded.
    Mentions contains metadata on a scientific paper that cites a software.
    """

    name = "coarnotify-mention-codemeta"

    @classmethod
    def supported_terms(cls) -> list[str]:
        return [term for term in CODEMETA_TERMS if not term.startswith("@")]

    @classmethod
    def extrinsic_metadata_formats(cls) -> tuple[str, ...]:
        return ("coarnotify-mention-v1",)

    def translate(self, content: bytes) -> dict[str, Any] | None:
        """Parse JSON and compact the payload to access the mention.

        The whole `context` of the `AnnounceRelationship` notification will be indexed
        as it contains metadata about the scientific paper citing the software.

        TODO: At some point we might need to fetch metadata from the paper URL as COAR
        Notifications are not made to contain **all** the metadata but to indicate
        where we should find them.

        TODO: We will need to handle cancellations of a mention if it was made by
        mistake. Maybe we could use the original notification id and an empty context
        to overwrite the previous citation when merging documents ? It is with this in
        mind that the notification ID is added to the citation.

        Args:
            content: the raw expanded COAR Notification

        Returns:
            A CodeMeta citation if the notification was valid or None
        """

        notification = load_and_compact_notification(content)
        if not notification:
            return None

        if not validate_mention(notification):
            return None

        citation = {
            "@context": ["http://schema.org/", "https://w3id.org/codemeta/3.0"],
            "citation": [
                {"id": notification["id"], "ScholarlyArticle": notification["context"]}
            ],
        }

        return citation
