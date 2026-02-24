# Copyright (C) 2024-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
from typing import Any

from pyld import jsonld

from swh.indexer.codemeta import CODEMETA_TERMS

from .base import BaseExtrinsicMapping

logger = logging.getLogger(__name__)


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
        False if the required props cannot be found in the notification
    """

    object_ = notification.get("as:object", {}).get("as:object")
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
