# Copyright (C) 2025-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging

import pytest

from swh.indexer.metadata_mapping import get_mapping
from swh.indexer.metadata_mapping.coarnotify import (
    load_and_compact_notification,
    validate_mention,
)


@pytest.fixture
def raw_mention():
    return json.dumps(
        [
            {
                "https://www.w3.org/ns/activitystreams#actor": [
                    {
                        "@id": "https://research-organisation.org",
                        "https://www.w3.org/ns/activitystreams#name": [
                            {"@value": "Research Organisation"}
                        ],
                        "@type": ["https://www.w3.org/ns/activitystreams#Organization"],
                    }
                ],
                "https://www.w3.org/ns/activitystreams#context": [
                    {
                        "@id": "https://research.local/item/201203/422/",
                        "@type": [
                            "https://www.w3.org/ns/activitystreams#Page",
                            "http://schema.org/AboutPage",
                        ],
                    }
                ],
                "@id": "urn:uuid:cf7e6dc8-c96f-4c85-b471-d2263c789ca7",
                "https://www.w3.org/ns/activitystreams#object": [
                    {
                        "https://www.w3.org/ns/activitystreams#object": [
                            {"@id": "https://research.local/item/201203/422/"}
                        ],
                        "https://www.w3.org/ns/activitystreams#relationship": [
                            {"@id": "http://purl.org/vocab/frbr/core#supplement"}
                        ],
                        "https://www.w3.org/ns/activitystreams#subject": [
                            {"@id": "https://github.com/rdicosmo/parmap"}
                        ],
                        "@id": "urn:uuid:74FFB356-0632-44D9-B176-888DA85758DC",
                        "@type": ["https://www.w3.org/ns/activitystreams#Relationship"],
                    }
                ],
                "https://www.w3.org/ns/activitystreams#origin": [
                    {
                        "@id": "https://research-organisation.org/repository",
                        "http://www.w3.org/ns/ldp#inbox": [
                            {"@id": "http://inbox.partner.local"}
                        ],
                        "@type": ["https://www.w3.org/ns/activitystreams#Service"],
                    }
                ],
                "https://www.w3.org/ns/activitystreams#target": [
                    {
                        "@id": "https://another-research-organisation.org/repository",
                        "http://www.w3.org/ns/ldp#inbox": [
                            {"@id": "http://inbox.swh/"}
                        ],
                        "@type": ["https://www.w3.org/ns/activitystreams#Service"],
                    }
                ],
                "@type": [
                    "https://www.w3.org/ns/activitystreams#Announce",
                    "http://coar-notify.net/specification/vocabulary/RelationshipAction",
                ],
            }
        ]
    )


@pytest.fixture
def compact_mention(raw_mention):
    return load_and_compact_notification(raw_mention)


def test_load_and_compact_notification(raw_mention, caplog):
    result = load_and_compact_notification(raw_mention)
    assert result["@context"] == [
        "https://www.w3.org/ns/activitystreams",
        "https://coar-notify.net",
    ]
    assert result["type"] == ["Announce", "RelationshipAction"]
    assert result["context"]["id"] == result["as:object"]["as:object"]


@pytest.mark.parametrize(
    "value,msg",
    [
        ("#", "Failed to parse JSON document"),
        ('{"@id": null}', "Failed to compact JSON-LD document"),
    ],
)
def test_load_and_compact_notification_failures(value, msg, caplog):
    caplog.set_level(logging.ERROR)
    assert load_and_compact_notification(value) is None
    assert msg in caplog.text


def test_validate_mention(compact_mention):
    assert validate_mention(compact_mention)


def test_validate_mention_object(compact_mention, caplog):
    caplog.set_level(logging.ERROR)
    msg = "Missing object[as:object] key"
    mention = compact_mention.copy()
    orignal_object = mention["as:object"]

    del mention["as:object"]
    assert not validate_mention(mention)
    assert msg in caplog.text

    caplog.clear()
    mention["as:object"] = orignal_object

    del mention["as:object"]["as:object"]
    assert not validate_mention(mention)
    assert msg in caplog.text


@pytest.mark.skip(
    reason=(
        "Current CN specs (1.0.1) are not clear about what should be in context.id, "
        "see codemeta:validate_mention"
    )
)
def test_validate_mention_context(compact_mention, caplog):
    caplog.set_level(logging.ERROR)
    msg = "Mismatch between context[id] and object[as:object]"
    mention = compact_mention.copy()
    mention["context"]["id"] = mention["context"]["id"] + "/fail/"
    assert not validate_mention(mention)
    assert msg in caplog.text


def test_validate_mention_id(compact_mention, caplog):
    caplog.set_level(logging.ERROR)
    msg = "id value is not a string"
    mention = compact_mention.copy()

    del mention["id"]
    assert not validate_mention(mention)

    caplog.clear()

    mention["id"] = 123
    assert not validate_mention(mention)
    assert msg in caplog.text


def test_coarnotify_mention(raw_mention):
    result = get_mapping("CoarNotifyMentionCodemetaMapping")().translate(raw_mention)
    assert result == {
        "@context": ["http://schema.org/", "https://w3id.org/codemeta/3.0"],
        "citation": [
            {
                "id": "urn:uuid:cf7e6dc8-c96f-4c85-b471-d2263c789ca7",
                "ScholarlyArticle": {
                    "id": "https://research.local/item/201203/422/",
                    "type": ["Page", "sorg:AboutPage"],
                },
            }
        ],
    }


def test_coarnotify_mention_invalid_json(raw_mention, mocker):
    mocker.patch(
        "swh.indexer.metadata_mapping.coarnotify.load_and_compact_notification",
        return_value=None,
    )
    result = get_mapping("CoarNotifyMentionCodemetaMapping")().translate(raw_mention)
    assert result is None


def test_coarnotify_mention_invalid_mention(raw_mention, mocker):
    mocker.patch(
        "swh.indexer.metadata_mapping.coarnotify.validate_mention",
        return_value=False,
    )
    result = get_mapping("CoarNotifyMentionCodemetaMapping")().translate(raw_mention)
    assert result is None
