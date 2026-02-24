# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

import pytest

from swh.indexer.metadata_dictionary.codemeta import load_and_compact_notification


@pytest.fixture
def compact_mention(raw_mention):
    return load_and_compact_notification(raw_mention)


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
                            {"@value": "https://research.local/item/201203/422/"}
                        ],
                        "https://www.w3.org/ns/activitystreams#relationship": [
                            {"@value": "http://purl.org/vocab/frbr/core#supplement"}
                        ],
                        "https://www.w3.org/ns/activitystreams#subject": [
                            {"@value": "https://github.com/rdicosmo/parmap"}
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
