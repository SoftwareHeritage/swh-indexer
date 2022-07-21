# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
from typing import Any, Dict, Tuple

from swh.indexer.codemeta import ACTIVITYSTREAMS_URI, CROSSWALK_TABLE, FORGEFED_URI

from .base import BaseExtrinsicMapping, JsonMapping, produce_terms


def _prettyprint(d):
    print(json.dumps(d, indent=4))


class GitHubMapping(BaseExtrinsicMapping, JsonMapping):
    name = "github"
    mapping = CROSSWALK_TABLE["GitHub"]
    string_fields = [
        "archive_url",
        "created_at",
        "updated_at",
        "description",
        "full_name",
        "html_url",
        "issues_url",
    ]

    @classmethod
    def extrinsic_metadata_formats(cls) -> Tuple[str, ...]:
        return ("application/vnd.github.v3+json",)

    def _translate_dict(self, content_dict: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        d = super()._translate_dict(content_dict, **kwargs)
        d["type"] = FORGEFED_URI + "Repository"
        return d

    @produce_terms(FORGEFED_URI, ["forks"])
    @produce_terms(ACTIVITYSTREAMS_URI, ["totalItems"])
    def translate_forks_count(
        self, translated_metadata: Dict[str, Any], v: Any
    ) -> None:
        """

        >>> translated_metadata = {}
        >>> GitHubMapping().translate_forks_count(translated_metadata, 42)
        >>> _prettyprint(translated_metadata)
        {
            "https://forgefed.org/ns#forks": [
                {
                    "@type": "https://www.w3.org/ns/activitystreams#OrderedCollection",
                    "https://www.w3.org/ns/activitystreams#totalItems": 42
                }
            ]
        }
        """
        if isinstance(v, int):
            translated_metadata.setdefault(FORGEFED_URI + "forks", []).append(
                {
                    "@type": ACTIVITYSTREAMS_URI + "OrderedCollection",
                    ACTIVITYSTREAMS_URI + "totalItems": v,
                }
            )

    @produce_terms(ACTIVITYSTREAMS_URI, ["likes"])
    @produce_terms(ACTIVITYSTREAMS_URI, ["totalItems"])
    def translate_stargazers_count(
        self, translated_metadata: Dict[str, Any], v: Any
    ) -> None:
        """

        >>> translated_metadata = {}
        >>> GitHubMapping().translate_stargazers_count(translated_metadata, 42)
        >>> _prettyprint(translated_metadata)
        {
            "https://www.w3.org/ns/activitystreams#likes": [
                {
                    "@type": "https://www.w3.org/ns/activitystreams#Collection",
                    "https://www.w3.org/ns/activitystreams#totalItems": 42
                }
            ]
        }
        """
        if isinstance(v, int):
            translated_metadata.setdefault(ACTIVITYSTREAMS_URI + "likes", []).append(
                {
                    "@type": ACTIVITYSTREAMS_URI + "Collection",
                    ACTIVITYSTREAMS_URI + "totalItems": v,
                }
            )

    @produce_terms(ACTIVITYSTREAMS_URI, ["followers"])
    @produce_terms(ACTIVITYSTREAMS_URI, ["totalItems"])
    def translate_watchers_count(
        self, translated_metadata: Dict[str, Any], v: Any
    ) -> None:
        """

        >>> translated_metadata = {}
        >>> GitHubMapping().translate_watchers_count(translated_metadata, 42)
        >>> _prettyprint(translated_metadata)
        {
            "https://www.w3.org/ns/activitystreams#followers": [
                {
                    "@type": "https://www.w3.org/ns/activitystreams#Collection",
                    "https://www.w3.org/ns/activitystreams#totalItems": 42
                }
            ]
        }
        """
        if isinstance(v, int):
            translated_metadata.setdefault(
                ACTIVITYSTREAMS_URI + "followers", []
            ).append(
                {
                    "@type": ACTIVITYSTREAMS_URI + "Collection",
                    ACTIVITYSTREAMS_URI + "totalItems": v,
                }
            )

    def normalize_license(self, d):
        """

        >>> GitHubMapping().normalize_license({'spdx_id': 'MIT'})
        {'@id': 'https://spdx.org/licenses/MIT'}
        """
        if isinstance(d, dict) and isinstance(d.get("spdx_id"), str):
            return {"@id": "https://spdx.org/licenses/" + d["spdx_id"]}
