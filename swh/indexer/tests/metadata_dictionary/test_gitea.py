# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.indexer.metadata_dictionary import MAPPINGS

CONTEXT = [
    "https://doi.org/10.5063/schema/codemeta-2.0",
    {
        "as": "https://www.w3.org/ns/activitystreams#",
        "forge": "https://forgefed.org/ns#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
    },
]


def test_compute_metadata_none():
    """
    testing content empty content is empty
    should return None
    """
    content = b""

    # None if no metadata was found or an error occurred
    declared_metadata = None
    result = MAPPINGS["GiteaMapping"]().translate(content)
    assert declared_metadata == result


def test_supported_terms():
    terms = MAPPINGS["GiteaMapping"].supported_terms()
    assert {
        "http://schema.org/name",
        "http://schema.org/dateCreated",
        "https://forgefed.org/ns#forks",
        "https://www.w3.org/ns/activitystreams#totalItems",
    } <= terms


def test_compute_metadata_gitea():
    content = b"""
{
  "id": 48043,
  "owner": {
    "id": 48018,
    "login": "ForgeFed",
    "full_name": "",
    "email": "",
    "avatar_url": "https://codeberg.org/avatars/c20f7a6733a6156304137566ee35ef33",
    "language": "",
    "is_admin": false,
    "last_login": "0001-01-01T00:00:00Z",
    "created": "2022-04-30T20:13:17+02:00",
    "restricted": false,
    "active": false,
    "prohibit_login": false,
    "location": "",
    "website": "https://forgefed.org/",
    "description": "",
    "visibility": "public",
    "followers_count": 0,
    "following_count": 0,
    "starred_repos_count": 0,
    "username": "ForgeFed"
  },
  "name": "ForgeFed",
  "full_name": "ForgeFed/ForgeFed",
  "description": "ActivityPub-based forge federation protocol specification",
  "empty": false,
  "private": false,
  "fork": false,
  "template": false,
  "parent": null,
  "mirror": false,
  "size": 3780,
  "language": "CSS",
  "languages_url": "https://codeberg.org/api/v1/repos/ForgeFed/ForgeFed/languages",
  "html_url": "https://codeberg.org/ForgeFed/ForgeFed",
  "ssh_url": "git@codeberg.org:ForgeFed/ForgeFed.git",
  "clone_url": "https://codeberg.org/ForgeFed/ForgeFed.git",
  "original_url": "https://notabug.org/peers/forgefed",
  "website": "https://forgefed.org",
  "stars_count": 30,
  "forks_count": 6,
  "watchers_count": 11,
  "open_issues_count": 61,
  "open_pr_counter": 10,
  "release_counter": 0,
  "default_branch": "main",
  "archived": false,
  "created_at": "2022-06-13T18:54:26+02:00",
  "updated_at": "2022-09-02T03:57:22+02:00",
  "permissions": {
    "admin": false,
    "push": false,
    "pull": true
  },
  "has_issues": true,
  "internal_tracker": {
    "enable_time_tracker": true,
    "allow_only_contributors_to_track_time": true,
    "enable_issue_dependencies": true
  },
  "has_wiki": false,
  "has_pull_requests": true,
  "has_projects": true,
  "ignore_whitespace_conflicts": false,
  "allow_merge_commits": false,
  "allow_rebase": false,
  "allow_rebase_explicit": false,
  "allow_squash_merge": true,
  "default_merge_style": "squash",
  "avatar_url": "",
  "internal": false,
  "mirror_interval": "",
  "mirror_updated": "0001-01-01T00:00:00Z",
  "repo_transfer": null
}
    """
    result = MAPPINGS["GiteaMapping"]().translate(content)
    assert result == {
        "@context": CONTEXT,
        "type": "forge:Repository",
        "id": "https://codeberg.org/ForgeFed/ForgeFed",
        "forge:forks": {
            "as:totalItems": {"type": "xsd:nonNegativeInteger", "@value": "6"},
            "type": "as:OrderedCollection",
        },
        "as:likes": {
            "as:totalItems": {
                "type": "xsd:nonNegativeInteger",
                "@value": "30",
            },
            "type": "as:Collection",
        },
        "as:followers": {
            "as:totalItems": {
                "type": "xsd:nonNegativeInteger",
                "@value": "11",
            },
            "type": "as:Collection",
        },
        "name": "ForgeFed",
        "description": "ActivityPub-based forge federation protocol specification",
        "codeRepository": "https://codeberg.org/ForgeFed/ForgeFed.git",
        "dateCreated": "2022-06-13T18:54:26+02:00",
        "dateModified": "2022-09-02T03:57:22+02:00",
        "programmingLanguage": "CSS",
        "url": "https://forgefed.org",
    }


def test_gitea_fork():
    content = b"""
{
  "name": "fork-name",
  "description": "fork description",
  "html_url": "http://example.org/test-fork",
  "parent": {
    "name": "parent-name",
    "description": "parent description",
    "html_url": "http://example.org/test-software"
  }
}
    """
    result = MAPPINGS["GiteaMapping"]().translate(content)
    assert result == {
        "@context": CONTEXT,
        "type": "forge:Repository",
        "id": "http://example.org/test-fork",
        "description": "fork description",
        "name": "fork-name",
        "forge:forkedFrom": {
            "id": "http://example.org/test-software",
        },
    }
