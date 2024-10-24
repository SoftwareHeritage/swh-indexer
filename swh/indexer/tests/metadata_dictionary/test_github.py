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
    result = MAPPINGS["GitHubMapping"]().translate(content)
    assert declared_metadata == result


def test_supported_terms():
    terms = MAPPINGS["GitHubMapping"].supported_terms()
    assert {
        "http://schema.org/name",
        "http://schema.org/license",
        "http://schema.org/dateCreated",
        "https://forgefed.org/ns#forks",
        "https://www.w3.org/ns/activitystreams#totalItems",
    } <= terms


def test_compute_metadata_github():
    content = b"""
{
  "id": 80521091,
  "node_id": "MDEwOlJlcG9zaXRvcnk4MDUyMTA5MQ==",
  "name": "swh-indexer",
  "full_name": "SoftwareHeritage/swh-indexer",
  "private": false,
  "owner": {
    "login": "SoftwareHeritage",
    "id": 18555939,
    "node_id": "MDEyOk9yZ2FuaXphdGlvbjE4NTU1OTM5",
    "avatar_url": "https://avatars.githubusercontent.com/u/18555939?v=4",
    "gravatar_id": "",
    "url": "https://api.github.com/users/SoftwareHeritage",
    "type": "Organization",
    "site_admin": false
  },
  "html_url": "https://github.com/SoftwareHeritage/swh-indexer",
  "description": "GitHub mirror of Metadata indexer",
  "fork": false,
  "url": "https://api.github.com/repos/SoftwareHeritage/swh-indexer",
  "created_at": "2017-01-31T13:05:39Z",
  "updated_at": "2022-06-22T08:02:20Z",
  "pushed_at": "2022-06-29T09:01:08Z",
  "archive_url": "https://api.github.com/repos/SoftwareHeritage/swh-indexer/{archive_format}{/ref}",
  "issues_url": "https://api.github.com/repos/SoftwareHeritage/swh-indexer/issues{/number}",
  "git_url": "git://github.com/SoftwareHeritage/swh-indexer.git",
  "ssh_url": "git@github.com:SoftwareHeritage/swh-indexer.git",
  "clone_url": "https://github.com/SoftwareHeritage/swh-indexer.git",
  "svn_url": "https://github.com/SoftwareHeritage/swh-indexer",
  "homepage": "https://forge.softwareheritage.org/source/swh-indexer/",
  "size": 2713,
  "stargazers_count": 13,
  "watchers_count": 12,
  "language": "Python",
  "has_issues": false,
  "has_projects": false,
  "has_downloads": true,
  "has_wiki": false,
  "has_pages": false,
  "forks_count": 1,
  "mirror_url": null,
  "archived": false,
  "disabled": false,
  "open_issues_count": 0,
  "license": {
    "key": "gpl-3.0",
    "name": "GNU General Public License v3.0",
    "spdx_id": "GPL-3.0",
    "url": "https://api.github.com/licenses/gpl-3.0",
    "node_id": "MDc6TGljZW5zZTk="
  },
  "allow_forking": true,
  "is_template": false,
  "web_commit_signoff_required": false,
  "topics": [

  ],
  "visibility": "public",
  "forks": 1,
  "open_issues": 0,
  "watchers": 13,
  "default_branch": "master",
  "temp_clone_token": null,
  "organization": {
    "login": "SoftwareHeritage",
    "id": 18555939,
    "node_id": "MDEyOk9yZ2FuaXphdGlvbjE4NTU1OTM5",
    "avatar_url": "https://avatars.githubusercontent.com/u/18555939?v=4",
    "gravatar_id": "",
    "type": "Organization",
    "site_admin": false
  },
  "network_count": 1,
  "subscribers_count": 6
}

    """  # noqa
    result = MAPPINGS["GitHubMapping"]().translate(content)
    assert result == {
        "@context": CONTEXT,
        "type": "forge:Repository",
        "id": "https://github.com/SoftwareHeritage/swh-indexer",
        "forge:forks": {
            "as:totalItems": {
                "type": "xsd:nonNegativeInteger",
                "@value": "1",
            },
            "type": "as:OrderedCollection",
        },
        "as:likes": {
            "as:totalItems": {
                "type": "xsd:nonNegativeInteger",
                "@value": "13",
            },
            "type": "as:Collection",
        },
        "as:followers": {
            "as:totalItems": {
                "type": "xsd:nonNegativeInteger",
                "@value": "12",
            },
            "type": "as:Collection",
        },
        "license": "https://spdx.org/licenses/GPL-3.0",
        "name": "SoftwareHeritage/swh-indexer",
        "description": "GitHub mirror of Metadata indexer",
        "codeRepository": "https://github.com/SoftwareHeritage/swh-indexer.git",
        "dateCreated": "2017-01-31T13:05:39Z",
        "dateModified": "2022-06-22T08:02:20Z",
        "programmingLanguage": "Python",
    }


def test_github_topics():
    content = b"""
{
  "html_url": "https://github.com/SoftwareHeritage/swh-indexer",
  "topics": [
    "foo",
    "bar"
  ]
}
    """
    result = MAPPINGS["GitHubMapping"]().translate(content)
    assert set(result.pop("keywords", [])) == {"foo", "bar"}, result
    assert result == {
        "@context": CONTEXT,
        "type": "forge:Repository",
        "id": "https://github.com/SoftwareHeritage/swh-indexer",
    }


def test_github_fork():
    content = b"""
{
  "name": "unicode_names2",
  "full_name": "progval/unicode_names2",
  "html_url": "https://github.com/progval/unicode_names2",
  "description": "char <-> Unicode character name (maintained fork of huonw/unicode_names)",
  "parent": {
    "id": 23110520,
    "node_id": "MDEwOlJlcG9zaXRvcnkyMzExMDUyMA==",
    "name": "unicode_names",
    "full_name": "huonw/unicode_names",
    "private": false,
    "html_url": "https://github.com/huonw/unicode_names",
    "description": "char <-> Unicode character name"
  }
}
    """
    result = MAPPINGS["GitHubMapping"]().translate(content)
    assert result == {
        "@context": CONTEXT,
        "type": "forge:Repository",
        "id": "https://github.com/progval/unicode_names2",
        "description": "char <-> Unicode character name (maintained fork of "
        "huonw/unicode_names)",
        "name": "progval/unicode_names2",
        "forge:forkedFrom": {
            "id": "https://github.com/huonw/unicode_names",
        },
    }


def test_github_issues():
    content = b"""
{
  "html_url": "https://github.com/SoftwareHeritage/swh-indexer",
  "has_issues": true
}
    """
    result = MAPPINGS["GitHubMapping"]().translate(content)
    assert result == {
        "@context": CONTEXT,
        "type": "forge:Repository",
        "id": "https://github.com/SoftwareHeritage/swh-indexer",
        "issueTracker": "https://github.com/SoftwareHeritage/swh-indexer/issues",
    }
