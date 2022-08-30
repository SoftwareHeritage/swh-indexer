Metadata on Social Code Repositories
====================================

MeSoCoRe is a vocabulary which complements ontologies like schema.org/CodeMeta
and DOAP in describing software projects. While the latter are meant to describe
source code projects, MeSoCoRe describes VCS repositories of these projects.
This includes, for example, "stars" and "watchers" on platforms like GitHub, Gitlab,
or Gitea.

The namespace is ``https://www.softwareheritage.org/schema/2022/mesocore/``;
and it is meant to be used primarily in alongside CodeMeta/schema.org
and ForgeFed/ActivityStreams.


The following prefixes are used throughout this document for readability:

.. code-block:: json

    {
        "schema": "http://schema.org/",
        "codemeta": "https://codemeta.github.io/terms/",
        "forge": "https://forgefed.org/vocabulary.html#",
        "as": "https://www.w3.org/TR/activitystreams-vocabulary/#",
        "wikidata": "http://www.wikidata.org/entity/",
        "mesocore": "https://www.softwareheritage.org/schema/2022/mesocore/",
    }

For example, here is a document using all three together:

.. code-block:: json

    {
      "@context": {
        "as": "https://www.w3.org/ns/activitystreams#",
        "codemeta": "https://codemeta.github.io/terms/",
        "forge": "https://forgefed.org/ns#",
        "mesocore": "https://www.softwareheritage.org/schema/2022/mesocore/",
        "schema":"http://schema.org/",
        "wikidata": "http://www.wikidata.org/entity/"
      },
      "@type": "forge:Repository",
      "@id": "https://github.com/octocat/linguist",
      "schema:name": "Linguist",
      "schema:description": "Language Savant",
      "codemeta:issueTracker": "https://github.com/octocat/linguist/issues",
      "mesocore:vcs": {
        "@type": "schema:SoftwareApplication",
        "@id": "wikidata:Q186055",
        "schema:name": "Git"
      },
      "mesocore:containsSourceCode": {
        "@type": "schema:SoftwareSourceCode",
        "schema:name": "Linguist",
        "schema:description": "This library is used on GitHub.com to detect blob languages"
      },
      "as:followers": {
        "@type": "as:OrderedCollection",
        "as:totalitems": 123
      },
      "forge:forks": {
        "@type": "as:OrderedCollection",
        "as:totalItems": 138
      },
      "forge:forkOf": {
        "@type": "forge:Repository",
        "@id": "https://github.com/github/linguist",
        "schema:name": "Linguist"
      }
    }


Types
-----

*This section is not normative*

None, MeSoCoRe reuses ``https://forgefed.org/ns#Repository``.

Relation properties
-------------------

Relation properties from other specifications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*This section is not normative*

The following properties are typically used in MeSoCoRe documents:

* The :prop:`as:followers` property lists actors (typically persons) subscribed
  to a repository.
  As it is an :prop:`as:OrderedCollection`, its :prop:`as:totalItems` attribute contains
  the number of followers, which matches ``watchers_count`` on GitHub and GitLab;
  which can also be expressed with :prop:`schema:InteractionCounter`
* :prop:`forge:forkedFrom` and :prop:`forge:forks` properties to describe relationships
  between repositories. (:prop:`forge:forks` is also an :prop:`as:OrderedCollection`).

Mirrors
^^^^^^^

TODO

Social properties
-----------------

Social properties from other specifications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*This section is not normative*

* The :prop:`as:likes` property lists actors (typically persons) who "like"
  to a repository.
  As it is an :prop:`as:OrderedCollection`, its :prop:`as:totalItems` attribute contains
  the number of followers, which matches ``stargazers_count`` on GitHub and
  ``star_count`` on GitLab;
  which can also be expressed with :prop:`schema:InteractionCounter`

.. comment:

    * The common concept of star(gazers) and watchers on GitHub/GitLab/Gitea can
      be represented using :prop:`schema:InteractionCounter` with types
      :prop:`schema:LikeAction` and :prop:`schema:FollowAction` (note that the latter can
      also be expressed using :prop:`as:followers`):
      TODO: use :prop:`as:likes` instead?

    .. code-block:: json

        {
          "@context": {
            "as": "https://www.w3.org/ns/activitystreams#",
            "codemeta": "https://codemeta.github.io/terms/",
            "forge": "https://forgefed.org/ns#",
            "mesocore": "https://www.softwareheritage.org/schema/2022/mesocore/",
            "schema":"http://schema.org/",
            "wikidata": "http://www.wikidata.org/entity/"
          },
          "@type": "forge:Repository",
          "@id": "https://github.com/octocat/linguist",
          "schema:interactionStatistic": [
            {
              "@type": "schema:InteractionCounter",
              "schema:interactionType": "http://schema.org/LikeAction",
              "schema:userInteractionCount": 146
            },
            {
              "@type": "schema:InteractionCounter",
              "schema:interactionType": "http://schema.org/FollowAction",
              "schema:userInteractionCount": 123
            },
          ]
        }

Configuration properties
------------------------

Statistics properties
---------------------
