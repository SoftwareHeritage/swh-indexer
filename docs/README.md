swh-indexer
============

Tools to compute multiple indexes on SWH's raw contents:
- content:
  - mimetype
  - ctags
  - language
  - fossology-license
  - metadata
- revision:
  - metadata

An indexer is in charge of:
- looking up objects
- extracting information from those objects
- store those information in the swh-indexer db

There are multiple indexers working on different object types:
  - content indexer: works with content sha1 hashes
  - revision indexer: works with revision sha1 hashes
  - origin indexer: works with origin identifiers

Indexation procedure:
- receive batch of ids
- retrieve the associated data depending on object type
- compute for that object some index
- store the result to swh's storage

Current content indexers:

- mimetype (queue swh_indexer_content_mimetype): detect the encoding
  and mimetype

- language (queue swh_indexer_content_language): detect the
  programming language

- ctags (queue swh_indexer_content_ctags): compute tags information

- fossology-license (queue swh_indexer_fossology_license): compute the
  license

- metadata: translate file into translated_metadata dict

Current revision indexers:

- metadata: detects files containing metadata and retrieves translated_metadata
  in content_metadata table in storage or run content indexer to translate
  files.
