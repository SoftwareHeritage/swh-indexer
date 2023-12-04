Software Heritage - Indexer
===========================

Tools to compute multiple indexes on SWH's raw contents:

- content:

  - mimetype
  - fossology-license
  - metadata

- origin:

  - metadata (intrinsic, using the content indexer; and extrinsic)

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

- fossology-license (queue swh_indexer_fossology_license): compute the
  license

- metadata: translate file from an ecosystem-specific formats to JSON-LD
  (using schema.org/CodeMeta vocabulary)

Current origin indexers:

- metadata: translate file from an ecosystem-specific formats to JSON-LD
  (using schema.org/CodeMeta and ForgeFed vocabularies)
