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

## Context

SWH has currently stored around 5B contents.  The table `content`
holds their checksums.

Those contents are physically stored in an object storage (using
disks) and replicated in another. Those object storages are not
destined for reading yet.

We are in the process to copy those contents over to azure's blob
storages.  As such, we will use that opportunity to trigger the
computations on these contents once those have been copied over.


## Workers

There are two types of workers:
- orchestrators (orchestrator, orchestrator-text)
- indexer (mimetype, language, ctags, fossology-license)

### Orchestrator


The orchestrator is in charge of dispatching a batch of sha1 hashes to
different indexers.

Orchestration procedure:
- receive batch of sha1s
- split those batches into groups (according to setup)
- broadcast those group to indexers

There are two types of orchestrators:

- orchestrator (swh_indexer_orchestrator_content_all): Receives and
  broadcast sha1 ids (of contents) to indexers (currently only the
  mimetype indexer)

- orchestrator-text (swh_indexer_orchestrator_content_text): Receives
  batch of sha1 ids (of textual contents) and broadcast those to
  indexers (currently language, ctags, and fossology-license
  indexers).


### Indexers


An indexer is in charge of the content retrieval and indexation of the
extracted information in the swh-indexer db.

There are two types of indexers:
  - content indexer: works with content sha1 hashes
  - revision indexer: works with revision sha1 hashes

Indexation procedure:
- receive batch of ids
- retrieve the associated data depending on object type
- compute for that object some index
- store the result to swh's storage
- (and possibly do some broadcast itself)

Current content indexers:

- mimetype (queue swh_indexer_content_mimetype): compute the mimetype,
  filter out the textual contents and broadcast the list to the
  orchestrator-text

- language (queue swh_indexer_content_language): detect the programming language

- ctags (queue swh_indexer_content_ctags): try and compute tags
  information

- fossology-license (queue swh_indexer_fossology_license): try and
  compute the license

- metadata : translate file into translated_metadata dict

Current revision indexers:

- metadata: detects files containing metadata and retrieves translated_metadata
  in content_metadata table in storage or run content indexer to translate
  files.
