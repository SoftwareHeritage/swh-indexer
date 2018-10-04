.. _swh-indexer:

Software Heritage - Indexer
===========================

Tools and workers used to mine the content of the archive and extract derived
information from archive source code artifacts.

Workers
-------
There are two types of workers:
  - orchestrators (orchestrator, orchestrator-text)
  - indexers (mimetype, language, ctags, fossology-license)

Orchestrator
************
The orchestrator is in charge of dispatching a batch of sha1 hashes to
different indexers.

There are two types of orchestrators:
  - orchestrator (swh_indexer_orchestrator_content_all): Receives and
    broadcast sha1 ids (of contents) to indexers (currently only the
    mimetype indexer)
  - orchestrator-text (swh_indexer_orchestrator_content_text): Receives
    batch of sha1 ids (of textual contents) and broadcast those to
    indexers (currently language, ctags, and fossology-license
    indexers).

Orchestration procedure:
  - receive batch of sha1s
  - split into small batches
  - broadcast batches to indexers



Indexers
********
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
-------------------------
    - mimetype: computes the mimetype,
      filter out the textual contents and broadcast the list to the
      orchestrator-text

    - language : detect the programming language with pygments

    - ctags : try and compute tags
      information

    - fossology-license : try and compute the license

    - metadata : translate file into translated_metadata dict

Current revision indexers:
--------------------------
    - metadata : detects files containing metadata and creates a minimal
      metadata set kept with the revision.


.. toctree::
   :maxdepth: 1
   :caption: Contents:

   dev-info.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
