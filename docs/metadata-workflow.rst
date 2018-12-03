Metadata workflow
=================

Intrinsic metadata
------------------

Indexing :term:`intrinsic metadata` requires extracting information from the
lowest levels of the :ref:`Merkle DAG <swh-merkle-dag>` (directories, files,
and content blobs) and associate them to the highest ones (origins).
In order to deduplicate the work between origins, we split this work between
multiple indexers, which coordinate with each other and save their results
at each step in the indexer storage.

.. thumbnail:: images/tasks-metadata-indexers.svg


Origin-Head Indexer
___________________

First, the Origin-Head indexer gets called externally, with an origin as
argument (or multiple origins, that are handled sequentially).
For now, its tasks are scheduled manually via recurring Scheduler tasks; but
in the near future, the :term:`journal` will be used to do that.

It first looks up the last :term:`snapshot` and determines what the main
branch of origin is (the "Head branch") and what revision it points to
(the "Head").
Intrinsic metadata for that origin will be extracted from that revision.

It schedules a Revision Metadata Indexer task for that revision, with a
hint that the revision is the Head of that particular origin.


Revision and Content Metadata Indexers
______________________________________

These two indexers do the hard part of the work. The Revision Metadata
Indexer fetches the root directory associated with a revision, then extracts
the metadata from that directory.

To do so, it lists files in that directory, and looks for known names, such
as `codemeta.json`, `package.json`, or `pom.xml`. If there are any, it
runs the Content Metadata Indexer on them, which in turn fetches their
contents and runs them through extraction dictionaries/mappings.

Their results are saved in a database (the indexer storage), associated with
the content and revision hashes.

If it received a hint that this revision is the head of an origin, the
Revision Metadata Indexer then schedules the Origin Metadata Indexer
to run on that origin.


Origin Metadata Indexer
_______________________

The job of this indexer is very simple: it takes an origin identifier and
a revision hash, and copies the metadata of the former to a new table, to
associate it with the latter.

The reason for this is to be able to perform searches on metadata, and
efficiently find out which origins matched the pattern.
Running that search on the `revision_metadata` table would require either
a reverse lookup from revisions to origins, which is costly.
