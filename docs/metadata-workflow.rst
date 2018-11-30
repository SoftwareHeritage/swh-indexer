Metadata workflow
=================

Indexing :term:`intrinsic metadata` requires extracting information from the
lowest levels of the :ref:`Merkle DAG <swh-merkle-dag>` (directories, files,
and content blobs) and associate them to the highest ones (origins).
In order to deduplicate the work between origins, we split this work between
multiple indexers, which coordinate with each other and save their results
at each step in the indexer storage.

.. thumbnail:: images/tasks-metadata-indexers.svg
