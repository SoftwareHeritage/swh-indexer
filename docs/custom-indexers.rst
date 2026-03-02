.. _custom_indexers:

Custom indexers and metadata mappings
=====================================

Indexers
--------

New indexers can be added by implementing a new indexer class and declaring it
in the ``swh.indexer.classes`` entry point of your package's ``pyproject.toml``
file, e.g.:

.. code-block:: toml

   [project.entry-points."swh.indexer.classes"]
   "my_indexer" = "swh.mypkg.indexer:MyIndexer"


An indexer class should inherit from the
:py:class:`swh.indexer.indexer.BaseIndexer` class, and usually more
specifically inherit from either
:py:class:`swh.indexer.indexer.ContentIndexer`,
:py:class:`swh.indexer.indexer.OriginIndexer` or
:py:class:`swh.indexer.indexer.DirectoryIndexer`.

For metadata indexer, you should probably only need to implement a Metadata
mapping (see below).

See :ref:`metadata-workflow` for a better view of the metadata handling architecture.


Metadata mappings
-----------------

Metadata indexers use mappings to convert a given source metadata format to
the internal metadata format, JSON-LD with Codemeta and ForgeFed vocabularies.

A metadata mapping is a class inheriting from either
:py:class:`swh.indexer.metadata_mapping.base.BaseExtrinsicMapping` or
:py:class:`swh.indexer.metadata_mapping.base.BaseIntrinsicMapping`.

Each mapping class should be declared in the ``swh.indexer.metadata_mappings``
entry point group, in the ``pyproject.toml`` package file, e.g.:

.. code-block:: toml

   [project.entry-points."swh.indexer.metadata_mappings"]
   "MyMapping" = "swh.mypkg.mymapping:MyMapping"


Intrinsic mappings
~~~~~~~~~~~~~~~~~~

Intrinsic mappings are used by an intrinsic metadata indexer (currently the
:ref:`origin-head-indexer`, the :ref:`directory-content-indexer` or the
:ref:`origin-indexer`). Adding intrinsic metadata mappings allows these
indexers to handle more metadata formats.

An intrinsic mapping is a class inheriting from
:py:class:`swh.indexer.metadata_mapping.base.BaseIntrinsicMapping` and
implementing at least the following methods:

-
  :py:meth:`swh.indexer.metadata_mapping.base.BaseIntrinsicMapping.detect_metadata_file(file_entries)`
  this is a class method used to filter files this mapping can handle.
-
  :py:meth:`swh.indexer.metadata_mapping.base.BaseIntrinsicMapping.translate(raw_content)`
  the actual method doing the mapping from the original format to known format
  (Codemeta).


Extrinsic mappings
~~~~~~~~~~~~~~~~~~

Extrinsic mappings are used to convert extrinsic metadata, like forge project
metadata. Adding extrinsic metadata mappings allows the extrinsic metadata
indexer to handle more extrinsic metadata formats.

An extrinsic mapping is a class inheriting from
:py:class:`swh.indexer.metadata_mapping.base.BaseExtrinsicMapping`
and implementing at least the following methods:

-
  :py:meth:`swh.indexer.metadata_mapping.base.BaseExtrinsicMapping.extrinsic_metadata_formats()`
  this is a class method returning :ref:`extrinsic metadata formats <extrinsic-metadata-formats>`
  supported by this class.

-
  :py:meth:`swh.indexer.metadata_mapping.base.BaseIntrinsicMapping.translate(raw_content)`
  the actual method doing the mapping from the original format to known format
  (Codemeta).
