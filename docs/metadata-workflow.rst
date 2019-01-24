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

Indexer architecture
--------------------

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
See below for details.

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


Translation from language-specific metadata to CodeMeta
-------------------------------------------------------

Intrinsic metadata are extracted from files provided with a project's source
code, and translated using `CodeMeta`_'s `crosswalk table`_.

All input formats supported so far are straightforward dictionaries (eg. JSON)
or can be accessed as such (eg. XML); and the first part of the translation is
to map their keys to a term in the CodeMeta vocabulary.
This is done by parsing the crosswalk table's `CSV file`_ and using it as a
map between these two vocabularies; and this does not require any
format-specific code in the indexers.

The second part is to normalize values. As language-specific metadata files
each have their way(s) of formating these values, we need to turn them into
the data type required by CodeMeta.
This normalization makes up for most of the code of
:py:mod:`swh.indexer.metadata_dictionary`.

.. _CodeMeta: https://codemeta.github.io/
.. _crosswalk table: https://codemeta.github.io/crosswalk/
.. _CSV file: https://github.com/codemeta/codemeta/blob/master/crosswalk.csv


Supported intrinsic metadata
----------------------------

The following sources of intrinsic metadata are supported:

* CodeMeta's `codemeta.json`_,
* Maven's `pom.xml`_,
* NPM's `package.json`_,
* Python's `PKG-INFO`_,
* Ruby's `.gemspec`_

.. _codemeta.json: https://codemeta.github.io/terms/
.. _pom.xml: https://maven.apache.org/pom.html
.. _package.json: https://docs.npmjs.com/files/package.json
.. _PKG-INFO: https://www.python.org/dev/peps/pep-0314/
.. _.gemspec: https://guides.rubygems.org/specification-reference/
