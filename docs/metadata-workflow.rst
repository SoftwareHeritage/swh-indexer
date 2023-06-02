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
^^^^^^^^^^^^^^^^^^^^

Sequence diagram
""""""""""""""""

.. thumbnail:: images/tasks-intrinsic-metadata-indexers.svg


Data flow and storage
"""""""""""""""""""""

.. thumbnail:: images/metadata-flow.svg


Origin-Head Indexer
^^^^^^^^^^^^^^^^^^^

First, the Origin-Head indexer gets called externally, with an origin as
argument (or multiple origins, that are handled sequentially).
For now, its tasks are scheduled manually via recurring Scheduler tasks; but
in the near future, the :term:`journal` will be used to do that.

It first looks up the last :term:`snapshot` and determines what the main
branch of origin is (the "Head branch") and what revision it points to
(the "Head").
Intrinsic metadata for that origin will be extracted from that revision.

It schedules a Directory Metadata Indexer task for the root directory of
that revision.


Directory and Content Metadata Indexers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These two indexers do the hard part of the work. The Directory Metadata
Indexer fetches the root directory associated with a revision, then extracts
the metadata from that directory.

To do so, it lists files in that directory, and looks for known names, such
as :file:`codemeta.json`, :file:`package.json`, or :file:`pom.xml`. If there are any, it
runs the Content Metadata Indexer on them, which in turn fetches their
contents and runs them through extraction dictionaries/mappings.
See below for details.

Their results are saved in a database (the indexer storage), associated with
the content and directory hashes.


Origin Metadata Indexer
^^^^^^^^^^^^^^^^^^^^^^^

The job of this indexer is very simple: it takes an origin identifier and
uses the Origin-Head and Directory indexers to get metadata from the head
directory of an origin, and copies the metadata of the former to a new table,
to associate it with the latter.

The reason for this is to be able to perform searches on metadata, and
efficiently find out which origins matched the pattern.
Running that search on the ``directory_metadata`` table would require either
a reverse lookup from directories to origins, which is costly.


Translation from ecosystem-specific metadata to CodeMeta
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Intrinsic metadata is extracted from files provided with a project's source
code, and translated using `CodeMeta`_'s `crosswalk table`_; which is vendored
in :file:`swh/indexer/data/codemeta/codemeta.csv`.
Ecosystems not yet included in Codemeta's crosswalk have their own
:file:`swh/indexer/data/*.csv` file, with one row for each CodeMeta property,
even when not supported by the ecosystem.

All input formats supported so far are straightforward dictionaries (eg. JSON)
or can be accessed as such (eg. XML); and the first part of the translation is
to map their keys to a term in the CodeMeta vocabulary.
This is done by parsing the crosswalk table's `CSV file`_ and using it as a
map between these two vocabularies; and this does not require any
format-specific code in the indexers.

The second part is to normalize values. As language-specific metadata files
each have their way(s) of formatting these values, we need to turn them into
the data type required by CodeMeta.
This normalization makes up for most of the code of
:py:mod:`swh.indexer.metadata_dictionary`.

.. _CodeMeta: https://codemeta.github.io/
.. _crosswalk table: https://codemeta.github.io/crosswalk/
.. _CSV file: https://github.com/codemeta/codemeta/blob/master/crosswalk.csv


Extrinsic metadata
------------------

Indexer architecture
^^^^^^^^^^^^^^^^^^^^

.. thumbnail:: images/tasks-extrinsic-metadata-indexers.svg

The :term:`extrinsic metadata` indexer works very differently from
the :term:`intrinsic metadata` indexers we saw above.
While the latter extract metadata from software artefacts (files and directories)
which are already a core part of the archive, the former extracts such data from
API calls pulled from forges and package managers, or pushed via the
:ref:`SWORD deposit <swh-deposit>`.

In order to preserve original information verbatim, the Software Heritage itself
stores the result of these calls, independently of indexers, in their own archive
as described in the :ref:`extrinsic-metadata-specification`.
In this section, we assume this information is already present in the archive,
but in the "raw extrinsic metadata" form, which needs to be translated to a common
vocabulary to be useful, as with intrinsic metadata.

The common vocabulary we chose is JSON-LD, with both CodeMeta and
`ForgeFed's vocabulary`_ (including `ActivityStream's vocabulary`_)

.. _ForgeFed's vocabulary: https://forgefed.org/vocabulary.html
.. _ActivityStream's vocabulary: https://www.w3.org/TR/activitystreams-vocabulary/

Instead of the four-step architecture above, the extrinsic-metadata indexer
is standalone: it reads "raw extrinsic metadata" from the :ref:`swh-journal`,
and produces new indexed entries in the database as they come.

The caveat is that, while intrinsic metadata are always unambiguously authoritative
(they are contained by their own origin repository, therefore they were added by
the origin's "owners"), extrinsic metadata can be authored by third-parties.
Support for third-party authorities is currently not implemented for this reason;
so extrinsic metadata is only indexed when provided by the same
forge/package-repository as the origin the metadata is about.
Metadata on non-origin objects (typically, directories), is also ignored for
this reason, for now.

Assuming the metadata was provided by such an authority, it is then passed
to metadata mappings; identified by a mimetype (or custom format name)
they declared rather than filenames.


Implementation status
---------------------

Supported intrinsic metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following sources of intrinsic metadata are supported:

* `CITATION.cff <https://citation-file-format.github.io/>`_
* CodeMeta's `codemeta.json`_,
* PHP's `composer.json <https://getcomposer.org/doc/04-schema.md>`_
* Maven's `pom.xml`_,
* NPM's `package.json`_,
* NuGet's `.nuspec <https://learn.microsoft.com/en-us/nuget/reference/nuspec>`_
* Pub.Dev's `pubspec.yaml <https://dart.dev/tools/pub/pubspec>`_
* Python's `PKG-INFO`_,
* Ruby's `.gemspec`_

.. _codemeta.json: https://codemeta.github.io/terms/
.. _pom.xml: https://maven.apache.org/pom.html
.. _package.json: https://docs.npmjs.com/files/package.json
.. _PKG-INFO: https://www.python.org/dev/peps/pep-0314/
.. _.gemspec: https://guides.rubygems.org/specification-reference/

Supported extrinsic metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following sources of extrinsic metadata are supported:

* Codemeta documents sent by clients of :ref:`swh-deposit <swh-deposit>` (`HAL <https://hal.science/>`_, `eLife <https://elifesciences.org/>`_, `IPOL <https://www.ipol.im/>`_, ...)
* Gitea's `"repoGet" API <https://gittea.dev/api/swagger#/repository/repoGet>`__
* GitHub's `"repo" API <https://docs.github.com/en/rest/repos/repos#get-a-repository>`__



Supported JSON-LD properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following terms may be found in the output of the metadata translation
(other than the `codemeta` mapping, which is the identity function, and
therefore supports all properties):

.. program-output:: python3 -m swh.indexer.cli mapping list-terms --exclude-mapping codemeta --exclude-mapping json-sword-codemeta --exclude-mapping sword-codemeta
    :nostderr:




Tutorials
---------

The rest of this page is made of two tutorials: one to index
:term:`intrinsic metadata` (ie. from a file in a VCS or in a tarball),
and one to index :term:`extrinsic metadata` (ie. obtained via external means,
such as GitHub's or GitLab's APIs).

Adding support for additional ecosystem-specific intrinsic metadata
-------------------------------------------------------------------

This section will guide you through adding code to the metadata indexer to
detect and translate new metadata formats.

First, you should start by picking one of the `CodeMeta crosswalks`_.
Then create a new file in :file:`swh-indexer/swh/indexer/metadata_dictionary/`, that
will contain your code, and create a new class that inherits from helper
classes, with some documentation about your indexer:

.. code-block:: python

	from .base import DictMapping, SingleFileIntrinsicMapping
	from swh.indexer.codemeta import CROSSWALK_TABLE

	class MyMapping(DictMapping, SingleFileIntrinsicMapping):
		"""Dedicated class for ..."""
		name = 'my-mapping'
		filename = b'the-filename'
		mapping = CROSSWALK_TABLE['Name of the CodeMeta crosswalk']

.. _CodeMeta crosswalks: https://github.com/codemeta/codemeta/tree/master/crosswalks

And reference it from :const:`swh.indexer.metadata_dictionary.INTRINSIC_MAPPINGS`.

Then, add a ``string_fields`` attribute, that is the list of all keys whose
values are simple text values. For instance, to
`translate Python PKG-INFO`_, it's:

.. code-block:: python

    string_fields = ['name', 'version', 'description', 'summary',
                     'author', 'author-email']


These values will be automatically added to the above list of
supported terms.

.. _translate Python PKG-INFO: https://forge.softwareheritage.org/source/swh-indexer/browse/master/swh/indexer/metadata_dictionary/python.py

Last step to get your code working: add a ``translate`` method that will
take a single byte string as argument, turn it into a Python dictionary,
whose keys are the ones of the input document, and pass it to
``_translate_dict``.

For instance, if the input document is in JSON, it can be as simple as:

.. code-block:: python

    def translate(self, raw_content):
        raw_content = raw_content.decode()  # bytes to str
        content_dict = json.loads(raw_content)  # str to dict
        return self._translate_dict(content_dict)  # convert to CodeMeta

``_translate_dict`` will do the heavy work of reading the crosswalk table for
each of ``string_fields``, read the corresponding value in the ``content_dict``,
and build a CodeMeta dictionary with the corresponding names from the
crosswalk table.

One last thing to run your code: add it to the list in
:file:`swh-indexer/swh/indexer/metadata_dictionary/__init__.py`, so the rest of the
code is aware of it.

Now, you can run it:

.. code-block:: shell

    python3 -m swh.indexer.metadata_dictionary MyMapping path/to/input/file

and it will (hopefully) returns a CodeMeta object.

If it works, well done!

You can now improve your translation code further, by adding methods that
will do more advanced conversion. For example, if there is a field named
``license`` containing an SPDX identifier, you must convert it to an URI,
like this:

.. code-block:: python

    def normalize_license(self, s):
        if isinstance(s, str):
            return rdflib.URIRef("https://spdx.org/licenses/" + s)

This method will automatically get called by ``_translate_dict`` when it
finds a ``license`` field in ``content_dict``.

Adding support for additional ecosystem-specific extrinsic metadata
-------------------------------------------------------------------

[this section is a work in progress]
