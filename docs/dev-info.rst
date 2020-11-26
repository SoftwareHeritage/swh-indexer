Hacking on swh-indexer
======================

This tutorial will guide you through the hacking on the swh-indexer.
If you do not have a local copy of the Software Heritage archive, go to the
`getting started tutorial
<https://docs.softwareheritage.org/devel/getting-started.html>`_

Configuration files
-------------------
You will need the following YAML configuration files to run the swh-indexer
commands:

- Orchestrator at
  ``~/.config/swh/indexer/orchestrator.yml``

.. code-block:: yaml

  indexers:
    mimetype:
      check_presence: false
      batch_size: 100

- Orchestrator-text at
  ``~/.config/swh/indexer/orchestrator-text.yml``

.. code-block:: yaml

  indexers:
    # language:
    #   batch_size: 10
    #   check_presence: false
    fossology_license:
      batch_size: 10
      check_presence: false
    # ctags:
    #   batch_size: 2
    #   check_presence: false

- Mimetype indexer at
  ``~/.config/swh/indexer/mimetype.yml``

.. code-block:: yaml

    # storage to read sha1's metadata (path)
  	# storage:
  	#   cls: local
  	#   db: "service=swh-dev"
  	#   objstorage:
  	#     cls: pathslicing
  	#     root: /home/storage/swh-storage/
  	#     slicing: 0:1/1:5

  	storage:
  	  cls: remote
	    url: http://localhost:5002/

  	indexer_storage:
  	  cls: remote
  	  args:
  	    url: http://localhost:5007/

  	# storage to read sha1's content
  	# adapt this to your need
  	# locally: this needs to match your storage's setup
  	objstorage:
  	  cls: pathslicing
	    slicing: 0:1/1:5
 	    root: /home/storage/swh-storage/

  	destination_task: swh.indexer.tasks.SWHOrchestratorTextContentsTask
  	rescheduling_task: swh.indexer.tasks.SWHContentMimetypeTask


- Fossology indexer at
  ``~/.config/swh/indexer/fossology_license.yml``

.. code-block:: yaml

    # storage to read sha1's metadata (path)
  	# storage:
  	#   cls: local
  	#   db: "service=swh-dev"
  	#   objstorage:
  	#     cls: pathslicing
  	#     root: /home/storage/swh-storage/
  	#     slicing: 0:1/1:5

  	storage:
  	  cls: remote
  	  url: http://localhost:5002/

  	indexer_storage:
  	  cls: remote
  	  args:
  	    url: http://localhost:5007/

  	# storage to read sha1's content
  	# adapt this to your need
  	# locally: this needs to match your storage's setup
  	objstorage:
  	  cls: pathslicing
	    slicing: 0:1/1:5
	    root: /home/storage/swh-storage/

  	workdir: /tmp/swh/worker.indexer/license/

  	tools:
  	  name: 'nomos'
  	  version: '3.1.0rc2-31-ga2cbb8c'
  	  configuration:
  	    command_line: 'nomossa <filepath>'


- Worker at
  ``~/.config/swh/worker.yml``

.. code-block:: yaml

  task_broker: amqp://guest@localhost//
  	task_modules:
  	  - swh.loader.svn.tasks
  	  - swh.loader.tar.tasks
  	  - swh.loader.git.tasks
  	  - swh.storage.archiver.tasks
  	  - swh.indexer.tasks
  	  - swh.indexer.orchestrator
  	task_queues:
  	  - swh_loader_svn
  	  - swh_loader_tar
  	  - swh_reader_git_to_azure_archive
  	  - swh_storage_archive_worker_to_backend
  	  - swh_indexer_orchestrator_content_all
  	  - swh_indexer_orchestrator_content_text
  	  - swh_indexer_content_mimetype
  	  - swh_indexer_content_language
  	  - swh_indexer_content_ctags
  	  - swh_indexer_content_fossology_license
  	  - swh_loader_svn_mount_and_load
  	  - swh_loader_git_express
  	  - swh_loader_git_archive
  	  - swh_loader_svn_archive
  	task_soft_time_limit: 0


Database
--------

swh-indxer uses a database to store the indexed content. The default
db is expected to be called swh-indexer-dev.

Create or add  ``swh-dev`` and ``swh-indexer-dev`` to
the ``~/.pg_service.conf`` and ``~/.pgpass`` files, which are postgresql's
configuration files.

Add data to local DB
--------------------
from within the ``swh-environment``, run the following command::

  make rebuild-testdata

and fetch some real data to work with, using::

   python3 -m swh.loader.git.updater --origin-url <github url>

Then you can list all content files using this script::

  #!/usr/bin/env bash

  psql service=swh-dev -c "copy (select sha1 from content) to stdin" | sed -e 's/^\\\\x//g'

Run the indexers
-----------------
Use the list off contents to feed the indexers with with the
following command::

  ./list-sha1.sh | python3 -m swh.indexer.producer --batch 100 --task-name orchestrator_all

Activate the workers
--------------------
To send messages to different queues using rabbitmq
(which should already be installed through dependencies installation),
run the following command in a dedicated terminal::

  python3 -m celery worker --app=swh.scheduler.celery_backend.config.app \
                 --pool=prefork \
                 --concurrency=1 \
                 -Ofair \
                 --loglevel=info \
                 --without-gossip \
                 --without-mingle \
                 --without-heartbeat 2>&1

With this command rabbitmq will consume message using the worker
configuration file.

Note: for the fossology_license indexer, you need a package fossology-nomossa
which is in our `public debian repository
<https://wiki.softwareheritage.org/index.php?title=Debian_packaging#Package_repository>`_.
