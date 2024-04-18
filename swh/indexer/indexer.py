# Copyright (C) 2016-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
from contextlib import contextmanager
import logging
import os
import shutil
import tempfile
from typing import Any, Dict, Generic, Iterator, List, Optional, Tuple, TypeVar, Union
import warnings

import sentry_sdk
from typing_extensions import TypedDict

from swh.core.config import load_from_envvar, merge_configs
from swh.indexer.storage import INDEXER_CFG_KEY, Sha1, get_indexer_storage
from swh.indexer.storage.interface import IndexerStorageInterface
from swh.model import hashutil
from swh.model.model import Directory, Origin, Sha1Git
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.objstorage.interface import objid_from_dict
from swh.storage import get_storage
from swh.storage.interface import StorageInterface


class ObjectsDict(TypedDict, total=False):
    """Typed objects whose keys are names of Kafka topics and values are list of values
    of messages in that topic."""

    content: List[Dict]
    directory: List[Dict]
    origin: List[Dict]
    origin_visit_status: List[Dict]
    raw_extrinsic_metadata: List[Dict]


@contextmanager
def write_to_temp(filename: str, data: bytes, working_directory: str) -> Iterator[str]:
    """Write the sha1's content in a temporary file.

    Args:
        filename: one of sha1's many filenames
        data: the sha1's content to write in temporary
          file
        working_directory: the directory into which the
          file is written

    Returns:
        The path to the temporary file created. That file is
        filled in with the raw content's data.

    """
    os.makedirs(working_directory, exist_ok=True)
    temp_dir = tempfile.mkdtemp(dir=working_directory)
    content_path = os.path.join(temp_dir, filename)

    with open(content_path, "wb") as f:
        f.write(data)

    yield content_path
    shutil.rmtree(temp_dir)


DEFAULT_CONFIG = {
    INDEXER_CFG_KEY: {"cls": "memory"},
    "storage": {"cls": "memory"},
    "objstorage": {"cls": "memory"},
}


TId = TypeVar("TId")
"""type of the ids of index()ed objects."""
TData = TypeVar("TData")
"""type of the objects passed to index()."""
TResult = TypeVar("TResult")
"""return type of index()"""


class BaseIndexer(Generic[TId, TData, TResult], metaclass=abc.ABCMeta):
    """Base class for indexers to inherit from.

    The main entry point is the :func:`run` function which is in
    charge of triggering the computations on the batch dict/ids
    received.

    Indexers can:

    - filter out ids whose data has already been indexed.
    - retrieve ids data from storage or objstorage
    - index this data depending on the object and store the result in
      storage.

    To implement a new object type indexer, inherit from the
    BaseIndexer and implement indexing:

    :meth:`~BaseIndexer.run`:
      object_ids are different depending on object. For example: sha1 for
      content, sha1_git for revision, directory, release, and id for origin

    To implement a new concrete indexer, inherit from the object level
    classes: :class:`ContentIndexer`, :class:`DirectoryIndexer`,
    :class:`OriginIndexer`.

    Then you need to implement the following functions:

    :meth:`~BaseIndexer.filter`:
      filter out data already indexed (in storage).

    :meth:`~BaseIndexer.index_object`:
      compute index on id with data (retrieved from the storage or the
      objstorage by the id key) and return the resulting index computation.

    :meth:`~BaseIndexer.persist_index_computations`:
      persist the results of multiple index computations in the storage.

    The new indexer implementation can also override the following functions:

    :meth:`~BaseIndexer.prepare`:
      Configuration preparation for the indexer.  When overriding, this must
      call the `super().prepare()` instruction.

    :meth:`~BaseIndexer.check`:
      Configuration check for the indexer.  When overriding, this must call the
      `super().check()` instruction.

    :meth:`~BaseIndexer.register_tools`:
      This should return a dict of the tool(s) to use when indexing or
      filtering.

    """

    results: List[TResult]

    USE_TOOLS = True

    catch_exceptions = True
    """Prevents exceptions in `index()` from raising too high. Set to False
    in tests to properly catch all exceptions."""

    storage: StorageInterface
    objstorage: Any
    idx_storage: IndexerStorageInterface

    def __init__(self, config=None, **kw) -> None:
        """Prepare and check that the indexer is ready to run."""
        super().__init__()
        if config is not None:
            self.config = config
        else:
            self.config = load_from_envvar()
        self.config = merge_configs(DEFAULT_CONFIG, self.config)
        self.prepare()
        self.check()
        self.log.debug("%s: config=%s", self, self.config)

    def prepare(self) -> None:
        """Prepare the indexer's needed runtime configuration.
        Without this step, the indexer cannot possibly run.

        """
        config_storage = self.config.get("storage")
        if config_storage:
            self.storage = get_storage(**config_storage)

        self.objstorage = get_objstorage(**self.config["objstorage"])

        idx_storage = self.config[INDEXER_CFG_KEY]
        self.idx_storage = get_indexer_storage(**idx_storage)

        _log = logging.getLogger("requests.packages.urllib3.connectionpool")
        _log.setLevel(logging.WARN)
        self.log = logging.getLogger("swh.indexer")

        if self.USE_TOOLS:
            self.tools = list(self.register_tools(self.config.get("tools", [])))
        self.results = []

    @property
    def tool(self) -> Dict:
        return self.tools[0]

    def check(self) -> None:
        """Check the indexer's configuration is ok before proceeding.
        If ok, does nothing. If not raise error.

        """
        if self.USE_TOOLS and not self.tools:
            raise ValueError("Tools %s is unknown, cannot continue" % self.tools)

    def _prepare_tool(self, tool: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare the tool dict to be compliant with the storage api."""
        return {"tool_%s" % key: value for key, value in tool.items()}

    def register_tools(
        self, tools: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Permit to register tools to the storage.

           Add a sensible default which can be overridden if not
           sufficient.  (For now, all indexers use only one tool)

           Expects the self.config['tools'] property to be set with
           one or more tools.

        Args:
            tools: Either a dict or a list of dict.

        Returns:
            list: List of dicts with additional id key.

        Raises:
            ValueError: if not a list nor a dict.

        """
        if isinstance(tools, list):
            tools = list(map(self._prepare_tool, tools))
        elif isinstance(tools, dict):
            tools = [self._prepare_tool(tools)]
        else:
            raise ValueError("Configuration tool(s) must be a dict or list!")

        if tools:
            return self.idx_storage.indexer_configuration_add(tools)
        else:
            return []

    def index(self, id: TId, data: Optional[TData], **kwargs) -> List[TResult]:
        """Index computation for the id and associated raw data.

        Args:
            id: identifier or Dict object
            data: id's data from storage or objstorage depending on
              object type

        Returns:
            dict: a dict that makes sense for the
            :meth:`.persist_index_computations` method.

        """
        raise NotImplementedError()

    def filter(self, ids: List[TId]) -> Iterator[TId]:
        """Filter missing ids for that particular indexer.

        Args:
            ids: list of ids

        Yields:
            iterator of missing ids

        """
        yield from ids

    @abc.abstractmethod
    def persist_index_computations(self, results: List[TResult]) -> Dict[str, int]:
        """Persist the computation resulting from the index.

        Args:

            results: List of results. One result is the
              result of the index function.

        Returns:
            a summary dict of what has been inserted in the storage

        """
        return {}

    def process_journal_objects(self, objects: ObjectsDict) -> Dict:
        """Read swh message objects (content, origin, ...) from the journal to:

        - retrieve the associated objects from the storage backend (e.g. storage,
          objstorage...)
        - execute the associated indexing computations
        - store the results in the indexer storage

        """
        raise NotImplementedError()


class ContentIndexer(BaseIndexer[Sha1, bytes, TResult], Generic[TResult]):
    """A content indexer working on the journal (method `process_journal_objects`) or on
    a list of ids directly (method `run`).

    Note: :class:`ContentIndexer` is not an instantiable object. To
    use it, one should inherit from this class and override the
    methods mentioned in the :class:`BaseIndexer` class.

    """

    def process_journal_objects(self, objects: ObjectsDict) -> Dict:
        """Read content objects from the journal, retrieve their raw content and compute
        content indexing (e.g. mimetype, fossology license, ...).

        Note that once this is deployed, this supersedes the main ContentIndexer.run
        method call and the class ContentPartitionIndexer.
        """
        summary: Dict[str, Any] = {"status": "uneventful"}
        try:
            results = []
            contents = objects.get("content", [])
            content_data = self.objstorage.get_batch(map(objid_from_dict, contents))
            for item, raw_content in zip(contents, content_data):
                id_ = item["sha1"]
                sentry_sdk.set_tag(
                    "swh-indexer-content-sha1", hashutil.hash_to_hex(id_)
                )
                if not raw_content:
                    self.log.warning(
                        "Content %s not found in objstorage",
                        hashutil.hash_to_hex(id_),
                    )
                    continue

                results.extend(self.index(id_, data=raw_content))
        except Exception:
            if not self.catch_exceptions:
                raise
            self.log.exception("Problem when reading contents metadata.")
            sentry_sdk.capture_exception()
            summary["status"] = "failed"
            return summary
        else:
            # Reset tag after we finished processing the given content
            sentry_sdk.set_tag("swh-indexer-content-sha1", "")

        summary_persist = self.persist_index_computations(results)
        self.results = results
        if summary_persist:
            for value in summary_persist.values():
                if value > 0:
                    summary["status"] = "eventful"
            summary.update(summary_persist)
        return summary

    def run(self, ids: List[Sha1], **kwargs) -> Dict:
        """Given a list of ids:

        - retrieve the content from the storage
        - execute the indexing computations
        - store the results

        Args:
            ids (Iterable[Union[bytes, str]]): sha1's identifier list
            **kwargs: passed to the `index` method

        Returns:
            A summary Dict of the task's status

        """
        if "policy_update" in kwargs:
            warnings.warn(
                "'policy_update' argument is deprecated and ignored.",
                DeprecationWarning,
            )
            del kwargs["policy_update"]

        sha1s = [
            hashutil.hash_to_bytes(id_) if isinstance(id_, str) else id_ for id_ in ids
        ]
        results = []
        summary: Dict = {"status": "uneventful"}
        try:
            for sha1 in sha1s:
                sentry_sdk.set_tag(
                    "swh-indexer-content-sha1", hashutil.hash_to_hex(sha1)
                )
                try:
                    raw_content = self.objstorage.get(sha1)
                except ObjNotFoundError:
                    self.log.warning(
                        "Content %s not found in objstorage"
                        % hashutil.hash_to_hex(sha1)
                    )
                    continue
                res = self.index(sha1, raw_content, **kwargs)
                if res:  # If no results, skip it
                    results.extend(res)
                    summary["status"] = "eventful"
            summary = self.persist_index_computations(results)
            self.results = results
        except Exception:
            if not self.catch_exceptions:
                raise
            self.log.exception("Problem when reading contents metadata.")
            sentry_sdk.capture_exception()
            summary["status"] = "failed"
        else:
            # Reset tag after we finished processing the given content
            sentry_sdk.set_tag("swh-indexer-content-sha1", "")
        return summary


class OriginIndexer(BaseIndexer[str, None, TResult], Generic[TResult]):
    """An object type indexer, inherits from the :class:`BaseIndexer` and
    implements Origin indexing using the run method

    Note: the :class:`OriginIndexer` is not an instantiable object.
    To use it in another context one should inherit from this class
    and override the methods mentioned in the :class:`BaseIndexer`
    class.

    """

    def run(self, origin_urls: List[str], **kwargs) -> Dict:
        """Given a list of origin urls:

        - retrieve origins from storage
        - execute the indexing computations
        - store the results

        Args:
            origin_urls: list of origin urls.
            **kwargs: passed to the `index` method

        """
        if "policy_update" in kwargs:
            warnings.warn(
                "'policy_update' argument is deprecated and ignored.",
                DeprecationWarning,
            )
            del kwargs["policy_update"]

        origins = [{"url": url} for url in origin_urls]

        return self.process_journal_objects({"origin": origins})

    def process_journal_objects(self, objects: ObjectsDict) -> Dict:
        """Worker function for ``JournalClient``."""
        origins = [
            Origin(url=status["origin"])
            for status in objects.get("origin_visit_status", [])
            if status["status"] == "full"
        ] + [Origin(url=origin["url"]) for origin in objects.get("origin", [])]

        summary: Dict[str, Any] = {"status": "uneventful"}
        try:
            results = self.index_list(
                origins,
                # no need to check they exist, as we just received either an origin
                # or visit status; which cannot be created by swh-storage unless
                # the origin already exists
                check_origin_known=False,
            )
        except Exception:
            if not self.catch_exceptions:
                raise
            self.log.exception("Problem when processing origins")
            sentry_sdk.capture_exception()
            summary["status"] = "failed"
            return summary

        summary_persist = self.persist_index_computations(results)
        self.results = results
        if summary_persist:
            for value in summary_persist.values():
                if value > 0:
                    summary["status"] = "eventful"
            summary.update(summary_persist)
        return summary

    def index_list(self, origins: List[Origin], **kwargs) -> List[TResult]:
        results = []
        for origin in origins:
            sentry_sdk.set_tag("swh-indexer-origin-url", origin.url)
            results.extend(self.index(origin.url, **kwargs))
        sentry_sdk.set_tag("swh-indexer-origin-url", "")
        return results


class DirectoryIndexer(BaseIndexer[Sha1Git, Directory, TResult], Generic[TResult]):
    """An object type indexer, inherits from the :class:`BaseIndexer` and
    implements Directory indexing using the run method

    Note: the :class:`DirectoryIndexer` is not an instantiable object.
    To use it in another context one should inherit from this class
    and override the methods mentioned in the :class:`BaseIndexer`
    class.

    """

    def run(self, ids: List[Sha1Git], **kwargs) -> Dict:
        """Given a list of sha1_gits:

        - retrieve directories from storage
        - execute the indexing computations
        - store the results

        Args:
            ids: sha1_git's identifier list

        """
        if "policy_update" in kwargs:
            warnings.warn(
                "'policy_update' argument is deprecated and ignored.",
                DeprecationWarning,
            )
            del kwargs["policy_update"]

        directory_ids = [
            hashutil.hash_to_bytes(id_) if isinstance(id_, str) else id_ for id_ in ids
        ]

        return self._process_directories([(dir_id, None) for dir_id in directory_ids])

    def process_journal_objects(self, objects: ObjectsDict) -> Dict:
        """Worker function for ``JournalClient``."""
        return self._process_directories(
            [
                (dir_["id"], Directory.from_dict(dir_))
                for dir_ in objects.get("directory", [])
            ]
        )

    def _process_directories(
        self,
        directories: Union[List[Tuple[Sha1Git, Directory]], List[Tuple[Sha1Git, None]]],
    ) -> Dict:
        summary: Dict[str, Any] = {"status": "uneventful"}
        results = []

        # TODO: fetch raw_manifest when useful?

        for dir_id, dir_ in directories:
            swhid = f"swh:1:dir:{hashutil.hash_to_hex(dir_id)}"
            sentry_sdk.set_tag("swh-indexer-directory-swhid", swhid)
            try:
                results.extend(self.index(dir_id, dir_))
            except Exception:
                if not self.catch_exceptions:
                    raise
                self.log.exception("Problem when processing directory")
                sentry_sdk.capture_exception()
                summary["status"] = "failed"
            else:
                sentry_sdk.set_tag("swh-indexer-directory-swhid", "")

        summary_persist = self.persist_index_computations(results)
        if summary_persist:
            for value in summary_persist.values():
                if value > 0:
                    summary["status"] = "eventful"
            summary.update(summary_persist)
        self.results = results
        return summary
