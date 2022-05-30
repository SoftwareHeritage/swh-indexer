# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import itertools
import logging
from typing import Any, Dict, Generator, List, Optional, Tuple

import sentry_sdk

from swh.core import utils
from swh.core.config import load_from_envvar
from swh.model import hashutil
from swh.model.model import Content
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.factory import get_objstorage
from swh.storage import get_storage

DEFAULT_CONFIG: Dict[str, Any] = {
    "storage": {"cls": "memory"},
    "objstorage": {"cls": "memory"},
    # the set of checksums that should be computed.
    # Examples: 'sha1_git', 'blake2b512', 'blake2s256'
    "compute_checksums": [],
    # whether checksums that already exist in the DB should be
    # recomputed/updated or left untouched
    "recompute_checksums": False,
    # Number of contents to retrieve blobs at the same time
    "batch_size_retrieve_content": 10,
    # Number of contents to update at the same time
    "batch_size_update": 100,
}


class RecomputeChecksums:
    """Class in charge of (re)computing content's hashes.

    Hashes to compute are defined across 2 configuration options:

    compute_checksums ([str])
      list of hash algorithms that
      py:func:`swh.model.hashutil.MultiHash.from_data` function should
      be able to deal with. For variable-length checksums, a desired
      checksum length should also be provided. Their format is
      <algorithm's name>:<variable-length> e.g: blake2:512

    recompute_checksums (bool)
      a boolean to notify that we also want to recompute potential existing
      hashes specified in compute_checksums.  Default to False.

    """

    def __init__(self) -> None:
        self.config = load_from_envvar(DEFAULT_CONFIG)
        self.storage = get_storage(**self.config["storage"])
        self.objstorage = get_objstorage(**self.config["objstorage"])
        self.compute_checksums = self.config["compute_checksums"]
        self.recompute_checksums = self.config["recompute_checksums"]
        self.batch_size_retrieve_content = self.config["batch_size_retrieve_content"]
        self.batch_size_update = self.config["batch_size_update"]
        self.log = logging.getLogger("swh.indexer.rehash")

        if not self.compute_checksums:
            raise ValueError("Checksums list should not be empty.")

    def _read_content_ids(
        self, contents: List[Dict[str, Any]]
    ) -> Generator[bytes, Any, None]:
        """Read the content identifiers from the contents."""
        for c in contents:
            h = c["sha1"]
            if isinstance(h, str):
                h = hashutil.hash_to_bytes(h)

            yield h

    def get_new_contents_metadata(
        self, all_contents: List[Dict[str, Any]]
    ) -> Generator[Tuple[Dict[str, Any], List[Any]], Any, None]:
        """Retrieve raw contents and compute new checksums on the
           contents. Unknown or corrupted contents are skipped.

        Args:
            all_contents: List of contents as dictionary with
              the necessary primary keys

        Yields:
            tuple: tuple of (content to update, list of checksums computed)

        """
        content_ids = self._read_content_ids(all_contents)
        for contents in utils.grouper(content_ids, self.batch_size_retrieve_content):
            contents_iter = itertools.tee(contents, 2)
            try:
                sha1s = [s for s in contents_iter[0]]
                content_metadata: List[Optional[Content]] = self.storage.content_get(
                    sha1s
                )
            except Exception:
                self.log.exception("Problem when reading contents metadata.")
                sentry_sdk.capture_exception()
                continue

            for sha1, content_model in zip(sha1s, content_metadata):
                if not content_model:
                    continue
                content: Dict = content_model.to_dict()
                # Recompute checksums provided in compute_checksums options
                if self.recompute_checksums:
                    checksums_to_compute = list(self.compute_checksums)
                else:
                    # Compute checksums provided in compute_checksums
                    # options not already defined for that content
                    checksums_to_compute = [
                        h for h in self.compute_checksums if not content.get(h)
                    ]

                if not checksums_to_compute:  # Nothing to recompute
                    continue

                try:
                    raw_content = self.objstorage.get(sha1)
                except ObjNotFoundError:
                    self.log.warning("Content %s not found in objstorage!", sha1)
                    continue

                content_hashes = hashutil.MultiHash.from_data(
                    raw_content, hash_names=checksums_to_compute
                ).digest()
                content.update(content_hashes)
                yield content, checksums_to_compute

    def run(self, contents: List[Dict[str, Any]]) -> Dict:
        """Given a list of content:

          - (re)compute a given set of checksums on contents available in our
            object storage
          - update those contents with the new metadata

        Args:
            contents: contents as dictionary with necessary keys.
                key present in such dictionary should be the ones defined in
                the 'primary_key' option.

        Returns:
            A summary dict with key 'status', task' status and 'count' the
            number of updated contents.

        """
        status = "uneventful"
        count = 0
        for data in utils.grouper(
            self.get_new_contents_metadata(contents), self.batch_size_update
        ):

            groups: Dict[str, List[Any]] = defaultdict(list)
            for content, keys_to_update in data:
                keys_str = ",".join(keys_to_update)
                groups[keys_str].append(content)

            for keys_to_update, contents in groups.items():
                keys: List[str] = keys_to_update.split(",")
                try:
                    self.storage.content_update(contents, keys=keys)
                    count += len(contents)
                    status = "eventful"
                except Exception:
                    self.log.exception("Problem during update.")
                    sentry_sdk.capture_exception()
                    continue

        return {
            "status": status,
            "count": count,
        }
