# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import List, Tuple, Any, Dict, Union

import re
import click
import logging

from swh.indexer.indexer import OriginIndexer
from swh.model.model import SnapshotBranch, TargetType
from swh.storage.algos.origin import origin_get_latest_visit_status
from swh.storage.algos.snapshot import snapshot_get_all_branches


class OriginHeadIndexer(OriginIndexer):
    """Origin-level indexer.

    This indexer is in charge of looking up the revision that acts as the
    "head" of an origin.

    In git, this is usually the commit pointed to by the 'master' branch."""

    USE_TOOLS = False

    def persist_index_computations(
        self, results: Any, policy_update: str
    ) -> Dict[str, int]:
        """Do nothing. The indexer's results are not persistent, they
        should only be piped to another indexer."""
        return {}

    # Dispatch

    def index(self, origin_url):
        visit_and_status = origin_get_latest_visit_status(
            self.storage, origin_url, allowed_statuses=["full"], require_snapshot=True
        )
        if not visit_and_status:
            return None
        visit, visit_status = visit_and_status
        snapshot = snapshot_get_all_branches(self.storage, visit_status.snapshot)
        if snapshot is None:
            return None
        method = getattr(
            self, "_try_get_%s_head" % visit.type, self._try_get_head_generic
        )

        rev_id = method(snapshot.branches)
        if rev_id is not None:
            return {
                "origin_url": origin_url,
                "revision_id": rev_id,
            }

        # could not find a head revision
        return None

    # Tarballs

    _archive_filename_re = re.compile(
        rb"^"
        rb"(?P<pkgname>.*)[-_]"
        rb"(?P<version>[0-9]+(\.[0-9])*)"
        rb"(?P<preversion>[-+][a-zA-Z0-9.~]+?)?"
        rb"(?P<extension>(\.[a-zA-Z0-9]+)+)"
        rb"$"
    )

    @classmethod
    def _parse_version(cls: Any, filename: bytes) -> Tuple[Union[float, int], ...]:
        """Extracts the release version from an archive filename,
        to get an ordering whose maximum is likely to be the last
        version of the software

        >>> OriginHeadIndexer._parse_version(b'foo')
        (-inf,)
        >>> OriginHeadIndexer._parse_version(b'foo.tar.gz')
        (-inf,)
        >>> OriginHeadIndexer._parse_version(b'gnu-hello-0.0.1.tar.gz')
        (0, 0, 1, 0)
        >>> OriginHeadIndexer._parse_version(b'gnu-hello-0.0.1-beta2.tar.gz')
        (0, 0, 1, -1, 'beta2')
        >>> OriginHeadIndexer._parse_version(b'gnu-hello-0.0.1+foobar.tar.gz')
        (0, 0, 1, 1, 'foobar')
        """
        res = cls._archive_filename_re.match(filename)
        if res is None:
            return (float("-infinity"),)
        version = [int(n) for n in res.group("version").decode().split(".")]
        if res.group("preversion") is None:
            version.append(0)
        else:
            preversion = res.group("preversion").decode()
            if preversion.startswith("-"):
                version.append(-1)
                version.append(preversion[1:])
            elif preversion.startswith("+"):
                version.append(1)
                version.append(preversion[1:])
            else:
                assert False, res.group("preversion")
        return tuple(version)

    def _try_get_ftp_head(self, branches: Dict[bytes, SnapshotBranch]) -> Any:
        archive_names = list(branches)
        max_archive_name = max(archive_names, key=self._parse_version)
        r = self._try_resolve_target(branches, max_archive_name)
        return r

    # Generic

    def _try_get_head_generic(self, branches: Dict[bytes, SnapshotBranch]) -> Any:
        # Works on 'deposit', 'pypi', and VCSs.
        return self._try_resolve_target(branches, b"HEAD") or self._try_resolve_target(
            branches, b"master"
        )

    def _try_resolve_target(
        self, branches: Dict[bytes, SnapshotBranch], branch_name: bytes
    ) -> Any:
        try:
            branch = branches[branch_name]
            if branch is None:
                return None
            while branch.target_type == TargetType.ALIAS:
                branch = branches[branch.target]
                if branch is None:
                    return None

            if branch.target_type == TargetType.REVISION:
                return branch.target
            elif branch.target_type == TargetType.CONTENT:
                return None  # TODO
            elif branch.target_type == TargetType.DIRECTORY:
                return None  # TODO
            elif branch.target_type == TargetType.RELEASE:
                return None  # TODO
            else:
                assert False, branch
        except KeyError:
            return None


@click.command()
@click.option(
    "--origins", "-i", help='Origins to lookup, in the "type+url" format', multiple=True
)
def main(origins: List[str]) -> None:
    rev_metadata_indexer = OriginHeadIndexer()
    rev_metadata_indexer.run(origins)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
