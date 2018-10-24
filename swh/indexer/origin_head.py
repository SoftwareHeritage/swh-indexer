# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re
import click
import logging

from celery import chain

from swh.indexer.indexer import OriginIndexer
from swh.indexer.tasks import OriginMetadata, RevisionMetadata


class OriginHeadIndexer(OriginIndexer):
    """Origin-level indexer.

    This indexer is in charge of looking up the revision that acts as the
    "head" of an origin.

    In git, this is usually the commit pointed to by the 'master' branch."""

    ADDITIONAL_CONFIG = {
        'tools': ('dict', {
            'name': 'origin-metadata',
            'version': '0.0.1',
            'configuration': {},
        }),
    }

    CONFIG_BASE_FILENAME = 'indexer/origin_head'

    revision_metadata_task = RevisionMetadata()
    origin_intrinsic_metadata_task = OriginMetadata()

    def filter(self, ids):
        yield from ids

    def persist_index_computations(self, results, policy_update):
        """Do nothing. The indexer's results are not persistent, they
        should only be piped to another indexer via the orchestrator."""
        pass

    def next_step(self, results):
        """Once the head is found, call the RevisionMetadataIndexer
        on these revisions, then call the OriginMetadataIndexer with
        both the origin_id and the revision metadata, so it can copy the
        revision metadata to the origin's metadata.

        Args:
            results (Iterable[dict]): Iterable of return values from `index`.

        """
        if self.revision_metadata_task is None and \
                self.origin_intrinsic_metadata_task is None:
            return
        assert self.revision_metadata_task is not None
        assert self.origin_intrinsic_metadata_task is not None
        return chain(
                self.revision_metadata_task.s(
                    ids=[res['revision_id'] for res in results],
                    policy_update='update-dups'),
                self.origin_intrinsic_metadata_task.s(
                    origin_head_pairs=results,
                    policy_update='update-dups'),
                )()

    # Dispatch

    def index(self, origin):
        origin_id = origin['id']
        latest_snapshot = self.storage.snapshot_get_latest(origin_id)
        method = getattr(self, '_try_get_%s_head' % origin['type'], None)
        if method is None:
            method = self._try_get_head_generic
        rev_id = method(latest_snapshot)
        if rev_id is None:
            return None
        result = {
                'origin_id': origin_id,
                'revision_id': rev_id,
                }
        return result

    # VCSs

    def _try_get_vcs_head(self, snapshot):
        try:
            if isinstance(snapshot, dict):
                branches = snapshot['branches']
                if branches[b'HEAD']['target_type'] == 'revision':
                    return branches[b'HEAD']['target']
        except KeyError:
            return None

    _try_get_hg_head = _try_get_git_head = _try_get_vcs_head

    # Tarballs

    _archive_filename_re = re.compile(
            b'^'
            b'(?P<pkgname>.*)[-_]'
            b'(?P<version>[0-9]+(\.[0-9])*)'
            b'(?P<preversion>[-+][a-zA-Z0-9.~]+?)?'
            b'(?P<extension>(\.[a-zA-Z0-9]+)+)'
            b'$')

    @classmethod
    def _parse_version(cls, filename):
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
            return (float('-infinity'),)
        version = [int(n) for n in res.group('version').decode().split('.')]
        if res.group('preversion') is None:
            version.append(0)
        else:
            preversion = res.group('preversion').decode()
            if preversion.startswith('-'):
                version.append(-1)
                version.append(preversion[1:])
            elif preversion.startswith('+'):
                version.append(1)
                version.append(preversion[1:])
            else:
                assert False, res.group('preversion')
        return tuple(version)

    def _try_get_ftp_head(self, snapshot):
        archive_names = list(snapshot['branches'])
        max_archive_name = max(archive_names, key=self._parse_version)
        r = self._try_resolve_target(snapshot['branches'], max_archive_name)
        return r

    # Generic

    def _try_get_head_generic(self, snapshot):
        # Works on 'deposit', 'svn', and 'pypi'.
        try:
            if isinstance(snapshot, dict):
                branches = snapshot['branches']
        except KeyError:
            return None
        else:
            return (
                    self._try_resolve_target(branches, b'HEAD') or
                    self._try_resolve_target(branches, b'master')
                    )

    def _try_resolve_target(self, branches, target_name):
        try:
            target = branches[target_name]
            while target['target_type'] == 'alias':
                target = branches[target['target']]
            if target['target_type'] == 'revision':
                return target['target']
            elif target['target_type'] == 'content':
                return None  # TODO
            elif target['target_type'] == 'directory':
                return None  # TODO
            elif target['target_type'] == 'release':
                return None  # TODO
            else:
                assert False
        except KeyError:
            return None


@click.command()
@click.option('--origins', '-i',
              help='Origins to lookup, in the "type+url" format',
              multiple=True)
def main(origins):
    rev_metadata_indexer = OriginHeadIndexer()
    rev_metadata_indexer.run(origins, 'update-dups', parse_ids=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
