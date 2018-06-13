# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.core.api import SWHRemoteAPI

from swh.storage.exc import StorageAPIError


class RemoteStorage(SWHRemoteAPI):
    """Proxy to a remote storage API"""
    def __init__(self, url, timeout=None):
        super().__init__(
            api_exception=StorageAPIError, url=url, timeout=timeout)

    def check_config(self, *, check_write):
        return self.post('check_config', {'check_write': check_write})

    def content_mimetype_add(self, mimetypes, conflict_update=False):
        return self.post('content_mimetype/add', {
            'mimetypes': mimetypes,
            'conflict_update': conflict_update,
        })

    def content_mimetype_missing(self, mimetypes):
        return self.post('content_mimetype/missing', {'mimetypes': mimetypes})

    def content_mimetype_get(self, ids):
        return self.post('content_mimetype', {'ids': ids})

    def content_language_add(self, languages, conflict_update=False):
        return self.post('content_language/add', {
            'languages': languages,
            'conflict_update': conflict_update,
        })

    def content_language_missing(self, languages):
        return self.post('content_language/missing', {'languages': languages})

    def content_language_get(self, ids):
        return self.post('content_language', {'ids': ids})

    def content_ctags_add(self, ctags, conflict_update=False):
        return self.post('content/ctags/add', {
            'ctags': ctags,
            'conflict_update': conflict_update,
        })

    def content_ctags_missing(self, ctags):
        return self.post('content/ctags/missing', {'ctags': ctags})

    def content_ctags_get(self, ids):
        return self.post('content/ctags', {'ids': ids})

    def content_ctags_search(self, expression, limit=10, last_sha1=None):
        return self.post('content/ctags/search', {
            'expression': expression,
            'limit': limit,
            'last_sha1': last_sha1,
        })

    def content_fossology_license_add(self, licenses, conflict_update=False):
        return self.post('content/fossology_license/add', {
            'licenses': licenses,
            'conflict_update': conflict_update,
        })

    def content_fossology_license_get(self, ids):
        return self.post('content/fossology_license', {'ids': ids})

    def content_metadata_add(self, metadata, conflict_update=False):
        return self.post('content_metadata/add', {
            'metadata': metadata,
            'conflict_update': conflict_update,
        })

    def content_metadata_missing(self, metadata):
        return self.post('content_metadata/missing', {'metadata': metadata})

    def content_metadata_get(self, ids):
        return self.post('content_metadata', {'ids': ids})

    def revision_metadata_add(self, metadata, conflict_update=False):
        return self.post('revision_metadata/add', {
            'metadata': metadata,
            'conflict_update': conflict_update,
        })

    def revision_metadata_missing(self, metadata):
        return self.post('revision_metadata/missing', {'metadata': metadata})

    def revision_metadata_get(self, ids):
        return self.post('revision_metadata', {'ids': ids})

    def indexer_configuration_add(self, tools):
        return self.post('indexer_configuration/add', {'tools': tools})

    def indexer_configuration_get(self, tool):
        return self.post('indexer_configuration/data', {'tool': tool})
