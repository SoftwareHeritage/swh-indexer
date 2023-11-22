# Copyright (C) 2016-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from celery import shared_task

from .fossology_license import FossologyLicenseIndexer
from .metadata import OriginMetadataIndexer
from .mimetype import MimetypeIndexer
from .rehash import RecomputeChecksums


@shared_task(name=__name__ + ".OriginMetadata")
def index_origin_metadata(*args, **kwargs):
    """Origin Metadata indexer task"""
    return OriginMetadataIndexer().run(*args, **kwargs)


@shared_task(name=__name__ + ".ContentFossologyLicense")
def index_fossology_license(*args, **kwargs):
    """Fossology license indexer task"""
    return FossologyLicenseIndexer().run(*args, **kwargs)


@shared_task(name=__name__ + ".RecomputeChecksums")
def recompute_checksums(*args, **kwargs):
    """Recompute checksums indexer task"""
    return RecomputeChecksums().run(*args, **kwargs)


@shared_task(name=__name__ + ".ContentMimetype")
def index_mimetype(*args, **kwargs):
    """Mimetype indexer task"""
    return MimetypeIndexer().run(*args, **kwargs)
