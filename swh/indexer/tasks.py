# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from celery import shared_task

from .mimetype import MimetypeIndexer, MimetypePartitionIndexer
from .ctags import CtagsIndexer
from .fossology_license import FossologyLicenseIndexer, FossologyLicensePartitionIndexer
from .rehash import RecomputeChecksums
from .metadata import OriginMetadataIndexer


@shared_task(name=__name__ + ".OriginMetadata")
def origin_metadata(*args, **kwargs):
    return OriginMetadataIndexer().run(*args, **kwargs)


@shared_task(name=__name__ + ".Ctags")
def ctags(*args, **kwargs):
    return CtagsIndexer().run(*args, **kwargs)


@shared_task(name=__name__ + ".ContentFossologyLicense")
def fossology_license(*args, **kwargs):
    return FossologyLicenseIndexer().run(*args, **kwargs)


@shared_task(name=__name__ + ".RecomputeChecksums")
def recompute_checksums(*args, **kwargs):
    return RecomputeChecksums().run(*args, **kwargs)


@shared_task(name=__name__ + ".ContentMimetype")
def mimetype(*args, **kwargs):
    return MimetypeIndexer().run(*args, **kwargs)


@shared_task(name=__name__ + ".ContentMimetypePartition")
def mimetype_partition(*args, **kwargs):
    return MimetypePartitionIndexer().run(*args, **kwargs)


@shared_task(name=__name__ + ".ContentFossologyLicensePartition")
def license_partition(*args, **kwargs):
    return FossologyLicensePartitionIndexer().run(*args, **kwargs)
