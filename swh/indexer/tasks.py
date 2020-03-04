# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from celery import current_app as app

from .mimetype import MimetypeIndexer, MimetypeRangeIndexer
from .ctags import CtagsIndexer
from .fossology_license import (
    FossologyLicenseIndexer, FossologyLicenseRangeIndexer
)
from .rehash import RecomputeChecksums
from .metadata import OriginMetadataIndexer


@app.task(name=__name__ + '.OriginMetadata')
def origin_metadata(*args, **kwargs):
    return OriginMetadataIndexer().run(*args, **kwargs)


@app.task(name=__name__ + '.Ctags')
def ctags(*args, **kwargs):
    return CtagsIndexer().run(*args, **kwargs)


@app.task(name=__name__ + '.ContentFossologyLicense')
def fossology_license(*args, **kwargs):
    return FossologyLicenseIndexer().run(*args, **kwargs)


@app.task(name=__name__ + '.RecomputeChecksums')
def recompute_checksums(*args, **kwargs):
    return RecomputeChecksums().run(*args, **kwargs)


@app.task(name=__name__ + '.ContentMimetype')
def mimetype(*args, **kwargs):
    return MimetypeIndexer().run(*args, **kwargs)


@app.task(name=__name__ + '.ContentRangeMimetype')
def range_mimetype(*args, **kwargs):
    return MimetypeRangeIndexer().run(*args, **kwargs)


@app.task(name=__name__ + '.ContentRangeFossologyLicense')
def range_license(*args, **kwargs):
    return FossologyLicenseRangeIndexer().run(*args, **kwargs)
