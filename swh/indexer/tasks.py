# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from celery import current_app as app

from .mimetype import MimetypeIndexer, MimetypeRangeIndexer
from .language import LanguageIndexer
from .ctags import CtagsIndexer
from .fossology_license import (
    FossologyLicenseIndexer, FossologyLicenseRangeIndexer
)
from .rehash import RecomputeChecksums
from .metadata import (
    RevisionMetadataIndexer, OriginMetadataIndexer
)
from .origin_head import OriginHeadIndexer


@app.task(name=__name__ + '.RevisionMetadata')
def revision_metadata(*args, **kwargs):
    results = RevisionMetadataIndexer().run(*args, **kwargs)
    return getattr(results, 'results', results)


@app.task(name=__name__ + '.OriginMetadata')
def origin_metadata(*args, **kwargs):
    results = OriginMetadataIndexer().run(*args, **kwargs)
    return getattr(results, 'results', results)


@app.task(name=__name__ + '.OriginHead')
def origin_head(*args, **kwargs):
    results = OriginHeadIndexer().run(*args, **kwargs)
    return getattr(results, 'results', results)


@app.task(name=__name__ + '.ContentLanguage')
def content_language(*args, **kwargs):
    results = LanguageIndexer().run(*args, **kwargs)
    return getattr(results, 'results', results)


@app.task(name=__name__ + '.Ctags')
def ctags(*args, **kwargs):
    results = CtagsIndexer().run(*args, **kwargs)
    return getattr(results, 'results', results)


@app.task(name=__name__ + '.ContentFossologyLicense')
def fossology_license(*args, **kwargs):
    results = FossologyLicenseIndexer().run(*args, **kwargs)
    return getattr(results, 'results', results)


@app.task(name=__name__ + '.RecomputeChecksums')
def recompute_checksums(*args, **kwargs):
    results = RecomputeChecksums().run(*args, **kwargs)
    return getattr(results, 'results', results)


@app.task(name=__name__ + '.ContentMimetype')
def mimetype(*args, **kwargs):
    results = MimetypeIndexer().run(*args, **kwargs)
    return {'status': 'eventful' if results else 'uneventful'}


@app.task(name=__name__ + '.ContentRangeMimetype')
def range_mimetype(*args, **kwargs):
    results = MimetypeRangeIndexer(*args, **kwargs)
    return {'status': 'eventful' if results else 'uneventful'}


@app.task(name=__name__ + '.ContentRangeFossologyLicense')
def range_license(*args, **kwargs):
    results = FossologyLicenseRangeIndexer(*args, **kwargs)
    return {'status': 'eventful' if results else 'uneventful'}
