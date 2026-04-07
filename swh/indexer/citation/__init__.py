# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from enum import Enum
from typing import Any, Callable, Dict, Optional

from pyld.jsonld import JsonLdError

from swh.indexer.citation.bibtex import codemeta_data_to_bibtex
from swh.indexer.citation.codemeta_data import extract_codemeta_data
from swh.indexer.citation.csl import codemeta_data_to_csl
from swh.indexer.citation.exceptions import CitationError
from swh.model.swhids import QualifiedSWHID


class CitationFormat(Enum):
    BIBTEX = "BibTeX"
    CSL = "CSL JSON"


CITATION_FORMAT_CONVERTER: dict[CitationFormat, Callable[..., str]] = {
    CitationFormat.BIBTEX: codemeta_data_to_bibtex,
    CitationFormat.CSL: codemeta_data_to_csl,
}


def codemeta_to_citation(
    doc: Dict[str, Any],
    format: CitationFormat,
    swhid: Optional[QualifiedSWHID] = None,
    force_codemeta_context: bool = False,
    resolve_unknown_context_url: bool = False,
) -> str:
    """Generate a citation from a parsed ``codemeta.json`` file.

    Args:
        doc: parsed ``codemeta.json`` file
        format: citation format to generate
        swhid: optional SWHID to embed in the citation
        force_codemeta_context: if :const:`True`, the ``@context`` field in the
            JSON-LD document will be set to the CodeMeta v3.0 one, this can be used
            to ensure citation can be generated when strict JSON-LD parsing failed
        resolve_unknown_context_url: if const:`True` unknown JSON-LD context URL
            will be fetched using ``requests`` instead of raising an exception,
            :const:`False` by default as it can lead sending requests to arbitrary
            URLs so use with caution

    Returns:
        A citation string in the requested format.

    Raises:
        CitationError: when citation could not be generated
    """
    try:
        codemeta_data = extract_codemeta_data(
            doc,
            swhid=swhid,
            force_codemeta_context=force_codemeta_context,
            resolve_unknown_context_url=resolve_unknown_context_url,
        )
    except JsonLdError as e:
        cause = e.__cause__
        while cause.__cause__ is not None:
            cause = cause.__cause__
        raise CitationError(str(cause))

    return CITATION_FORMAT_CONVERTER[format](codemeta_data)
