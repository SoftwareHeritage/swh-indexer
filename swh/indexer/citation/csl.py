# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import json

import iso8601

from swh.indexer.citation.codemeta_data import CodeMetaData, CodeMetaPerson
from swh.indexer.namespaces import SPDX_LICENSES


def codemeta_person_to_csl(person: CodeMetaPerson) -> dict[str, str]:
    csl_person: dict[str, str] = {}
    if person.is_organization:
        csl_person["family"] = "".join(person.names)

    if person.given_names:
        csl_person["given"] = "".join(person.given_names)

    if person.family_names:
        csl_person["family"] = csl_person.get("family", "") + "".join(
            person.family_names
        )

    if not person.is_organization and person.names and not csl_person:
        # CSL expects separated name parts; fall back to a simple split when
        # codemeta provides only `name`.
        full_name = person.names[-1].strip()
        name_parts = full_name.split()
        if len(name_parts) == 1:
            csl_person["family"] = name_parts[0]
        elif name_parts:
            csl_person["given"] = " ".join(name_parts[:-1])
            csl_person["family"] = name_parts[-1]

    return csl_person


def codemeta_data_to_csl(
    codemeta_data: CodeMetaData,
) -> str:
    swhid = codemeta_data.swhid
    csl: dict = {"type": "software"}

    if codemeta_data.name:
        csl["title"] = codemeta_data.name

    if codemeta_data.description:
        csl["abstract"] = codemeta_data.description

    authors: list[dict[str, str]] = []
    for author in codemeta_data.author or []:
        csl_author = codemeta_person_to_csl(author)
        if csl_author and csl_author not in authors:
            authors.append(csl_author)
    if authors:
        csl["author"] = authors

    date = (
        codemeta_data.datePublished
        or codemeta_data.dateCreated
        or codemeta_data.dateModified
    )
    if date:
        try:
            parsed = iso8601.parse_date(date)
            csl["issued"] = {"date-parts": [[parsed.year, parsed.month, parsed.day]]}
        except iso8601.ParseError:
            pass

    for identifier in codemeta_data.identifier or []:
        if identifier.startswith("https://doi.org/"):
            csl["DOI"] = identifier

    if swhid:
        csl["id"] = str(swhid)

    if codemeta_data.publisher:
        p = codemeta_data.publisher[0]
        if p.names:
            publisher = "".join(p.names)
        else:
            given = "".join(p.given_names)
            family = "".join(p.family_names)
            publisher = f"{given} {family}" if given and family else given or family
        if publisher:
            csl["publisher"] = publisher

    if codemeta_data.codeRepository:
        csl["source"] = codemeta_data.codeRepository

    csl_url_candidates = (
        codemeta_data.url,
        codemeta_data.relatedLink,
        codemeta_data.downloadUrl,
        codemeta_data.installUrl,
    )
    for csl_url in csl_url_candidates:
        if csl_url is not None:
            csl["URL"] = csl_url
            break

    licenses = []
    for license in codemeta_data.license or []:
        if license.startswith(str(SPDX_LICENSES)):
            license_name = license[len(str(SPDX_LICENSES)) :]
            if license_name.endswith(".html"):
                license_name = license_name[:-5]
            licenses.append(license_name)
    if licenses:
        csl["license"] = " and ".join(licenses)

    version = codemeta_data.softwareVersion or codemeta_data.version
    if version:
        csl["version"] = version

    return json.dumps(csl, indent=2)
