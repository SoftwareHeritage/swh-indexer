# Copyright (C) 2023-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import calendar
import collections
import json
import secrets
import sys
from typing import Any, Dict, List, Optional

import iso8601
from pybtex.database import Entry, Person
from pybtex.database.output.bibtex import Writer
from pybtex.plugin import register_plugin

from swh.indexer.citation.codemeta_data import CodeMetaData, CodeMetaPerson
from swh.indexer.citation.exceptions import CitationError
from swh.indexer.namespaces import SPDX_LICENSES
from swh.model.swhids import ObjectType

MACRO_PREFIX = "macro" + secrets.token_urlsafe(16).replace("_", "")


class BibTeXWithMacroWriter(Writer):
    def quote(self, s):
        r"""
        >>> w = BibTeXWithMacroWriter()
        >>> print(w.quote(f'{MACRO_PREFIX}:jan'))
        jan
        """

        if s.startswith(f"{MACRO_PREFIX}:"):
            return s[len(MACRO_PREFIX) + 1 :]
        return super().quote(s)


register_plugin("pybtex.database.output", "bibtex_with_macro", BibTeXWithMacroWriter)


def codemeta_person_to_pybtex_person(person: CodeMetaPerson) -> Optional[Person]:
    pybtex_person = Person()
    for name in person.names:
        if person.is_organization:
            # prevent interpreting the name as "Firstname Lastname" and reformatting
            # it to "Lastname, Firstname"
            pybtex_person.last_names.append(name)
        else:
            pybtex_person = Person(name)

    for given_name in person.given_names:
        pybtex_person.first_names.append(given_name)

    for family_name in person.family_names:
        pybtex_person.last_names.append(family_name)

    if not str(pybtex_person):
        return None

    return pybtex_person


def codemeta_data_to_bibtex(
    codemeta_data: CodeMetaData,
) -> str:
    swhid = codemeta_data.swhid
    persons: Dict[str, List[Person]] = collections.defaultdict(list)
    fields: Dict[str, Any] = {}

    if codemeta_data.description:
        fields["abstract"] = codemeta_data.description

    for author in codemeta_data.author or []:
        pybtex_person = codemeta_person_to_pybtex_person(author)
        if pybtex_person and pybtex_person not in persons["author"]:
            persons["author"].append(pybtex_person)

    for affiliation in codemeta_data.affiliation or []:
        pybtex_person = codemeta_person_to_pybtex_person(affiliation)
        if pybtex_person and pybtex_person not in persons["organization"]:
            persons["organization"].append(pybtex_person)

    date = (
        codemeta_data.datePublished
        or codemeta_data.dateCreated
        or codemeta_data.dateModified
    )
    if date:
        fields["date"] = date
        try:
            parsed_date = iso8601.parse_date(date)
            fields["year"] = str(parsed_date.year)
            fields["month"] = (
                f"{MACRO_PREFIX}:{calendar.month_abbr[parsed_date.month].lower()}"
            )
        except iso8601.ParseError:
            pass

    entry_key = None
    for identifier in codemeta_data.identifier or []:
        if entry_key is None and "/" not in identifier:
            # Avoid URLs
            entry_key = identifier
        if identifier.startswith("https://doi.org/"):
            fields["doi"] = identifier
        if identifier.startswith("hal-"):
            fields["hal_id"] = identifier

    for editor in codemeta_data.editor or []:
        pybtex_person = codemeta_person_to_pybtex_person(editor)
        if pybtex_person and pybtex_person not in persons["editor"]:
            persons["editor"].append(pybtex_person)

    if codemeta_data.downloadUrl:
        fields["file"] = codemeta_data.downloadUrl

    # license (represented by "Person" as it's the only way to make pybtex format
    # them as a list)
    for license in codemeta_data.license or []:
        if license.startswith(str(SPDX_LICENSES)):
            license_name = license[len(str(SPDX_LICENSES)) :]
            if license_name.endswith(".html"):
                license_name = license_name[:-5]
            persons["license"].append(Person(last=license_name))

    for publisher in codemeta_data.publisher or []:
        pybtex_person = codemeta_person_to_pybtex_person(publisher)
        if pybtex_person and pybtex_person not in persons["publisher"]:
            persons["publisher"].append(pybtex_person)

    if codemeta_data.codeRepository:
        fields["repository"] = codemeta_data.codeRepository

    if codemeta_data.name:
        fields["title"] = codemeta_data.name

    if codemeta_data.url:
        fields["url"] = codemeta_data.url

    version = codemeta_data.softwareVersion or codemeta_data.version
    if version:
        fields["version"] = version

    if not fields:
        raise CitationError(
            "No BibTex fields could be extracted from citation metadata file "
            "(codemeta.json or citation.cff), please check its content is valid."
        )

    # entry_type
    if swhid:
        fields["swhid"] = str(swhid)
        if swhid.object_type == ObjectType.SNAPSHOT:
            entry_type = "software"
        elif swhid.object_type == ObjectType.CONTENT:
            entry_type = "codefragment"
        else:
            entry_type = "softwareversion"

        if entry_key is None:
            entry_key = f"swh-{swhid.object_type.value}-{swhid.object_id.hex()[:7]}"
            if swhid.lines:
                line_start, line_end = swhid.lines
                if line_start:
                    entry_key += f"-L{line_start}"
                if line_end:
                    entry_key += f"-L{line_end}"
    elif "version" in fields:
        entry_type = "softwareversion"
    else:
        entry_type = "software"

    entry = Entry(
        entry_type,
        persons=persons,
        fields=fields,
    )

    entry.key = entry_key or "REPLACEME"
    return entry.to_string(bib_format="bibtex_with_macro")


if __name__ == "__main__":
    from swh.indexer.citation import CitationFormat, codemeta_to_citation

    for filename in sys.argv[1:]:
        if filename == "-":
            print(codemeta_to_citation(json.load(sys.stdin), CitationFormat.BIBTEX))
        else:
            with open(filename) as f:
                print(codemeta_to_citation(json.load(f), CitationFormat.BIBTEX))
