# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import json
import sys
from typing import Any, Dict, List
import uuid

from pybtex.database import Entry, Person
import rdflib

from swh.indexer.codemeta import compact, expand
from swh.indexer.namespaces import SCHEMA, SPDX_LICENSES

TMP_ROOT_URI_PREFIX = "https://www.softwareheritage.org/schema/2022/indexer/tmp-node/"


def codemeta_to_bibtex(doc: Dict[str, Any]) -> str:
    doc = compact(doc, False)

    identifiers = []

    if "id" in doc:
        identifiers.append(doc["id"])
    else:
        doc["id"] = f"_:{uuid.uuid4()}"

    id_: rdflib.term.Node
    if doc["id"].startswith("_:"):
        id_ = rdflib.term.BNode(doc["id"][2:])
    else:
        # using a base in case the id is not an absolute URI
        id_ = rdflib.term.URIRef(doc["id"], base=TMP_ROOT_URI_PREFIX)
        doc["id"] = str(id_)

    # workaround for https://github.com/codemeta/codemeta/pull/322
    if "identifier" in doc:
        if isinstance(doc["identifier"], list):
            for identifier in doc["identifier"]:
                if isinstance(identifier, str) and "/" not in identifier:
                    identifiers.append(identifier)
        elif isinstance(doc["identifier"], str) and "/" not in doc["identifier"]:
            identifiers.append(doc["identifier"])

    doc = expand(doc)
    g = rdflib.Graph().parse(data=json.dumps(doc), format="json-ld")
    persons: Dict[str, List[Person]] = collections.defaultdict(list)
    fields: Dict[str, Any] = {}

    def add_person(persons: List[Person], person_id: rdflib.term.Node) -> None:
        for _, _, name in g.triples((person_id, SCHEMA.name, None)):
            if (person_id, rdflib.RDF.type, SCHEMA.Organization) in g:
                # prevent interpreting the name as "Firstname Lastname" and reformatting
                # it to "Lastname, Firstname"
                person = Person(last=name)
            else:
                person = Person(name)
            if person not in persons:
                persons.append(person)

    def add_affiliations(person: rdflib.term.Node) -> None:
        for _, _, organization in g.triples((person, SCHEMA.affiliation, None)):
            add_person(persons["organization"], organization)

    # abstract
    for _, _, description in g.triples((id_, SCHEMA.description, None)):
        fields["abstract"] = description
        break

    # authors, which are an ordered list
    for _, _, author_list in g.triples((id_, SCHEMA.author, None)):
        for author in rdflib.collection.Collection(g, author_list):
            add_person(persons["author"], author)
            add_affiliations(author)

    # date
    for _, _, date in g.triples((id_, SCHEMA.datePublished, None)):
        fields["date"] = date
        break
    else:
        for _, _, date in g.triples((id_, SCHEMA.dateCreated, None)):
            fields["date"] = date
            break
        else:
            for _, _, date in g.triples((id_, SCHEMA.dateModified, None)):
                fields["date"] = date
                break
    if "date" in fields:
        (fields["year"], fields["month"], _) = fields["date"].split("-")

    # identifier, doi, hal_id
    entry_key = None
    for _, _, identifier in g.triples((id_, SCHEMA.identifier, None)):
        identifiers.append(identifier)
    for identifier in identifiers:
        if entry_key is None and "/" not in identifier:
            # Avoid URLs
            entry_key = identifier
        if identifier.startswith("https://doi.org/"):
            fields["doi"] = identifier
        if identifier.startswith("hal-"):
            fields["hal_id"] = identifier

    # editor
    for _, _, editor in g.triples((id_, SCHEMA.editor, None)):
        add_person(persons["editor"], editor)
        add_affiliations(editor)

    # file
    for _, _, download_url in g.triples((id_, SCHEMA.downloadUrl, None)):
        fields["file"] = download_url
        break

    # license (represented by "Person" as it's the only way to make pybtex format
    # them as a list)
    for _, _, license in g.triples((id_, SCHEMA.license, None)):
        if license is None:
            continue
        license_ = str(license)
        if license_.startswith(str(SPDX_LICENSES)):
            license_ = license_[len(str(SPDX_LICENSES)) :]
            if license_.endswith(".html"):
                license_ = license_[:-5]
            persons["license"].append(Person(last=license_))

    # publisher
    for _, _, publisher in g.triples((id_, SCHEMA.publisher, None)):
        add_person(persons["publisher"], publisher)
        add_affiliations(publisher)

    # repository
    for _, _, code_repository in g.triples((id_, SCHEMA.codeRepository, None)):
        fields["repository"] = code_repository
        break

    # title
    for _, _, name in g.triples((id_, SCHEMA.name, None)):
        fields["title"] = name
        break

    # url
    for _, _, name in g.triples((id_, SCHEMA.url, None)):
        fields["url"] = name
        break

    # version
    for _, _, version in g.triples((id_, SCHEMA.softwareVersion, None)):
        fields["version"] = version
        break
    else:
        for _, _, version in g.triples((id_, SCHEMA.version, None)):
            fields["version"] = version

    entry_type = "softwareversion" if "version" in fields else "software"
    entry = Entry(
        entry_type,
        persons=persons,
        fields=fields,
    )

    entry.key = entry_key or "REPLACEME"
    return entry.to_string(bib_format="bibtex")


if __name__ == "__main__":
    for filename in sys.argv[1:]:
        if filename == "-":
            print(codemeta_to_bibtex(json.load(sys.stdin)))
        else:
            with open(filename) as f:
                print(codemeta_to_bibtex(json.load(f)))
