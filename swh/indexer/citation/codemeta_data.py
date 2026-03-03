# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
from dataclasses import dataclass
import json
from typing import Any, Dict, Optional
import uuid

import rdflib

from swh.indexer.codemeta import CODEMETA_V3_CONTEXT_URL, compact, expand
from swh.indexer.namespaces import CODEMETA, RDF, SCHEMA
from swh.model.swhids import QualifiedSWHID

TMP_ROOT_URI_PREFIX = "https://www.softwareheritage.org/schema/2022/indexer/tmp-node/"
"""IRI used for skolemization.

See <https://www.w3.org/TR/rdf11-concepts/#section-skolemization>.
"""


@dataclass
class CodeMetaData:
    """Class to store data extracted from a codemeta.json"""

    id: Optional[str] = None
    swhid: Optional[QualifiedSWHID] = None
    address: Optional[str] = None
    affiliation: Optional[list["CodeMetaPerson"]] = None
    applicationCategory: Optional[str] = None
    applicationSubCategory: Optional[str] = None
    author: Optional[list["CodeMetaPerson"]] = None
    buildInstructions: Optional[str] = None
    citation: Optional[str] = None
    codeRepository: Optional[str] = None
    continuousIntegration: Optional[str] = None
    contributor: Optional[str] = None
    copyrightHolder: Optional[str] = None
    copyrightYear: Optional[str] = None
    dateCreated: Optional[str] = None
    dateModified: Optional[str] = None
    datePublished: Optional[str] = None
    description: Optional[str] = None
    developmentStatus: Optional[str] = None
    downloadUrl: Optional[str] = None
    editor: Optional[list["CodeMetaPerson"]] = None
    email: Optional[str] = None
    embargoEndDate: Optional[str] = None
    encoding: Optional[str] = None
    endDate: Optional[str] = None
    familyName: Optional[str] = None
    fileFormat: Optional[str] = None
    fileSize: Optional[str] = None
    funder: Optional[str] = None
    funding: Optional[str] = None
    givenName: Optional[str] = None
    hasPart: Optional[str] = None
    hasSourceCode: Optional[str] = None
    identifier: Optional[list[str]] = None
    installUrl: Optional[str] = None
    isAccessibleForFree: Optional[str] = None
    isPartOf: Optional[str] = None
    isSourceCodeOf: Optional[str] = None
    issueTracker: Optional[str] = None
    keywords: Optional[str] = None
    license: Optional[list[str]] = None
    maintainer: Optional[str] = None
    memoryRequirements: Optional[str] = None
    name: Optional[str] = None
    operatingSystem: Optional[list[str]] = None
    permissions: Optional[str] = None
    position: Optional[str] = None
    processorRequirements: Optional[str] = None
    producer: Optional[str] = None
    programmingLanguage: Optional[list[str]] = None
    provider: Optional[str] = None
    publisher: Optional[list["CodeMetaPerson"]] = None
    readme: Optional[str] = None
    referencePublication: Optional[str] = None
    relatedLink: Optional[str] = None
    releaseNotes: Optional[str] = None
    review: Optional[str] = None
    reviewAspect: Optional[str] = None
    reviewBody: Optional[str] = None
    roleName: Optional[str] = None
    runtimePlatform: Optional[str] = None
    sameAs: Optional[str] = None
    softwareHelp: Optional[str] = None
    softwareRequirements: Optional[str] = None
    softwareSuggestions: Optional[str] = None
    softwareVersion: Optional[str] = None
    sponsor: Optional[str] = None
    startDate: Optional[str] = None
    storageRequirements: Optional[str] = None
    supportingData: Optional[str] = None
    targetProduct: Optional[str] = None
    url: Optional[str] = None
    version: Optional[str] = None


@dataclass
class CodeMetaPerson:
    names: tuple[str, ...] = ()
    given_names: tuple[str, ...] = ()
    family_names: tuple[str, ...] = ()
    is_organization: bool = False


def rdf_str_values(
    graph: rdflib.Graph,
    subject: rdflib.term.Node,
    predicate: rdflib.term.URIRef,
) -> tuple[str, ...]:
    return tuple(
        str(object_) for _, _, object_ in graph.triples((subject, predicate, None))
    )


def rdf_first_str_value(
    graph: rdflib.Graph,
    subject: rdflib.term.Node,
    predicate: rdflib.term.URIRef,
) -> Optional[str]:
    for _, _, object_ in graph.triples((subject, predicate, None)):
        return str(object_)
    return None


def extract_person(
    graph: rdflib.Graph, person_id: rdflib.term.Node
) -> Optional[CodeMetaPerson]:
    names = rdf_str_values(graph, person_id, SCHEMA.name)
    given_names = rdf_str_values(graph, person_id, SCHEMA.givenName)
    family_names = rdf_str_values(graph, person_id, SCHEMA.familyName)

    if not (names or given_names or family_names):
        return None

    return CodeMetaPerson(
        names=names,
        given_names=given_names,
        family_names=family_names,
        is_organization=(person_id, RDF.type, SCHEMA.Organization) in graph,
    )


def resolve_role_nodes(
    graph: rdflib.Graph,
    person_id: rdflib.term.Node,
    role_property: rdflib.term.URIRef,
    seen: Optional[set[rdflib.term.Node]] = None,
) -> list[rdflib.term.Node]:
    if seen is None:
        seen = set()
    if person_id in seen:
        return []
    seen.add(person_id)
    if (person_id, RDF.type, SCHEMA.Role) not in graph:
        return [person_id]

    resolved_nodes: list[rdflib.term.Node] = []
    for _, _, inner_person in graph.triples((person_id, role_property, None)):
        resolved_nodes.extend(
            resolve_role_nodes(
                graph, inner_person, role_property=role_property, seen=seen
            )
        )
    return resolved_nodes


def extract_people(
    graph: rdflib.Graph,
    entity_id: rdflib.term.Node,
    role_property: rdflib.term.URIRef,
) -> tuple[list[CodeMetaPerson], list[CodeMetaPerson]]:
    people: list[CodeMetaPerson] = []
    affiliations: list[CodeMetaPerson] = []

    for _, _, person_or_person_list in graph.triples((entity_id, role_property, None)):
        person_ids = resolve_role_nodes(graph, person_or_person_list, role_property)
        if (
            person_or_person_list != RDF.nil
            and (person_or_person_list, RDF.first, None) in graph
        ):
            for person_id in rdflib.collection.Collection(graph, person_or_person_list):
                person_ids.extend(resolve_role_nodes(graph, person_id, role_property))

        for person_id in person_ids:
            person = extract_person(graph, person_id)
            if person and person not in people:
                people.append(person)

            for _, _, organization in graph.triples(
                (person_id, SCHEMA.affiliation, None)
            ):
                for organization_id in resolve_role_nodes(
                    graph, organization, role_property=SCHEMA.affiliation
                ):
                    affiliation = extract_person(graph, organization_id)
                    if affiliation and affiliation not in affiliations:
                        affiliations.append(affiliation)

    return (people, affiliations)


def extract_rdf_metadata(
    graph: rdflib.Graph, entity_id: rdflib.term.Node, *, swhid: Optional[QualifiedSWHID]
) -> dict[str, Any]:
    authors, author_affiliations = extract_people(graph, entity_id, SCHEMA.author)
    editors, editor_affiliations = extract_people(graph, entity_id, SCHEMA.editor)
    publishers, publisher_affiliations = extract_people(
        graph, entity_id, SCHEMA.publisher
    )

    affiliations: list[CodeMetaPerson] = []
    for affiliation in (
        author_affiliations + editor_affiliations + publisher_affiliations
    ):
        if affiliation not in affiliations:
            affiliations.append(affiliation)

    identifiers = [
        str(identifier)
        for _, _, identifier in graph.triples((entity_id, SCHEMA.identifier, None))
    ]
    licenses = [
        str(license)
        for _, _, license in graph.triples((entity_id, SCHEMA.license, None))
        if license is not None
    ]

    d = {
        "swhid": swhid,
        "affiliation": affiliations or None,
        "author": authors or None,
        "developmentStatus": rdf_first_str_value(
            graph, entity_id, CODEMETA.developmentStatus
        ),
        "editor": editors or None,
        "identifier": identifiers or None,
        "issueTracker": rdf_first_str_value(graph, entity_id, CODEMETA.issueTracker),
        "license": licenses or None,
        "publisher": publishers or None,
        "referencePublication": rdf_first_str_value(
            graph, entity_id, CODEMETA.referencePublication
        ),
    }

    for term in [
        "applicationCategory",
        "codeRepository",
        "dateCreated",
        "dateModified",
        "datePublished",
        "description",
        "downloadUrl",
        "installUrl",
        "name",
        "relatedLink",
        "softwareVersion",
        "url",
        "version",
    ]:
        d[term] = rdf_first_str_value(graph, entity_id, SCHEMA[term])

    for term in [
        "operatingSystem",
        "programmingLanguage",
    ]:
        d[term] = list(rdf_str_values(graph, entity_id, SCHEMA[term])) or None

    return d


def extract_compact_identifiers(doc: Dict[str, Any]) -> list[str]:
    # workaround for https://github.com/codemeta/codemeta/pull/322
    compact_identifiers: list[str] = []
    if "identifier" in doc:
        if isinstance(doc["identifier"], list):
            for identifier in doc["identifier"]:
                if isinstance(identifier, str) and "/" not in identifier:
                    compact_identifiers.append(identifier)
        elif isinstance(doc["identifier"], str) and "/" not in doc["identifier"]:
            compact_identifiers.append(doc["identifier"])
    return compact_identifiers


def merge_identifiers(
    *,
    rdf_data: dict[str, Any],
    codemeta_id: str,
    had_explicit_id: bool,
    compact_identifiers: list[str],
) -> None:
    identifiers = [*(rdf_data.pop("identifier") or [])]
    if had_explicit_id:
        identifiers.insert(0, codemeta_id)
    identifiers.extend(compact_identifiers)
    if identifiers:
        rdf_data["identifier"] = identifiers


def normalize_doc_id(
    doc: Dict[str, Any],
) -> tuple[bool, str, rdflib.term.Node]:
    had_explicit_id = "id" in doc
    if had_explicit_id:
        codemeta_id = str(doc["id"])
    else:
        doc["id"] = f"_:{uuid.uuid4()}"
        codemeta_id = str(doc["id"])

    doc_id = str(doc["id"])
    if doc_id.startswith("_:"):
        entity_id: rdflib.term.Node = rdflib.term.BNode(doc_id[2:])
    else:
        # using a base in case the id is not an absolute URI
        entity_id = rdflib.term.URIRef(doc_id, base=TMP_ROOT_URI_PREFIX)
        doc["id"] = str(entity_id)

    return (had_explicit_id, codemeta_id, entity_id)


def extract_codemeta_data(
    doc: Dict[str, Any],
    swhid: Optional[QualifiedSWHID] = None,
    *,
    resolve_unknown_context_url: bool = False,
    force_codemeta_context: bool = False,
) -> CodeMetaData:

    if force_codemeta_context:
        doc["@context"] = CODEMETA_V3_CONTEXT_URL

    doc = compact(
        doc, forgefed=False, resolve_unknown_context_url=resolve_unknown_context_url
    )

    had_explicit_id, codemeta_id, entity_id = normalize_doc_id(doc)

    compact_identifiers = extract_compact_identifiers(doc)

    expanded_doc = expand(doc, resolve_unknown_context_url=resolve_unknown_context_url)

    graph = rdflib.Graph().parse(
        data=json.dumps(expanded_doc),
        format="json-ld",
        # replace invalid URIs with blank node ids, instead of discarding whole nodes:
        generalized_rdf=True,
    )

    rdf_data = extract_rdf_metadata(graph, entity_id, swhid=swhid)

    merge_identifiers(
        rdf_data=rdf_data,
        codemeta_id=codemeta_id,
        had_explicit_id=had_explicit_id,
        compact_identifiers=compact_identifiers,
    )

    return CodeMetaData(
        id=codemeta_id,
        **rdf_data,
    )
