from typing import Dict, List, Optional

import yaml

from swh.indexer.codemeta import CODEMETA_CONTEXT_URL, CROSSWALK_TABLE, SCHEMA_URI

from .base import Author, Authors, Date, DictMapping, SchemaEntry, SingleFileMapping

yaml.SafeLoader.yaml_implicit_resolvers = {
    k: [r for r in v if r[0] != "tag:yaml.org,2002:timestamp"]
    for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()
}


class CffMapping(DictMapping, SingleFileMapping):
    """Dedicated class for Citation (CITATION.cff) mapping and translation"""

    name = "cff"
    filename = b"CITATION.cff"
    mapping = CROSSWALK_TABLE["Citation File Format Core (CFF-Core) 1.0.2"]
    string_fields = ["keywords", "license", "abstract", "version", "doi"]

    def translate(self, raw_content: bytes) -> Dict:
        content: str = raw_content.decode()
        content_dict = yaml.load(content, Loader=yaml.SafeLoader)
        metadata = self._translate_dict(content_dict)

        metadata["@context"] = CODEMETA_CONTEXT_URL

        return metadata

    def normalize_authors(self, d) -> Authors:
        result: List[Author] = []
        for author in d:
            author_data = {"@type": SCHEMA_URI + "Person"}
            if "orcid" in author:
                author_data["@id"] = author["orcid"]
            if "affiliation" in author:
                author_data[SCHEMA_URI + "affiliation"] = {
                    "@type": SCHEMA_URI + "Organization",
                    SCHEMA_URI + "name": author["affiliation"],
                }  # type: ignore
            if "family-names" in author:
                author_data[SCHEMA_URI + "familyName"] = author["family-names"]
            if "given-names" in author:
                author_data[SCHEMA_URI + "givenName"] = author["given-names"]

            result.append(author_data)  # type: ignore

        return {"@list": result}

    def normalize_doi(self, s) -> Optional[SchemaEntry]:
        if isinstance(s, str):
            return {"@id": "https://doi.org/" + s}
        return None

    def normalize_license(self, s) -> Optional[SchemaEntry]:
        if isinstance(s, str):
            return {"@id": "https://spdx.org/licenses/" + s}
        return None

    def normalize_repository_code(self, s) -> Optional[SchemaEntry]:
        if isinstance(s, str):
            return {"@id": s}
        return None

    def normalize_date_released(self, s) -> Optional[Date]:
        if isinstance(s, str):
            return {"@value": s, "@type": SCHEMA_URI + "Date"}
        return None
