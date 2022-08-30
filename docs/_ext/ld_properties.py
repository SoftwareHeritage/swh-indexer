from typing import Any, Dict, List, Tuple

from docutils.nodes import (
    Element,
    Node,
    TextElement,
    literal,
    reference,
    system_message,
)
from sphinx.util.docutils import ReferenceRole

PREFIXES = {
    "schema": "http://schema.org/",
    "codemeta": "https://codemeta.github.io/terms/",
    "forge": "https://forgefed.org/vocabulary.html#",
    "as": "https://www.w3.org/TR/activitystreams-vocabulary/#",
    "wikidata": "http://www.wikidata.org/entity/",
    "mesocore": "https://www.softwareheritage.org/schema/2022/mesocore/",
}


class PropertyRole(ReferenceRole):
    def run(self) -> Tuple[List[Node], List[system_message]]:
        (prefix, local_name) = self.target.split(":", 1)
        try:
            namespace = PREFIXES[prefix]
        except KeyError:
            msg = self.inliner.reporter.error(
                __("invalid prefix %s") % prefix, line=self.lineno
            )
            prb = self.inliner.problematic(self.rawtext, self.rawtext, msg)
            return [prb], [msg]

        full_name = namespace + local_name

        text = literal("", self.target)
        link = reference(
            "", "", text, internal=False, refuri=full_name, classes=["ld-property"]
        )
        return [link], []


def setup(app: "Sphinx") -> Dict[str, Any]:
    from docutils.parsers.rst import roles

    roles.register_local_role("prop", PropertyRole())
