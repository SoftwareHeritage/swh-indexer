import ast
import itertools
import os.path
import re
from typing import List

from rdflib import Graph, Literal, URIRef

from swh.indexer.codemeta import _DATA_DIR, _read_crosstable
from swh.indexer.metadata_dictionary.base import DirectoryLsEntry
from swh.indexer.namespaces import CODEMETA, SCHEMA
from swh.indexer.storage.interface import Sha1

from .base import DictMapping, SingleFileIntrinsicMapping

PODSPEC_TABLE_PATH = os.path.join(_DATA_DIR, "podspec.csv")

with open(PODSPEC_TABLE_PATH) as fd:
    (CODEMETA_TERMS, PODSPEC_TABLE) = _read_crosstable(fd)


class PodspecMapping(DictMapping, SingleFileIntrinsicMapping):
    """
    dedicated class for Podspec mapping and translation
    """

    name = "podspec"
    mapping = PODSPEC_TABLE["Podspec"]
    string_fields = [
        "description",
        "name",
        "softwareVersion",
    ]

    _re_spec_new = re.compile(r".*Pod::Spec.new +(do|\{) +\|.*\|.*")
    _re_spec_entry = re.compile(r"\s*\w+\.(?P<key>\w+)\s*=\s*(?P<expr>.*)")

    @classmethod
    def detect_metadata_files(cls, file_entries: List[DirectoryLsEntry]) -> List[Sha1]:
        for entry in file_entries:
            if entry["name"].endswith(b".podspec"):
                return [entry["sha1"]]
        return []

    def translate(self, raw_content):
        try:
            raw_content = raw_content.decode()
        except UnicodeDecodeError:
            self.log.warning("Error unidecoding from %s", self.log_suffix)
            return

        lines = itertools.dropwhile(
            lambda x: not self._re_spec_new.match(x), raw_content.split("\n")
        )

        try:
            next(lines)
        except StopIteration:
            self.log.warning("Could not find Pod::Specification in %s", self.log_suffix)
            return

        content_dict = {}
        for line in lines:
            match = self._re_spec_entry.match(line)
            if match:
                value = self.eval_podspec_expression(match.group("expr"))
                if value:
                    content_dict[match.group("key")] = value
        return self._translate_dict(content_dict)

    def eval_podspec_expression(self, expr):
        def evaluator(node):
            if isinstance(node, ast.Str):
                return node.s
            elif isinstance(node, ast.List):
                res = []
                for element in node.elts:
                    val = evaluator(element)
                    if not val:
                        return
                    res.append(val)
                return res

        expr = expr.replace(".freeze", "")
        try:
            tree = ast.parse(expr, mode="eval")
        except (SyntaxError, ValueError):
            return
        if isinstance(tree, ast.Expression):
            return evaluator(tree.body)

    def translate_summary(self, graph: Graph, root, s):
        if isinstance(s, str):
            graph.add((root, SCHEMA.description, Literal(s)))

    def parse_enum(self, enum_string):
        if enum_string.startswith("{"):
            items = enum_string.strip("{ }\n").split(",")
            parsed = {}
            for item in items:
                parsed[item.split("=>")[0].strip("\n ")] = item.split("=>")[1].strip(
                    "\n "
                )[1:-1]

            return parsed

    def translate_source(self, graph: Graph, root, s):
        if isinstance(s, str):
            parsed = self.parse_enum(s)
            if parsed:
                if ":git" in parsed:
                    s = parsed[":git"]
            graph.add((root, CODEMETA.codeRepository, URIRef(s)))
