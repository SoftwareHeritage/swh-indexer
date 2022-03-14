# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import ast
import itertools
import re
from typing import Any, Dict, List, Optional, Union

from swh.indexer.codemeta import CROSSWALK_TABLE, SCHEMA_URI

from .base import DictMapping, FileEntry


def name_to_person(name: str) -> Dict[str, str]:
    return {
        "@type": SCHEMA_URI + "Person",
        SCHEMA_URI + "name": name,
    }


class GemspecMapping(DictMapping):
    name = "gemspec"
    mapping = CROSSWALK_TABLE["Ruby Gem"]
    string_fields = ["name", "version", "description", "summary", "email"]

    _re_spec_new = re.compile(r".*Gem::Specification.new +(do|\{) +\|.*\|.*")
    _re_spec_entry = re.compile(r"\s*\w+\.(?P<key>\w+)\s*=\s*(?P<expr>.*)")

    @classmethod
    def detect_metadata_files(cls, file_entries: List[FileEntry]) -> List[bytes]:
        for entry in file_entries:
            if entry["name"].endswith(b".gemspec"):
                return [entry["sha1"]]
        return []

    def translate(self, raw_content: bytes) -> Optional[Dict[str, str]]:
        try:
            content: str = raw_content.decode()
        except UnicodeDecodeError:
            self.log.warning("Error unidecoding from %s", self.log_suffix)
            return None

        # Skip lines before 'Gem::Specification.new'
        lines = itertools.dropwhile(
            lambda x: not self._re_spec_new.match(x), content.split("\n")
        )

        try:
            next(lines)  # Consume 'Gem::Specification.new'
        except StopIteration:
            self.log.warning("Could not find Gem::Specification in %s", self.log_suffix)
            return None

        content_dict = {}
        for line in lines:
            match = self._re_spec_entry.match(line)
            if match:
                value = self.eval_ruby_expression(match.group("expr"))
                if value:
                    content_dict[match.group("key")] = value
        return self._translate_dict(content_dict)

    def eval_ruby_expression(self, expr: str) -> Optional[Union[str, List]]:
        """Very simple evaluator of Ruby expressions.

        >>> GemspecMapping().eval_ruby_expression('"Foo bar"')
        'Foo bar'
        >>> GemspecMapping().eval_ruby_expression("'Foo bar'")
        'Foo bar'
        >>> GemspecMapping().eval_ruby_expression("['Foo', 'bar']")
        ['Foo', 'bar']
        >>> GemspecMapping().eval_ruby_expression("'Foo bar'.freeze")
        'Foo bar'
        >>> GemspecMapping().eval_ruby_expression( \
                "['Foo'.freeze, 'bar'.freeze]")
        ['Foo', 'bar']
        """

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
            # We're parsing Ruby expressions here, but Python's
            # ast.parse works for very simple Ruby expressions
            # (mainly strings delimited with " or ', and lists
            # of such strings).
            tree = ast.parse(expr, mode="eval")
        except (SyntaxError, ValueError):
            return None
        if isinstance(tree, ast.Expression):
            return evaluator(tree.body)
        return None

    def normalize_homepage(self, s) -> Optional[Dict[str, str]]:
        if isinstance(s, str):
            return {"@id": s}
        return None

    def normalize_license(self, s) -> Optional[List[Dict[str, str]]]:
        if isinstance(s, str):
            return [{"@id": "https://spdx.org/licenses/" + s}]
        return None

    def normalize_licenses(self, licenses) -> Optional[List[Dict[str, str]]]:
        if isinstance(licenses, list):
            return [
                {"@id": "https://spdx.org/licenses/" + license}
                for license in licenses
                if isinstance(license, str)
            ]
        return None

    def normalize_author(self, author) -> Optional[Dict[str, Any]]:
        if isinstance(author, str):
            return {"@list": [name_to_person(author)]}
        return None

    def normalize_authors(self, authors) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        if isinstance(authors, list):
            return {
                "@list": [
                    name_to_person(author)
                    for author in authors
                    if isinstance(author, str)
                ]
            }
        return None
