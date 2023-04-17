# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import ast
import itertools
import re

from rdflib import RDF, BNode, Graph, Literal, URIRef

from swh.indexer.codemeta import CROSSWALK_TABLE
from swh.indexer.namespaces import SCHEMA

from .base import DictMapping, SingleFileIntrinsicMapping
from .utils import add_map

SPDX = URIRef("https://spdx.org/licenses/")


def name_to_person(graph: Graph, name):
    if not isinstance(name, str):
        return None
    author = BNode()
    graph.add((author, RDF.type, SCHEMA.Person))
    graph.add((author, SCHEMA.name, Literal(name)))
    return author


class GemspecMapping(DictMapping, SingleFileIntrinsicMapping):
    name = "gemspec"
    filename = re.compile(rb".*\.gemspec")
    mapping = CROSSWALK_TABLE["Ruby Gem"]
    string_fields = ["name", "version", "description", "summary", "email"]
    uri_fields = ["homepage"]

    _re_spec_new = re.compile(r".*Gem::Specification.new +(do|\{) +\|.*\|.*")
    _re_spec_entry = re.compile(r"\s*\w+\.(?P<key>\w+)\s*=\s*(?P<expr>.*)")

    def translate(self, raw_content):
        try:
            raw_content = raw_content.decode()
        except UnicodeDecodeError:
            self.log.warning("Error unidecoding from %s", self.log_suffix)
            return

        # Skip lines before 'Gem::Specification.new'
        lines = itertools.dropwhile(
            lambda x: not self._re_spec_new.match(x), raw_content.split("\n")
        )

        try:
            next(lines)  # Consume 'Gem::Specification.new'
        except StopIteration:
            self.log.warning("Could not find Gem::Specification in %s", self.log_suffix)
            return

        content_dict = {}
        for line in lines:
            match = self._re_spec_entry.match(line)
            if match:
                value = self.eval_ruby_expression(match.group("expr"))
                if value:
                    content_dict[match.group("key")] = value
        return self._translate_dict(content_dict)

    def eval_ruby_expression(self, expr):
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
            return
        if isinstance(tree, ast.Expression):
            return evaluator(tree.body)

    def normalize_license(self, s):
        if isinstance(s, str):
            return SPDX + s

    def normalize_licenses(self, licenses):
        if isinstance(licenses, list):
            return [SPDX + license for license in licenses if isinstance(license, str)]

    def translate_author(self, graph: Graph, root, author):
        if isinstance(author, str):
            add_map(graph, root, SCHEMA.author, name_to_person, [author])

    def translate_authors(self, graph: Graph, root, authors):
        if isinstance(authors, list):
            add_map(graph, root, SCHEMA.author, name_to_person, authors)
