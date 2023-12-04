# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from hypothesis import HealthCheck, given, settings, strategies
import pytest

from swh.indexer.metadata_dictionary import MAPPINGS


def test_gemspec_base():
    raw_content = b"""
Gem::Specification.new do |s|
s.name        = 'example'
s.version     = '0.1.0'
s.licenses    = ['MIT']
s.summary     = "This is an example!"
s.description = "Much longer explanation of the example!"
s.authors     = ["Ruby Coder"]
s.email       = 'rubycoder@example.com'
s.files       = ["lib/example.rb"]
s.homepage    = 'https://rubygems.org/gems/example'
s.metadata    = { "source_code_uri" => "https://github.com/example/example" }
end"""
    result = MAPPINGS["GemspecMapping"]().translate(raw_content)
    assert set(result.pop("description")) == {
        "This is an example!",
        "Much longer explanation of the example!",
    }
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [{"type": "Person", "name": "Ruby Coder"}],
        "name": "example",
        "license": "https://spdx.org/licenses/MIT",
        "codeRepository": "https://rubygems.org/gems/example",
        "email": "rubycoder@example.com",
        "version": "0.1.0",
    }


@pytest.mark.xfail(reason="https://github.com/w3c/json-ld-api/issues/547")
def test_gemspec_two_author_fields():
    raw_content = b"""
Gem::Specification.new do |s|
s.authors     = ["Ruby Coder1"]
s.author      = "Ruby Coder2"
end"""
    result = MAPPINGS["GemspecMapping"]().translate(raw_content)
    assert result.pop("author") in (
        [
            {"type": "Person", "name": "Ruby Coder1"},
            {"type": "Person", "name": "Ruby Coder2"},
        ],
        [
            {"type": "Person", "name": "Ruby Coder2"},
            {"type": "Person", "name": "Ruby Coder1"},
        ],
    )
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }


def test_gemspec_invalid_author():
    raw_content = b"""
Gem::Specification.new do |s|
s.author      = ["Ruby Coder"]
end"""
    result = MAPPINGS["GemspecMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }
    raw_content = b"""
Gem::Specification.new do |s|
s.author      = "Ruby Coder1",
end"""
    result = MAPPINGS["GemspecMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
    }
    raw_content = b"""
Gem::Specification.new do |s|
s.authors     = ["Ruby Coder1", ["Ruby Coder2"]]
end"""
    result = MAPPINGS["GemspecMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "author": [{"type": "Person", "name": "Ruby Coder1"}],
    }


def test_gemspec_alternative_header():
    raw_content = b"""
require './lib/version'

Gem::Specification.new { |s|
s.name = 'rb-system-with-aliases'
s.summary = 'execute system commands with aliases'
}
"""
    result = MAPPINGS["GemspecMapping"]().translate(raw_content)
    assert result == {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "rb-system-with-aliases",
        "description": "execute system commands with aliases",
    }


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(
    strategies.dictionaries(
        # keys
        strategies.one_of(
            strategies.text(),
            *map(strategies.just, MAPPINGS["GemspecMapping"].mapping),  # type: ignore
        ),
        # values
        strategies.recursive(
            strategies.characters(),
            lambda children: strategies.lists(children, min_size=1),
        ),
    )
)
def test_gemspec_adversarial(doc):
    parts = [b"Gem::Specification.new do |s|\n"]
    for k, v in doc.items():
        parts.append("  s.{} = {}\n".format(k, repr(v)).encode())
    parts.append(b"end\n")
    MAPPINGS["GemspecMapping"]().translate(b"".join(parts))
