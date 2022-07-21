# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.indexer.metadata_dictionary import MAPPINGS


def test_compute_metadata_composer():
    raw_content = """{
"name": "symfony/polyfill-mbstring",
"type": "library",
"description": "Symfony polyfill for the Mbstring extension",
"keywords": [
    "polyfill",
    "shim",
    "compatibility",
    "portable"
],
"homepage": "https://symfony.com",
"license": "MIT",
"authors": [
    {
        "name": "Nicolas Grekas",
        "email": "p@tchwork.com"
    },
    {
        "name": "Symfony Community",
        "homepage": "https://symfony.com/contributors"
    }
],
"require": {
    "php": ">=7.1"
},
"provide": {
    "ext-mbstring": "*"
},
"autoload": {
    "files": [
        "bootstrap.php"
    ]
},
"suggest": {
    "ext-mbstring": "For best performance"
},
"minimum-stability": "dev",
"extra": {
    "branch-alias": {
        "dev-main": "1.26-dev"
    },
    "thanks": {
        "name": "symfony/polyfill",
        "url": "https://github.com/symfony/polyfill"
    }
}
}
    """.encode(
        "utf-8"
    )

    result = MAPPINGS["ComposerMapping"]().translate(raw_content)

    expected = {
        "@context": "https://doi.org/10.5063/schema/codemeta-2.0",
        "type": "SoftwareSourceCode",
        "name": "symfony/polyfill-mbstring",
        "keywords": ["polyfill", "shim", "compatibility", "portable"],
        "description": "Symfony polyfill for the Mbstring extension",
        "url": "https://symfony.com",
        "license": "https://spdx.org/licenses/MIT",
        "author": [
            {
                "type": "Person",
                "name": "Nicolas Grekas",
                "email": "p@tchwork.com",
            },
            {
                "type": "Person",
                "name": "Symfony Community",
            },
        ],
    }

    assert result == expected
