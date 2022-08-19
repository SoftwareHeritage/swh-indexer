# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class _Namespace:
    """Handy class to get terms within a namespace by accessing them as attributes.

    This is similar to `rdflib's namespaces
    <https://rdflib.readthedocs.io/en/stable/namespaces_and_bindings.html>`__
    """

    def __init__(self, uri: str):
        if not uri.endswith(("#", "/")):
            # Sanity check, to make sure it doesn't end with an alphanumerical
            # character, which is very likely to be invalid.
            raise ValueError(f"Invalid trailing character for namespace URI: {uri}")
        self._uri = uri

    def __getattr__(self, term: str) -> str:
        return self._uri + term


SCHEMA = _Namespace("http://schema.org/")
CODEMETA = _Namespace("https://codemeta.github.io/terms/")
FORGEFED = _Namespace("https://forgefed.org/ns#")
ACTIVITYSTREAMS = _Namespace("https://www.w3.org/ns/activitystreams#")
