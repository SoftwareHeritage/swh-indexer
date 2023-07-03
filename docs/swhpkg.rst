SwhPkg Vocabulary
================================

.. note:: This is an early draft and hasn't been implemented yet


SwhPkg is a vocabulary that complements ontologies like schema.org and CodeMeta
in describing software projects. While the latter are meant to describe
source code projects, SwhPkg describes relationships between different packages released
by such projects.

The namespace is ``https://www.softwareheritage.org/schema/2023/packages/``;
and it is meant to be used primarily alongside CodeMeta/schema.org
and ForgeFed/ActivityStreams.


The following prefixes are used throughout this document for readability:

.. code-block:: json

    {
        "schema": "http://schema.org/",
        "codemeta": "https://codemeta.github.io/terms/",
        "swhpkg": "https://www.softwareheritage.org/schema/2023/packages/",
        "swhpackages": "https://archive.softwareheritage.org/packages/",
    }

For example, here is a document using all three together:

.. code-block:: json

    {
      "@context": {
        "schema": "http://schema.org/",
        "codemeta": "https://codemeta.github.io/terms/",
        "swhpkg": "https://www.softwareheritage.org/schema/2023/packages/",
        "swhpackages": "https://archive.softwareheritage.org/packages/",
        "package": {"@id": "swhpkg:package", "@type": "@id"},
        "release": {"@id": "swhpkg:release", "@type": "@id"},
        "dependencies": {"@id": "swhpkg:dependencies"},
        "dependency": {"@id": "swhpkg:dependency", "@type": "@id"},
        "dependent": {"@id": "swhpkg:dependent", "@type": "@id"},
        "kind": {"@id": "swhpkg:kind"},
        "optional": {"@id": "swhpkg:optional"}
      },
      "@type": "schema:SoftwareSourceCode",
      "@id": "https://npmjs.com/package/d3@7.8.2",
      "package": "swhpackages:js/d3",
      "release": "swhpackages:js/d3@7.8.2",
      "schema:name": "d3",
      "schema:version": "7.8.2",
      "schema:description": "Data-Driven Documents",
      "dependencies": [
        {
          "@type": "swhpkg:dependencies",
          "@id": "swhpackages:js/d3@7.8.2#d3-array",
          "dependent": "swhpackages:js/d3@7.8.2",
          "dependency": "swhpackages:js/d3-array",
          "constraint": "^3.0.0",
          "kind": "runtime",
          "optional": false
        },
        {
          "@type": "swhpkg:dependencies",
          "@id": "swhpackages:js/d3@7.8.2#mocha",
          "dependent": "swhpackages:js/d3@7.8.2",
          "dependency": "swhpackages:js/mocha",
          "constraint": ">10.0.0",
          "kind": "development",
          "optional": true
        }
      ]
    }

SwhPkg Terms
-------------

.. list-table::
   :header-rows: 1

   * - Property
     - Type
     - Examples
     - Description
   * - ``package``
     - ``swhpkg:package``
     - ``swhpackages:js/d3``, ``swhpackages:python/numpy``
     - Package that is released by the SoftwareSourceCode/SofwtareApplication.
   * - ``release``
     - ``swhpkg:release``
     - ``swhpackages:js/d3@7.8.2``, ``swhpackages:python/numpy@1.24.2``
     - Specific version of the package that is released by the SoftwareSourceCode/SoftwareApplication
   * - ``dependencies``
     - ``swhpkg:dependencies``
     - d3 depends on d3-array and mocha.
     - Dependencies of the project. There can be many of them.
   * - ``dependent``
     - ``swhpkg:release``
     - ``swhpkg:js/d3``
     - A reference to the package release that depends on the dependency.
   * - ``dependency``
     - ``swhpkg:package``
     - ``swhpackages:js/d3``, ``swhpackages:python/django``
     - A reference to the package that is depended on.
   * - ``constraint``
     - Text
     - ``^3.0.0``, ``>10.0.0``
     - The constraint on a dependency relation. It can be a version range, or a git commit hash, or even a file path.
   * - ``kind``
     - Text
     - ``runtime``, ``development``
     - The type of dependency relation. Some common values are ``runtime``, ``development``.
   * - ``optional``
     - boolean
     - ``true``, ``false``
     - Whether the dependency is optional or not.

