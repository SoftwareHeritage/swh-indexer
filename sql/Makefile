# Depends: postgresql-client, postgresql-autodoc

DBNAME = softwareheritage-indexer-dev
DOCDIR = autodoc

SQL_INIT    = swh-init.sql
SQL_ENUMS   = swh-enums.sql
SQL_SCHEMA  = swh-schema.sql
SQL_FUNC    = swh-func.sql
SQL_DATA    = swh-data.sql
SQL_INDEX   = swh-indexes.sql
SQLS = $(SQL_INIT) $(SQL_ENUMS) $(SQL_SCHEMA) $(SQL_FUNC) $(SQL_INDEX) $(SQL_DATA)

PSQL_BIN = psql
PSQL_FLAGS = --echo-all -X -v ON_ERROR_STOP=1
PSQL = $(PSQL_BIN) $(PSQL_FLAGS)

all:

createdb: createdb-stamp
createdb-stamp: $(SQL_INIT)
	createdb $(DBNAME)
	touch $@

filldb: filldb-stamp
filldb-stamp: createdb-stamp
	cat $(SQLS) | $(PSQL) $(DBNAME)
	touch $@

dropdb:
	-dropdb $(DBNAME)

dumpdb: swh-indexer.dump
swh-indexer.dump: filldb-stamp
	pg_dump -Fc $(DBNAME) > $@

clean:
	rm -rf *-stamp $(DOCDIR)/

distclean: clean dropdb
	rm -f swh-indexer.dump

.PHONY: all initdb createdb dropdb doc clean
