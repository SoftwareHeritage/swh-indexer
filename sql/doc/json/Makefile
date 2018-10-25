# Depends: json-glib-tools

JSONVAL = json-glib-validate
JSONS = $(wildcard *.json)

all: validate
check: validate
test: validate

validate: validate-stamp
validate-stamp: $(JSONS)
	make $(patsubst %,validate/%,$?)
	touch $@

validate/%:
	$(JSONVAL) $*

clean:
	rm -f validate-stamp
