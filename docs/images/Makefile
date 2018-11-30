
UML_DIAGS_SRC = $(wildcard *.uml)
UML_DIAGS = $(patsubst %.uml,%.svg,$(UML_DIAGS_SRC))

all: $(UML_DIAGS)

%.svg: %.uml
	DISPLAY="" plantuml -tsvg $<

clean:
	-rm -f $(DEP_GRAPHS) $(UML_DIAGS)
