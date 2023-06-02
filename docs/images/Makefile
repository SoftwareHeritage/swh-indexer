
UML_DIAGS_SRC = $(wildcard *.uml)
UML_DIAGS = $(patsubst %.uml,%.svg,$(UML_DIAGS_SRC))

DOT_DIAGS_SRC = $(wildcard *.dot)
DOT_DIAGS = $(patsubst %.dot,%.svg,$(DOT_DIAGS_SRC))

all: $(UML_DIAGS) $(DOT_DIAGS)

%.svg: %.uml
	DISPLAY="" plantuml -tsvg $<

%.svg: %.dot
	dot $< -T svg -o $@

clean:
	-rm -f $(DEP_GRAPHS) $(UML_DIAGS) $(DOT_DIAGS)
