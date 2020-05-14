-include config.mk

build: config.mk
	@echo "Nothing to build -- just do this:"
	@echo "  $$ sudo make install"
	@echo "Or this as root:"
	@echo "  # make install"

config.mk: config.mk.def
	cp $< $@

install: classes check
	./install $(PREFIX)

classes: lib/Biblio/Folio/Classes.pm

lib/Biblio/Folio/Classes.pm: classes.ini make-classes
	chmod 644 $@
	./make-classes < $< > $@
	chmod 444 $@

diff:
	diff -ur lib /usr/local/folio/lib || true
	diff -ur bin /usr/local/folio/bin || true

check:
	@for f in bin/folio $(shell find lib -name \*.pm); do perl -Ilib -I$(PREFIX)/lib -c $$f; done

.PHONY: build install classes diff check
