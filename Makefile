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

#diff:
#	diff -ur lib /usr/local/folio/lib || true
#	diff -ur bin /usr/local/folio/bin || true

check:
	@for f in $(shell find bin -maxdepth 1 -type f -executable) $(shell find lib -name \*.pm); do perl -Ilib -I$(PREFIX)/lib -c $$f; done

diff:
	@for f in $(shell find bin -maxdepth 1 -type f -executable) $(shell find lib -name \*.pm); do cmp -s $(PREFIX)/$$f $$f || diff -u $(PREFIX)/$$f $$f; done | less

.PHONY: build install classes diff check
