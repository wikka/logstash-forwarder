VERSION=0.11.0
URL=https://github.com/antirez/hiredis/tarball/v$(VERSION)
TARBALL=$(shell basename $(URL))
WORKDIR=hiredis-$(VERSION)

default: install
include ../Makefile.ext

$(WORKDIR): $(TARBALL)
	tar $(TARFLAGS) -xf $<
	mv antirez-hiredis-* $@

configure: 
	@# nothing to do

compile: | $(WORKDIR)/libhiredis.so
$(WORKDIR)/libhiredis.so:
	$(MAKE) -C $(WORKDIR)

install: | compile $(PREFIX)/lib $(PREFIX)/include
install:
	$(MAKE) -C $(WORKDIR) install PREFIX=$(PREFIX)
