VERSION=4.53
URL=http://www.stunnel.org/downloads/stunnel-$(VERSION).tar.gz
TARBALL=$(shell basename $(URL))
WORKDIR=stunnel-$(VERSION)

ifeq ($(DEBUG),1)
CONFIGURE_FLAGS=--enable-debug
endif

default: install
include ../Makefile.ext

CFLAGS=-I$(PREFIX)/include
LDFLAGS=-L$(PREFIX)/lib 
CXXFLAGS=-I$(PREFIX)/include

# Disable eliptic-curve stuff because I can't figure out how to get openssl
# to build with it. Can fix this later.
CFLAGS+=-DOPENSSL_NO_ECDH

configure: | $(WORKDIR)/Makefile
$(WORKDIR)/Makefile: | $(WORKDIR) 
$(WORKDIR)/Makefile: $(PREFIX)/lib/libcrypto.$(LIBEXT) $(PREFIX)/lib/libssl.$(LIBEXT)
$(WORKDIR)/Makefile: $(PREFIX)/include/openssl/ssl.h
	cd $(WORKDIR); CFLAGS="$(CFLAGS)" CXXFLAGS="$(CXXFLAGS)" LDFLAGS="$(LDFLAGS)" ./configure --prefix=$(PREFIX) $(CONFIGURE_FLAGS)

compile: | $(WORKDIR)/src/stunnel
$(WORKDIR)/src/stunnel: | configure
$(WORKDIR)/src/stunnel:
	$(MAKE) -C $(WORKDIR) CFLAGS="$(CFLAGS)" CXXFLAGS="$(CXXFLAGS)" LDFLAGS="$(LDFLAGS)"

install: | compile $(PREFIX)/lib $(PREFIX)/include
install: $(PREFIX)/bin/stunnel

$(PREFIX)/bin/stunnel: $(WORKDIR)/src/stunnel
	cp $^ $@

$(WORKDIR)/Makefile: $(PREFIX)/lib/libcrypto.$(LIBEXT) $(PREFIX)/lib/libssl.$(LIBEXT)
$(WORKDIR)/Makefile: $(PREFIX)/include/openssl/ssl.h

$(PREFIX)/lib/libz.$(LIBEXT) $(PREFIX)/include/zlib.h:
	$(MAKE) -C ../zlib PREFIX=$(PREFIX) $@

$(PREFIX)/lib/libcrypto.$(LIBEXT) $(PREFIX)/lib/libssl.$(LIBEXT) $(PREFIX)/include/openssl/ssl.h:
	$(PREFIX)/lib/libz.$(LIBEXT) $(PREFIX)/include/zlib.h

$(PREFIX)/lib/libcrypto.$(LIBEXT) $(PREFIX)/lib/libssl.$(LIBEXT) $(PREFIX)/include/openssl/ssl.h:
	$(MAKE) -C ../openssl PREFIX=$(PREFIX) install
