VERSION=0.0.6

# By default, all dependencies (zeromq, etc) will be downloaded and installed
# locally. You can change this if you are deploying your own.
VENDOR?=zeromq jemalloc hiredis
#openssl zlib hiredis

# Where to install to.
PREFIX?=/opt/lumberjack

FETCH=sh fetch.sh

CFLAGS+=-D_POSIX_C_SOURCE=199309 -std=c99 -Wall -Wextra -Werror -pipe
CFLAGS+=-g
CFLAGS+=-Wno-unused-function
LDFLAGS+=-pthread
#LIBS=-lzmq -ljemalloc -lssl -lcrypto -luuid -lz
LIBS=-lzmq -ljemalloc -luuid -lz -lhiredis

CFLAGS+=-Ibuild/include 
LDFLAGS+=-Lbuild/lib -Wl,-rpath,'$$ORIGIN/../lib'

default: build-all
build-all: build/bin/lumberjack build/bin/lumberjack.sh
build-all: build/bin/stunnel build/bin/stunnel.sh
include Makefile.ext

ifeq ($(UNAME),Linux)
# clock_gettime is in librt on linux.
LIBS+=-lrt
endif

clean:
	-@rm -fr lumberjack unixsock *.o build

vendor-clean:
	-make -C vendor/msgpack/ clean
	-make -C vendor/jansson/ clean
	-make -C vendor/jemalloc/ clean
	-make -C vendor/libuuid/ clean
	-make -C vendor/zeromq/ clean
	-make -C vendor/zlib/ clean
	-make -C vendor/hiredis/ clean

rpm deb: | build-all
	fpm -s dir -t $@ -n lumberjack -v $(VERSION) --prefix /opt/lumberjack \
		--exclude '*.a' --exclude 'lib/pkgconfig/zlib.pc' -C build \
		--description "a log shipping tool" \
		--url "https://github.com/jordansissel/lumberjack" \
		bin/lumberjack bin/lumberjack.sh lib

backoff.o: backoff.c backoff.h
harvester.o: harvester.c harvester.h proto.h str.h sleepdefs.h
emitter.o: emitter.c emitter.h sleepdefs.h
lumberjack.o: lumberjack.c backoff.h harvester.h emitter.h
str.o: str.c str.h
proto.o: proto.c proto.h str.h sleepdefs.h

harvester.o: build/include/insist.h 
lumberjack.o: build/include/insist.h 

# Vendor'd dependencies
# If VENDOR contains 'zeromq' download and build it.
ifeq ($(filter zeromq,$(VENDOR)),zeromq)
emitter.o: build/include/zmq.h 
harvester.o: build/include/zmq.h
lumberjack.o:  build/include/zmq.h 
build/bin/lumberjack: | build/bin build/lib/libzmq.$(LIBEXT)
endif # zeromq

ifeq ($(filter jemalloc,$(VENDOR)),jemalloc)
harvester.o lumberjack.o str.o: build/include/jemalloc/jemalloc.h
build/bin/lumberjack: | build/lib/libjemalloc.$(LIBEXT)
endif # jemalloc

ifeq ($(filter openssl,$(VENDOR)),openssl)
proto.o: build/include/openssl/ssl.h
lumberjack.o:  build/include/openssl/ssl.h
build/bin/lumberjack: | build/lib/libssl.$(LIBEXT)
build/bin/lumberjack: | build/lib/libcrypto.$(LIBEXT)
endif # openssl

ifeq ($(filter zlib,$(VENDOR)),zlib)
proto.o: build/include/zlib.h
build/bin/lumberjack: | build/lib/libz.$(LIBEXT)
endif # zlib

ifeq ($(filter hiredis,$(VENDOR)),hiredis)
emitter.o: build/include/hiredis/hiredis.h
build/bin/lumberjack: | build/lib/libhiredis.$(LIBEXT)
endif # hiredis

build/bin/lumberjack.sh: lumberjack.sh | build/bin
	install -m 755 $^ $@

build/bin/stunnel.sh: stunnel.sh | build/bin
	install -m 755 $^ $@

build/bin/lumberjack: | build/bin
build/bin/lumberjack: lumberjack.o backoff.o harvester.o emitter.o str.o proto.o
	$(CC) $(LDFLAGS) -o $@ $^ $(LIBS)
	@echo " => Build complete: $@"
	@echo " => Run 'make rpm' to build an rpm (or deb or tarball)"

build/include/insist.h: | build/include
	PATH=$$PWD:$$PATH fetch.sh -o $@ https://raw.github.com/jordansissel/experiments/master/c/better-assert/insist.h

build/include/zmq.h build/lib/libzmq.$(LIBEXT): | build/include build/lib
	@echo " => Building zeromq"
	PATH=$$PWD:$$PATH $(MAKE) -C vendor/zeromq/ install PREFIX=$$PWD/build DEBUG=$(DEBUG)

build/include/msgpack.h build/lib/libmsgpack.$(LIBEXT): | build/include build/lib
	@echo " => Building msgpack"
	PATH=$$PWD:$$PATH $(MAKE) -C vendor/msgpack/ install PREFIX=$$PWD/build DEBUG=$(DEBUG)

build/include/jemalloc/jemalloc.h build/lib/libjemalloc.$(LIBEXT): | build/include build/lib
	@echo " => Building jemalloc"
	PATH=$$PWD:$$PATH $(MAKE) -C vendor/jemalloc/ install PREFIX=$$PWD/build DEBUG=$(DEBUG)

build/include/lz4.h build/lib/liblz4.$(LIBEXT): | build/include build/lib
	@echo " => Building lz4"
	PATH=$$PWD:$$PATH $(MAKE) -C vendor/lz4/ install PREFIX=$$PWD/build DEBUG=$(DEBUG)

build/include/zlib.h build/lib/libz.$(LIBEXT): | build/include build/lib
	@echo " => Building zlib"
	PATH=$$PWD:$$PATH $(MAKE) -C vendor/zlib/ install PREFIX=$$PWD/build DEBUG=$(DEBUG)

build/include/openssl/ssl.h build/lib/libssl.$(LIBEXT) build/lib/libcrypto.$(LIBEXT): | build/include build/lib
	@echo " => Building openssl"
	PATH=$$PWD:$$PATH $(MAKE) -C vendor/openssl install PREFIX=$$PWD/build DEBUG=$(DEBUG)

build/include/hiredis/hiredis.h build/lib/libhiredis.$(LIBEXT): | build/include build/lib
	@echo " => Building libhiredis"
	PATH=$$PWD:$$PATH $(MAKE) -C vendor/hiredis install PREFIX=$$PWD/build DEBUG=$(DEBUG)

build/bin/stunnel: | build/bin build/lib
	@echo " => Building stunnel"
	PATH=$$PWD:$$PATH $(MAKE) -C vendor/stunnel install PREFIX=$$PWD/build DEBUG=$(DEBUG)

build:
	mkdir $@

build/include build/bin build/test build/lib: | build
	mkdir $@
