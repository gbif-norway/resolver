VERSION = 0.1

PREFIX ?= /usr

install:
	install bin/plzresolve ${DESTDIR}${PREFIX}/bin/plzresolve
	install bin/iptresolve ${DESTDIR}${PREFIX}/bin/iptresolve

