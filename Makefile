#!/usr/bin/make -f

PACKAGE=myback
SRCTOP := $(shell if [ "$$PWD" != "" ]]; then echo $$PWD; else pwd; fi)
DESTDIR= $(SRCTOP)/debian/$(PACKAGE)
DOCSDIR=$(DESTDIR)/usr/share/doc/html/$(PACKAGE)
BINDIR=$(DESTDIR)/usr/bin
CACHEDIR=$(DESTDIR)/var/cache/$(PACKAGE)
MANDIR=$(DESTDIR)/usr/share/man/man8

build:

install:clean 
	install -d $(SYSCONFIGDIR) $(CACHEDIR) $(MANDIR) $(DOCSDIR) $(BINDIR)
	cp -pRl docs/* $(DOCSDIR)/
	install -pm 0755 ./$(PACKAGE).pl $(BINDIR)/$(PACKAGE)
	install -pm 0644 man/$(PACKAGE).8 $(MANDIR)

binary-indep: install

binary: binary-indep

clean:
	rm -Rf $(DESTDIR)

