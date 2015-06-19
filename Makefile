#!/usr/bin/make -f

PACKAGE=myback
SRCTOP := $(shell if [ "$$PWD" != "" ]]; then echo $$PWD; else pwd; fi)
DESTDIR= $(SRCTOP)/debian/$(PACKAGE)
DOCSDIR=$(DESTDIR)/usr/share/doc/$(PACKAGE)/html
BINDIR=$(DESTDIR)/usr/bin
APPDIR=$(DESTDIR)/opt/$(PACKAGE)
APPLIBDIR=$(APPDIR)/lib
APPCONFDIR=$(APPDIR)/etc
DATADIR=$(DESTDIR)/var/lib/$(PACKAGE)
MANDIR=$(DESTDIR)/usr/share/man/man8

build:

install:clean 
	install -d $(SYSCONFIGDIR) $(DATADIR) $(MANDIR) $(DOCSDIR) $(BINDIR) $(APPDIR) $(APPLIBDIR) $(APPCONFDIR)
	cp -pRl docs/* $(DOCSDIR)/
        cp -pR data/* $(DATADIR)/
	install -pm 0755 ./$(PACKAGE).pl $(APPDIR)/$(PACKAGE)
        cp -pRl lib/* $(APPLIBDIR)/
        cp -pRl etc/* $(APPCONFDIR)
	install -pm 0644 man/$(PACKAGE).8 $(MANDIR)

binary-indep: install

binary: binary-indep

clean:
	rm -Rf $(DESTDIR)

