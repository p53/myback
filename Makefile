#!/usr/bin/make -f

PACKAGE=myback
SRCTOP := $(shell if [ "$$PWD" != "" ]]; then echo $$PWD; else pwd; fi)
DESTDIR= $(SRCTOP)/debian/$(PACKAGE)
DOCSDIR=$(DESTDIR)/usr/share/doc/$(PACKAGE)/html
APPDIR=$(DESTDIR)/opt/$(PACKAGE)
APPLIBDIR=$(APPDIR)/lib
APPCONFDIR=$(APPDIR)/etc
APPBINDIR=$(APPDIR)/bin
DATADIR=$(DESTDIR)/var/lib/$(PACKAGE)
MANDIR=$(DESTDIR)/usr/share/man/man8

build:

install:clean 
	install -d $(SYSCONFIGDIR) $(DATADIR) $(MANDIR) $(DOCSDIR) $(APPBINDIR) $(APPDIR) $(APPLIBDIR) $(APPCONFDIR)
	cp -pRl docs/* $(DOCSDIR)/
        cp -pR data/* $(DATADIR)/
	install -pm 0755 bin/$(PACKAGE).pl $(APPBINDIR)/$(PACKAGE)
	install -pm 0755 bin/$(PACKAGE)-glacier.pl $(APPBINDIR)/$(PACKAGE)-glacier
        cp -pRl lib/* $(APPLIBDIR)/
        cp -pRl etc/* $(APPCONFDIR)
	pod2man bin/$(PACKAGE).pl $(MANDIR)/$(PACKAGE).8
	pod2man bin/$(PACKAGE)-glacier.pl $(MANDIR)/$(PACKAGE)-glacier.8

binary-indep: install

binary: binary-indep

clean:
	rm -Rf $(DESTDIR)

