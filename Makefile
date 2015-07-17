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
LOGROTATEDIR=$(DESTDIR)/etc/logrotate.d

build:

install:clean 
	install -d $(SYSCONFIGDIR) $(DATADIR) $(MANDIR) $(DOCSDIR) $(APPBINDIR) $(APPDIR) $(APPLIBDIR) $(APPCONFDIR) $(LOGROTATEDIR)
	cp -pRl docs/* $(DOCSDIR)/
        cp -pR data/* $(DATADIR)/
	install -pm 0755 bin/$(PACKAGE).pl $(APPBINDIR)/$(PACKAGE)
	install -pm 0755 bin/$(PACKAGE)-glacier.pl $(APPBINDIR)/$(PACKAGE)-glacier
	install -pm 0755 bin/$(PACKAGE)-hostcfg.pl $(APPBINDIR)/$(PACKAGE)-hostcfg
        cp -pRl lib/* $(APPLIBDIR)/
        cp -pRl etc/* $(APPCONFDIR)
        install -pm 0644 install_files/$(PACKAGE)-logrotate $(LOGROTATEDIR)/$(PACKAGE)
	pod2man bin/$(PACKAGE).pl $(MANDIR)/$(PACKAGE).8
	pod2man bin/$(PACKAGE)-glacier.pl $(MANDIR)/$(PACKAGE)-glacier.8
	pod2man bin/$(PACKAGE)-hostcfg.pl $(MANDIR)/$(PACKAGE)-hostcfg.8

binary-indep: install

binary: binary-indep

clean:
	rm -Rf $(DESTDIR)

