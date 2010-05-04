#
# Steer Makefile for afdsmgrd.
#
# by Dario Berzano <dario.berzano@gmail.com>
#
# This Makefile only requires the ROOTSYS environment variable to be set. This
# is only a "steer" Makefile where you set the variables: the real rules are in
# Makefiles contained into the proper subdirectories.
#

### BEGIN OF THE CUSTOMIZABLE SECTION ###

# Extra variables created by the configure script
include Makefile.vars

export ROOTCFG = $(ROOTSYS)/bin/root-config

export CXX = $(shell $(ROOTCFG) --cxx)
export LD = $(shell $(ROOTCFG) --ld)
export RCINT = $(ROOTSYS)/bin/rootcint
export OBJEXT = .o
export CXXEXT = .cc
export HEXT = .h

#export SVNREV = $(shell svnversion 2>/dev/null|cut -f2 -d:|tr -d '[:alpha:]')
export CXXFLAGS = $(shell $(ROOTCFG) --cflags) -g -DSVNREV=$(SVNREV)

export ROOTLIBS = $(shell $(ROOTCFG) --libs)
export EXTRALIBS = Proof

export PROG = afdsmgrd
export MODS = AfConfReader AfDataSetsManager AfDataSetSrc AfLog AfStageUrl

export MAIN = $(PROG)

export GENDICT = AfDataSetSrc AfStageUrl
export DICT = AfDict

export INSTALLPATH = $(PREFIX)/usr/bin

SRCDIR = src
ETCDIR = etc

### END OF THE CUSTOMIZABLE SECTION ###

.PHONY = all install uninstall

all:
	@$(MAKE) all -C $(SRCDIR) --no-print-directory

install:
	@echo "Copying program to $(BINPREFIX)..."
	@mkdir -p $(BINPREFIX)
	@cp $(SRCDIR)/$(PROG) $(BINPREFIX)/

	@echo "Copying startup script to $(ETCPREFIX)/init.d..."
	@mkdir -p $(ETCPREFIX)/init.d
	@cp $(ETCDIR)/init.d/$(PROG) $(ETCPREFIX)/init.d/ 2> /dev/null

# Custom sysconfig and example configuration are copied only if not installing
# via xrd-installer
ifeq ($(XRD_INSTALLER),0)
	@echo "Copying startup variables to $(ETCPREFIX)/sysconfig (doesn't overwrite)..."
	@mkdir -p $(ETCPREFIX)/sysconfig
	@yes n | cp -i $(ETCDIR)/sysconfig/$(PROG) $(ETCPREFIX)/sysconfig/ 2> /dev/null

	@echo "Copying example configuration to $(ETCPREFIX)/xrootd..."
	@mkdir -p $(ETCPREFIX)/xrootd
	@yes n | cp -i $(ETCDIR)/xrootd/$(PROG).cf.example $(ETCPREFIX)/xrootd/ 2> /dev/null
endif

uninstall:
	@echo "Uninstalling (custom settings will not be deleted)..."
	@rm -f $(BINPREFIX)/$(PROG) $(ETCPREFIX)/init.d/$(PROG) $(ETCPREFIX)/xrootd/$(PROG).cf.example

.DEFAULT:
	@$(MAKE) $@ -C $(SRCDIR) --no-print-directory
