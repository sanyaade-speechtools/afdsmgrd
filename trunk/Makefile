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

export ROOTCFG = $(ROOTSYS)/bin/root-config

export CXX = $(shell $(ROOTCFG) --cxx)
export LD = $(shell $(ROOTCFG) --ld)
export RCINT = $(ROOTSYS)/bin/rootcint
export OBJEXT = .o
export CXXEXT = .cc
export HEXT = .h

export SVNREV = $(shell svnversion 2>/dev/null|cut -f2 -d:|tr -d '[:alpha:]')
export CXXFLAGS = $(shell $(ROOTCFG) --cflags) -g -DSVNREV=$(SVNREV)

export ROOTLIBS = $(shell $(ROOTCFG) --libs)
export EXTRALIBS = Proof

export PROG = afdsmgrd
export MODS = AfConfReader AfDataSetsManager AfDataSetSrc AfLog AfStageUrl

export MAIN = $(PROG)

export GENDICT = AfDataSetSrc AfStageUrl
export DICT = AfDict

export INSTALLPATH = $(PWD)/bin

SRCDIR = src

### END OF THE CUSTOMIZABLE SECTION ###

.PHONY = all

all:
	@$(MAKE) all -C $(SRCDIR) --no-print-directory

.DEFAULT:
	@$(MAKE) $@ -C $(SRCDIR) --no-print-directory
