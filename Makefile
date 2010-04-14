# Makefile for afdsmgrd
# The only requirement is the ROOTSYS environment variable

### BEGIN OF THE CUSTOMIZABLE SECTION ###

ROOTCFG = $(ROOTSYS)/bin/root-config

CXX = $(shell $(ROOTCFG) --cxx)
LD = $(shell $(ROOTCFG) --ld)
RCINT = $(ROOTSYS)/bin/rootcint
OBJEXT = .o
CXXEXT = .cc
HEXT = .h

CXXFLAGS = $(shell $(ROOTCFG) --cflags) -g

ROOTLIBS = $(shell $(ROOTCFG) --libs)
EXTRALIBS = Proof

PROG = afdsmgrd
MODS = AfConfReader AfDataSetManager AfDataSetSrc AfLog

MAIN = main

GENDICT = AfDataSetSrc
DICT = AfDict

### END OF THE CUSTOMIZABLE SECTION ###

.PHONY: all clean

LIBS = $(ROOTLIBS) $(addprefix -l,$(EXTRALIBS))
OBJS = $(addsuffix $(OBJEXT),$(MODS))
OBJS += $(addsuffix $(OBJEXT),$(DICT))
GENDICTHEADERS = $(addsuffix $(HEXT),$(GENDICT))

all: $(PROG)

$(PROG): $(OBJS) main$(OBJEXT)
	@echo "Linking to $@..."
	@$(LD) -o $@ $(OBJS) main$(OBJEXT) $(LIBS)

$(DICT)$(OBJEXT): $(GENDICTHEADERS)
	@echo "Generating dictionary for $(GENDICT)..."
	@LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(ROOTSYS)/lib $(RCINT) -f $(DICT)$(CXXEXT) -c $(CXXFLAGS) -p $^
	@$(CXX) $(CXXFLAGS) -c -o $(DICT)$(OBJEXT) $(DICT)$(CXXEXT)

%$(OBJEXT): %$(CXXEXT) %$(HEXT)
	@echo "Compiling $@..."
	@$(CXX) $(CXXFLAGS) -c -o $@ $<

$(MAIN)$(OBJEXT): $(MAIN)$(CXXEXT)
	@echo "Compiling $@..."
	@$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	@echo "Cleaning up..."
	@rm -rf *$(OBJEXT) $(PROG)
	@rm -f $(DICT)$(CXXEXT) $(DICT)$(HEXT)

install: all
	@echo "Copying $(PROG) to $(ROOTSYS)/bin..."
	@cp $(PROG) $(ROOTSYS)/bin

