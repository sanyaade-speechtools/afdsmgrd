#
# Real Makefile for afdsmgrd.
#
# by Dario Berzano <dario.berzano@gmail.com>
#
# This Makefile should be called by the main Makefile located in the project's
# root directory: please refer to it for the variables.
#

.PHONY: all clean install apmon

LIBS = $(ROOTLIBS) $(addprefix -l,$(EXTRALIBS))
OBJS = $(addsuffix $(OBJEXT),$(MODS))
OBJS += $(addsuffix $(OBJEXT),$(DICT))
GENDICTHEADERS = $(addsuffix $(HEXT),$(GENDICT))

# First target is the default target
all: $(PROG)

# Build ApMon
$(APMONDIR)/ApMon$(OBJEXT):
	@echo "Compiling builtin ApMon on $(APMONDIR)..."
	@cd $(APMONDIR) && ./configure --prefix=`pwd`/build > /dev/null 2>&1 && make > /dev/null 2>&1

# Program linking
$(PROG): $(APMONDIR)/ApMon$(OBJEXT) $(OBJS) $(MAIN)$(OBJEXT)
	@echo "Linking to $@..."
	@$(LD) -o $@ $(OBJS) $(addsuffix $(OBJEXT),$(APMONDIR)/*) $(MAIN)$(OBJEXT) $(LIBS)

# Generation of dictionaries
$(DICT)$(OBJEXT): $(GENDICTHEADERS)
	@echo "Generating dictionary for $(GENDICT)..."
	@LD_LIBRARY_PATH=$(LD_LIBRARY_PATH):$(ROOTSYS)/lib $(RCINT) -f $(DICT)$(CXXEXT) -c $(CXXFLAGS) -p $^
	@$(CXX) $(CXXFLAGS) -c -o $(DICT)$(OBJEXT) $(DICT)$(CXXEXT)

# Compilation of each source file
%$(OBJEXT): %$(CXXEXT) %$(HEXT)
	@echo "Compiling $@..."
	@$(CXX) $(CXXFLAGS) -c -o $@ $<

# Compilation of main executable
$(MAIN)$(OBJEXT): $(MAIN)$(CXXEXT)
	@echo "Compiling entry point $@..."
	@$(CXX) $(CXXFLAGS) -c -o $@ $<

# Cleaning up of compiled stuff (except ApMon)
clean:
	@echo "Cleaning up..."
	@rm -rf *$(OBJEXT) $(PROG)
	@rm -f $(DICT)$(CXXEXT) $(DICT)$(HEXT)

# Cleaning up of all compiled stuff, including ApMon
distclean: clean
	@echo "Cleaning up ApMon..."
	@cd $(APMONDIR) ; [ -e Makefile ] && make distclean > /dev/null
