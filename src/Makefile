#
# Real Makefile for afdsmgrd.
#
# by Dario Berzano <dario.berzano@gmail.com>
#
# This Makefile should be called by the main Makefile located in the project's
# root directory: please refer to it for the variables.
#

.PHONY: all clean install

LIBS = $(ROOTLIBS) $(addprefix -l,$(EXTRALIBS))
OBJS = $(addsuffix $(OBJEXT),$(MODS))
OBJS += $(addsuffix $(OBJEXT),$(DICT))
GENDICTHEADERS = $(addsuffix $(HEXT),$(GENDICT))

# First target is the default target
all: $(PROG)

$(PROG): $(OBJS) $(MAIN)$(OBJEXT)
	@echo "Linking to $@..."
	@$(LD) -o $@ $(OBJS) $(MAIN)$(OBJEXT) $(LIBS)

$(DICT)$(OBJEXT): $(GENDICTHEADERS)
	@echo "Generating dictionary for $(GENDICT)..."
	@LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(ROOTSYS)/lib $(RCINT) -f $(DICT)$(CXXEXT) -c $(CXXFLAGS) -p $^
	@$(CXX) $(CXXFLAGS) -c -o $(DICT)$(OBJEXT) $(DICT)$(CXXEXT)

%$(OBJEXT): %$(CXXEXT) %$(HEXT)
	@echo "Compiling $@..."
	@$(CXX) $(CXXFLAGS) -c -o $@ $<

$(MAIN)$(OBJEXT): $(MAIN)$(CXXEXT)
	@echo "Compiling entry point $@..."
	@$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	@echo "Cleaning up..."
	@rm -rf *$(OBJEXT) $(PROG)
	@rm -f $(DICT)$(CXXEXT) $(DICT)$(HEXT)

install: all
	@echo "Installing $(PROG) to $(PREFIX)..."
	@mkdir -p $(INSTALLPATH)
	@cp $(PROG) $(INSTALLPATH)
	@cp ../etc/afdsmgrd_start_stop $(PREFIX)/etc/init.d/afdsmgrd
	@cp ../etc/afdsmgrd_sysconfig $(PREFIX)/etc/sysconfig/afdsmgrd

uninstall:
	@echo "Removing installed binary and scripts from $(PREFIX)..."
	@rm -f $(INSTALLPATH)/$(PROG)
	@rm -f $(PREFIX)/etc/init.d/afdsmgrd
	@rm -f $(PREFIX)/etc/sysconfig/afdsmgrd
