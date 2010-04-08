# Makefile for afdsmgrd

# Note: making this program requires the ROOTSYS environment variable to be set

### BEGIN OF CUSTOMIZABLE PART ###

ROOTCFG = $(ROOTSYS)/bin/root-config

CXX = $(shell $(ROOTCFG) --cxx)
LD = $(shell $(ROOTCFG) --ld)
OBJEXT = .o

CXXFLAGS = $(shell $(ROOTCFG) --cflags) -g

ROOTLIBS = $(shell $(ROOTCFG) --libs)
EXTRALIBS = Proof

PROG = afdsmgrd
MODS = AfConfReader AfDataSetManager AfDataSetSrc AfLog

### END OF CUSTOMIZABLE PART ###

.PHONY: all clean

LIBS = $(ROOTLIBS) $(addprefix -l,$(EXTRALIBS))
OBJS = $(addsuffix $(OBJEXT),$(MODS))

all: $(PROG)

$(PROG): $(OBJS) main$(OBJEXT)
	@echo "Linking to $@..."
	@$(LD) -o $@ $(OBJS) main$(OBJEXT) $(LIBS)

%.o: %.cc %.h
	@echo "Compiling $@..."
	@$(CXX) $(CXXFLAGS) -c -o $@ $<

main.o: main.cc
	@echo "Compiling $@..."
	@$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	@echo "Cleaning up..."
	@rm -rf *.o $(PROG)

install: all
	@echo "Copying $(PROG) to $(ROOTSYS)/bin..."
	@cp $(PROG) $(ROOTSYS)/bin
