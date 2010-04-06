CXX = $(shell root-config --cxx)
LD = $(shell root-config --ld)

CXXFLAGS = $(shell root-config --cflags) -g

ROOTLIBS = $(shell root-config --libs) -lProof

all: afdsmgrd

clean:
	rm -f *.o *.d afdsmgrd

install: all
	cp afdsmgrd $(ROOTSYS)/bin

afdsmgrd: main.o AfConfReader.o AfDataSetManager.o AfDataSetSrc.o
	$(LD) -o afdsmgrd main.o AfConfReader.o AfDataSetManager.o AfDataSetSrc.o $(ROOTLIBS)

main.o: main.cc
	$(CXX) $(CXXFLAGS) -c -o main.o main.cc

AfConfReader.o: AfConfReader.cc AfConfReader.h
	$(CXX) $(CXXFLAGS) -c -o AfConfReader.o AfConfReader.cc

AfDataSetManager.o: AfDataSetManager.cc AfDataSetManager.h
	$(CXX) $(CXXFLAGS) -c -o AfDataSetManager.o AfDataSetManager.cc

AfDataSetSrc.o: AfDataSetSrc.cc AfDataSetSrc.h
	$(CXX) $(CXXFLAGS) -c -o AfDataSetSrc.o AfDataSetSrc.cc
