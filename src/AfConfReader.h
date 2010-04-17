#ifndef AFCONFREADER_H
#define AFCONFREADER_H

#include <regex.h>

#include "AfLog.h"

#include <Riostream.h>
#include <TList.h>
#include <TObjString.h>
#include <TMap.h>

class AfConfReader {

  public:

    ~AfConfReader();
    TString *GetVar(const char *vn);
    TString *GetDir(const char *dn);
    TList *GetDirs(const char *dn);
    static AfConfReader *Open(const char *cffn, Bool_t subVars = kTRUE);
    void PrintVarsAndDirs();

  private:

    // Private methods
    AfConfReader();  // constructor is private
    void SubstVar();
    void ReadConf();

    // Private variables
    ifstream    fFp;
    TList     *fDirs;
    TMap      *fVars;

};

#endif // AFCONFREADER_H
