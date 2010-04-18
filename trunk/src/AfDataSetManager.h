#ifndef AFDATASETMANAGER_H
#define AFDATASETMANAGER_H

#include "AfConfReader.h"
#include "AfDataSetSrc.h"
#include "AfLog.h"

class AfDataSetManager {

  public:

    AfDataSetManager();
    ~AfDataSetManager();
    void   Loop(UInt_t nLoops = 0);
    Bool_t ReadConf(const char *cf);
    void   SetSuid(bool suid = kTRUE)          { fSuid = suid; }
    void   SetResetDataSets(bool suid = kTRUE) { fReset = suid; }

  private:

    // Private methods

    // Private variables
    TList              *fSrcList;
    Bool_t              fSuid;
    Bool_t              fReset;
    TString             fRedirHost;
    UShort_t            fRedirPort;
    Int_t               fLoopSleep_s;
    static const Int_t  fDefaultLoopSleep_s = 3600;
};

#endif // AFDATASETMANAGER_H
