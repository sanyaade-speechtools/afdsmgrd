#ifndef AFDATASETSMANAGER_H
#define AFDATASETSMANAGER_H

#include "AfConfReader.h"
#include "AfDataSetSrc.h"
#include "AfStageUrl.h"
#include "AfLog.h"

#include <TThread.h>
#include <TObjArray.h>
#include <TObjString.h>

class AfDataSetsManager {

  public:

    AfDataSetsManager();
    ~AfDataSetsManager();
    void   Loop();
    void   Reset();
    Bool_t ReadConf(const char *cf);
    void   SetSuid(bool suid = kTRUE) { fSuid = suid; }

    StgStatus_t GetStageStatus(const char *url);
    Bool_t EnqueueUrl(const char *url);
    Bool_t DequeueUrl(const char *url);
    void ProcessTransferQueue();
    void PrintStageList();

  private:

    // Private methods
    static void *Stage(void *args);

    // Private variables
    static const Int_t  kDefaultParallelXfrs = 1;
    static const Int_t  kDefaultLoopSleep_s = 3600;
    static const Int_t  kDefaultScanDsEvery = 10;

    TList              *fSrcList;
    Bool_t              fSuid;
    TString             fRedirHost;
    UShort_t            fRedirPort;
    Int_t               fLoopSleep_s;
    Int_t               fScanDsEvery;
    TString             fStageCmd;
    Int_t               fParallelXfrs;

    TList              *fStageQueue;
    TList              *fStageCmds;
};

#endif // AFDATASETSMANAGER_H
