#ifndef AFDATASETSMANAGER_H
#define AFDATASETSMANAGER_H

#include "AfConfigure.h"
#include "AfConfReader.h"
#include "AfDataSetSrc.h"
#include "AfStageUrl.h"
#include "AfLog.h"

#include <TThread.h>
#include <TObjArray.h>
#include <TObjString.h>
#include <TObjectTable.h>

#ifdef WITH_APMON
#include "ApMon.h"
#endif // WITH_APMON

class AfDataSetsManager {

  public:

    AfDataSetsManager();
    ~AfDataSetsManager();
    Int_t  ProcessAllDataSetsOnce(DsAction_t action);
    void   Loop();
    void   Reset();
    Bool_t ReadConf(const char *cf);
    void   SetSuid(bool suid = kTRUE) { fSuid = suid; }

    StgStatus_t GetStageStatus(const char *url);
    StgStatus_t EnqueueUrl(const char *url);
    StgStatus_t DequeueUrl(const char *url);
    void ProcessTransferQueue();
    void PrintStageList(const char *header, Bool_t debug);
    void SetBinPrefix(const char *prefix);

    void NotifyDataSetStatus(const char *dsName, Float_t pctStaged,
      Float_t pctCorrupted);

  private:

    // Private methods
    static void *Stage(void *args);
    void CreateApMon(TUrl *monUrl);

    // Private variables
    static const Int_t  kDefaultParallelXfrs = 1;
    static const Int_t  kDefaultLoopSleep_s = 3600;
    static const Int_t  kDefaultScanDsEvery = 10;
    static const Int_t  kDefaultMaxFilesInQueue = 0;
    TString             kDefaultApMonDsPrefix;

    TList              *fSrcList;
    Bool_t              fSuid;
    TString             fRedirHost;
    UShort_t            fRedirPort;
    Int_t               fLoopSleep_s;
    Int_t               fScanDsEvery;
    TString             fStageCmd;
    Int_t               fParallelXfrs;

    THashList          *fStageQueue;
    TList              *fStageCmds;

    Int_t               fLastQueue;
    Int_t               fLastStaging;
    Int_t               fLastFail;
    Int_t               fLastDone;
    Int_t               fMaxFilesInQueue;

    TString             fApMonDsPrefix;
    TString             fBinPrefix;

    #ifdef WITH_APMON
    ApMon              *fApMon;
    #endif // WITH_APMON
};

#endif // AFDATASETSMANAGER_H
