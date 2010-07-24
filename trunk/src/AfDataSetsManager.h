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

const Int_t kRequeueNotFound = -1;
const Int_t kRequeueLimitReached = -2;

class AfDataSetsManager {

  public:

    AfDataSetsManager();
    ~AfDataSetsManager();
    Int_t ProcessAllDataSetsOnce(DsAction_t action);
    void Loop();
    void Reset();
    Bool_t ReadConf(const char *cf);
    void SetSuid(bool suid = kTRUE) { fSuid = suid; }

    StgStatus_t GetStageStatus(const char *url);
    StgStatus_t EnqueueUrl(const char *url, UInt_t uid, Int_t *nQueue = NULL);
    StgStatus_t DequeueUrl(const char *url, UInt_t uid, Int_t *nQueue = NULL);
    Int_t       RequeueUrl(const char *url, UInt_t uid, Bool_t hasFailed);

    void ProcessTransferQueue();
    void PrintStageList(const char *header, Bool_t debug);
    void SetBinPrefix(const char *prefix);

    void DoSuid();
    void UndoSuid();

    void NotifyDataSetStatus(UInt_t uniqueId, const char *dsName, Int_t nFiles,
      Int_t nStaged, Int_t nCorrupted, const char *treeName, Int_t nEvts,
      Int_t totalSizeBytes);

  private:

    // Private methods
    static void *Stage(void *args);
    void CreateApMon(TUrl *monUrl);

    // Private variables
    static const Int_t  kDefaultParallelXfrs = 1;
    static const Int_t  kDefaultLoopSleep_s = 3600;
    static const Int_t  kDefaultScanDsEvery = 10;
    static const Int_t  kDefaultMaxFilesInQueue = 0;
    static const Int_t  kDefaultMaxFails = 0;
    TString             kDefaultApMonDsPrefix;

    TList              *fSrcList;
    Bool_t              fSuid;
    Int_t               fUnpUid;
    Int_t               fUnpGid;

    TString             fRedirHost;
    UShort_t            fRedirPort;
    Int_t               fLoopSleep_s;
    Int_t               fScanDsEvery;
    Int_t               fSyncDsEvery;
    TString             fStageCmd;
    Int_t               fParallelXfrs;

    THashList          *fStageQueue;
    THashList          *fStageCmds;

    Int_t               fLastQueue;
    Int_t               fLastStaging;
    Int_t               fLastFail;
    Int_t               fLastDone;
    Int_t               fMaxFilesInQueue;

    Int_t               fDsNotifCounter;
    Int_t               fMaxFails;

    TString             fApMonDsPrefix;
    TString             fBinPrefix;

    #ifdef WITH_APMON
    ApMon              *fApMon;
    #endif // WITH_APMON
};

#endif // AFDATASETSMANAGER_H
