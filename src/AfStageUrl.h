#ifndef AFSTAGEURL_H
#define AFSTAGEURL_H

#include "AfConfigure.h"

#include <TUrl.h>

#include <vector>

typedef enum { kStgDone = 'D', kStgFail = 'F', kStgQueue = 'Q',
  kStgStaging = 'S', kStgAbsent = 'A', kStgQueueFull = 'X',
  kStgDelPending = 'P', kStgCopyQueued = 'C', kStgAlreadyQueued = 'Z',
  kStgDeqLast = 'X', kStgDeqLeft = 'Y', kStgNoSuchId = 'N' } StgStatus_t;

class AfStageUrl : public TObject {

  public:

    AfStageUrl(const char *url, UInt_t dsId = 0, StgStatus_t status = kStgQueue);
    StgStatus_t GetStageStatus() { return fStgStatus; }
    void SetStageStatus(StgStatus_t status) { fStgStatus = status; }
    virtual ULong_t Hash() const;
    const TUrl *GetUrlObj() { return fUrl; }
    TString GetUrl() const { return fUrl->GetUrl(); }
    virtual Bool_t IsEqual(const TObject* obj) const;
    virtual void ResetFailures() { fFailures = 0; }
    virtual Int_t IncreaseFailures() { return ++fFailures; }
    virtual Int_t GetFailures() { return fFailures; }
    virtual Bool_t AddDsId(UInt_t dsId);
    virtual Bool_t HasDsId(UInt_t dsId);
    virtual Bool_t RemoveDsId(UInt_t dsId);
    virtual Int_t GetNDs() { return fListDsIds.size(); }
    virtual ~AfStageUrl();

  private:

    // Private variables
    StgStatus_t          fStgStatus;
    TUrl                *fUrl;
    UInt_t               fStrHash;
    UInt_t               fFailures;
    std::vector<UInt_t>  fListDsIds;

  // ROOT stuff
  ClassDef(AfStageUrl, 1);
};

#endif // AFSTAGEURL_H
