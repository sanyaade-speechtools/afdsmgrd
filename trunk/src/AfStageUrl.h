#ifndef AFSTAGEURL_H
#define AFSTAGEURL_H

#include <TUrl.h>

typedef enum { kStgDone = 'D', kStgFail = 'F', kStgQueue = 'Q',
  kStgStaging = 'S', kStgAbsent = 'A', kStgQueueFull = 'X' } StgStatus_t;

class AfStageUrl : public TObject {

  public:

    AfStageUrl(const char *url, StgStatus_t status = kStgQueue);
    StgStatus_t GetStageStatus() { return fStgStatus; };
    void SetStageStatus(StgStatus_t status) { fStgStatus = status; };
    virtual ULong_t Hash() const;
    const TUrl *GetUrlObj() { return fUrl; }
    TString GetUrl() const { return fUrl->GetUrl(); };
    virtual Bool_t IsEqual(const TObject* obj) const;
    virtual ~AfStageUrl();

  private:

    // Private variables
    StgStatus_t  fStgStatus;
    TUrl             *fUrl;

  // ROOT stuff
  ClassDef(AfStageUrl, 1);
};

#endif // AFSTAGEURL_H
