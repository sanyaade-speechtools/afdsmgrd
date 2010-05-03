#include "AfStageUrl.h"

#include "AfLog.h"

ClassImp(AfStageUrl);

AfStageUrl::AfStageUrl(const char *url, StgStatus_t status) {
  fUrl = new TUrl(url);
  fStgStatus = status;
}

AfStageUrl::~AfStageUrl() {
  delete fUrl;
}

ULong_t AfStageUrl::Hash() const {
  TString s = fUrl->GetUrl();
  return (ULong_t)s.Hash();
}

Bool_t AfStageUrl::IsEqual(const TObject* obj) const {
  return ( dynamic_cast<const AfStageUrl *>(obj)->GetUrl() == GetUrl() );
}
