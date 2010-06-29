#include "AfStageUrl.h"

#include "AfLog.h"

ClassImp(AfStageUrl);

AfStageUrl::AfStageUrl(const char *url, StgStatus_t status) {
  fUrl = new TUrl(url);
  fStgStatus = status;
  const char *u = fUrl->GetUrl(); // may be different from the initial url...
  Int_t len = strlen(u);
  fStrHash = TString::Hash(u, len);  // once for all
}

AfStageUrl::~AfStageUrl() {
  delete fUrl;
}

Bool_t AfStageUrl::IsEqual(const TObject* obj) const {
  return ( dynamic_cast<const AfStageUrl *>(obj)->GetUrl() == GetUrl() );
}

ULong_t AfStageUrl::Hash() const {
  return fStrHash;
}
