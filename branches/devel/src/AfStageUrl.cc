#include "AfStageUrl.h"

#include "AfLog.h"

ClassImp(AfStageUrl);

AfStageUrl::AfStageUrl(const char *url, UInt_t dsId, StgStatus_t status) {
  fUrl = new TUrl(url);
  fStgStatus = status;
  const char *u = fUrl->GetUrl(); // may be different from the initial url...
  Int_t len = strlen(u);
  fStrHash = TString::Hash(u, len);  // once for all
  fFailures = 0;
  if (dsId != 0) fListDsIds.push_back(dsId);
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

Bool_t AfStageUrl::AddDsId(UInt_t dsId) {
  if (HasDsId(dsId)) return kFALSE;
  fListDsIds.push_back(dsId);
  return kTRUE;
}

Bool_t AfStageUrl::HasDsId(UInt_t dsId) {
  Int_t s = fListDsIds.size();
  for (Int_t i=0; i<s; i++) {
    if (fListDsIds[i] == dsId) {
      return kTRUE;
    }
  }
  return kFALSE;
}

Bool_t AfStageUrl::RemoveDsId(UInt_t dsId) {
  Int_t s = fListDsIds.size();
  Int_t idx = -1;
  for (Int_t i=0; i<s; i++) {
    if (fListDsIds[i] == dsId) {
      idx = i;
      break;
    }
  }

  if (idx != -1) {
    fListDsIds.erase( fListDsIds.begin()+idx );
    return kTRUE;  // element was present and removed
  }

  return kFALSE;  // element was not present
}
