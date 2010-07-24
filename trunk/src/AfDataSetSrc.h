#ifndef AFDATASETSRC_H
#define AFDATASETSRC_H

typedef enum { kDsReset, kDsProcess, kDsSync } DsAction_t;

#include "AfConfigure.h"
#include "AfLog.h"
#include "AfDataSetsManager.h"

#include <THashList.h>
#include <TDataSetManagerFile.h>
#include <TFileStager.h>
#include <TFileCollection.h>
#include <TFileInfo.h>
#include <TUrl.h>
#include <TFile.h>
#include <TKey.h>
#include <TTree.h>
#include <TGrid.h>
#include <TGridResult.h>

// Reciprocal inclusions between AfDataSetSrc and AfDataSetsManager require the
// following forward declaration
class AfDataSetsManager;

// Helper class that contains a dataset URI and its Unique ID (calculated as you
// like
class AfDsUri : public TObject {

  public:
    AfDsUri() : fUri(""), fUId(0x0) {}
    AfDsUri(const char *uri, UInt_t uid = 0x0) : fUri(uri), fUId(uid) {};
    virtual const char *GetUri() { return fUri.Data(); }
    virtual UInt_t GetUId() { return fUId; }
    virtual Bool_t IsEqual(const TObject *r) const {
      return (fUri == dynamic_cast<const AfDsUri *>(r)->fUri);
    }

  private:
    TString fUri;
    UInt_t  fUId;

};

class AfDataSetSrc : public TObject {

  public:

    AfDataSetSrc();
    AfDataSetSrc(const char *url, TUrl *redirUrl, const char *opts,
      const char *syncUrl, AfDataSetsManager *parentManager);
    Int_t Process(DsAction_t action);
    void SetDsProcessList(TList *dsList);
    ~AfDataSetSrc();

  private:

    // URL type enum (implicit conversion to Int_t)
    typedef enum { kUrlRoot = 1, kUrlAliEn = 2 } UrlType_t;

    // Private methods
    void   FlattenListOfDataSets();
    Int_t  ProcessDataSet(AfDsUri *dsUri);
    Int_t  ResetDataSet(const char *uri);
    Int_t  Sync();
    void   ListDataSetContent(const char *uri, const char *header, Bool_t debug);
    Int_t  TranslateUrl(TFileInfo *ti, Int_t whichUrls = kUrlRoot | kUrlAliEn);
    Int_t  KeepOnlyLastUrl(TFileInfo *fi);
    Bool_t AddRealUrlAndMetaData(TFileInfo *fi);

    // Private variables
    TDataSetManagerFile *fManager;
    TList               *fDsUris;
    TUrl                *fRedirUrl;
    TString              fUrl;
    TString              fOpts;
    TUrl                 fSyncUrl;
    TList               *fDsToProcess;
    AfDataSetsManager   *fParentManager;  //!

  // ROOT stuff
  ClassDef(AfDataSetSrc, 1);
};

#endif // AFDATASETSRC_H
