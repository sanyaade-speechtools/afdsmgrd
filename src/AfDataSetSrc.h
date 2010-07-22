#ifndef AFDATASETSRC_H
#define AFDATASETSRC_H

typedef enum { kDsReset, kDsProcess } DsAction_t;

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

// Reciprocal inclusions between AfDataSetSrc and AfDataSetsManager require the
// following forward declaration
class AfDataSetsManager;

// Helper class that contains a dataset URI and its Unique ID (calculated as you
// like
class AfDsUri : public TObject {

  public:
    AfDsUri() : fUri(""), fUId(0x0) {}
    AfDsUri(const char *uri, UInt_t hash) : fUri(uri), fUId(hash) {};
    const char *GetUri() { return fUri.Data(); }
    UInt_t GetUId() { return fUId; }

  private:
    TString fUri;
    UInt_t  fUId;

};

class AfDataSetSrc : public TObject {

  public:

    AfDataSetSrc();
    AfDataSetSrc(const char *url, TUrl *redirUrl, const char *opts,
      AfDataSetsManager *parentManager);
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
    TList               *fDsToProcess;
    AfDataSetsManager   *fParentManager;  //!

  // ROOT stuff
  ClassDef(AfDataSetSrc, 1);
};

#endif // AFDATASETSRC_H
