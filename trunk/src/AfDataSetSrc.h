#ifndef AFDATASETSRC_H
#define AFDATASETSRC_H

typedef enum { kDsReset, kDsProcess } DsAction_t;

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

class AfDataSetSrc : public TObject {

  public:

    AfDataSetSrc();
    AfDataSetSrc(const char *url, TUrl *redirUrl, const char *opts,
      Bool_t suid, AfDataSetsManager *parentManager);
    void Process(DsAction_t action);
    void SetDsProcessList(TList *dsList);
    ~AfDataSetSrc();

  private:

    // URL type enum (implicit conversion to Int_t)
    typedef enum { kUrlRoot = 1, kUrlAliEn = 2 } UrlType_t;

    // Private methods
    void   FlattenListOfDataSets();
    void   ProcessDataSet(const char *uri);
    void   ResetDataSet(const char *uri);
    void   VerifyDataSet(const char *uri);
    void   ListDataSetContent(const char *uri, const char *header, Bool_t debug);
    Int_t  TranslateUrl(TFileInfo *ti, Int_t whichUrls = kUrlRoot | kUrlAliEn);
    Int_t  KeepOnlyLastUrl(TFileInfo *fi);
    Bool_t AddRealUrlAndMetaData(TFileInfo *fi);
    void   DoSuid();
    void   UndoSuid();

    // Private variables
    TDataSetManagerFile *fManager;
    TList               *fDsUris;
    TUrl                *fRedirUrl;
    TString              fUrl;
    TString              fOpts;
    Bool_t               fSuid;
    Int_t                fUnpUid;
    Int_t                fUnpGid;
    TList               *fDsToProcess;
    AfDataSetsManager   *fParentManager;  //!

  // ROOT stuff
  ClassDef(AfDataSetSrc, 1);
};

#endif // AFDATASETSRC_H