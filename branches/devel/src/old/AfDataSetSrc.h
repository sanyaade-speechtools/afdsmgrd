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
    virtual const char *GetUri() const { return fUri.Data(); }
    virtual UInt_t GetUId() const { return fUId; }
    virtual Bool_t IsEqual(const TObject *r) const {
      return (fUri == dynamic_cast<const AfDsUri *>(r)->fUri);
    }

  private:
    TString fUri;
    UInt_t  fUId;

};

// Helper class that wraps a PERL regexp into a TObject, so that it becomes
// collectable in a TList
class AfDsMatch : public TObject {

  public:
    AfDsMatch() : fRegEx(NULL), fStr("") {}
    AfDsMatch(const TString &s, Bool_t isRe) : fStr(s) {
      if (isRe) fRegEx = new TPMERegexp(s);
      else fRegEx = NULL;
    }
    virtual ~AfDsMatch() { if (fRegEx) delete fRegEx; }
    TPMERegexp &GetRegEx() const { return *fRegEx; }
    const TString &GetPattern() const { return fStr; }
    Bool_t IsRegEx() const { return (fRegEx != NULL); }
    Bool_t Match(const char *s) const {
      if (fRegEx) return (fRegEx->Match(s) > 0);
      return (s == fStr);
    }

  private:
    TPMERegexp *fRegEx;
    TString    fStr;

};

class AfDataSetSrc : public TObject {

  public:

    AfDataSetSrc();
    AfDataSetSrc(const char *url, TUrl *redirUrl, const char *opts,
      const char *dsConf, AfDataSetsManager *parentManager);
    Int_t Process(DsAction_t action);
    void SetDsProcessList(TList *dsList);
    ~AfDataSetSrc();

  private:

    // URL type enum (implicit conversion to Int_t)
    typedef enum { kUrlRoot = 1, kUrlAliEn = 2 } UrlType_t;
    enum { kActIgnore = (1 << 4) - 1, kActTranslate = (1 << 3),
      kActStage = (1 << 2), kActVerify = (1 << 1), kActAddEndUrl = 1 };

    // Private methods
    void   FlattenListOfDataSets();
    void   ReadDsActionsConf();
    UInt_t ParseActionsToBits(TString &act, TString *name = NULL);
    Int_t  ProcessDataSet(AfDsUri *dsUri);
    Int_t  ResetDataSet(const char *uri);
    Int_t  Sync();
    void   ListDataSetContent(const char *uri, const char *header, Bool_t debug);
    Int_t  TranslateUrl(TFileInfo *ti, Int_t whichUrls = kUrlRoot | kUrlAliEn);
    Int_t  KeepOnlyLastUrl(TFileInfo *fi);
    Bool_t AddRealUrlAndMetaData(TFileInfo *fi, Bool_t addEndUrl,
      Bool_t addMeta);
    AfDsMatch *GetActionsForDs(AfDsUri *dsUri);
    static Bool_t AliEnConnect();


    // Private variables
    TDataSetManagerFile *fManager;
    TList               *fDsUris;
    TUrl                *fRedirUrl;
    TString              fUrl;
    TString              fOpts;
    TUrl                 fSyncUrl;
    TString              fActionsConf;
    TList               *fDsToProcess;
    TList               *fActions;
    AfDsMatch            fDefaultAction;
    AfDataSetsManager   *fParentManager;  //!

  // ROOT stuff
  ClassDef(AfDataSetSrc, 1);
};

#endif // AFDATASETSRC_H
