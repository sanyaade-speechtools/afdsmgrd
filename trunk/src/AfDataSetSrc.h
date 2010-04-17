#ifndef AFDATASETSRC_H
#define AFDATASETSRC_H

#include "AfLog.h"

#include <THashList.h>
#include <TDataSetManagerFile.h>
#include <TFileStager.h>
#include <TFileCollection.h>
#include <TFileInfo.h>
#include <TUrl.h>

class AfDataSetSrc : public TObject {

  public:

    AfDataSetSrc();
    AfDataSetSrc(const char *url, TUrl *redirUrl, const char *opts,
      Bool_t suid);
    void Process(Bool_t resetBits = kFALSE);
    void SetDsProcessList(TList *dsList);
    ~AfDataSetSrc();

  private:

    // Private methods
    void  FlattenListOfDataSets();
    void  ProcessDataSet(const char *uri);
    void  ResetDataSet(const char *uri);
    void  ListDataSetContent(const char *uri, Bool_t debug = kFALSE);
    Int_t TranslateUrl(TFileInfo *ti,
      Int_t whichUrls = kTranslateROOT | kTranslateAliEn);
    Int_t KeepOnlyFirstUrl(TFileInfo *fi);
    Int_t PutIntoStageQueue();
    void  DoSuid();
    void  UndoSuid();

    // Private variables
    TDataSetManagerFile *fManager;
    TList               *fDsUris;
    TUrl                *fRedirUrl;
    TList               *fToStage;
    TString              fUrl;
    TString              fOpts;
    Bool_t               fSuid;
    Int_t                fUnpUid;
    Int_t                fUnpGid;
    TList               *fDsToProcess;

    // Some constants
    static const Int_t   kTranslateROOT  = 1;
    static const Int_t   kTranslateAliEn = 2;

  // ROOT stuff
  ClassDef(AfDataSetSrc, 1);
};

#endif // AFDATASETSRC_H
