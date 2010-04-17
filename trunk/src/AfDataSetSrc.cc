#include "AfDataSetSrc.h"

ClassImp(AfDataSetSrc);

AfDataSetSrc::AfDataSetSrc() {}

AfDataSetSrc::AfDataSetSrc(const char *url, TUrl *redirUrl, const char *opts,
  Bool_t suid) {

  fUrl  = url;
  fOpts = opts;
  fSuid = suid;
  fUnpUid = 0;
  fUnpGid = 0;
  fRedirUrl = redirUrl;

  fDsToProcess = new TList();
  fDsToProcess->SetOwner();
  fDsToProcess->Add( new TObjString("/*/*") );

  fManager = new TDataSetManagerFile(NULL, NULL, Form("dir:%s opt:%s",
    fUrl.Data(), fOpts.Data()) );

  fDsUris = new TList();  // TList of TObjString
  fDsUris->SetOwner();

  fToStage = new TList();  // TList of TFileInfo, NOT OWNER
}

AfDataSetSrc::~AfDataSetSrc() {
  delete fManager;
  delete fRedirUrl;
  delete fDsToProcess;
  delete fDsUris;
  delete fToStage;
}

// Processes all the datasets in this dataset source
void AfDataSetSrc::Process(Bool_t resetBits) {

  AfLogInfo("+++ Started processing of dataset source %s +++", fUrl.Data());

  // Creates a flattened list of ds uris
  FlattenListOfDataSets();

  // Clears the list of files to stage this time
  fToStage->Clear();

  TIter i(fDsUris);
  TObjString *s;
  while (s = dynamic_cast<TObjString *>(i.Next())) {
    if (resetBits) {
      ResetDataSet( s->GetString().Data() );
    }
    else {
      ProcessDataSet( s->GetString().Data() );
    }
  }

  // Processes the staging queue by using the appropriate method (AsyncOpen or
  // simply Stage) <-- TODO!! Only stage works for now
  Int_t nQueued = PutIntoStageQueue();
  AfLogInfo("+++ %d file(s) put into stage queue for dataset source %s +++",
    nQueued, fUrl.Data());

}

Int_t AfDataSetSrc::PutIntoStageQueue() {

  TUrl stagerUrl( Form("%s://%s:%d",
    fRedirUrl->GetProtocol(), fRedirUrl->GetHost(), fRedirUrl->GetPort()) );

  TFileStager *stager = TFileStager::Open( stagerUrl.GetUrl() );

  if (fToStage->GetSize() == 0) {
    //AfLogInfo("No files need to be staged");
    return 0;
  }

  if (!stager) {
    AfLogError("Can't open the data stager for redirector %s",
      stagerUrl.GetUrl());
    return -1;
  }
  //else {
  //  AfLogOk("Stager instantiated");
  //}

  Int_t nQueued = 0;
  TIter i(fToStage);
  TFileInfo *fi;

  while ( fi = dynamic_cast<TFileInfo *>(i.Next()) ) {
    TUrl url( *(fi->GetCurrentUrl()) );
    url.SetAnchor("");  // we want to stage the full archive
    Bool_t res = stager->Stage( url.GetUrl() );
    AfLogInfo(">> Put into staging queue %s (stager returned %s)", url.GetUrl(),
      (res ? "true" : "false"));
    nQueued++;
  }

  delete stager;

  return nQueued;
}

void AfDataSetSrc::ListDataSetContent(const char *uri, Bool_t debug) {
  TFileCollection *fc = fManager->GetDataSet(uri);
  TFileInfo *fi;
  TIter i( fc->GetList() );

  while (fi = dynamic_cast<TFileInfo *>(i.Next())) {
    Bool_t isStaged = fi->TestBit(TFileInfo::kStaged);
    Bool_t isCorrupted = fi->TestBit(TFileInfo::kCorrupted);
    if (debug) {
      AfLogDebug(">> %c%c | %s",
        (isStaged ? 'S' : 's'),
        (isCorrupted ? 'C' : 'c'),
        fi->GetCurrentUrl()->GetUrl()
      );
    }
    else {
      AfLogInfo(">> %c%c | %s",
        (isStaged ? 'S' : 's'),
        (isCorrupted ? 'C' : 'c'),
        fi->GetCurrentUrl()->GetUrl()
      );
    }
  }
}

void AfDataSetSrc::ResetDataSet(const char *uri) {

  AfLogDebug("Dataset %s before reset:", uri);
  ListDataSetContent(uri, kTRUE);

  TFileCollection *fc = fManager->GetDataSet(uri);

  // Reset "staged" and "corrupted" bits
  fc->ResetBitAll( TFileInfo::kStaged );
  fc->ResetBitAll( TFileInfo::kCorrupted );
  fc->Update();

  // Loop to restore only root:// URIs
  TFileInfo *fi;
  TIter i( fc->GetList() );

  Int_t countChanged = 0;

  while (fi = dynamic_cast<TFileInfo *>(i.Next())) {
    KeepOnlyFirstUrl( fi );
    countChanged += TranslateUrl( fi, AfDataSetSrc::kTranslateROOT );
  }

  // Save the modified dataset
  TString group, user, dsName;
  fManager->ParseUri(uri, &group, &user, &dsName);
  DoSuid();
  Int_t r = fManager->WriteDataSet(group, user, dsName, fc, \
    TDataSetManager::kFileMustExist);
  UndoSuid();
  if (r == 0) {
    AfLogError("Dataset reset: %s, but can't write (check permissions)", uri);
  }
  else {
    AfLogOk("Dataset reset: %s", uri);
  }

  AfLogDebug("Dataset %s after reset:", uri);
  ListDataSetContent(uri, kTRUE);
}

void AfDataSetSrc::ProcessDataSet(const char *uri) {

  AfLogDebug("Dataset %s before processing:", uri);
  ListDataSetContent(uri, kTRUE);

  TFileCollection *fc = fManager->GetDataSet(uri);

  // Preliminar verification of dataset: for marking as staged files that
  // already are

  // Return code of ScanDataSet:
  //
  // Error conditions:
  //  * -1 : dataset not found
  //  * -2 : dataset can not be written after verification
  //
  // Success conditions:
  //  *  1 : dataset was not changed
  //  *  2 : dataset was changed

  // Please note that ScanDataSet has two overloaded methods: this one does not
  // write automatically the dataset upon verification, while the other one does

  // ScanDataSet needs to write the results, so elevate permissions here
  DoSuid();
  Bool_t newVerified = (fManager->ScanDataSet(fc, TDataSetManager::kReopen)==2);
  UndoSuid();

  // TODO: remove kReopen to speedup, it is not useful; if problems, use '-r'
  // for reset

  // ScanDataSet actually modifies the list
  TFileInfo *fi;
  TIter i( fc->GetList() );

  Int_t countChanged = 0;

  while (fi = dynamic_cast<TFileInfo *>(i.Next())) {
    Bool_t isStaged = fi->TestBit(TFileInfo::kStaged);
    Bool_t isCorrupted = fi->TestBit(TFileInfo::kCorrupted);
    if (!isStaged) {
      Int_t ch = TranslateUrl( fi );  // ch is nonzero if the object contains one
                                    // or more files that can be staged, this
                                    // means root or alien urls: staging should
                                    // not be triggered on incompatible urls
      if (ch) {
        //AfLogWarning("FILE %s NEEDS TO BE STAGED",
        //  fi->GetCurrentUrl()->GetUrl());

        // File is added into staging queue
        fToStage->Add( fi );
      }
      countChanged += ch;
    }
  }

  // Save the modified dataset
  if ((countChanged) || (newVerified)) {
    TString group, user, dsName;
    fManager->ParseUri(uri, &group, &user, &dsName);

    DoSuid();
    // kFileMustExist saves ds only if it already exists: it updates it
    Int_t r = fManager->WriteDataSet(group, user, dsName, fc, \
      TDataSetManager::kFileMustExist);
    UndoSuid();

    if (r == 0) {
      AfLogError("Dataset modif: %s, but can't write (check permissions)", uri);
    }
    else {
      AfLogOk("Dataset saved: %s", uri);
    }
  }
  else {
    AfLogInfo("Dataset unmod: %s", uri);
  }

  AfLogDebug("Dataset %s after processing:", uri);
  ListDataSetContent(uri, kTRUE);
}

// Returns number of urls removed
Int_t AfDataSetSrc::KeepOnlyFirstUrl(TFileInfo *fi) {

  TList *urlList = new TList();
  urlList->SetOwner();
  Int_t count = 0;
  TUrl *url;

  fi->ResetUrl();

  while (url = fi->NextUrl()) {
    if (count++ > 0) {
      urlList->Add( new TObjString(url->GetUrl()) );
    }
  }

  TIter i(urlList);
  TObjString *s;
  while ( s = dynamic_cast<TObjString *>(i.Next()) ) {
    fi->RemoveUrl( s->GetString().Data() );
  }

  fi->ResetUrl();

  delete urlList;
}

Int_t AfDataSetSrc::TranslateUrl(TFileInfo *fi, Int_t whichUrls) {

  // TODO: whichUrls not implemented yet

  fi->ResetUrl();
  TUrl *url;
  Int_t countChanged = 0;

  Bool_t doAliEn = kTranslateAliEn & whichUrls;
  Bool_t doROOT = kTranslateROOT & whichUrls;

  while (url = fi->NextUrl()) {

    // We are staging on a xrootd pool
    if ( strcmp(fRedirUrl->GetProtocol(), "root") == 0 ) {

      if (( strcmp(url->GetProtocol(), "alien") == 0 ) && (doAliEn)) {
        url->SetProtocol( fRedirUrl->GetProtocol() );
        url->SetFile( Form("%s%s", fRedirUrl->GetFile(), url->GetFile()) );
        url->SetHost( fRedirUrl->GetHost() );
        url->SetPort( fRedirUrl->GetPort() );
        //AfLogInfo(">>>> [A] Changed: %s", url->GetUrl());
        countChanged++;
      }
      else if (( strcmp(url->GetProtocol(), "root") == 0 ) && (doROOT)) {
        url->SetHost( fRedirUrl->GetHost() );
        url->SetPort( fRedirUrl->GetPort() );
        //AfLogInfo(">>>> [R] Changed: %s", url->GetUrl());
        countChanged++;
      }

    }
    else {
      AfLogWarning("Can't change URL %s: redirector protocol \"%s\" is not" \
        "supported, only xrootd protocol is", url->GetUrl(),
        fRedirUrl->GetProtocol());
    }

  }

  fi->ResetUrl();

  return countChanged;
}

void AfDataSetSrc::SetDsProcessList(TList *dsList) {
  TIter i(dsList);
  TObjString *o;
  fDsToProcess->Clear();
  // List is copied, so does its content
  while ( o = dynamic_cast<TObjString *>(i.Next()) ) {
    fDsToProcess->Add( new TObjString( o->GetString().Data() ) );
  }
}

void AfDataSetSrc::FlattenListOfDataSets() {

  fDsUris->Clear();

  TIter pi(fDsToProcess);
  TObjString *o;

  while (o = dynamic_cast<TObjString *>(pi.Next())) {

    TString dsMask = o->GetString();

    AfLogDebug("Processing mask %s on dataset source %s", dsMask.Data(),
      fUrl.Data());

    TMap *groups = fManager->GetDataSets(dsMask.Data(),
      TDataSetManager::kReadShort);
    TIter gi(groups);
    TObjString *gn;

    while ( gn = dynamic_cast<TObjString *>( gi.Next() ) ) {
      //AfLogInfo(">> Group: %s", gn->String().Data());

      TMap *users = dynamic_cast<TMap *>( groups->GetValue( gn->String() ) );
      TIter ui(users);
      TObjString *un;

      while ( un = dynamic_cast<TObjString *>( ui.Next() ) ) {
        //AfLogInfo(">>>> User: %s", un->GetString().Data());

        TMap *dss = dynamic_cast<TMap *>( users->GetValue( un->String() ) );
        TIter di(dss);
        TObjString *dn;

        while ( dn = dynamic_cast<TObjString *>( di.Next() ) ) {

          //AfLogInfo(">>>>>> Dataset: %s", dn->String().Data());

          // This should give an URI in the form /GROUP/USER/dataset
          TString dsUri = TDataSetManager::CreateUri( gn->String(),
            un->String(), dn->String() );
          fDsUris->Add( new TObjString(dsUri.Data()) );

          //AfLogInfo(">>>>>> Dataset URI: %s", dsUri.Data());

        } // while over datasets
      } // while over users
    } // while over groups

  } // while over datasets mask (e.g. /*/*)

}

void AfDataSetSrc::DoSuid() {
  if (!fSuid) {
    return;
  }
  fUnpUid = geteuid();
  fUnpGid = getegid();
  if (!((seteuid(0) == 0) && (setegid(0) == 0))) {
    AfLogFatal("Failed obtaining privileges");
    gSystem->Exit(51);
  }
}

void AfDataSetSrc::UndoSuid() {
  if (!fSuid) {
    return;
  }
  if (!((setegid(fUnpUid) == 0) && (seteuid(fUnpGid) == 0))) {
    AfLogFatal("Can't drop privileges!");
    gSystem->Exit(51);
  }
}
