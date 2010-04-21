#include "AfDataSetSrc.h"

ClassImp(AfDataSetSrc);

AfDataSetSrc::AfDataSetSrc() {}

AfDataSetSrc::AfDataSetSrc(const char *url, TUrl *redirUrl, const char *opts,
  Bool_t suid, AfDataSetsManager *parentManager) {

  fUrl  = url;
  fOpts = opts;
  fSuid = suid;
  fUnpUid = 0;
  fUnpGid = 0;
  fRedirUrl = redirUrl;
  fParentManager = parentManager;

  fDsToProcess = new TList();
  fDsToProcess->SetOwner();
  fDsToProcess->Add( new TObjString("/*/*") );

  fManager = new TDataSetManagerFile(NULL, NULL, Form("dir:%s opt:%s",
    fUrl.Data(), fOpts.Data()) );

  fDsUris = new TList();  // TList of TObjString
  fDsUris->SetOwner();
}

AfDataSetSrc::~AfDataSetSrc() {
  delete fManager;
  delete fRedirUrl;
  delete fDsToProcess;
  delete fDsUris;
}

// Processes all the datasets in this dataset source
void AfDataSetSrc::Process(Bool_t resetBits) {

  AfLogDebug("+++ Started processing of dataset source %s +++", fUrl.Data());

  // Creates a flattened list of dataset URIs
  FlattenListOfDataSets();

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

  AfLogDebug("+++ Ended processing dataset source %s +++", fUrl.Data());

}

void AfDataSetSrc::ListDataSetContent(const char *uri, const char *header,
  Bool_t debug) {

  if (debug) {
    AfLogDebug(header);
  }
  else {
    AfLogInfo(header);
  }

  TFileCollection *fc = fManager->GetDataSet(uri);
  TFileInfo *fi;
  TIter i( fc->GetList() );

  while (fi = dynamic_cast<TFileInfo *>(i.Next())) {

    Bool_t stg = fi->TestBit(TFileInfo::kStaged);
    Bool_t cor = fi->TestBit(TFileInfo::kCorrupted);
    TString s = Form(">> %c%c | %s", (stg ? 'S' : 's'), (cor ? 'C' : 'c'),
      fi->GetCurrentUrl()->GetUrl());

    if (debug) {
      AfLogDebug(s.Data());
    }
    else {
      AfLogInfo(s.Data());
    }

  }
}

void AfDataSetSrc::ResetDataSet(const char *uri) {

  if (gLog->GetDebug()) {
    ListDataSetContent(uri, Form("Dataset %s before reset:", uri), kTRUE);
  }

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
    countChanged += TranslateUrl(fi, kUrlRoot);
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

  if (gLog->GetDebug()) {
    ListDataSetContent(uri, Form("Dataset %s after reset:", uri), kTRUE);
  }
}

void AfDataSetSrc::ProcessDataSet(const char *uri) {

  if (gLog->GetDebug()) {
    ListDataSetContent(uri, Form("Dataset %s before processing:", uri), kTRUE);
  }

  TFileCollection *fc = fManager->GetDataSet(uri);

  // If you want to do a ScanDataSet here, you need to do fc->GetList() after
  // that, because ScanDataSet actually modifies the list and writes it to disk

  TFileInfo *fi;
  TIter i( fc->GetList() );

  Bool_t changed = kFALSE;

  while (fi = dynamic_cast<TFileInfo *>(i.Next())) {

    Bool_t s = fi->TestBit(TFileInfo::kStaged);
    Bool_t c = fi->TestBit(TFileInfo::kCorrupted);

    if (!s) {
      // Only AliEn URLs are translated properly
      if (TranslateUrl(fi, AfDataSetSrc::kUrlAliEn) > 0) {
        changed = kTRUE;
      }

      const char *url = fi->GetCurrentUrl()->GetUrl();
      StgStatus_t st = fParentManager->GetStageStatus(url);

      if (st == kStgDone) {
        fi->SetBit( TFileInfo::kStaged );  // info is changed in dataset
        fParentManager->DequeueUrl(url);
        changed = kTRUE;
        AfLogDebug("Dequeued: %s", url);
      }
      else if (st == kStgFail) {
        fParentManager->DequeueUrl(url);  // removed from current position
        fParentManager->EnqueueUrl(url);  // pushed at the end with status Q
        AfLogInfo("Requeued (has failed): %s", url);
      }
      else if (st == kStgAbsent) {
        fParentManager->EnqueueUrl(url);  // pushed at the end with status Q
        AfLogInfo("Queued: %s", url);
      }
    }

  }

  // Save the modified dataset
  if (changed) {
    TString group, user, dsName;
    fManager->ParseUri(uri, &group, &user, &dsName);

    DoSuid();
    // With kFileMustExist it saves ds only if it already exists: it updates it
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
    AfLogDebug("Dataset unmod: %s", uri);
  }

  if (gLog->GetDebug()) {
    ListDataSetContent(uri, Form("Dataset %s after processing:", uri), kTRUE);
  }
}

// Returns number of URLs removed
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

  fi->ResetUrl();
  TUrl *url;
  Int_t countChanged = 0;

  Bool_t doAliEn = kUrlAliEn & whichUrls;
  Bool_t doRoot = kUrlRoot & whichUrls;

  while (url = fi->NextUrl()) {

    // We are staging on a xrootd pool
    if ( strcmp(fRedirUrl->GetProtocol(), "root") == 0 ) {

      if ((doAliEn) && (strcmp(url->GetProtocol(), "alien") == 0)) {
        url->SetProtocol( fRedirUrl->GetProtocol() );
        url->SetFile( Form("%s%s", fRedirUrl->GetFile(), url->GetFile()) );
        url->SetHost( fRedirUrl->GetHost() );
        url->SetPort( fRedirUrl->GetPort() );
        //AfLogInfo(">>>> [A] Changed: %s", url->GetUrl());
        countChanged++;
      }
      else if ((doRoot) && (strcmp(url->GetProtocol(), "root") == 0)) {
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
