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
Int_t AfDataSetSrc::Process(DsAction_t action) {

  AfLogDebug("+++ Started processing of dataset source %s +++", fUrl.Data());

  // Creates a flattened list of dataset URIs
  FlattenListOfDataSets();

  Int_t nChanged = 0;

  TIter i(fDsUris);
  TObjString *s;
  while (s = dynamic_cast<TObjString *>(i.Next())) {

    switch (action) {
      case kDsReset:
        nChanged = ResetDataSet( s->GetString().Data() );
      break;

      case kDsProcess:
        nChanged = ProcessDataSet( s->GetString().Data() );
      break;
    }
  }

  AfLogDebug("+++ Ended processing dataset source %s +++", fUrl.Data());

  return nChanged;
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

  delete fc;
}

Int_t AfDataSetSrc::ResetDataSet(const char *uri) {

  if (gLog->GetDebug()) {
    ListDataSetContent(uri, Form("Dataset %s before reset:", uri), kTRUE);
  }

  TFileCollection *fc = fManager->GetDataSet(uri);

  Int_t nChanged = 0;

  // Reset "staged" and "corrupted" bits
  fc->ResetBitAll( TFileInfo::kStaged );
  fc->ResetBitAll( TFileInfo::kCorrupted );
  fc->Update();

  // Loop to restore only root:// URIs
  TFileInfo *fi;
  TIter i( fc->GetList() );

  while (fi = dynamic_cast<TFileInfo *>(i.Next())) {
    KeepOnlyLastUrl( fi );
    TranslateUrl(fi, kUrlRoot);
  }

  // Save the modified dataset
  TString group, user, dsName;
  fManager->ParseUri(uri, &group, &user, &dsName);
  DoSuid();
  Int_t r = fManager->WriteDataSet(group, user, dsName, fc, \
    TDataSetManager::kFileMustExist);
  UndoSuid();

  delete fc;

  AfLogDebug("WriteDataSet() for %s has returned %d", uri, r);

  if (r == 0) {
    AfLogError("Dataset reset: %s, but can't write (check permissions)", uri);
  }
  else {
    AfLogOk("Dataset reset: %s", uri);
    nChanged++;
  }

  if (gLog->GetDebug()) {
    ListDataSetContent(uri, Form("Dataset %s after reset:", uri), kTRUE);
  }

  return nChanged;
}

Int_t AfDataSetSrc::ProcessDataSet(const char *uri) {

  if (gLog->GetDebug()) {
    ListDataSetContent(uri, Form("Dataset %s before processing:", uri), kTRUE);
  }

  // NOTE: I see 4 bytes of memory loss per loop inside TFileCollection...
  TFileCollection *fc = fManager->GetDataSet(uri);
  Int_t nChanged = 0;

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

      TUrl url( fi->GetCurrentUrl()->GetUrl() );
      url.SetAnchor("");  // download the full file, not only the #Anchor.root
      const char *surl = url.GetUrl();
      StgStatus_t st = fParentManager->GetStageStatus(surl);

      if (st == kStgDone) {
        fi->SetBit( TFileInfo::kStaged );  // info is changed in dataset

        AddRealUrlAndMetaData(fi);

        fParentManager->DequeueUrl(surl);
        changed = kTRUE;
        AfLogDebug("Dequeued (staged): %s", surl);
      }
      else if (st == kStgFail) {
        fParentManager->DequeueUrl(surl);  // removed from current position
        if (c) {
          // After download has started the file may have been marked as corr.
          AfLogInfo("Dequeued after failure (marked as corrupted): %s", surl);
        }
        else {
          // Not corrupted, retry but as the last file in queue
          fParentManager->EnqueueUrl(surl);  // pushed at the end with status Q
          AfLogInfo("Requeued (has failed): %s", surl);
        }
      }
      else if (st == kStgAbsent) {
        if (c) {
          AfLogInfo("Not queuing (marked as corrupted): %s", surl);
        }
        else {
          fParentManager->EnqueueUrl(surl);  // pushed at the end with status Q
          AfLogInfo("Queued: %s", surl);
        }
      }
      else if ((st == kStgQueue) && (c)) {
        // After queuing the file may have been marked as corrupted
        AfLogInfo("Dequeued (marked as corrupted): %s", surl);
        fParentManager->DequeueUrl(surl);
      }
    }

  }

  // Save the modified dataset
  if (changed) {

    // Update the count of staged/corrupted files; it also updates metadata
    // which summarizes the metadatas inside the TFileInfos it contains
    fc->Update();

    TString group, user, dsName;
    fManager->ParseUri(uri, &group, &user, &dsName);

    DoSuid();
    // With kFileMustExist it saves ds only if it already exists: it updates it
    Int_t r = fManager->WriteDataSet(group, user, dsName, fc, \
      TDataSetManager::kFileMustExist);
    UndoSuid();

    AfLogDebug("WriteDataSet() for %s has returned %d", uri, r);

    if (r == 0) {
      AfLogError("Dataset modif: %s, but can't write (check permissions)", uri);
    }
    else {
      AfLogOk("Dataset saved: %s", uri);
      nChanged++;
    }
  }
  else {
    AfLogDebug("Dataset unmod: %s", uri);
  }

  if (gLog->GetDebug()) {
    ListDataSetContent(uri, Form("Dataset %s after processing:", uri), kTRUE);
  }

  delete fc;

  return nChanged;
}

// Returns number of URLs removed
Int_t AfDataSetSrc::KeepOnlyLastUrl(TFileInfo *fi) {

  TList *urlList = new TList();
  urlList->SetOwner();
  Int_t count = 0;
  Int_t nUrls = fi->GetNUrls();
  TUrl *url;

  fi->ResetUrl();

  while (url = fi->NextUrl()) {
    if (++count < nUrls) {
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
    groups->SetOwnerKeyValue();  // important to avoid leaks!
    TIter gi(groups);
    TObjString *gn;

    while ( gn = dynamic_cast<TObjString *>( gi.Next() ) ) {
      //AfLogInfo(">> Group: %s", gn->String().Data());

      TMap *users = dynamic_cast<TMap *>( groups->GetValue( gn->String() ) );
      users->SetOwnerKeyValue();
      TIter ui(users);
      TObjString *un;

      while ( un = dynamic_cast<TObjString *>( ui.Next() ) ) {
        //AfLogInfo(">>>> User: %s", un->GetString().Data());

        TMap *dss = dynamic_cast<TMap *>( users->GetValue( un->String() ) );
        dss->SetOwnerKeyValue();
        TIter di(dss);
        TObjString *dn;

        while ( dn = dynamic_cast<TObjString *>( di.Next() ) ) {

          // This should give an URI in the form /GROUP/USER/dataset
          // COMMON user/group mapping is a concept which is used in PROOF only,
          // here we see the real directory names
          TString dsUri = TDataSetManager::CreateUri( gn->String(),
            un->String(), dn->String() );
          //AfLogInfo(">>>>>> Dataset: %s", dn->String().Data());
          //AfLogInfo(">> Dataset URI: %s", dsUri.Data());
          fDsUris->Add( new TObjString(dsUri.Data()) );

        } // while over datasets

      } // while over users

    } // while over groups

    delete groups;  // groups and all other TMaps are owners of Keys and Values

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

Bool_t AfDataSetSrc::AddRealUrlAndMetaData(TFileInfo *fi) {

  fi->RemoveMetaData();

  TUrl *url = fi->GetCurrentUrl();
  TFile *f = TFile::Open(url->GetUrl());

  if (!f) {
    return kFALSE;
  }

  // Get the real URL
  TUrl *realUrl = new TUrl( const_cast<TUrl *>(f->GetEndpointUrl())->GetUrl() );
  realUrl->SetAnchor(url->GetAnchor());
  fi->AddUrl( realUrl->GetUrl(), kTRUE );  // kTRUE = first elm of list
  delete realUrl;

  // Get the associated metadata
  TIter k( f->GetListOfKeys() );
  TKey *key;

  while ( key = dynamic_cast<TKey *>(k.Next()) ) {
    if ( strcmp(key->GetClassName(), "TTree") == 0 ) {
      // Every tree will be filled with data
      TFileInfoMeta *meta = new TFileInfoMeta( Form("/%s", key->GetName()) );
      TTree *tree = dynamic_cast<TTree *>( key->ReadObj() );
      meta->SetEntries( tree->GetEntries() );
      fi->AddMetaData(meta);  // TFileInfo is owner of its metadata
      //delete tree;  // CHECK: should I delete it or not?
    }
  }

  f->Close();
  delete f;

  return kTRUE;
}
