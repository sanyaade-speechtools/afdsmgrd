#include "AfDataSetSrc.h"

ClassImp(AfDataSetSrc);

AfDataSetSrc::AfDataSetSrc() {}

AfDataSetSrc::AfDataSetSrc(const char *url, TUrl *redirUrl, const char *opts,
  const char *dsConf, AfDataSetsManager *parentManager) {

  fUrl  = url;
  fOpts = opts;
  fRedirUrl = redirUrl;
  fParentManager = parentManager;

  fDsToProcess = new TList();
  fDsToProcess->SetOwner();
  fDsToProcess->Add( new TObjString("/*/*") );

  fManager = new TDataSetManagerFile(NULL, NULL, Form("dir:%s opt:%s",
    fUrl.Data(), fOpts.Data()) );

  fActionsConf = dsConf;

  fActions = new TList();
  fActions->SetOwner();

  fDsUris = new TList();  // TList of AfDsUri
  fDsUris->SetOwner();
}

AfDataSetSrc::~AfDataSetSrc() {
  delete fManager;
  delete fRedirUrl;
  delete fDsToProcess;
  delete fDsUris;
  delete fActions;
}

// Processes all the datasets in this dataset source
Int_t AfDataSetSrc::Process(DsAction_t action) {

  AfLogDebug(20, "+++ Started processing of dataset source %s +++",
    fUrl.Data());

  Int_t nChanged = 0;

  ReadDsActionsConf();

  if (action == kDsSync) {
    if (!fSyncUrl.IsValid()) {
      AfLogWarning("Synchronization is disabled for this dataset source");
      return 0;
    }
    FlattenListOfDataSets();
    nChanged += Sync();
  }
  else {

    FlattenListOfDataSets();

    TIter i(fDsUris);
    AfDsUri *s;
    while (s = dynamic_cast<AfDsUri *>(i.Next())) {

      switch (action) {
        case kDsReset:
          nChanged += ResetDataSet( s->GetUri() );  // will be removed soon
        break;

        case kDsProcess:
          nChanged += ProcessDataSet( s );
        break;
      }
    }

  }

  AfLogDebug(20, "+++ Ended processing dataset source %s +++", fUrl.Data());

  return nChanged;
}

Bool_t AfDataSetSrc::AliEnConnect() {

  if ((!gGrid) || (!gGrid->IsConnected())) {
    if (TGrid::Connect("alien:") && (gGrid) && (gGrid->IsConnected())) {
      AfLogDebug(10, "AliEn connection created");
    }
    else {
      AfLogError("Can't open AliEn connection");
      return kFALSE;
    }
  }
  else {
    AfLogDebug(10, "Reusing existing AliEn connection");
  }

  return kTRUE;
}

Int_t AfDataSetSrc::Sync() {

  if (!AliEnConnect()) return 0;

  // Do an AliEn "find" on the AliEn datasets directory

  TGridResult *res = gGrid->Query(fSyncUrl.GetFile(), "*.root");
  if (!res) {
    AfLogError("Can't search for new datasets: AliEn query error");
    return 0;
  }

  Int_t nEntries = res->GetEntries();
  Int_t nSaved = 0;

  AfLogDebug(10, "Searching AliEn: path=%s, found=%d", fSyncUrl.GetFile(),
    nEntries);
  TPMERegexp reDs("alien://.*/(.*)/(.*)/(.*)\\.root$");

  TDataSetManagerFile *tmpMgr = NULL;

  if (nEntries) {
    TString rmDir = Form("rm -rf \"/tmp/afdsmgrd-tmpds\"");
    gSystem->Exec(rmDir.Data());

    tmpMgr = new TDataSetManagerFile(NULL, NULL, "dir:/tmp/afdsmgrd-tmpds");

    if (!tmpMgr) {
      delete res;
      AfLogError("Can't create the temporary dataset manager");
      return 0;
    }
  }

  for (Int_t i=0; i<nEntries; i++) {
    TString tUrl = res->GetKey(i, "turl");
    TString dsUri;
    if (reDs.Match(tUrl) != 4) {
      AfLogWarning("Invalid AliEn dataset URL: %s", tUrl.Data());
      continue;
    }

    // Compose DS URI
    dsUri = Form("/%s/%s/%s", reDs[1].Data(), reDs[2].Data(), reDs[3].Data());

    // Search if dataset exists in current list
    AfDsUri search(dsUri.Data());

    if (fDsUris->FindObject(&search)) {
      AfLogDebug(10, "Remote dataset skipped (exists): %s", dsUri.Data());
      continue;
    }

    // This is to be added
    AfLogDebug(10, "New remote dataset found: %s", dsUri.Data());

    TString tempDest = Form("%s/%s/%s", "/tmp/afdsmgrd-tmpds/",
      reDs[1].Data(), reDs[2].Data());
    gSystem->mkdir(tempDest.Data(), kTRUE);

    tempDest.Append( Form("/%s.root", reDs[3].Data()) );

    // Dataset is copied on the temporary repository
    TFile::Cp(tUrl.Data(), tempDest.Data());

    // Dataset is read from temporary repository
    TFileCollection *fc = tmpMgr->GetDataSet(dsUri);
    if (!fc) {
      AfLogError("Can't fetch remote dataset: %s", dsUri.Data());
      continue;
    }

    TString group, user, dsName;
    fManager->ParseUri(dsUri, &group, &user, &dsName);
    fParentManager->DoSuid();
    Int_t r = fManager->WriteDataSet(group, user, dsName, fc);
    fParentManager->UndoSuid();

    if (r == 0) {
      AfLogError("Fetched dataset but can't save: %s [%d] (check permissions)",
        dsUri.Data(), r);
    }
    else {
      AfLogOk("Fetched and added new dataset: %s [%d items]", dsUri.Data(),
        fc->GetNFiles());
      nSaved++;
    }

    delete fc;
  }

  delete res;
  delete tmpMgr;

  return nSaved;
}

void AfDataSetSrc::ListDataSetContent(const char *uri, const char *header,
  Bool_t debug) {

  // Show messages only with debug level >= 30
  if ((debug) && (gLog->GetDebugLevel() < 30)) {
    return;
  }

  if (debug) {
    AfLogDebug(30, header);
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
      AfLogDebug(30, s.Data());
    }
    else {
      AfLogInfo(s.Data());
    }

  }

  delete fc;
}

Int_t AfDataSetSrc::ResetDataSet(const char *uri) {

  ListDataSetContent(uri, Form("Dataset %s before reset:", uri), kTRUE);

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
  fParentManager->DoSuid();
  Int_t r = fManager->WriteDataSet(group, user, dsName, fc, \
    TDataSetManager::kFileMustExist);
  fParentManager->UndoSuid();

  delete fc;

  AfLogDebug(20, "WriteDataSet() for %s has returned %d", uri, r);

  if (r == 0) {
    AfLogError("Dataset reset: %s, but can't write (check permissions)", uri);
  }
  else {
    AfLogOk("Dataset reset: %s", uri);
    nChanged++;
  }

  ListDataSetContent(uri, Form("Dataset %s after reset:", uri), kTRUE);

  return nChanged;
}

AfDsMatch *AfDataSetSrc::GetActionsForDs(AfDsUri *dsUri) {

  TIter i(fActions);
  AfDsMatch *m;
  while (( m = dynamic_cast<AfDsMatch *>(i.Next()) )) {
    if ( m->Match(dsUri->GetUri()) ) return m;
  }

  return &fDefaultAction;
}

Int_t AfDataSetSrc::ProcessDataSet(AfDsUri *dsUri) {

  ListDataSetContent(dsUri->GetUri(), Form("Dataset %s before processing:",
    dsUri->GetUri()), kTRUE);

  TFileCollection *fc = fManager->GetDataSet(dsUri->GetUri());
  Int_t nChanged = 0;

  // If you want to do a ScanDataSet here, you need to do fc->GetList() after
  // that, because ScanDataSet actually modifies the list and writes it to disk

  AfDsMatch *acts = GetActionsForDs(dsUri);
  AfLogDebug(10, ">> Actions to take: %c%c%c%c | %s",
    acts->TestBit(kActTranslate) ? 'T' : 't',
    acts->TestBit(kActStage)     ? 'S' : 's',
    acts->TestBit(kActVerify)    ? 'V' : 'v',
    acts->TestBit(kActAddEndUrl) ? 'A' : 'a',
    dsUri->GetUri()
  );

  Bool_t aTra = acts->TestBit(kActTranslate);
  Bool_t aStg = acts->TestBit(kActStage);
  Bool_t aVer = acts->TestBit(kActVerify);
  Bool_t aUrl = acts->TestBit(kActAddEndUrl);

  /*if ((!aTra) && (!aStg) && (!aVer) && (!aUrl)) {
    // Ignore this dataset
    delete fc;
    return;
  }*/

  TFileInfo *fi;
  TIter i( fc->GetList() );

  Bool_t changed = kFALSE;

  while (fi = dynamic_cast<TFileInfo *>(i.Next())) {

    Bool_t s = fi->TestBit(TFileInfo::kStaged);
    Bool_t c = fi->TestBit(TFileInfo::kCorrupted);

    if (!s) {

      // Translate URL
      if ((!c) && (aTra)) {
        if (TranslateUrl(fi, AfDataSetSrc::kUrlAliEn) > 0) {
          changed = kTRUE;
          if ((!aStg) && (!aVer) && (!aUrl)) {
            fi->SetBit(TFileInfo::kStaged);
            AfLogInfo("URL translated: %s", fi->GetCurrentUrl()->GetUrl());
          }
        }
      }

      TUrl url( fi->GetCurrentUrl()->GetUrl() );
      url.SetAnchor("");  // download the full file, not only the #Anchor.root
      const char *surl = url.GetUrl();
      StgStatus_t st = fParentManager->GetStageStatus(surl, dsUri->GetUId());

      if (st == kStgDone) {

        // Staging complete

        Int_t nLeft;

        if ((aUrl) || (aVer)) {

          if (AddRealUrlAndMetaData(fi, aUrl, aVer)) {

            // Verification succeeded
            fi->SetBit( TFileInfo::kStaged );

            // We should save a TFileInfo somewhere to avoid verifying again and
            // again...
            fParentManager->DequeueUrl(surl, dsUri->GetUId(), &nLeft);
            AfLogInfo("Dequeued (staged and verified) [%d left]: %s", nLeft,
              surl);
          }
          else {
            // If verification fails, immediately dequeue it and mark as corrupted
            fParentManager->DequeueUrl(surl, dsUri->GetUId(), &nLeft);
            fi->SetBit( TFileInfo::kCorrupted );
            AfLogError("Dequeued last one and marked as corrupted (failed to "
              "verify) [%d left]: %s", nLeft, surl);
          }

        }
        else {

          // Neither verify nor add endpoint url
          fi->SetBit( TFileInfo::kStaged );

          // DequeueUrl may fail silently if the URL was not staged, but we are
          // obliged to deal with this case because aStg may have changed and
          // staging may have actually begun in the past
          StgStatus_t s = fParentManager->DequeueUrl(surl, dsUri->GetUId(),
            &nLeft);
          if ((s == kStgDeqLast) || (s == kStgDeqLeft)) {
            AfLogInfo("Dequeued (staged) [%d left]: %s", nLeft, surl);
          }

        }

        changed = kTRUE;  // info has changed in dataset

      }
      else if (st == kStgFail) {

        if (c) {
          // After download has started the file may have been marked as corr.
          Int_t nLeft;
          fParentManager->DequeueUrl(surl, dsUri->GetUId(), &nLeft);
          AfLogInfo("Dequeued after fail (marked as corrupted) [%d left]: %s",
            nLeft, surl);
        }
        else {
          // Not corrupted, retry but as the last file in queue
          Int_t nPrevFail = fParentManager->RequeueUrl(surl, dsUri->GetUId(),
            kTRUE);
          if (nPrevFail ==  kRequeueLimitReached) {
            fi->SetBit( TFileInfo::kCorrupted );
            AfLogError("Dequeued and marked corrupted (too many "
              "failures): %s", surl);
            changed = kTRUE;
          }
          else {
            AfLogWarning("Requeued, failed %d time(s): %s", nPrevFail,
              surl);
          }
        }

      }
      else if (st == kStgDelPending) {
        Int_t nLeft;
        char c = fParentManager->DequeueUrl(surl, dsUri->GetUId(), &nLeft);
        AfLogInfo("Dequeued (marked for dequeuing) [%d left]: %s [%c]",
          nLeft, surl, c);
      }
      else if (st == kStgAbsent) {
        if (c) {
          AfLogDebug(10, "Not queuing (marked as corrupted): %s", surl);
        }
        else if (aStg) {
          // Try to push at the end with status Q, only if actions include
          // staging
          Int_t nQueue;
          if ( fParentManager->EnqueueUrl(surl, dsUri->GetUId(), &nQueue) !=
            kStgQueueFull ) {
            AfLogInfo("Queued [%d]: %s", nQueue, surl);
          }
        }
        else {

          // File is not corrupted and not to be staged: verify it, if told
          if ((aUrl) || (aVer)) {

            if (AddRealUrlAndMetaData(fi, aUrl, aVer)) {
              fi->SetBit(TFileInfo::kStaged);
              AfLogOk("Verified: %s", surl);
            }
            else {
              fi->SetBit(TFileInfo::kCorrupted);
              AfLogError("Cannot verify (marked as corrupted): %s", surl);
            }

            changed = kTRUE;

          }

        }
      }
      else if (st == kStgQueue) {
        if (c) {
          Int_t nLeft;
          // After queuing the file may have been marked as corrupted
          fParentManager->DequeueUrl(surl, dsUri->GetUId(), &nLeft);
          AfLogInfo("Dequeued (has been marked as corrupted) [%d left]: %s",
            nLeft, surl);
        }
      }
    }  // end if (s)

  }

  // Save the modified dataset
  if (changed) {

    // Update the count of staged/corrupted files; it also updates metadata
    // which summarizes the metadatas inside the TFileInfos it contains
    fc->Update();

    TString group, user, dsName;
    fManager->ParseUri(dsUri->GetUri(), &group, &user, &dsName);

    fParentManager->DoSuid();
    // With kFileMustExist it saves ds only if it already exists: it updates it
    Int_t r = fManager->WriteDataSet(group, user, dsName, fc, \
      TDataSetManager::kFileMustExist);
    fParentManager->UndoSuid();

    AfLogDebug(20, "WriteDataSet() for %s has returned %d", dsUri->GetUri(), r);

    if (r == 0) {
      AfLogError("Dataset modif: %s, but can't write (check permissions)",
        dsUri->GetUri());
    }
    else {
      AfLogOk("Dataset saved: %s", dsUri->GetUri());
      nChanged++;
    }
  }
  else {
    AfLogDebug(10, "Dataset unmod: %s", dsUri->GetUri());
  }

  // Always notify via manager method (it usually uses MonALISA)
  const char *treeName = fc->GetDefaultTreeName();
  const char *fallbackTreeName = "";
  Long64_t nEvts = 0;
  if (!treeName) {
    treeName = fallbackTreeName;
  }
  else {
    nEvts = fc->GetTotalEntries(treeName);
    if (nEvts < 0) { nEvts = 0; }
  }

  Long64_t totSize = fc->GetTotalSize();  // it's given in bytes
  if (totSize < 0) { totSize = 0; }

  fParentManager->NotifyDataSetStatus(dsUri->GetUId(), dsUri->GetUri(),
    fc->GetNFiles(), fc->GetNStagedFiles(), fc->GetNCorruptFiles(), treeName,
    nEvts, totSize);
  
  ListDataSetContent(dsUri->GetUri(), Form("Dataset %s after processing:",
    dsUri->GetUri()), kTRUE);

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

  // We are *not* staging on a xrootd pool
  if ( strcmp(fRedirUrl->GetProtocol(), "root") != 0 ) {
    AfLogWarning("Can't translate URLs: redirector protocol \"%s\" is not" \
      "supported, only xrootd protocol is allowed", fRedirUrl->GetProtocol());
  }

  fi->ResetUrl();
  TUrl *url;
  Int_t countChanged = 0;

  Bool_t doAliEn = kUrlAliEn & whichUrls;
  Bool_t doRoot = kUrlRoot & whichUrls;

  TUrl *curUrl = fi->GetCurrentUrl();
  TUrl *newUrl = NULL;

  // Only the current URL is translated; the eventually-created new URL is
  // inserted before the others in the TFileInfo's URL list

  if ((doAliEn) && (strcmp(curUrl->GetProtocol(), "alien") == 0)) {
    newUrl = new TUrl(*curUrl);
    newUrl->SetProtocol( fRedirUrl->GetProtocol() );
    newUrl->SetFile( Form("%s%s", fRedirUrl->GetFile(), curUrl->GetFile()) );
    newUrl->SetHost( fRedirUrl->GetHost() );
    newUrl->SetPort( fRedirUrl->GetPort() );

    countChanged++;
  }
  else if ((doRoot) && (strcmp(curUrl->GetProtocol(), "root") == 0)) {
    newUrl = new TUrl(*curUrl);
    newUrl->SetHost( fRedirUrl->GetHost() );
    newUrl->SetPort( fRedirUrl->GetPort() );

    countChanged++;
  }

  if (newUrl) {
    fi->AddUrl( newUrl->GetUrl(), kTRUE );  // kTRUE --> insert *before*
    delete newUrl;
  }

  fi->ResetUrl();

  return countChanged;
}

void AfDataSetSrc::SetDsProcessList(TList *dsList) {
  TIter i(dsList);
  TObjString *o;
  fDsToProcess->Clear();
  // List is copied, its content too
  while ( o = dynamic_cast<TObjString *>(i.Next()) ) {
    fDsToProcess->Add( new TObjString( o->GetString().Data() ) );
  }
}

void AfDataSetSrc::ReadDsActionsConf() {

  if (fActionsConf.IsNull()) {
    AfLogDebug(10, "No configuration file specified for this dataset");
    return;
  }

  AfLogInfo("Reading configuration");

  // Reset default configuration
  fDefaultAction.ResetBit(kActIgnore);  // this cleans the bits
  fActions->Clear();
  fSyncUrl.SetUrl("");

  AfConfReader *cf = AfConfReader::Open(fActionsConf.Data(), kTRUE);

  if (!cf) {
    AfLogError("Can't open actions configuration file %s, defaulted to IGNORE",
      fActionsConf.Data());
    return;
  }

  AfLogDebug(20, "Actions configuration file %s opened", fActionsConf.Data());

  // Actions per dataset or regexp
  {
    TList *matches;
    for (Char_t t=0; t<=1; t++) {

      if (t == 0) matches = cf->GetDirs("ds");
      else        matches = cf->GetDirs("dsre");

      TIter i(matches);
      TObjString *o;

      while (( o = dynamic_cast<TObjString *>(i.Next()) )) {
        TString *name = new TString();
        UInt_t bits = ParseActionsToBits(o->String(), name);
        AfDsMatch *dsAct = new AfDsMatch(name->Data(), (t == 1));
        delete name;
        dsAct->SetBit(bits);

        AfLogDebug(30, ">> dsptn=%s isre=%c acts=%c%c%c%c",
          dsAct->GetPattern().Data(),
          dsAct->IsRegEx() ? 'Y' : 'N',
          dsAct->TestBit(kActTranslate) ? 'T' : 't',
          dsAct->TestBit(kActStage)     ? 'S' : 's',
          dsAct->TestBit(kActVerify)    ? 'V' : 'v',
          dsAct->TestBit(kActAddEndUrl) ? 'A' : 'a'
        );

        fActions->Add(dsAct);
      }

      delete matches;

    }

  }

  // Default action
  {
    TString *defAct = cf->GetDir("defaultactions");
    if (defAct) {
      UInt_t bits = ParseActionsToBits(*defAct);
      fDefaultAction.SetBit(bits);
      AfLogInfo(">> Default actions: %c%c%c%c",
        (bits & kActTranslate) ? 'T' : 't',
        (bits & kActStage)     ? 'S' : 's',
        (bits & kActVerify)    ? 'V' : 'v',
        (bits & kActAddEndUrl) ? 'A' : 'a'
      );
    }
  }

  // Sync URL
  {
    TString *syncUrl = cf->GetDir("sync");
    if (syncUrl) {
      fSyncUrl.SetUrl(*syncUrl);
      AfLogInfo(">> Sync URL: %s", fSyncUrl.GetUrl());
    }
  }

  delete cf;
}

UInt_t AfDataSetSrc::ParseActionsToBits(TString &act, TString *name) {

  TObjArray *tokensPtr = act.Tokenize(" \t");
  TObjArray &tokens = *tokensPtr;

  if (name) {
    *name = dynamic_cast<TObjString *>(tokens[0])->GetString();
  }

  UInt_t bits = 0;

  for (Int_t i=(name ? 1 : 0); i<tokens.GetEntries(); i++) {
    TString tok = dynamic_cast<TObjString *>(tokens[i])->GetString();
    tok.ToLower();
    if (tok == "ignore") {
      bits = 0;
      break;
    }
    else if (tok == "translate") bits |= kActTranslate;
    else if (tok == "stage")     bits |= kActStage;
    else if (tok == "verify")    bits |= kActVerify;
    else if (tok == "addendurl") bits |= kActAddEndUrl;
  }

  delete tokensPtr;

  return bits;
}

void AfDataSetSrc::FlattenListOfDataSets() {

  fDsUris->Clear();

  TIter pi(fDsToProcess);
  TObjString *o;

  while (o = dynamic_cast<TObjString *>(pi.Next())) {

    TString dsMask = o->GetString();

    AfLogDebug(20, "Processing mask %s on dataset source %s", dsMask.Data(),
      fUrl.Data());

    TMap *groups = fManager->GetDataSets(dsMask.Data(),
      TDataSetManager::kReadShort);

    if (!groups) {
      AfLogWarning("No dataset found!");
      return;
    }

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

          UInt_t uid = fUrl.Hash()+dsUri.Hash();
          fDsUris->Add( new AfDsUri(dsUri, uid) );

        } // ds

      } // users

    } // groups

    delete groups;  // groups and all other TMaps are owners of Keys and Values

  } // while over datasets mask (e.g. /*/*)

}

Bool_t AfDataSetSrc::AddRealUrlAndMetaData(TFileInfo *fi, Bool_t addEndUrl,
  Bool_t addMeta) {

  fi->RemoveMetaData();

  TUrl *url = fi->GetCurrentUrl();
  AfLogDebug(20, "Verifying %s", url->GetUrl());

  if (strcmp(url->GetProtocol(), "alien") == 0) {
    if (!AliEnConnect()) return kFALSE;
  }

  TFile *f = TFile::Open(url->GetUrl());

  if (!f) {
    AfLogError("Can't open file %s, server went down?", url->GetUrl());
    return kFALSE;
  }

  AfLogDebug(20, ">> Opened successfully");

  // Get the real URL
  if (addEndUrl) {
    const TUrl *realUrl = f->GetEndpointUrl();

    if (realUrl) {
      TUrl *newUrl = new TUrl(*realUrl);
      newUrl->SetAnchor(url->GetAnchor());
      fi->AddUrl(newUrl->GetUrl(), kTRUE);  // kTRUE = added as first URL
      delete newUrl;
    }
    else {
      AfLogWarning("Can't read endpoint URL of %s", url->GetUrl());
    }
  }

  // Get the associated metadata
  if (addMeta) {

    TIter k( f->GetListOfKeys() );
    TKey *key;

    while ( key = dynamic_cast<TKey *>(k.Next()) ) {

      if ( TClass::GetClass(key->GetClassName())->InheritsFrom("TTree") ) {

        // Every tree will be filled with data
        TFileInfoMeta *meta = new TFileInfoMeta( Form("/%s", key->GetName()) );
        TTree *tree = dynamic_cast<TTree *>( key->ReadObj() );

        // Maybe the file is now unaccessible for some reason, and the tree is
        // unreadable!
        if (tree) {
          meta->SetEntries( tree->GetEntries() );
          fi->AddMetaData(meta);  // TFileInfo is owner of its metadata
          //delete tree;  // CHECK: should I delete it or not?
        }
        else {
          AfLogError("In file %s, can't read TTree %s - server went down?",
            url->GetUrl(), key->GetName());
          f->Close();
          delete f;
          return kFALSE;
        }
      }

    } // while
  }

  f->Close();

  AfLogDebug(20, ">> File closed");

  delete f;

  return kTRUE;
}
