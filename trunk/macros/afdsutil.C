/** Init function, it only prints a welcome message with the default parameters.
 */
void afdsutil() {
  const char *dsPath = gEnv->GetValue("af.dspath", "/tmp");
  Printf("Dataset management utilities loaded.");
  Printf("Available functions start with \"af\", use autocompletion to list");
  Printf(">> Datasets path, change with afSetDsPath(): %s", dsPath);
}

/** Sets the path of the dataset source, and saves it to the af.dspath variable
 *  inside the user's ~/.rootrc.
 */
void afSetDsPath(const char *dsPath) {
  gEnv->SetValue("af.dspath", gDsPath.Data());
  gEnv->SaveLevel(kEnvUser);
}

/** A TDataSetManagerFile object is created using default parameters.
 */
TDataSetManagerFile *afCreateDsMgr() {
  TDataSetManagerFile *mgr = new TDataSetManagerFile( NULL, NULL,
    Form("dir:%s", gEnv->GetValue("af.dspath", "/pool/datasets")) );
  return mgr;
}

/** A content of a dataset is shown. There is the possibility to show only files
 *  that are (un)staged or (un)corrupted by combining one or more of SsCc in the
 *  showOnly string parameter in any order.
 *
 *  Usage examples:
 *
 *   - To show every staged file:                  "S" or "SCc"
 *   - To show every corrupted file:               "C" or "CSs"
 *   - To show every unstaged and corrupted file:  "sC"
 *   - To show every file (default):               "Ss" or "Cc" or "SsCc"
 */
void afShowDsContent(const char *dsUri, TString showOnly = "SsCc") {

  TDataSetManagerFile *mgr = afCreateDsMgr();
  TFileCollection *fc = mgr->GetDataSet(dsUri);

  if (!fc) {
    Printf("Error opening dataset URI %s", dsUri);
    return;
  }

  Bool_t bS, bs, bC, bc;
  bS = bs = bC = bc = kFALSE;

  if (showOnly.Index('S') >= 0) bS = kTRUE;
  if (showOnly.Index('s') >= 0) bs = kTRUE;
  if (showOnly.Index('C') >= 0) bC = kTRUE;
  if (showOnly.Index('c') >= 0) bc = kTRUE;

  // If Ss (or Cc) omitted, show both Ss (or Cc)
  if (!bc && !bC) bc = bC = kTRUE;
  if (!bs && !bS) bs = bS = kTRUE;

  TIter i(fc->GetList());
  TFileInfo *inf;
  UInt_t count = 1;

  while ( inf = dynamic_cast<TFileInfo *>(i.Next()) ) {
    Bool_t s = inf->TestBit(TFileInfo::kStaged);
    Bool_t c = inf->TestBit(TFileInfo::kCorrupted);

    if ( ((s && bS) || (!s && bs)) && ((c && bC) || (!c && bc)) ) {
      Printf("% 4u. %c%c | %s", count++, (s ? 'S' : 's'), (c ? 'C' : 'c'),
        inf->GetCurrentUrl()->GetUrl());
      TUrl *url;
      inf->NextUrl();

      // Every URL is shown, not only first
      while (url = inf->NextUrl()) {
        Printf("         | %s", url->GetUrl());
      }
      inf->ResetUrl();
    }
  }

  delete fc;
  delete mgr;
}

/** Gets a flattened TList of datasets. The list must be destroyed by the user,
 *  and it is owner of its content.
 */
TList *afGetListOfDs(const char *mask = "/*/*") {

  TDataSetManagerFile *mgr = afCreateDsMgr();
  TList *listOfDs = new TList();
  listOfDs->SetOwner();

  TMap *groups = mgr->GetDataSets(mask, TDataSetManager::kReadShort);
  groups->SetOwnerKeyValue();  // important to avoid leaks!
  TIter gi(groups);
  TObjString *gn;

  while (gn = dynamic_cast<TObjString *>(gi.Next())) {

    TMap *users = dynamic_cast<TMap *>( groups->GetValue( gn->String() ) );
    users->SetOwnerKeyValue();
    TIter ui(users);
    TObjString *un;

    while (un = dynamic_cast<TObjString *>(ui.Next())) {

      TMap *dss = dynamic_cast<TMap *>( users->GetValue( un->String() ) );
      dss->SetOwnerKeyValue();
      TIter di(dss);
      TObjString *dn;

      while (dn = dynamic_cast<TObjString *>(di.Next())) {

        // No COMMON user/group mapping is done here...
        TString dsUri = TDataSetManager::CreateUri( gn->String(), un->String(),
          dn->String() );

        listOfDs->Add( new TObjString(dsUri.Data()) );

      }
    }
  }

  delete groups;
  delete mgr;

  return listOfDs;
}

/** Marks a file matching the URL as (un)staged or (un)corrupted: choose mode
 *  with one or more among "SsCc". Clearly, "S" is incompatible with "s" and "C"
 *  is incompatible with "c". The URL may match any URL of a TFileInfo, even not
 *  the first one.
 */
void afMarkUrlAs(const char *fileUrl, TString bits = "") {

  Bool_t setStaged = kFALSE;
  Bool_t unsetStaged = kFALSE;
  Bool_t setCorr = kFALSE;
  Bool_t unsetCorr = kFALSE;

  if (bits.Index('S') >= 0) {
    setStaged = kTRUE;
  }

  if (bits.Index('s') >= 0) {
    unsetStaged = kTRUE;
  }

  if (bits.Index('C') >= 0) {
    setCorr = kTRUE;
  }

  if (bits.Index('c') >= 0) {
    unsetCorr = kTRUE;
  }

  Bool_t err = kFALSE;

  if (setStaged && unsetStaged) {
    Printf("Cannot set STAGED and NOT STAGED at the same time!");
    err = kTRUE;
  }

  if (setCorr && unsetCorr) {
    Printf("Cannot set CORRUPTED and NOT CORRUPTED at the same time!");
    err = kTRUE;
  }

  if (!setCorr && !setStaged && !setCorr && !unsetCorr) {
    Printf("Nothing to do: specify sScC as options to (un)mark STAGED or "
      "CORRUPTED bit");
    err = kTRUE;
  }

  if (err) {
    return;
  }

  TDataSetManagerFile *mgr = afCreateDsMgr();
  TList *listOfDs = afGetListOfDs();
  Int_t regErrors = 0;
  TIter i(listOfDs);
  TObjString *dsUriObj;

  while (dsUriObj = dynamic_cast<TObjString *>(i.Next())) {

    TString dsUri = dsUriObj->GetString();
    Int_t nChanged = 0;

    TFileCollection *fc = mgr->GetDataSet(dsUri.Data());
    TIter j(fc->GetList());
    TFileInfo *fi;

    while (fi = dynamic_cast<TFileInfo *>(j.Next())) {
      if ( fi->FindByUrl(fileUrl) ) {
        Printf(">> Found on dataset %s", dsUri.Data());

        if (setCorr) {
          fi->SetBit(TFileInfo::kCorrupted);
        }
        else if (unsetCorr) {
          fi->ResetBit(TFileInfo::kCorrupted);
        }

        if (setStaged) {
          fi->SetBit(TFileInfo::kStaged);
        }
        else if (unsetStaged) {
          fi->ResetBit(TFileInfo::kStaged);
        }

        nChanged++;
      }
    }

    if (nChanged > 0) {
      fc->Update();
      // O = overwrite existing dataset
      // T = trust information (i.e. don't reset STAGED and CORRUPTED bits):
      //     the dataset manager must be configured to allow so (it is by
      //     default on PROOF)
      TString group, user, dsName;
      mgr->ParseUri(dsUri, &group, &user, &dsName);
      Int_t r = mgr->WriteDataSet(group, user, dsName, fc);

      if (r == 0) {
        regErrors++;
      }
    }

    delete fc;
  }

  delete mgr;
  delete listOfDs;

  if (regErrors > 0) {
    Printf("%d error(s) writing back datasets encountered, check permissions",
      regErrors);
  }
  else {
    Printf("Success, no errors");
  }
}

/** Repair datasets: this function gives the possibility to take actions on
 *  corrupted files. Possible actions are:
 *
 *   - ignore:  corrupted files are just listed
 *   - unlist:  they are removed from dataset
 *   - unstage: they are removed from dataset and they are deleted from storage
 *   - uncorrupt: files are marked as uncorrupted (and unstaged)
 *
 *  TODO:
 *
 *   - implement unstage
 *   - save the list of "bad" files somewhere (by removing duplicates)
 */
void afRepairDs(TString action = "ignore") {

  // What to do?
  char a;

  if (action == "uncorrupt") {
    a = 'C';
  }
  else if (action == "unstage") {
    a = 'S';
  }
  else if (action == "unlist") {
    a = 'L';
  }
  else if (action == "ignore") {
    a = 'I';
  }
  else {
    Printf("Action may only be one of uncorrupt, unstage, unlist or ignore.");
    return;
  }

  TDataSetManagerFile *mgr = afCreateDsMgr();
  TList *listOfDs = afGetListOfDs();
  TObjString *dsUriObj;
  TIter i(listOfDs);

  // Loop over datasets
  while (dsUriObj = dynamic_cast<TObjString *>(i.Next())) {

    TString dsUri = dsUriObj->GetString();
    TFileCollection *fc = mgr->GetDataSet(dsUri.Data());
    TFileCollection *newFc;
    
    if (a != 'I') {
      newFc = new TFileCollection();
      newFc->SetDefaultTreeName( fc->GetDefaultTreeName() );
    }

    Printf("Scanning dataset %s for corrupted files...", dsUri.Data());

    TIter j(fc->GetList());
    TFileInfo *fi;
    Int_t nChanged = 0;

    while (fi = dynamic_cast<TFileInfo *>(j.Next())) {

      Bool_t c = fi->TestBit(TFileInfo::kCorrupted);

      if (c) {

        if (a == 'C') {
          Printf(">> Marking as uncorrupted: %s", fi->GetFirstUrl()->GetUrl());
          fi->ResetBit(TFileInfo::kCorrupted);
          fi->ResetBit(TFileInfo::kStaged);
          TFileInfo *newFi = new TFileInfo(*fi);
          newFc->Add(newFi);
          nChanged++;
        }
        else if (a == 'L') {
          Printf(">> Unlisting: %s", fi->GetFirstUrl()->GetUrl());
          nChanged++;
        }
        else if (a == 'S') {
          Printf(">> Unstaging (not yet implemented): %s",
            fi->GetFirstUrl()->GetUrl());
          nChanged++;
        }
        else {
          Printf(">> Corrupted: %s", fi->GetFirstUrl()->GetUrl());
        }

      }
      else if (a != 'I') {
        // If file is good, put it in the new TFileCollection
        TFileInfo *newFi = new TFileInfo(*fi);
        newFc->Add(newFi);
      }

    }

    delete fc;

    if (nChanged > 0) {

      newFc->Update();

      TString group, user, dsName;
      mgr->ParseUri(dsUri, &group, &user, &dsName);
      Int_t r = mgr->WriteDataSet(group, user, dsName, newFc);

      if (r != 0) {
        Printf("");
        Printf("*** The dataset %s has effectively changed! ***", dsUri.Data());
        Printf(">> Default tree: %s", newFc->GetDefaultTreeName());
        Printf(">> Number of files: %ld (%.2f%% staged)", newFc->GetNFiles(),
          newFc->GetStagedPercentage());
      }
      else {
        Printf("*** Can't write back modified dataset %s! ***", dsUri.Data());
      }

      Printf("");
    }

    if (a != 'I') {
      delete newFc;
    }

  }

  delete mgr;
  delete listOfDs;
}
