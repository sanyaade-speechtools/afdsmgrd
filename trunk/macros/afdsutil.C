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
  UInt_t count = 0;
  UInt_t countMatching = 0;

  while ( inf = dynamic_cast<TFileInfo *>(i.Next()) ) {
    count++;

    Bool_t s = inf->TestBit(TFileInfo::kStaged);
    Bool_t c = inf->TestBit(TFileInfo::kCorrupted);

    if ( ((s && bS) || (!s && bs)) && ((c && bC) || (!c && bc)) ) {
      Printf("% 4u. %c%c | %s", count, (s ? 'S' : 's'), (c ? 'C' : 'c'),
        inf->GetCurrentUrl()->GetUrl());
      TUrl *url;
      inf->NextUrl();

      // Every URL is shown, not only first
      while (url = inf->NextUrl()) {
        Printf("         | %s", url->GetUrl());
      }
      inf->ResetUrl();

      countMatching++;
    }
  }

  delete fc;
  delete mgr;

  Printf(">> There are %u file(s) in the dataset, %u matched your criteria",
    count, countMatching);
}

/** Gets a flattened TList of datasets. The list must be destroyed by the user,
 *  and it is owner of its content.
 *
 *  The dsMask parameter can both specify a single dataset name or a mask which
 *  must be understandable by the TDataSetManagerFile::GetDataSets() function.
 */
TList *afGetListOfDs(const char *dsMask = "/*/*") {

  TDataSetManagerFile *mgr = afCreateDsMgr();
  TList *listOfDs = new TList();
  listOfDs->SetOwner();

  // First of all it tries to open a dataset named "mask"; if does not succeed,
  // it considers it a mask.
  Int_t oldErrorIgnoreLevel = gErrorIgnoreLevel;
  gErrorIgnoreLevel = 10000;
  TFileCollection *fc = mgr->GetDataSet(dsMask);
  gErrorIgnoreLevel = oldErrorIgnoreLevel;
  if (fc) {
    listOfDs->Add( new TObjString(dsMask) );
    delete fc;
    return listOfDs;
  }

  // Not a single dataset: let's try with a mask
  TMap *groups = mgr->GetDataSets(dsMask, TDataSetManager::kReadShort);
  if (!groups) {
    return listOfDs;
  }
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
 *
 *  The URL is searched in the dataset(s) specified by dsMask.
 *
 *  If "*" is used as URL, it applies to every entry in the dataset.
 */
void afMarkUrlAs(const char *fileUrl, TString bits = "",
  const char *dsMask = "/*/*") {

  Bool_t allFiles = kFALSE;

  if (strcmp(fileUrl, "*") == 0) {
    allFiles = kTRUE;
  }

  Bool_t bS = kFALSE;
  Bool_t bs = kFALSE;
  Bool_t bC = kFALSE;
  Bool_t bc = kFALSE;

  if (bits.Index('S') >= 0) bS = kTRUE;
  if (bits.Index('s') >= 0) bs = kTRUE;
  if (bits.Index('C') >= 0) bC = kTRUE;
  if (bits.Index('c') >= 0) bc = kTRUE;

  Bool_t err = kFALSE;

  if (bs && bS) {
    Printf("Cannot set STAGED and NOT STAGED at the same time!");
    err = kTRUE;
  }

  if (bc && bC) {
    Printf("Cannot set CORRUPTED and NOT CORRUPTED at the same time!");
    err = kTRUE;
  }

  if (!bs && !bS && !bc && !bC) {
    Printf("Nothing to do: specify sScC as options to (un)mark STAGED or "
      "CORRUPTED bit");
    err = kTRUE;
  }

  if (err) {
    return;
  }

  TDataSetManagerFile *mgr = afCreateDsMgr();
  TList *listOfDs = afGetListOfDs(dsMask);
  Int_t regErrors = 0;
  TIter i(listOfDs);
  TObjString *dsUriObj;

  while (dsUriObj = dynamic_cast<TObjString *>(i.Next())) {

    TString dsUri = dsUriObj->GetString();
    Int_t nChanged = 0;

    TFileCollection *fc = mgr->GetDataSet(dsUri.Data());

    if (allFiles) {
      if (bC)      fc->SetBitAll(TFileInfo::kCorrupted);
      else if (bc) fc->ResetBitAll(TFileInfo::kCorrupted);

      if (bS)      fc->SetBitAll(TFileInfo::kStaged);
      else if (bs) fc->ResetBitAll(TFileInfo::kStaged);

      nChanged = fc->GetNFiles();
    }
    else {

      TIter j(fc->GetList());
      TFileInfo *fi;

      while (fi = dynamic_cast<TFileInfo *>(j.Next())) {

        if (fi->FindByUrl(fileUrl)) {

          if (!allFiles) {
            Printf(">> Found on dataset %s", dsUri.Data());
          }

          if (bC)      fi->SetBit(TFileInfo::kCorrupted);
          else if (bc) fi->ResetBit(TFileInfo::kCorrupted);

          if (bS)      fi->SetBit(TFileInfo::kStaged);
          else if (bs) fi->ResetBit(TFileInfo::kStaged);

          nChanged++;

        }
      }

    }

    if (nChanged > 0) {
      fc->Update();
      TString group, user, dsName;
      mgr->ParseUri(dsUri, &group, &user, &dsName);
      Int_t r = mgr->WriteDataSet(group, user, dsName, fc);

      if (r == 0) {
        Printf(">> Can't write back dataset %s, check permissions",
          dsUri.Data());
        regErrors++;
      }
      else {
        Printf(">> Dataset %s written back with success", dsUri.Data());
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
}

/** Repair datasets: this function gives the possibility to take actions on
 *  corrupted files. Possible actions are:
 *
 *   - unlist:    files are removed from dataset
 *   - unstage:   files are deleted from storage and marked as unstaged
 *   - uncorrupt: files are marked as unstaged/uncorrupted
 *
 *  Actions can be combined, separe them with colon ':'. Actions "unlist" and
 *  "uncorrupt" can not be combined.
 *
 *  For instance, to remove files both from the list and from the disk, you can
 *  use "unlist:unstage".
 *
 *  If no valid action is given, corrupted files are only listed.
 *
 *  The dataset(s) to be repaired are limited by the dsMask parameter, which can
 *  be both a single dataset name and a mask.
 */
void afRepairDs(const char *dsMask = "/*/*", const TString action = "",
  const TString listOutFile = "") {

  Bool_t aUncorrupt = kFALSE;
  Bool_t aUnstage = kFALSE;
  Bool_t aUnlist = kFALSE;

  TObjArray *tokens = action.Tokenize(":");

  for (Int_t i=0; i<tokens->GetEntries(); i++) {

   TObjString *tok = tokens->At(i);

    if (tok->GetString() == "uncorrupt") {
      aUncorrupt = kTRUE;
    }
    else if (tok->GetString() == "unstage") {
      aUnstage = kTRUE;
    }
    else if (tok->GetString() == "unlist") {
      aUnlist = kTRUE;
    }

  }

  delete tokens;

  // Check for incompatible options
  if (aUncorrupt && aUnlist) {
    Printf("Can't mark as uncorrupted and unlist at the same time.");
    return;
  }

  // Output a text file with the list of "bad" files
  ofstream outList;
  if (listOutFile != "") {
    outList.open(listOutFile.Data());
    if (!outList) {
      Printf("The desired output text file can not be opened, aborting.");
      return;
    }
  }

  TDataSetManagerFile *mgr = afCreateDsMgr();
  TList *listOfDs = afGetListOfDs(dsMask);
  TObjString *dsUriObj;
  TIter iDs(listOfDs);

  // Loop over datasets
  while (dsUriObj = dynamic_cast<TObjString *>(iDs.Next())) {

    TString dsUri = dsUriObj->GetString();
    TFileCollection *fc = mgr->GetDataSet(dsUri.Data());
    TFileCollection *newFc = NULL;
    
    if (aUncorrupt || aUnstage || aUnlist) {
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

        TUrl *url = fi->GetFirstUrl();

        Printf(">> CORRUPTED: %s", url->GetUrl());
        if (outList.is_open()) {
          outList << url->GetUrl() << endl;
        }

        if (aUncorrupt || aUnstage || aUnlist) {

          if (aUncorrupt) {
            fi->ResetBit(TFileInfo::kCorrupted);
            fi->ResetBit(TFileInfo::kStaged);
          }

          if (aUnstage) {
            TString cmd = Form("xrd %s:%d rm %s", url->GetHost(),
              url->GetPort(), url->GetFile());
            Printf(">>>> Unstaging: %s", cmd.Data());
            fi->ResetBit(TFileInfo::kStaged);
            gSystem->Exec( cmd );
          }

          if (!aUnlist) {
            TFileInfo *newFi = new TFileInfo(*fi);
            newFc->Add(newFi);
          }

          nChanged++;
        }

      }
    }

    delete fc;

    if (nChanged > 0) {

      newFc->Update();

      TString group, user, dsName;
      mgr->ParseUri(dsUri, &group, &user, &dsName);
      Int_t r = mgr->WriteDataSet(group, user, dsName, newFc);

      if (r != 0) {
        char *defTree = newFc->GetDefaultTreeName();
        Printf("");
        Printf("*** The dataset %s has changed! ***", dsUri.Data());
        Printf(">> Default tree: %s", (defTree ? defTree :
          "(no default tree)"));
        Printf(">> Number of files: %ld (%.2f%% staged)", newFc->GetNFiles(),
          newFc->GetStagedPercentage());
      }
      else {
        Printf("*** Can't write back modified dataset %s! ***", dsUri.Data());
      }

      Printf("");
    }

    if (newFc) {
      delete newFc;
    }

  }

  delete mgr;
  delete listOfDs;

  if (outList.is_open()) {
    outList.close();
  }
}

/** Shows on the screen the list of datasets that match the search mask.
 */
void afShowListOfDs(const char *dsMask = "/*/*") {

  TList *dsList = afGetListOfDs(dsMask);
  TObjString *nameObj;
  TIter i(dsList);

  while (nameObj = dynamic_cast<TObjString *>(i.Next())) {
    Printf(nameObj->GetString().Data());
  }

  Printf(">> There are %d dataset(s) matching your criteria",
    dsList->GetSize());

  delete dsList;
}


/** A shortcut to mark every file on a specified dataset mask as nonstaged and
 *  noncorrupted.
 */
void afResetDataSet(const char *dsMask = "/*/*") {
  afMarkUrlAs("*", "sc", dsMask);
}
