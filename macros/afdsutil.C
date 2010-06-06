/** Init function, it only prints a welcome message with the default parameters.
 */
void afdsutil() {
  const char *dsPath = gEnv->GetValue("af.dspath", "/tmp");
  Printf("Dataset management utilities loaded.");
  Printf("Available functions start with \"af\", use autocompletion to list");
  Printf(">> Datasets path: %s - change it with afSetDsPath()", dsPath);
}

/** Sets the path of the dataset source, and saves it to the af.dspath variable
 *  inside the user's ~/.rootrc.
 */
void afSetDsPath(const char *dsPath) {
  gEnv->SetValue("af.dspath", dsPath);
  gEnv->SaveLevel(kEnvUser);
}

/** A TDataSetManagerFile object is created using default parameters.
 */
TDataSetManagerFile *_afCreateDsMgr() {
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

  TDataSetManagerFile *mgr = _afCreateDsMgr();
  TFileCollection *fc = mgr->GetDataSet(dsUri);

  if (!fc) {
    Printf("Error opening dataset URI %s", dsUri);
    delete mgr;
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
TList *_afGetListOfDs(const char *dsMask = "/*/*") {

  TDataSetManagerFile *mgr = _afCreateDsMgr();
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
 *
 *  If keepOnlyLastUrl is kTRUE, it removes every URL in each TFileInfo but the
 *  last one.
 */
void afMarkUrlAs(const char *fileUrl, TString bits = "",
  const char *dsMask = "/*/*", Bool_t keepOnlyLastUrl = kFALSE) {

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

  TDataSetManagerFile *mgr = _afCreateDsMgr();
  TList *listOfDs = _afGetListOfDs(dsMask);
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

      if ((allFiles) || (fi->FindByUrl(fileUrl))) {

        if (!allFiles) {
          Printf(">> Found on dataset %s", dsUri.Data());
        }

        if (bC)      fi->SetBit(TFileInfo::kCorrupted);
        else if (bc) fi->ResetBit(TFileInfo::kCorrupted);

        if (bS)      fi->SetBit(TFileInfo::kStaged);
        else if (bs) fi->ResetBit(TFileInfo::kStaged);

        if (keepOnlyLastUrl) {
          _afKeepOnlyLastUrl(fi);
        }

        nChanged++;

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
 *
 *  If you specify a non-empty filename for listOutFile, the list of bad files
 *  is written there.
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

  TDataSetManagerFile *mgr = _afCreateDsMgr();
  TList *listOfDs = _afGetListOfDs(dsMask);
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

          if ((aUnstage) && (_afUnstage(url, kTRUE))) {
            fi->ResetBit(TFileInfo::kStaged);
          }

          if (!aUnlist) {
            TFileInfo *newFi = new TFileInfo(*fi);
            newFc->Add(newFi);
          }

          nChanged++;
        }

      }
      else if (newFc) {
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

  TList *dsList = _afGetListOfDs(dsMask);
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
  afMarkUrlAs("*", "sc", dsMask, kTRUE);
}

/** Reads the list of URLs inside a TFileInfo and keeps only the last URL in the
 *  list.
 */
Int_t _afKeepOnlyLastUrl(TFileInfo *fi) {

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

/** Launch xrd to unstage the file. Return value of xrd is ignored. By default,
 *  output is not suppressed.
 *
 *  Returns kTRUE on success, kFALSE otherwise.
 */
Bool_t _afUnstage(TUrl *url, Bool_t suppressOutput = kFALSE) {

  TString cmd = Form("xrd %s:%d rm %s", url->GetHost(), url->GetPort(),
    url->GetFile());

  if (suppressOutput) {
    cmd.Append(" > /dev/null 2>&1");
  }

  Printf(">>>> Unstaging: %s", cmd.Data());

  gSystem->Exec( cmd );
}

/** Creates a collection directly from AliEn and registers it as a dataset. If
 *  the Grid connection does not exist, it creates it.
 *
 *  You have to specify:
 *
 *  - The basePath of the search, e.g. "/alice/data/2010/LHC10b/".
 *
 *  - A list of runs, separated by colons, as a string, even without initial
 *    zeroes, like "114783:114786:114798".
 *
 *  - The files of which you wish to make a collection: one of "esd", "esd.tag",
 *    "aod", "zip". You may also specify an anchor: if you want, for instance,
 *    to collect every root_archive.zip and make the dataset point to the
 *    AliESDs.root (the most common scenario), you would specify "zip#esd".
 *
 *  - The pass number (applies only for data, ignored in sim).
 *
 *  The dataset name is guessed automatically in most cases. If not, you will be
 *  prompted for each dataset.
 *
 *  Event information on the datasets is filled at the end if verifyDs = kTRUE.
 *  Please note that the standard TFileCollection::ScanDataSet() function is
 *  used, so if a file is unavailable for some reason, the file is marked as
 *  unstaged.
 *
 *  If you want to try the search WITHOUT SAVING THE DATASETS, set dryRun to
 *  kTRUE.
 *
 */
void afCreateCollectionFromAliEn(TString basePath, TString runList,
  TString dataType = "zip#esd", Int_t passNum = 1, Bool_t verifyDs = kFALSE,
  Bool_t dryRun = kFALSE) {

  if (!gGrid) {
    if (!TGrid::Connect("alien:")) {
      Printf("Can't connect to AliEn.");
      return;
    }
  }

  // What to search for
  dataType.ToLower();
  TString fileAnchor;
  TString dataSub;
  TString treeName = "";

  Ssiz_t sharpIdx = dataType.Index('#');

  if (sharpIdx >= 0) {
    fileAnchor = dataType(sharpIdx+1, dataType.Length()-sharpIdx-1);
    dataType = dataType(0, sharpIdx);
  }
  else {
    fileAnchor = "";
  }

  TString filePtn;

  if (dataType == "esd.tag") {
    dataType = "ESDs";
    filePtn  = "*ESD.tag.root";
  }
  else if (dataType == "esd") {
    dataType = "ESDs";
    filePtn  = "AliESDs.root";
    treeName = "/esdTree";
  }
  else if (dataType == "aod") {
    dataType = "AODs";
    filePtn  = "AliAODs.root";
    treeName = "/aodTree";
  }
  else if (dataType == "zip") {
    dataType = "ESDs";
    filePtn  = "root_archive.zip";
  }
  else {
    Printf("Unsupported data type: %s", dataType.Data());
    return;
  }

  if (fileAnchor == "esd") {
    fileAnchor = "AliESDs.root";
    treeName = "/esdTree";
  }
  else if (fileAnchor == "esd.tag") {
    fileAnchor = "AliESDs.tag.root";
  }
  else if (fileAnchor != "") {
    Printf("Unsupported anchor: %s", fileAnchor.Data());
    return;
  }

  // Guess name of the dataset
  TString baseDsPath = "";
  TString dsNameFormat = "";
  Bool_t guessed = kTRUE;
  if (basePath.Contains("data")) {
    baseDsPath = "/alice/data/";
    dsNameFormat = "%s%s_%09d_p%d";
  }
  else if (basePath.Contains("sim")) {
    baseDsPath = "/alice/sim/";
    dsNameFormat = "%s%s_%06d";
  }
  else {
    guessed = kFALSE;
  }

  // Guess LHC period
  TString lhcPeriod = "";
  if (guessed) {
    TPMERegexp reLhc("/(LHC[^/]+)");
    if (reLhc.Match(basePath) == 2) {
      lhcPeriod = reLhc[1];
    }
    else {
      guessed = kFALSE;
    }
  }

  // Print a warning if it is impossible to guess the names of the dataests
  if (!guessed) {
    Printf("Warning: can't guess the final name of the datasets!");
    Printf("Dataset names will be asked one by one.");
    const char *ret = Getline("Do you want to proceed [y|n]? ");
    if ( *ret != 'y' ) {
      Printf("Aborting.");
      return;
    }
  }

  // The dataset manager
  TDataSetManagerFile *mgr = NULL;
  if (!dryRun) {
    mgr = _afCreateDsMgr();
  }

  // The final list of written datasets
  TList *writtenDsList = new TList();
  writtenDsList->SetOwner(kTRUE);

  TObjArray *runs = runList.Tokenize(":");
  TIter run(runs);
  TObjString *runOs;

  while (runOs = dynamic_cast<TObjString *>(run.Next())) {

    TFileCollection *fc = new TFileCollection("dummy", "dummy");

    Int_t runNum = runOs->GetString().Atoi();

    TString searchPtn = Form("%09d/%s/pass%d/*%d*/%s", runNum, dataType.Data(),
      passNum, runNum, filePtn.Data());

    //Printf("basePath=%s searchPtn=%s", basePath.Data(), searchPtn.Data());

    TGridResult *res = gGrid->Query(basePath.Data(), searchPtn.Data());
    Int_t nEntries = res->GetEntries();

    if (nEntries < 1) {
      Printf("No results found in basedir %s matching pattern %s, skipping",
        basePath.Data(), searchPtn.Data());
      delete res;
      continue;
    }

    for (Int_t i=0; i<nEntries; i++) {

      Long64_t size = TString(res->GetKey(i, "size")).Atoll();

      TString tUrl = res->GetKey(i, "turl");
      if (fileAnchor != "") {
        tUrl.Append("#");
        tUrl.Append(fileAnchor);
      }

      fc->Add( new TFileInfo( tUrl, size, res->GetKey(i,"guid"),
        res->GetKey(i,"md5") ) );

      //Printf(">> %s (%lld bytes)", tUrl.Data(), size);
    }

    fc->Update();

    TString um;
    Double_t fmtSize;
    _afNiceSize(fc->GetTotalSize(), um, fmtSize);

    TString dsName;

    if (guessed) {
      // Form the guessed ds name
      dsName = Form(dsNameFormat.Data(), baseDsPath.Data(), lhcPeriod.Data(),
        runNum, passNum);
    }
    else {
      // Ask user
      TString ask = Form("Found %d files (%.1lf %s total). Dataset name? ",
        fc->GetNFiles(), fmtSize, um.Data());
      char *buf = Getline(ask);
      Int_t l = strlen(ret);
      while ((--l >= 0) && ((buf[l] == '\n') || (buf[l] == '\r'))) {
        buf[l] = '\0';
      }
      dsName = buf;
    }

    writtenDsList->Add(new TObjString(dsName.Data()));

    if (treeName != "") {
      fc->SetDefaultTreeName(treeName.Data());
    }

    TString opStatus;

    if (mgr) {

      TString group, user, name;
      mgr->ParseUri(dsName, &group, &user, &name);
      Int_t r = mgr->WriteDataSet(group, user, name, fc);

      if (r == 0) {
        opStatus = "can't write!";
      }
      else {
        opStatus = "saved";
      }

    }
    else {
      opStatus = "not saved";
    }

    Printf(">> %- 45s : % 4d files, %6.1lf %s total size [%s]", dsName.Data(),
      fc->GetNFiles(), fmtSize, um.Data(), opStatus.Data());

    delete res;
    delete fc;

  }

  delete runs;

  if (mgr) {
    delete mgr;
  }

  // Eventually verify the datasets
  if (verifyDs) {
    Printf("\n*** Starting verification phase ***\n");
    TObjString *dsNameObj;
    TIter iDs(writtenDsList);
    while (dsNameObj = dynamic_cast<TObjString *>(iDs.Next())) {
      TString dsName = dsNameObj->GetString();
      afScanDs(dsName);
    }
  }

  delete writtenDsList;
}

/** Removes a dataset from the disk. Files associated to the dataset are not
 *  removed.
 */
void afRemoveDs(TString dsUri) {
  TString path = Form("%s/%s.root", gEnv->GetValue("af.dspath", "/tmp"),
    dsUri.Data());
  if (gSystem->Unlink(path.Data()) < 0) {
    Printf("Can't remove %s from disk", path.Data());
  }
  else {
    Printf("Dataset removed: %s", dsUri.Data());
  }
}

/** Formats a file size returning the new size and the unit of measurement.
 */
void _afNiceSize(Long64_t bytes, TString &um, Double_t &size) {

  const char *ums[] = { "bytes", "KiB", "MiB", "GiB", "TiB" };
  Int_t maxDiv = sizeof(ums)/sizeof(const char *);
  Int_t nDiv = 0;
  Double_t b = bytes;

  while ((b >= 1024.) && (nDiv+1 < maxDiv)) {
    b /= 1024.;
    nDiv++;
  }

  um = ums[nDiv];
  size = b;
}

/** Scans a given dataset (or a list of datasets through a mask), filling
 *  information about the number of events, etc. Run with quiet = kTRUE to have
 *  cleaner output.
 *
 *  Please note that the Grid connection is not opened automatically!
 */
void afScanDs(TString dsMask, Bool_t quiet = kFALSE) {

  Int_t oldErrorIgnoreLevel = gErrorIgnoreLevel;

  if (quiet) {
    oldErrorIgnoreLevel = gErrorIgnoreLevel;
    gErrorIgnoreLevel = 10000;
  }

  TDataSetManagerFile *mgr = _afCreateDsMgr();
  TList *listOfDs = _afGetListOfDs(dsMask);
  TObjString *dsUriObj;
  TIter iDs(listOfDs);

  while (dsUriObj = dynamic_cast<TObjString *>(iDs.Next())) {

    TString dsUri = dsUriObj->GetString();

    TFileCollection *fc = mgr->GetDataSet(dsUri.Data());
    fc->ResetBitAll(TFileInfo::kStaged);
    fc->ResetBitAll(TFileInfo::kCorrupted);

    if (!fc) {
      Printf("Can't find dataset %s", dsUri.Data());
      return;
    }
    else {
      Printf("Scanning dataset %s, be patient...", dsUri.Data());
    }

    Int_t s = mgr->ScanDataSet(fc, 0);

    if (s == 2) {
      TString group, user, dsName;
      mgr->ParseUri(dsUri, &group, &user, &dsName);
      Int_t r = mgr->WriteDataSet(group, user, dsName, fc);

      if (r == 0) {
        Printf(">> Can't write back dataset %s, check permissions",
          dsUri.Data());
      }
      else {
        Printf(">> Dataset %s written back with success", dsUri.Data());
      }
    }
    else {
      Printf(">> Dataset %s not modified", dsUri.Data());
    }
  }

  delete mgr;

  if (quiet) {
    gErrorIgnoreLevel = oldErrorIgnoreLevel;
  }
}
