#if !defined(__CINT__) || defined (__MAKECINT__)

#include <Getline.h>
#include <Riostream.h>
#include <TDataSetManagerFile.h>
#include <TEnv.h>
#include <TError.h>
#include <TFileCollection.h>
#include <TFileInfo.h>
#include <TGrid.h>
#include <TGridResult.h>
#include <THashList.h>
#include <TList.h>
#include <TObjArray.h>
#include <TObjString.h>
#include <TPRegexp.h>
#include <TProof.h>

#endif

/* ========================================================================== *
 *                            "PRIVATE" FUNCTIONS                             *
 * ========================================================================== */

// A global variable
Int_t _afOldErrorIgnoreLevel = -1;

/** Return kTRUE if PROOF mode is active, and connects to PROOF (masteronly) if
 *  no PROOF connection is active.
 */
Bool_t _afProofMode() {
  Bool_t proofMode = gEnv->GetValue("af.proofmode", 0);
  if ((proofMode) && (!gProof)) {
    TProof::Open(gEnv->GetValue("af.userhost", "localhost"), "masteronly");
  }
  return proofMode;
}

/** A TDataSetManagerFile object is created using default parameters.
 */
TDataSetManagerFile *_afCreateDsMgr() {
  TDataSetManagerFile *mgr = new TDataSetManagerFile( NULL, NULL,
    Form("dir:%s", gEnv->GetValue("af.dspath", "/pool/datasets")) );
  return mgr;
}

/** Gets a flattened TList of datasets. The list must be destroyed by the user,
 *  and it is owner of its content. The list is alphabetically ordered.
 *
 *  The dsMask parameter can both specify a single dataset name or a mask which
 *  must be understandable by the TDataSetManagerFile::GetDataSets() function.
 */
TList *_afGetListOfDs(const char *dsMask = "/*/*") {

  TDataSetManagerFile *mgr = NULL;
  TList *listOfDs = new TList();
  listOfDs->SetOwner();

  if (!_afProofMode()) {
    mgr = _afCreateDsMgr();
  }

  // First of all it tries to open a dataset named "mask"; if does not succeed,
  // it considers it a mask.
  Int_t oldErrorIgnoreLevel = gErrorIgnoreLevel;
  gErrorIgnoreLevel = 10000;
  TFileCollection *fc = NULL;
  if (mgr) {
    fc = mgr->GetDataSet(dsMask);
  }
  else {
    fc = gProof->GetDataSet(dsMask);
  }
  gErrorIgnoreLevel = oldErrorIgnoreLevel;

  if (fc) {
    listOfDs->Add( new TObjString(dsMask) );
    delete fc;
    if (mgr) {
      delete mgr;
    }
    return listOfDs;
  }

  // Not a single dataset: let's try with a mask
  TMap *groups = NULL;
  if (mgr) {
    groups = mgr->GetDataSets(dsMask, TDataSetManager::kReadShort);
  }
  else {
    groups = gProof->GetDataSets(dsMask);
  }

  if (!groups) {
    if (mgr) {
      delete mgr;
    }
    return listOfDs;  // will be empty
  }

  groups->SetOwnerKeyValue();  // important to avoid leaks!
  TIter gi(groups);
  TObjString *gn;

  if (mgr) {

    // TDataSetManagerFile mode

    while ((gn = dynamic_cast<TObjString *>(gi.Next()))) {

      TMap *users = dynamic_cast<TMap *>( groups->GetValue( gn->String() ) );
      users->SetOwnerKeyValue();
      TIter ui(users);
      TObjString *un;

      while ((un = dynamic_cast<TObjString *>(ui.Next()))) {

        TMap *dss = dynamic_cast<TMap *>( users->GetValue( un->String() ) );
        dss->SetOwnerKeyValue();
        TIter di(dss);
        TObjString *dn;

        while ((dn = dynamic_cast<TObjString *>(di.Next()))) {

          // No COMMON user/group mapping is done here...
          TString dsUri = TDataSetManager::CreateUri( gn->String(), un->String(),
            dn->String() );

          listOfDs->Add( new TObjString(dsUri.Data()) );

        }
      }
    }

    delete mgr;
  }
  else {
    // PROOF mode
    while ((gn = dynamic_cast<TObjString *>(gi.Next()))) {
      listOfDs->Add( new TObjString( gn->String().Data() ) );
    }
  }

  delete groups;

  listOfDs->Sort();

  return listOfDs;
}

/** Reads the list of URLs inside a TFileInfo and keeps only the last URL in the
 *  list.
 */
void _afKeepOnlyLastUrl(TFileInfo *fi) {

  TList *urlList = new TList();
  urlList->SetOwner();
  Int_t count = 0;
  Int_t nUrls = fi->GetNUrls();
  TUrl *url;

  fi->ResetUrl();

  while ((url = fi->NextUrl())) {
    if (++count < nUrls) {
      urlList->Add( new TObjString(url->GetUrl()) );
    }
  }

  TIter i(urlList);
  TObjString *s;
  while ((s = dynamic_cast<TObjString *>(i.Next()))) {
    fi->RemoveUrl( s->String().Data() );
  }

  fi->ResetUrl();

  delete urlList;
}

/** Appends an alien:// URL at the end of the URL list, if it is able to guess
 *  it from the current URL.
 *
 *  It returns kTRUE on success (i.e. URL appended) and kFALSE if it was
 *  impossible to determine the alien:// URL.
 */
Bool_t _afAppendRecoveredAliEnUrl(TFileInfo *fi) {

  TString curUrl = fi->GetCurrentUrl()->GetUrl();
  TString newUrl;

  TPMERegexp reAliEn("/alice/.*");
  if (reAliEn.Match( curUrl ) == 1) {
    newUrl = "alien://" + reAliEn[0];
    fi->AddUrl( newUrl.Data() );  // by default it is appended to the end
    return kTRUE;
  }

  return kFALSE;
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

  Printf(">> Unstaging: %s", cmd.Data());

  gSystem->Exec( cmd );

  return kTRUE;
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

/** Saves a dataset to the disk or on PROOF, depending on the opened connection.
 *  It returns kTRUE on success, kFALSE on failure. If quiet is kTRUE, it does
 *  print messages on success/failure.
 */
Bool_t _afSaveDs(TString dsUri, TFileCollection *fc, Bool_t overwrite,
  Bool_t quiet = kFALSE) {

  Bool_t regSuccess;

  if (!_afProofMode()) {
    TDataSetManagerFile *mgr = _afCreateDsMgr();
    TString group, user, name;
    mgr->ParseUri(dsUri, &group, &user, &name);
    if (mgr->WriteDataSet(group, user, name, fc) == 0) {
      regSuccess = kFALSE;
    }
    else {
      regSuccess = kTRUE;
    }
    delete mgr;
  }
  else {
    TString opts = "T";
    if (overwrite) {
      opts.Append("O");
    }
    regSuccess = gProof->RegisterDataSet(dsUri.Data(), fc, opts.Data());
  }

  if (regSuccess) {
    if (!quiet) {
      Printf(">> Dataset %s written with success", dsUri.Data());
    }
    return kTRUE;
  }

  if (!quiet) {
    Printf(">> Can't write dataset %s, check permissions", dsUri.Data());
  }

  return kFALSE;
}

/** Makes a collection from AliEn find. This is equivalent to the AliEn command:
 *
 *    find <basePath> <fileName>
 *
 *  An "anchor" is added to every file, if specified. The default tree of the
 *  collection can also be set.
 */
TFileCollection *_afAliEnFind(TString basePath, TString fileName,
  TString anchor, TString defaultTree) {

  if (!gGrid) {
    if (!TGrid::Connect("alien:")) {
      Printf("Can't connect to AliEn.");
      return NULL;
    }
  }

  TFileCollection *fc = new TFileCollection();

  TGridResult *res = gGrid->Query(basePath.Data(), fileName.Data());
  Int_t nEntries = res->GetEntries();

  for (Int_t i=0; i<nEntries; i++) {

    Long64_t size = TString(res->GetKey(i, "size")).Atoll();

    TString tUrl = res->GetKey(i, "turl");
    if (anchor != "") {
      tUrl.Append("#");
      tUrl.Append(anchor);
    }

    fc->Add( new TFileInfo( tUrl, size, res->GetKey(i, "guid"),
      res->GetKey(i,"md5") ) );
  }

  if (defaultTree != "") {
    fc->SetDefaultTreeName(defaultTree.Data());
  }

  fc->Update();  // needed

  return fc;
}

/** Obtains user's input without the final '\n'.
 */
TString _afGetLine(const char *prompt) {
  char *buf = Getline(prompt);
  Int_t l = strlen(buf);
  while ((--l >= 0) && ((buf[l] == '\n') || (buf[l] == '\r'))) {
    buf[l] = '\0';
  }
  return TString(buf);
}

/** Fills the metadata of the given TFileInfo by reading information from the
 *  file pointed by the first URL in the list. Information about TTrees and
 *  classes that inherit thereof are read.
 *
 *  In case of success it returns kTRUE. If any failure occurs it returns
 *  kFALSE.
 */
Bool_t _afFillMetaDataFile(TFileInfo *fi, Bool_t quiet = kFALSE) {

  if (quiet) {
    _afRootQuietOn();
  }

  // Let's start by removing old metadata
  fi->RemoveMetaData();

  // Get the URL and eventually open an AliEn connection
  TUrl *url = fi->GetCurrentUrl();

  if ((!gGrid) && (strcmp(url->GetProtocol(), "alien") == 0)) {
    if (!TGrid::Connect("alien:")) {
      _afRootQuietOff();
      return kFALSE;
    }
  }

  // Let us open the file
  TFile *f = TFile::Open(url->GetUrl());

  if (!f) {
    Printf("Can't open file %s!", url->GetUrl());
    _afRootQuietOff();
    return kFALSE;
  }

  // Get the ROOT file content
  TIter k( f->GetListOfKeys() );
  TKey *key;

  while ( key = dynamic_cast<TKey *>(k.Next()) ) {

    if ( TClass::GetClass(key->GetClassName())->InheritsFrom("TTree") ) {

      // Every TTree (or inherited thereof) will be scanned for entries
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
        Printf("!! In file %s, can't read TTree %s!",
          url->GetUrl(), key->GetName());
        f->Close();
        delete f;
        _afRootQuietOff();
        return kFALSE;
      }

    }
  }

  f->Close();
  delete f;

  _afRootQuietOff();
  return kTRUE;
}

/** Switches on the quiet ROOT mode.
 */
void _afRootQuietOn() {
  _afOldErrorIgnoreLevel = gErrorIgnoreLevel;
  gErrorIgnoreLevel = 10000;
}

/** Switches off the quiet ROOT mode.
 */
void _afRootQuietOff() {
  if (_afOldErrorIgnoreLevel != -1) {
    gErrorIgnoreLevel = _afOldErrorIgnoreLevel;
    _afOldErrorIgnoreLevel = -1;
  }
}

/* ========================================================================== *
 *                             "PUBLIC" FUNCTIONS                             *
 * ========================================================================== */

/** Print current dataset utilities settings.
 */
void afPrintSettings() {
  Printf(">> Datasets path: %s - change it with afSetDsPath()",
    gEnv->GetValue("af.dspath", "/tmp"));
  Printf(">> PROOF connect string: %s - change it with afSetProofUserHost()", 
    gEnv->GetValue("af.userhost", "localhost"));
  Printf(">> PROOF mode is active? %s - toggle it with afSetProofMode()",
    (gEnv->GetValue("af.proofmode", 0) ? "YES" : "NO"));
}

/** Sets the path of the dataset source, and saves it to the af.dspath variable
 *  inside the user's ~/.rootrc.
 */
void afSetDsPath(const char *dsPath) {
  gEnv->SetValue("af.dspath", dsPath);
  gEnv->SaveLevel(kEnvUser);
}

/** Sets the default PROOF connection parameters and saves them.
 */
void afSetProofUserHost(const char *userHost = "localhost") {
  gEnv->SetValue("af.userhost", userHost);
  gEnv->SaveLevel(kEnvUser);
}

/** Sets the PROOF mode: do not use the TDataSetManagerFile directly, but use a
 *  PROOF connection. Useful for users who are not administrators.
 */
void afSetProofMode(Bool_t proofMode = kTRUE) {
  gEnv->SetValue("af.proofmode", (Int_t)proofMode);
  gEnv->SaveLevel(kEnvUser);
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

  TDataSetManagerFile *mgr = NULL;
  TFileCollection *fc;

  if (_afProofMode()) {
    fc = gProof->GetDataSet(dsUri);
  }
  else {
    mgr = _afCreateDsMgr();
    fc = mgr->GetDataSet(dsUri);
  }

  if (!fc) {
    Printf("Error opening dataset URI %s", dsUri);
    if (mgr) {
      delete mgr;
    }
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

  while ((inf = dynamic_cast<TFileInfo *>(i.Next()))) {
    count++;

    Bool_t s = inf->TestBit(TFileInfo::kStaged);
    Bool_t c = inf->TestBit(TFileInfo::kCorrupted);

    if ( ((s && bS) || (!s && bs)) && ((c && bC) || (!c && bc)) ) {
      TFileInfoMeta *meta = inf->GetMetaData();  // gets the first one
      Int_t entries = (meta ? meta->GetEntries() : -1);
      Printf("% 4u. %c%c | % 7d | %s", ++countMatching,
        (s ? 'S' : 's'), (c ? 'C' : 'c'),
        entries,
        inf->GetCurrentUrl()->GetUrl());
      TUrl *url;
      inf->NextUrl();

      // Every URL is shown, not only first
      while ((url = inf->NextUrl())) {
        Printf("         |         | %s", url->GetUrl());
      }
      inf->ResetUrl();
    }
  }

  delete fc;

  if (mgr) {
    delete mgr;
  }

  Printf(">> There are %u file(s) in the dataset, %u matched your criteria",
    count, countMatching);
}

/** Marks a file matching the URL as (un)staged or (un)corrupted: choose mode
 *  with one or more among "SsCc". Clearly, "S" is incompatible with "s" and "C"
 *  is incompatible with "c". The URL may match any URL of a TFileInfo, even not
 *  the first one.
 *
 *  A value of "M" refreshes current metadata (i.e. file is "verified"), while
 *  a value of "m" deletes the metadata from the object.
 *
 *  EXAMPLE: to remove metadata and mark as corrupted, use "Cm"; instead, to
 *  uncorrupt, mark as staged and fill metadata, use "cSm".
 *
 *  The URL is searched in the dataset(s) specified by dsMask.
 *
 *  If "*" is used as URL, it applies to every entry in the dataset.
 *
 *  If keepOnlyLastUrl is kTRUE, it removes every URL in each TFileInfo but the
 *  last one.
 *
 *  If told to do so, it appends an eventually guessed AliEn path *before*
 *  removing all the other URLs. Useful if the dataset was created without
 *  keeping the originating URL in each TFileInfo (afdsmgrd <= v0.1.7).
 */
void afMarkUrlAs(const char *fileUrl, TString bits = "",
  const char *dsMask = "/*/*", Bool_t keepOnlyLastUrl = kFALSE,
  Bool_t appendRecoveredAliEnPath = kFALSE) {

  Bool_t allFiles = kFALSE;

  if (strcmp(fileUrl, "*") == 0) {
    allFiles = kTRUE;
  }

  Bool_t bS = kFALSE;
  Bool_t bs = kFALSE;
  Bool_t bC = kFALSE;
  Bool_t bc = kFALSE;
  Bool_t bM = kFALSE;
  Bool_t bm = kFALSE;

  if (bits.Index('S') >= 0) bS = kTRUE;
  if (bits.Index('s') >= 0) bs = kTRUE;
  if (bits.Index('C') >= 0) bC = kTRUE;
  if (bits.Index('c') >= 0) bc = kTRUE;
  if (bits.Index('M') >= 0) bM = kTRUE;
  if (bits.Index('m') >= 0) bm = kTRUE;

  Bool_t err = kFALSE;

  if (bs && bS) {
    Printf("Cannot set STAGED and NOT STAGED at the same time!");
    err = kTRUE;
  }

  if (bc && bC) {
    Printf("Cannot set CORRUPTED and NOT CORRUPTED at the same time!");
    err = kTRUE;
  }

  if (bm && bM) {
    Printf("Cannot REMOVE and REFRESH metadata at the same time!");
    err = kTRUE;
  }

  if ( !(bs || bS || bc || bC || bm || bM) ) {
    Printf("Nothing to do: specify sScC as options to (un)mark STAGED or "
      "CORRUPTED bit, add mM to remove/refresh metadata");
    err = kTRUE;
  }

  if (err) {
    return;
  }

  TDataSetManagerFile *mgr = NULL;

  if (!_afProofMode()) {
    mgr = _afCreateDsMgr();
  }

  TList *listOfDs = _afGetListOfDs(dsMask);
  Int_t regErrors = 0;
  TIter i(listOfDs);
  TObjString *dsUriObj;

  while ( (dsUriObj = dynamic_cast<TObjString *>(i.Next())) ) {

    TString dsUri = dsUriObj->String();
    Int_t nChanged = 0;

    TFileCollection *fc;
    if (mgr) {
      fc = mgr->GetDataSet(dsUri.Data());
    }
    else {
      fc = gProof->GetDataSet(dsUri.Data());
    }

    TIter j(fc->GetList());
    TFileInfo *fi;

    while ( (fi = dynamic_cast<TFileInfo *>(j.Next())) ) {

      if ((allFiles) || (fi->FindByUrl(fileUrl))) {

        if (!allFiles) {
          Printf(">> Found in dataset %s", dsUri.Data());
        }

        if (bC)      fi->SetBit(TFileInfo::kCorrupted);
        else if (bc) fi->ResetBit(TFileInfo::kCorrupted);

        if (bS)      fi->SetBit(TFileInfo::kStaged);
        else if (bs) fi->ResetBit(TFileInfo::kStaged);

        if (bm) {
          fi->RemoveMetaData();
        }
        else if (bM) {
          if (!_afFillMetaDataFile( fi, kTRUE )) {
            // The following is non-fatal
            Printf("There was a problem updating metadata of file %s",
              fi->GetCurrentUrl()->GetUrl());
          }
        }

        if (appendRecoveredAliEnPath) {
          _afAppendRecoveredAliEnUrl(fi);
        }

        if (keepOnlyLastUrl) {
          _afKeepOnlyLastUrl(fi);
        }

        nChanged++;

      }
    }

    if (nChanged > 0) {
      fc->Update();
      _afSaveDs(dsUri, fc, kTRUE);
    }

    delete fc;
  }

  if (mgr) {
    delete mgr;
  }
  delete listOfDs;

  if (regErrors > 0) {
    Printf("%d error(s) writing back datasets encountered, check permissions",
      regErrors);
  }
}

/** Finds the exact match of a given URL within the mask of datasets given (by
 *  default in all datasets).
 */
void afFindUrl(const char *fileUrl, const char *dsMask = "/*/*") {

  TDataSetManagerFile *mgr = NULL;

  if (!_afProofMode()) {
    mgr = _afCreateDsMgr();
  }

  TList *listOfDs = _afGetListOfDs(dsMask);
  TIter i(listOfDs);
  TObjString *dsUriObj;
  Int_t nFoundDs = 0;
  Int_t nFoundTotal = 0;

  while ( (dsUriObj = dynamic_cast<TObjString *>(i.Next())) ) {

    TString dsUri = dsUriObj->String();

    TFileCollection *fc;
    if (mgr) {
      fc = mgr->GetDataSet(dsUri.Data());
    }
    else {
      fc = gProof->GetDataSet(dsUri.Data());
    }

    TIter j(fc->GetList());
    TFileInfo *fi;
    Int_t nFoundIntoDs = 0;

    while ( (fi = dynamic_cast<TFileInfo *>(j.Next())) ) {
      if (fi->FindByUrl(fileUrl)) {
        nFoundIntoDs++;
        nFoundTotal++;
      }
    }

    if (nFoundIntoDs) {
      if (nFoundIntoDs == 1) {
        Printf(">> Found in dataset %s once", dsUri.Data());
      }
      else {
        Printf(">> Found in dataset %s (%d times)", dsUri.Data(), nFoundIntoDs);
      }
      nFoundDs++;
    }

    delete fc;
  }

  if (mgr) {
    delete mgr;
  }
  delete listOfDs;

  Printf("Found %d time(s) in %d different dataset(s)", nFoundTotal, nFoundDs);
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

   TObjString *tok = dynamic_cast<TObjString *>(tokens->At(i));

    if (tok->String() == "uncorrupt") {
      aUncorrupt = kTRUE;
    }
    else if (tok->String() == "unstage") {
      aUnstage = kTRUE;
    }
    else if (tok->String() == "unlist") {
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

  TDataSetManagerFile *mgr = NULL;
  if (!_afProofMode()) {
    mgr = _afCreateDsMgr();
  }

  TList *listOfDs = _afGetListOfDs(dsMask);
  TObjString *dsUriObj;
  TIter iDs(listOfDs);

  // Loop over datasets
  while ( (dsUriObj = dynamic_cast<TObjString *>(iDs.Next())) ) {

    TString dsUri = dsUriObj->String();
    TFileCollection *fc;
    TFileCollection *newFc = NULL;

    if (mgr) {
      fc = mgr->GetDataSet(dsUri.Data());
    }
    else {
      fc = gProof->GetDataSet(dsUri.Data());
    }

    if (aUncorrupt || aUnstage || aUnlist) {
      newFc = new TFileCollection();
      newFc->SetDefaultTreeName( fc->GetDefaultTreeName() );
    }

    Printf("Scanning dataset %s for corrupted files...", dsUri.Data());

    TIter j(fc->GetList());
    TFileInfo *fi;
    Int_t nChanged = 0;

    while ( (fi = dynamic_cast<TFileInfo *>(j.Next())) ) {

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
      if ( _afSaveDs(dsUri, newFc, kTRUE, kTRUE) ) {
        Printf("Dataset %s has changed - # of files: %ld (%.2f%% staged)",
          dsUri.Data(), newFc->GetNFiles(), newFc->GetStagedPercentage());
      }
      else {
        Printf("Error while writing dataset %s", dsUri.Data());
      }
    }

    if (newFc) {
      delete newFc;
    }

  }

  if (mgr) {
    delete mgr;
  }
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
  Int_t count = 0;

  while ( (nameObj = dynamic_cast<TObjString *>(i.Next())) ) {
    Printf("% 4d. %s", ++count, nameObj->String().Data());
  }

  Printf(">> There are %d dataset(s) matching your criteria",
    dsList->GetSize());

  delete dsList;
}

/** A shortcut to mark every file on a specified dataset mask as nonstaged and
 *  noncorrupted.
 *
 *  Also, only the last URL of each file is kept, which should be the
 *  originating one.
 *
 *  If the dataset was staged by afdsmgrd <= v0.1.7, then you can set
 *  recoverAliEnUrl = kTRUE to try to restore the originating alien:// URL.
 */
void afResetDs(const char *dsMask = "/*/*", Bool_t recoverAliEnUrl = kFALSE) {
  afMarkUrlAs("*", "scm", dsMask, kTRUE, recoverAliEnUrl);
}

/** Scans a given dataset (or a list of datasets through a mask), filling
 *  information about the number of events, etc. Run with quiet = kTRUE to have
 *  cleaner output.
 *
 *  Please note that the Grid connection is not opened automatically!
 */
Bool_t afScanDs(TString dsMask, Bool_t quiet = kFALSE) {

  if (_afProofMode()) {
    // PROOF mode: simply call VerifyDataSet after a reset
    afResetDs(dsMask);
    gProof->VerifyDataSet(dsMask);
    return;
  }

  Int_t oldErrorIgnoreLevel = gErrorIgnoreLevel;

  if (quiet) {
    oldErrorIgnoreLevel = gErrorIgnoreLevel;
    gErrorIgnoreLevel = 10000;
  }

  TDataSetManagerFile *mgr = _afCreateDsMgr();
  TList *listOfDs = _afGetListOfDs(dsMask);
  TObjString *dsUriObj;
  TIter iDs(listOfDs);

  while ( (dsUriObj = dynamic_cast<TObjString *>(iDs.Next())) ) {

    TString dsUri = dsUriObj->String();

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

    if (mgr->ScanDataSet(fc, 0) == 2) {
      _afSaveDs(dsUri, fc, kTRUE);
    }

  }

  delete mgr;

  if (quiet) {
    gErrorIgnoreLevel = oldErrorIgnoreLevel;
  }
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
 *  If you want to test the search WITHOUT SAVING THE DATASETS, set dryRun to
 *  kTRUE.
 *
 *  ==========================================================================
 *
 *  Some examples of basePath and the resulting name of the dataset:
 *
 *  Simulation in PDC (run number is 6 digits, zero-padded):
 *    /alice/sim/PDC_09/LHC09a9/* --> /alice/sim/PDC09_LHC09a9_082002
 *
 *  Simulation not in PDC (run number is still 6 digits, zero-padded):
 *    /alice/sim/LHC10b4/*        --> /alice/sim/LHC10b4_114933
 *
 *  Real data (we include the pass number and the run is 9 digits, zero-padded):
 *    /alice/data/2010/LHC10b/*   --> /alice/data/LHC10b_000117113_p1
 */
void afCreateDsFromAliEn(TString basePath, TString runList,
  TString dataType = "zip#esd", Int_t passNum = 1, Bool_t verifyDs = kFALSE,
  Bool_t preMarkAsStaged = kFALSE, Bool_t dryRun = kFALSE) {

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
    filePtn  = "AliAOD.root";
    treeName = "/aodTree";
  }
  else if (dataType == "zip") {

    if (fileAnchor == "esd") {
      filePtn = "root_archive.zip";
      dataType = "ESDs";
      fileAnchor = "AliESDs.root";
      treeName = "/esdTree";
    }
    else if (fileAnchor == "esd.tag") {
      filePtn = "root_archive.zip";
      dataType = "ESDs";
      fileAnchor = "AliESDs.tag.root";
    }
    else if (fileAnchor == "aod") {
      filePtn = "aod_archive.zip";
      dataType = "AODs";
      fileAnchor = "AliAOD.root";
    }
    else if (fileAnchor != "") {
      Printf("Unsupported anchor: %s", fileAnchor.Data());
      return;
    }

  }
  else {
    Printf("Unsupported data type: %s", dataType.Data());
    return;
  }

  // No anchor allowed except for archives
  if (!filePtn.EndsWith(".zip") && (fileAnchor != "")) {
    Printf("Anchor not supported for types different from zip.");
    return;
  }

  //Printf("==> dataType=%s, fileAnchor=%s, filePtn=%s", dataType.Data(),
  //  fileAnchor.Data(), filePtn.Data());

  // Guess name of the dataset
  TString dsNameFormat = "";
  TString lhcPeriod = "";
  TString pdcPeriod = "";

  if (_afProofMode()) {
    dsNameFormat = Form("/%s/%s/", gProof->GetGroup(), gProof->GetUser());
  }

  Bool_t guessed = kTRUE;
  Bool_t isSim = kFALSE;

  if (basePath.Contains("data")) {
    if (dsNameFormat == "") {
      dsNameFormat = "/alice/data/";
    }
    else {
      dsNameFormat.Append("DATA_");
    }
    isSim = kFALSE;
    dsNameFormat.Append("%s_%09d_p%d");
  }
  else if (basePath.Contains("sim")) {
    if (dsNameFormat == "") {
      dsNameFormat = "/alice/sim/";
    }
    else {
      dsNameFormat.Append("SIM_");
    }
    isSim = kTRUE;
    dsNameFormat.Append("%s_%06d");
  }
  else {
    guessed = kFALSE;
  }

  // Guess LHC period
  if (guessed) {
    TPMERegexp reLhc("/(LHC[^/]+)");
    if (reLhc.Match(basePath) == 2) {
      lhcPeriod = reLhc[1];
    }
    else {
      guessed = kFALSE;
    }
  }

  // Guess PDC period
  if ((isSim) && (guessed)) {
    TPMERegexp rePdc("/PDC_([^/]+)");
    if (rePdc.Match(basePath) == 2) {
      lhcPeriod = "PDC" + rePdc[1] + "_" + lhcPeriod;
    }
  }

  // Print a warning if it is impossible to guess the names of the dataests
  if (!guessed) {
    Printf("Warning: can't guess the final name of the datasets!");
    Printf("Dataset names will be asked one by one.");
    TString ret = _afGetLine("Do you want to proceed [y|n]? ");
    ret.ToLower();
    if ( !ret.BeginsWith("y") ) {
      Printf("Aborting.");
      return;
    }
  }

  // If verifying the dataset, pre-mark files as staged in order to prevent the
  // dataset manager to stage them
  if (verifyDs) {
    preMarkAsStaged = kTRUE;
  }

  // The dataset manager, if running locally
  TDataSetManagerFile *mgr = NULL;
  if ((!dryRun) && (!_afProofMode())) {
    mgr = _afCreateDsMgr();
  }

  TObjArray *runs = runList.Tokenize(":");
  TIter run(runs);
  TObjString *runOs;

  while ( (runOs = dynamic_cast<TObjString *>(run.Next())) ) {

    Int_t runNum = runOs->String().Atoi();

    TString searchPtn;

    if (!isSim) { // data
      searchPtn = Form("%09d/%s/pass%d/*%d*/%s", runNum, dataType.Data(),
        passNum, runNum, filePtn.Data());
    }
    else {
      // Any other case, inc. sim: the search mask should be big enough to
      // suit custom patterns
      searchPtn = Form("*%d*/*/%s", runNum, filePtn.Data());
    }

    //Printf("basePath=%s searchPtn=%s", basePath.Data(), searchPtn.Data());

    TFileCollection *fc = _afAliEnFind(basePath, searchPtn, fileAnchor,
      treeName);

    if (fc->GetNFiles() == 0) {
      Printf("No results found for basePath=%s searchPtn=%s, skipping",
        basePath.Data(), searchPtn.Data());
      delete fc;
      continue;
    }

    if (preMarkAsStaged) {
      fc->SetBitAll(TFileInfo::kStaged);
      fc->Update();
    }

    TString um;
    Double_t fmtSize;
    _afNiceSize(fc->GetTotalSize(), um, fmtSize);

    TString dsUri;

    if (guessed) {
      // Form the guessed ds name
      dsUri = Form(dsNameFormat.Data(), lhcPeriod.Data(), runNum, passNum);
    }
    else {
      // Ask user
      dsUri = _afGetLine(Form("Found %d files (%.1lf %s total). Dataset name? ",
        fc->GetNFiles(), fmtSize, um.Data()));
    }

    Bool_t verified = kFALSE;

    // Eventually verify the dataset *before* registering it (if mgr)
    if ((verifyDs) && (mgr)) {
      Printf("*** Verifying the dataset %s before saving it ***", dsUri.Data());
      fc->ResetBitAll(TFileInfo::kStaged);  // to allow verification
      if (mgr->ScanDataSet(fc, 0) == 2) {
        verified = kTRUE;
      }
    }

    Bool_t saved;

    if ((!dryRun) && ( _afSaveDs(dsUri, fc, kFALSE, kTRUE) )) {
      saved = kTRUE;
    }
    else {
      saved = kFALSE;
    }

    if ((saved) && (verifyDs) && (!mgr)) {
      Printf("*** Verifying the dataset %s after saving it ***", dsUri.Data());
      afScanDs(dsUri.Data());  // reset "S" bit automatically by afScanDs()
    }

    TString opStatus;

    if ((verified) && (saved)) {
      opStatus = "verified";
    }
    else if (verified) {
      opStatus = "can't write!";
    }
    else if (saved) {
      opStatus = "saved";
    }
    else {
      opStatus = "not saved";
    }

    Printf(">> %- 45s : % 4d files, %6.1lf %s total size [%s]", dsUri.Data(),
      fc->GetNFiles(), fmtSize, um.Data(), opStatus.Data());

    delete fc;

  }

  delete runs;

  if (mgr) {
    delete mgr;
  }
}

/** Removes a dataset from the disk. Files associated to the dataset are not
 *  removed.
 */
void afRemoveDs(TString dsUri) {
  if (_afProofMode()) {
    gProof->RemoveDataSet(dsUri.Data());
  }
  else {
    TString path = Form("%s/%s.root", gEnv->GetValue("af.dspath", "/tmp"),
      dsUri.Data());
    if (gSystem->Unlink(path.Data()) < 0) {
      Printf("Can't remove %s from disk", path.Data());
    }
    else {
      Printf("Dataset removed: %s", dsUri.Data());
    }
  }
}

/** Returns a shorter version of the supplied string
 */
TString _afShortStr(const char *str, UInt_t maxlen) {
  TString s = str;
  TString o;
  Int_t ns = 10;          // n. chars from the start
  Int_t ne = maxlen-3-3;  // n. chars from the end
  if ((ne >= 3) && (s.Length() > maxlen)) {
    o = s(0, ns);
    o += "...";
    o += s(s.Length()-ne, ne);
  }
  else {
    o = str;
  }
  return o;
}

/** Fills metadata information inside the TFileInfo for the given dataset or
 *  list of datasets.
 *
 *  Beware that scanning implies a TFile::Open(). AliEn connection is opened
 *  automatically if needed.
 *
 *  By default only files with no metadata are rescanned; if rescanAll, every
 *  file is rescanned.
 *
 *  If corruptIfFail, files whose metadata can't be obtained are marked as
 *  corrupted.
 */
void afFillMetaData(TString dsMask, Bool_t rescanAll = kFALSE,
  Bool_t corruptIfFail = kFALSE) {

  TList *dsList = _afGetListOfDs(dsMask);

  TObjString *dsUriObj;
  TIter i(dsList);
  Int_t count = 0;

  while ( (dsUriObj = dynamic_cast<TObjString *>(i.Next())) ) {

    const char *dsUri = dsUriObj->String().Data();
    TDataSetManagerFile *mgr = NULL;
    TFileCollection *fc;

    // Gets the current dataset, both in PROOF and "offline" mode
    if (_afProofMode()) {
      fc = gProof->GetDataSet(dsUri);
    }
    else {
      mgr = _afCreateDsMgr();
      fc = mgr->GetDataSet(dsUri);
    }

    // Dataset may not exist anymore!
    if (!fc) {
      Printf("*** Error opening dataset URI %s, skipping ***", dsUri);
      continue;
    }

    Printf("*** Processing dataset %s ***", dsUri);

    // Loop over all files in dataset
    Int_t nChanged = 0;
    TIter j(fc->GetList());
    TFileInfo *fi;
    while (( fi = dynamic_cast<TFileInfo *>(j.Next()) )) {

      TString status;

      // Metadata already present and told not to rescan all?
      if ((fi->GetMetaData()) && (!rescanAll)) {
        status = "SKIP";
      }
      else {

        if (!_afFillMetaDataFile( fi, kTRUE )) {
          if (corruptIfFail) {
            fi->SetBit(TFileInfo::kCorrupted);
            status = "CORR";
            nChanged++;
          }
          else {
            status = "FAIL";
          }
        }
        else {
          status = " OK ";
          nChanged++;
        }      
      }

      // Prints out the status (skipped, marked as corrupted, failed, ok)
      Printf(">> [%s] %s", status.Data(), fi->GetCurrentUrl()->GetUrl());

    }

    // Save, if necessary
    if (nChanged > 0) {
      fc->Update();
      _afSaveDs(dsUri, fc, kTRUE);
    }

    delete fc;
  }

  delete dsList;

  // Deletes the manager, only if created
  if (mgr) { delete mgr; }

}

/* ========================================================================== *
 *                                ENTRY POINT                                 *
 * ========================================================================== */

/** Init function, it only prints a welcome message with the default parameters.
 */
void afdsutil() {
  Printf("");
  Printf("Dataset management utilities loaded.");
  Printf("Available functions start with \"af\", use autocompletion to list.");
  Printf("");
  afPrintSettings();
  Printf("");
}
