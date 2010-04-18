#include "AfDataSetManager.h"

AfDataSetManager::AfDataSetManager() {
  fSrcList = new TList();
  fSrcList->SetOwner();
}

AfDataSetManager::~AfDataSetManager() {
  delete fSrcList;
}

Bool_t AfDataSetManager::ReadConf(const char *cf) {

  AfConfReader *cfr = AfConfReader::Open(cf);

  //cfr->PrintVarsAndDirs();

  if (!cfr) {
    AfLogFatal("Cannot read configuration file: %s", cf);
    return kFALSE;
  }

  // List is emptied before refilling it
  fSrcList->Clear();

  // Loop pause
  TString *loopSleep_s = cfr->GetDir("dsmgrd.sleepsecs");
  if (!loopSleep_s) {
    AfLogWarning("Variable dsmgrd.sleepsecs not set, using default (%d)",
      fDefaultLoopSleep_s);
    fLoopSleep_s = fDefaultLoopSleep_s;
  }
  else if ( (fLoopSleep_s = loopSleep_s->Atoi()) <= 0 )  {
    AfLogWarning("Invalid value for dsmgrd.sleepsecs (%s), using default (%d)",
      loopSleep_s, fDefaultLoopSleep_s);
    fLoopSleep_s = fDefaultLoopSleep_s;
    delete loopSleep_s;
  }
  else {
    AfLogInfo("Sleep between scans set to %d seconds", fLoopSleep_s);
    delete loopSleep_s;
  }

  // Which datasets to process? The format is the same of TDataSetManagerFile
  TList *procDs = cfr->GetDirs("dsmgrd.processds");
  TIter j( procDs );
  TObjString *o;

  while ( o = dynamic_cast<TObjString *>(j.Next()) ) {
    AfLogInfo("Dataset mask to process: %s", o->GetString().Data());
  }

  // Parse dataset sources

  TPMERegexp reMss("([ \t]|^)mss:([^ \t]*)");  // regex that matches mss:
  TPMERegexp reUrl("([ \t]|^)url:([^ \t]*)");  // regex that matches url:
  TPMERegexp reOpt("([ \t]|^)opt:([^ \t]*)");  // regex that matches opt:
  TPMERegexp reDsp("([ \t]|^)destpath:([^ \t]*)");
  TPMERegexp reRw("([ \t]|^)rw=1([^ \t]*)");

  // Watch out: getDirs returns a poiter to a TList that must be deleted, and it
  // is owner of its content!
  TList *dsSrcList = cfr->GetDirs("xpd.datasetsrc");
  TIter i( dsSrcList );

  while ( o = dynamic_cast<TObjString *>(i.Next()) ) {

    TString dir = o->GetString();

    AfLogInfo("Found dataset configuration: %s", dir.Data());

    Bool_t dsValid = kTRUE;
    TUrl *redirUrl;
    TString destPath;
    TString dsUrl;
    TString opts;
    Bool_t rw = kFALSE;

    if (reMss.Match(dir) == 3) {

      redirUrl = new TUrl(reMss[2]);

      if ((!redirUrl->IsValid()) ||
        (strcmp(redirUrl->GetProtocol(), "root") != 0)) {
        AfLogError(">> Invalid MSS URL: only URLs in the form " \
          "root://host[:port] are supported");
        delete redirUrl;
        dsValid = kFALSE;
      }
      else {
        // URL is "flattened": only proto, host and port are retained
        redirUrl->SetUrl( Form("%s://%s:%d", redirUrl->GetProtocol(),
          redirUrl->GetHost(), redirUrl->GetPort()) );
        AfLogInfo(">> MSS: %s", redirUrl->GetUrl());
      }
    }
    else {
      AfLogError(">> MSS URL not set");
      dsValid = kFALSE;
    }

    if (reDsp.Match(dir) == 3) {
      destPath = reDsp[2];
      AfLogInfo(">> Destination path: %s", destPath.Data());
    }
    else {
      AfLogWarning(">> Destination path on pool (destpath) not set, using " \
        "default (/alien)");
      destPath = "/alien";
    }

    if (reUrl.Match(dir) == 3) {
      dsUrl = reUrl[2];
      AfLogInfo(">> URL: %s", dsUrl.Data());
    }
    else {
      AfLogError(">> Dataset local path not set");
      dsValid = kFALSE;
    }

    if (reRw.Match(dir) == 3) {
      AfLogInfo(">> R/W: true");
      rw = kTRUE;
    }

    if (reOpt.Match(dir) == 3) {
      opts = reOpt[2];
      AfLogInfo(">> Opt: %s", opts.Data());
    }
    else {
      // Default options: do not allow register and verify if readonly
      opts = rw ? "Ar:Av:" : "-Ar:-Av:";
    }

    if (dsValid) {
      redirUrl->SetFile( destPath.Data() );
      AfDataSetSrc *dsSrc = new AfDataSetSrc(dsUrl.Data(), redirUrl,
        opts.Data(), fSuid);
      if (procDs->GetSize() > 0) {
        dsSrc->SetDsProcessList(procDs);  // Set... makes a copy of the list
      }
      fSrcList->Add(dsSrc);
    }
    else {
      AfLogError(">> Invalid dataset configuration, ignoring", *i);
    }

  } // for over xpd.datasetsrc

  delete dsSrcList;
  delete cfr;
  delete procDs;

  if (fSrcList->GetSize() == 0) {
    AfLogFatal("No valid dataset source found!");
    return kFALSE;
  }

  return kTRUE;
}

void AfDataSetManager::Loop(UInt_t nLoops) {

  TIter i(fSrcList);
  UInt_t countLoops = 0;
  AfDataSetSrc *dsSrc;

  while (kTRUE) {
    AfLogInfo("++++ Started processing loop over all dataset sources ++++");

    while ( dsSrc = dynamic_cast<AfDataSetSrc *>(i.Next()) ) {
      dsSrc->Process(fReset);
    }

    AfLogInfo("++++ Loop %d completed ++++", countLoops++);

    if ((nLoops > 0) && (countLoops == nLoops)) {
      break;
    }

    AfLogInfo("Sleeping %d seconds before new loop", fLoopSleep_s),

    sleep(fLoopSleep_s);
  }
}
