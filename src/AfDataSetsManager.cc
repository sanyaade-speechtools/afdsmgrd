#include "AfDataSetsManager.h"

AfDataSetsManager::AfDataSetsManager() {
  fSrcList = new TList();
  fSrcList->SetOwner();
  fSuid = false;
  fReset = false;
  fLoopSleep_s = kDefaultLoopSleep_s;
  fStageQueue = new THashList();
  fStageQueue->SetOwner();
  fStageCmds = new TList();  // not owner, threads must be cancelled manually

  AfLogOk("#### afdsmgrd rev. %d started its operations ####", SVNREV);
}

AfDataSetsManager::~AfDataSetsManager() {
  delete fSrcList;
  delete fStageQueue;
}

Bool_t AfDataSetsManager::ReadConf(const char *cf) {

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
      kDefaultLoopSleep_s);
    fLoopSleep_s = kDefaultLoopSleep_s;
  }
  else if ( (fLoopSleep_s = loopSleep_s->Atoi()) <= 0 )  {
    AfLogWarning("Invalid value for dsmgrd.sleepsecs (%s), using default (%d)",
      loopSleep_s, kDefaultLoopSleep_s);
    fLoopSleep_s = kDefaultLoopSleep_s;
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
        opts.Data(), fSuid, this);
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

void AfDataSetsManager::Loop(Bool_t runOnce) {

  TIter i(fSrcList);
  AfDataSetSrc *dsSrc;

  while (kTRUE) {

    AfLogInfo("++++ Started loop over dataset sources ++++");
    while ( dsSrc = dynamic_cast<AfDataSetSrc *>(i.Next()) ) {
      dsSrc->Process(fReset);
    }
    AfLogInfo("++++ Loop over dataset sources completed ++++");

    if (runOnce) {
      break;
    }

    // XXX Staging queue is not processed if running once

    AfLogInfo("++++ Started loop over transfer queue ++++");
    /*while ( dsSrc = dynamic_cast<AfDataSetSrc *>(i.Next()) ) {
      dsSrc->Process(fReset);
    }*/
    PrintStageList();
    ProcessTransferQueue();
    PrintStageList();
    AfLogInfo("++++ Loop over transfer queue completed ++++");

    i.Reset();

    AfLogInfo("Sleeping %d seconds before new loop", fLoopSleep_s),
    gSystem->Sleep(fLoopSleep_s * 1000);
  }
}

StgStatus_t AfDataSetsManager::GetStageStatus(const char *url) {

  AfStageUrl search(url);
  AfStageUrl *found =
    dynamic_cast<AfStageUrl *>( fStageQueue->FindObject(&search) );

  if (found == NULL) {
    return kStgAbsent;
  }

  return found->GetStageStatus();
}

Bool_t AfDataSetsManager::EnqueueUrl(const char *url) {

  AfStageUrl search(url);

  // Only adds elements not already there
  if ( fStageQueue->FindObject( &search ) == NULL ) {
    fStageQueue->AddLast( new AfStageUrl(url) );
    return kTRUE;
  }

  return kFALSE;
}

Bool_t AfDataSetsManager::DequeueUrl(const char *url) {
  /* TODO */
}

void AfDataSetsManager::PrintStageList() {

  if (!gLog->GetDebug()) {
    return;
  }

  TIter i( fStageQueue );
  AfStageUrl *su;

  AfLogDebug("Staging queue has %d element(s) (D=done, Q=queued, S=staging, "
    "F=fail):",
    fStageQueue->GetSize());
  while ( su = dynamic_cast<AfStageUrl *>(i.Next()) ) {
    AfLogDebug(">> %c | %s", su->GetStageStatus(), su->GetUrl().Data());
  }

}

void AfDataSetsManager::ProcessTransferQueue() {

  AfLogInfo("Processing transfer queue");

  // Loop over all elements:
  //
  // If an element is Q:
  //  - start a thread if enough threads are free
  //  - change its status into S
  //
  // If an element is S:
  //  - check if corresponding thread has finished
  //    - purge thread (join + delete)
  //    - change status to DONE or FAILED (it depends)
  //    - delete thread from the list
  //
  // In any other case (D, F) ignore it: the list must be cleaned up by the
  // single dataset manager accordingly.

  TIter i( fStageQueue );
  AfStageUrl *su;

  // First loop: only look for elements "staging" and check if their
  // corresponding thread has finished
  while ( su = dynamic_cast<AfStageUrl *>(i.Next()) ) {

    TString url = su->GetUrl();

    if (su->GetStageStatus() == kStgStaging) {
      AfLogDebug(">> %s is currently in threads list, is it true?",
        url.Data());
      TThread *t = dynamic_cast<TThread *>(fStageCmds->FindObject(url.Data()));
      if (t) {

        if (t->GetState() == TThread::kRunningState) {
          AfLogDebug(">>>> YES and it is RUNNING");
        }
        else if (t->GetState() == TThread::kCanceledState) {
          Bool_t *retVal = NULL;
          Long_t l = t->Join((void **)&retVal);

          AfLogDebug(">>>> YES but it has finished %s %x %d",
            (*retVal ? "GOOD" : "BAD"), retVal, *retVal);

          if (*retVal) {
            su->SetStageStatus(kStgDone);
          }
          else {
            su->SetStageStatus(kStgFail);
          }

          delete retVal;
          if (!fStageCmds->Remove(t)) {
            AfLogDebug(">>>> FAIL removing from list");
          }
          else {
            AfLogDebug(">>>> SUCCESSFULLY removed from list");
          }

          t->Delete();
        }

      }
      else {
        AfLogError("Can't find thread associated to transfer %s - this should "
          "not happen!", url.Data());
      }
    }

  }

  i.Reset();

  // Second loop: only look for elements "staging" and check if their
  // corresponding thread has finished
  while ( su = dynamic_cast<AfStageUrl *>(i.Next()) ) {

    Int_t nXfr = fStageCmds->GetSize();

    if ((su->GetStageStatus() == kStgQueue) && (nXfr < kParallelXfr)) {
      TString *url = new TString(su->GetUrl());
      TThread *t = new TThread(url->Data(),
        (TThread::VoidRtnFunc_t)&Stage, url);
      fStageCmds->AddLast(t);
      t->Run();
      su->SetStageStatus(kStgStaging);
    }

  }

}

void *AfDataSetsManager::Stage(void *args) {
  TString *url = (TString *)args;
  TString cmd = Form("xrdstagetool -d 0 %s 2> /dev/null", url->Data());
  delete url;
  TString out = gSystem->GetFromPipe(cmd.Data());
  Bool_t *retVal = new Bool_t;
  if (out.BeginsWith("OK ")) {
    AfLogOk("OKOKOKOKOKOKOKOKOKOKOKOKOKOKOKOKOKOKOKOKOKOKOK");
    *retVal = kTRUE;
  }
  else {
    AfLogError("ERRERRERRERRERRERRERRERRERRERRERRERRERRERRERR");
    *retVal = kFALSE;
  }
  return retVal;
}
