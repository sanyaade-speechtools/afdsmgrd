#include "AfDataSetsManager.h"

AfDataSetsManager::AfDataSetsManager() {
  fSrcList = new TList();
  fSrcList->SetOwner();
  fSuid = false;
  fLoopSleep_s = kDefaultLoopSleep_s;
  fScanDsEvery = kDefaultScanDsEvery;
  fParallelXfrs = kDefaultParallelXfrs;
  fStageQueue = new THashList();
  fStageQueue->SetOwner();
  fStageCmds = new TList();  // not owner, threads must be cancelled manually

  fLastQueue = fLastStaging = fLastFail = fLastDone = -1;

  kDefaultApMonDsPrefix = "PROOF::CAF::STORAGE_datasets";

  fBinPrefix = ".";  // set with SetBinPrefix();

  #ifdef WITH_APMON
  fApMon = NULL;
  #endif // WITH_APMON
}

AfDataSetsManager::~AfDataSetsManager() {
  delete fSrcList;
  delete fStageQueue;
  delete fStageCmds;

  #ifdef WITH_APMON
  // It causes a segfault?!?!
  /*if (fApMon) {
    delete fApMon;
  }
  fApMon = NULL;
  */
  #endif // WITH_APMON
}

void AfDataSetsManager::SetBinPrefix(const char *prefix) {
  AfLogInfo("Helper binaries will be searched under %s", prefix);
  fBinPrefix = prefix;
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
      loopSleep_s->Data(), kDefaultLoopSleep_s);
    fLoopSleep_s = kDefaultLoopSleep_s;
    delete loopSleep_s;
  }
  else {
    AfLogInfo("Sleep between scans set to %d second(s)", fLoopSleep_s);
    delete loopSleep_s;
  }

  // Scan dataset every X loops
  TString *scanDsEvery = cfr->GetDir("dsmgrd.scandseveryloops");
  if (!scanDsEvery) {
    AfLogWarning("Variable dsmgrd.scandseveryloops not set, using default (%d)",
      kDefaultScanDsEvery);
    fScanDsEvery = kDefaultScanDsEvery;
  }
  else if ( (fScanDsEvery = scanDsEvery->Atoi()) <= 0 )  {
    AfLogWarning("Invalid value for dsmgrd.scandseveryloops (%s), using "
      "default (%d)", scanDsEvery->Data(), kDefaultScanDsEvery);
    fScanDsEvery = kDefaultScanDsEvery;
    delete scanDsEvery;
  }
  else {
    AfLogInfo("Datasets are checked every %d loop(s)", fScanDsEvery);
    delete scanDsEvery;
  }

  // Number of parallel staging command on the whole facility
  TString *parallelXfrs = cfr->GetDir("dsmgrd.parallelxfrs");
  if (!parallelXfrs) {
    AfLogWarning("Variable dsmgrd.parallelxfrs not set, using default (%d)",
      kDefaultParallelXfrs);
    fParallelXfrs = kDefaultParallelXfrs;
  }
  else if ( (fParallelXfrs = parallelXfrs->Atoi()) <= 0 )  {
    AfLogWarning("Invalid value for dsmgrd.parallelxfrs (%s), using "
      "default (%d)", parallelXfrs->Data(), kDefaultParallelXfrs);
    fParallelXfrs = kDefaultParallelXfrs;
    delete parallelXfrs;
  }
  else {
    AfLogInfo("Number of parallel staging commands set to %d", fParallelXfrs);
    delete parallelXfrs;
  }

  // Maximum number of files to place in queue (0 == no limit)
  TString *maxFilesInQueue = cfr->GetDir("dsmgrd.maxfilesinqueue");
  if (!maxFilesInQueue) {
    AfLogWarning("Variable dsmgrd.maxfilesinqueue not set, queue not limited "
      "by default");
    fMaxFilesInQueue = kDefaultMaxFilesInQueue;
  }
  else if ( (fMaxFilesInQueue = maxFilesInQueue->Atoi()) <= 0 )  {
    AfLogWarning("Invalid value for dsmgrd.maxfilesinqueue (%s), using "
      "default (%d)", maxFilesInQueue->Data(), kDefaultMaxFilesInQueue);
    fMaxFilesInQueue = kDefaultMaxFilesInQueue;
    delete maxFilesInQueue;
  }
  else {
    AfLogInfo("Queue limited to a maximum of %d file(s)", fMaxFilesInQueue);
    delete maxFilesInQueue;
  }

  if (fMaxFilesInQueue != 0) {
    fStageQueue->Rehash( fMaxFilesInQueue );
  }

  // Stage command
  TString *stageCmd = cfr->GetDir("dsmgrd.stagecmd");
  if (stageCmd) {
    fStageCmd = *stageCmd;
    delete stageCmd;
    AfLogInfo("Staging command is %s", fStageCmd.Data());
  }
  else {
    AfLogFatal("No stage command specified.");
    delete stageCmd;
    return kFALSE;
  }

  // Which datasets to process? The format is the same of TDataSetManagerFile
  TList *procDs = cfr->GetDirs("dsmgrd.processds");
  TIter j( procDs );
  TObjString *o;

  while ( o = dynamic_cast<TObjString *>(j.Next()) ) {
    AfLogInfo("Dataset mask to process: %s", o->GetString().Data());
  }

  #ifdef WITH_APMON

  // MonALISA configuration: it is possible to specify either the URL where to
  // retrieve the config file or directly the server given in the custom form
  // apmon://mymonserver[:8884] 
  TString *monRawUrl = cfr->GetDir("dsmgrd.apmonurl");
  if (monRawUrl) {
    TUrl monUrl(monRawUrl->Data());
    delete monRawUrl;
    const char *prot = monUrl.GetProtocol();
    if (strcmp(prot, "http") == 0) {
      AfLogInfo("Retrieving MonALISA configuration from %s", monUrl.GetUrl());
      CreateApMon(&monUrl);
    }
    else if (strcmp(prot, "apmon") == 0) {
      if (monUrl.GetPort() == 0) {
        monUrl.SetPort(8884);  // default
      }
      AfLogInfo("Using MonALISA server %s:%d", monUrl.GetHost(),
        monUrl.GetPort());
      CreateApMon(&monUrl);
    }
    else {
      AfLogError("Unsupported protocol in dsmgrd.apmonurl, MonALISA "
        "monitoring disabled");
    }
  }
  else {
    AfLogWarning("No dsmgrd.apmonurl given, MonALISA monitoring disabled");
  }

  // MonALISA prefix (prefix of the so-called "cluster name")
  TString *clusterPrefix = cfr->GetDir("dsmgrd.apmondsprefix");
  if (clusterPrefix) {
    fApMonDsPrefix = *clusterPrefix;
    AfLogInfo("MonALISA notifications will be under cluster prefix %s",
      fApMonDsPrefix.Data());
  }
  else {
    AfLogWarning("No MonALISA cluster prefix specified, using default (%s)",
      kDefaultApMonDsPrefix.Data());
    fApMonDsPrefix = kDefaultApMonDsPrefix;
  }
  delete clusterPrefix;

  #else

  AfLogWarning("Daemon compiled without ApMon (MonALISA) support");

  #endif // WITH_APMON

  // Parse dataset sources

  TPMERegexp reMss("([ \t]|^)mss:([^ \t]*)");  // regex that matches mss:
  TPMERegexp reUrl("([ \t]|^)url:([^ \t]*)");  // regex that matches url:
  TPMERegexp reOpt("([ \t]|^)opt:([^ \t]*)");  // regex that matches opt:
  TPMERegexp reDsp("([ \t]|^)destpath:([^ \t]*)");
  TPMERegexp reRw("([ \t]|^)rw=1([^ \t]*)");

  // Watch out: getDirs returns a poiter to a TList that must be deleted, and
  // it is owner of its content!
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
      AfLogInfo(">> R/W: yes");
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

Int_t AfDataSetsManager::ProcessAllDataSetsOnce(DsAction_t action) {

  TIter i(fSrcList);
  AfDataSetSrc *dsSrc;
  Int_t nChanged = 0;

  AfLogDebug(20, "++++ Started loop over dataset sources ++++");
  while ( dsSrc = dynamic_cast<AfDataSetSrc *>(i.Next()) ) {
    nChanged = dsSrc->Process(action);
  }
  AfLogDebug(20, "++++ Loop over dataset sources completed ++++");

  return nChanged;
}

void AfDataSetsManager::Loop() {

  Int_t loops = fScanDsEvery;

  while (kTRUE) {

    AfLogDebug(20, "++++ Started loop over transfer queue ++++");
    PrintStageList("Stage queue before processing:", kTRUE);
    ProcessTransferQueue();
    PrintStageList("Stage queue after processing:", kTRUE);
    AfLogDebug(20, "++++ Loop over transfer queue completed ++++");

    if (loops == fScanDsEvery) {
      AfLogInfo("Scanning datasets");
      Int_t nChanged = ProcessAllDataSetsOnce(kDsProcess);
      if (nChanged) {
        AfLogOk("%d dataset(s) were changed", nChanged);
      }
      loops = 1;
    }
    else {
      loops++;
      Int_t scansLeft = fScanDsEvery-loops+1;
      if (scansLeft == 1) {
        AfLogInfo("Not scanning datasets now: they will be scanned in the "
          "next loop");
      }
      else {     
        AfLogInfo("Not scanning datasets now: %d sleep(s) left before a new "
          "dataset scan", scansLeft);
      }
    }

    //if (TObject::GetObjectStat()) {
    //  gObjectTable->Print();
    //}

    AfLogDebug(10, "Sleeping %d seconds before a new loop", fLoopSleep_s),
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

StgStatus_t AfDataSetsManager::EnqueueUrl(const char *url) {

  // Check if queue is full
  if ((fMaxFilesInQueue != 0) &&
    (fStageQueue->GetEntries() >= fMaxFilesInQueue)) {
    return kStgQueueFull;
  }

  AfStageUrl search(url);

  // Only adds elements not already there
  if ( fStageQueue->FindObject( &search ) == NULL ) {
    AfStageUrl *u = new AfStageUrl(url);
    fStageQueue->AddLast(u);
    return u->GetStageStatus();
  }

  return kStgAbsent;  // error indicator
}

StgStatus_t AfDataSetsManager::DequeueUrl(const char *url) {

  AfStageUrl search(url);
  AfStageUrl *removed;

  if (removed = dynamic_cast<AfStageUrl *>( fStageQueue->Remove(&search) )) {
    delete removed;
    return kStgAbsent;
  }

  return kStgQueue;  // error indicator
}

void AfDataSetsManager::PrintStageList(const char *header, Bool_t debug) {

  // Show messages only with debug level >= 30
  if ((debug) && (gLog->GetDebugLevel() < 30)) {
    return;
  }

  TIter i( fStageQueue );
  AfStageUrl *su;

  TString summary = Form("Staging queue has %d element(s) (D=done, Q=queued, "
    "S=staging, F=failed):", fStageQueue->GetSize());

  if (debug) {
    AfLogDebug(30, header);
    AfLogDebug(30, summary);
  }
  else {
    AfLogInfo(header);
    AfLogInfo(summary);
  }

  while ( su = dynamic_cast<AfStageUrl *>(i.Next()) ) {
    TString itm = Form(">> %c | %s", su->GetStageStatus(), su->GetUrl().Data());
    if (debug) {
      AfLogDebug(30, itm);
    }
    else {
      AfLogInfo(itm);
    }
  }

}

void AfDataSetsManager::ProcessTransferQueue() {

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

      TThread *t = dynamic_cast<TThread *>(fStageCmds->FindObject(url.Data()));
      if (t) {

        if (t->GetState() == TThread::kRunningState) {
          AfLogDebug(30, "Thread #%ld running for staging %s", t->GetId(),
            url.Data());
        }
        else if (t->GetState() == TThread::kCanceledState) {

          AfLogDebug(30, "Thread #%ld completed", t->GetId());

          Bool_t *retVal = NULL;
          Long_t l = t->Join((void **)&retVal);

          if (*retVal) {
            AfLogOk("Staging completed: %s", url.Data());
            su->SetStageStatus(kStgDone);
          }
          else {
            AfLogError("Staging failed: %s", url.Data());
            su->SetStageStatus(kStgFail);
          }

          delete retVal;

          if (!fStageCmds->Remove(t)) {
            AfLogError(">>>> Failed removing staging thread from list - this "
              "should not happen!");
          }

          //t->Delete(); // safe to use delete: thread has been cancelled
          delete t;
        }

      }
      else {
        AfLogError("Can't find thread associated to transfer %s - this should "
          "not happen!", url.Data());
      }
    }

  }

  i.Reset();

  Int_t nStaging;
  Int_t nQueue;
  Int_t nFail;
  Int_t nDone;

  nStaging = nQueue = nFail = nDone = 0;

  // Second loop: only look for elements "queued" and start thread accordingly
  while ( su = dynamic_cast<AfStageUrl *>(i.Next()) ) {

    Int_t nXfr = fStageCmds->GetSize();

    if ((su->GetStageStatus() == kStgQueue) && (nXfr < fParallelXfrs)) {

      TString url = su->GetUrl();

      // Arguments for the staging command: this TObjArray is owner
      TObjArray *args = new TObjArray(2);  // capacity=2
      args->SetOwner();
      args->AddAt(new TObjString(url), 0);  // the url
      args->AddAt(new TObjString(fStageCmd), 1);     // the cmd

      TThread *t = new TThread(url.Data(), (TThread::VoidRtnFunc_t)&Stage,
        args);
      fStageCmds->AddLast(t);
      t->Run();
      su->SetStageStatus(kStgStaging);
      AfLogInfo("Staging started: %s", url.Data());
      AfLogDebug(30, "Spawned thread #%ld to stage %s", t->GetId(), url.Data());

      nStaging++;
    }
    else {

      switch( su->GetStageStatus() ) {
        case kStgQueue:   nQueue++;   break;
        case kStgStaging: nStaging++; break;
        case kStgDone:    nDone++;    break;
        case kStgFail:    nFail++;    break;
      }

    }
  }

  // Eventually print statistics (only if changed since last time)
  if ((nStaging != fLastStaging) || (nQueue != fLastQueue) || 
    (nDone != fLastDone) || (nFail != fLastFail)) {

    AfLogInfo("Elements in queue: Total=%d | Queued=%d | Staging=%d | "
      "Done=%d | Failed=%d", nQueue+nStaging+nDone+nFail, nQueue, nStaging,
       nDone, nFail);

    fLastStaging = nStaging;
    fLastQueue = nQueue;
    fLastDone = nDone;
    fLastFail = nFail;
  }

}

void AfDataSetsManager::NotifyDataSetStatus(const char *dsName, Float_t pctStaged,
  Float_t pctCorrupted) {

  #ifdef WITH_APMON

  if (!fApMon) {
    return;
  }

  try {
    fApMon->sendParameter((char *)fApMonDsPrefix.Data(), (char *)dsName,
      (char *)"stagedpct", pctStaged);

    fApMon->sendParameter((char *)fApMonDsPrefix.Data(), (char *)dsName,
      (char *)"corruptedpct", pctCorrupted);
  }
  catch (runtime_error &e) {
    AfLogError("Error sending information to MonALISA");
  }

  #endif // WITH_APMON
}

void AfDataSetsManager::CreateApMon(TUrl *monUrl) {

  #ifdef WITH_APMON

  try {
    if (strcmp(monUrl->GetProtocol(), "apmon") == 0) {
      char *host = (char *)monUrl->GetHost();
      Int_t port = monUrl->GetPort();
      char *pwd = (char *)monUrl->GetPasswd();
      fApMon = new ApMon(1, &host, &port, &pwd);
    }
    else {
      fApMon = new ApMon((char *)monUrl->GetUrl());
    }
  }
  catch (runtime_error &e) {
    AfLogError("Can't create ApMon object from URL %s, error was: %s",
      monUrl->GetUrl(), e.what());
    delete fApMon;
    fApMon = NULL;
  }

  #endif // WITH_APMON
}

void *AfDataSetsManager::Stage(void *args) {

  TObjArray *o = (TObjArray *)args;
  TString url = dynamic_cast<TObjString *>( o->At(0) )->GetString();
  TString cmd = dynamic_cast<TObjString *>( o->At(1) )->GetString();
  delete o;  // owner: TObjStrings are also deleted

  // Output of stderr will be redirected to a file; if everything goes fine, the
  // file is finally deleted. If not, the contents of the file is printed on the
  // logfile (then it is deleted).
  TString tfn = "afdsmgrd-stagecmd-err-";
  FILE *errFp = gSystem->TempFileName(tfn);
  if (errFp) {
    fclose(errFp);
  }
  else {
    AfLogWarning("Can't create temporary file for saving stderr of the stage "
    "command!");
    tfn = "/dev/null";
  }

  TPMERegexp re("\\$URLTOSTAGE([ \t]|$)", "g");
  url.Append(" ");
  if (re.Substitute(cmd, url, kFALSE) == 0) {
    // If no URLTOSTAGE is found, URL is appended at the end of command by def.
    cmd.Append(" ");
    cmd.Append(url);
  }
  cmd.Append("2> ");
  cmd.Append(tfn.Data());

  AfLogDebug(30, "Thread #%ld is spawning staging command: %s",
    TThread::SelfId(), cmd.Data());

  TString out = gSystem->GetFromPipe(cmd.Data());

  Bool_t *retVal = new Bool_t;
  if (out.BeginsWith("OK ")) {
    *retVal = kTRUE;
  }
  else {
    *retVal = kFALSE;

    if ((tfn != "/dev/null") && (errFp = fopen(tfn.Data(), "r")))  {
      char buf[1000];

      TThread::Lock();

      AfLogDebug(10, "Staging of URL %s failed, stderr of stagecmd follows",
        url.Data());
      AfLogDebug(10, "[stagecmd] -8<-------------- CUT HERE -----------------");
      while ( fgets(buf, 1000, errFp) ) {
        Int_t l = strlen(buf);
        while ((--l >= 0) && ((buf[l] == '\n') || (buf[l] == '\r'))) {
          buf[l] = '\0';
        }
        AfLogDebug(10, "[stagecmd] %s", buf);
      }
      AfLogDebug(10, "[stagecmd] -8<-------------- CUT HERE -----------------");

      TThread::UnLock();

      fclose(errFp);
    }
  }

  if (tfn != "/dev/null") {
    // Can't use gSystem->Unlink(), see https://savannah.cern.ch/bugs/?68276
    unlink(tfn.Data());
  }

  return retVal;
}
