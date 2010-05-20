void afdsutil() {
  Printf("Remember to connect to PROOF!");
}

void afds_list(const char *uri) {

  TFileCollection *fc = gProof->GetDataSet(uri);

  if (!fc) {
    Printf("Couldn't open dataset URI %s", uri);
    return;
  }

  TIter i(fc->GetList());
  TFileInfo *inf;

  while ( inf = dynamic_cast<TFileInfo *>(i.Next()) ) {
    Bool_t s = inf->TestBit(TFileInfo::kStaged);
    Bool_t c = inf->TestBit(TFileInfo::kCorrupted);
    Printf("%c%c | %s", (s ? 'S' : 's'), (c ? 'C' : 'c'),
      inf->GetCurrentUrl()->GetUrl());
    TUrl *url;
    inf->NextUrl();
    while (url = inf->NextUrl()) {
      Printf("   | %s", url->GetUrl());
    }
    inf->ResetUrl();
  }

  delete fc;
}

void afds_mark(const char *fileUrl, TString bits = "") {

  // Due to PROOF limitations, only datasets from /GROUP/USER are allowed. We
  // also note that GetDataSets() returns COMMON substitutions if commonuser and
  // commongroup are set in the groupfile, but RegisterDataSet does not support
  // substitutions from COMMON back to the original user and group... bug or
  // feature?
  // A possible option to solve this kind of problems is to manually instantiate
  // a TDataSetManager, but this ROOT session should be run on the master
  // (because datasets are "local" wrt the master), and from a privileged user
  // (because we need to write back saved datasets).
  Printf("*** Searching only in datasets under /%s/%s ***", gProof->GetGroup(),
    gProof->GetUser());

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

  Int_t regErrors = 0;

  TMap *uris = gProof->GetDataSets(Form("/%s/%s", gProof->GetGroup(),
    gProof->GetUser()));
  TIter i(uris);
  TObjString *u;

  while (u = dynamic_cast<TObjString *>(i.Next())) {

    TString dsName = u->GetString();

    TFileCollection *fc = gProof->GetDataSet(dsName.Data());
    TIter j(fc->GetList());
    TFileInfo *fi;

    Int_t nChanged = 0;

    while (fi = dynamic_cast<TFileInfo *>(j.Next())) {
      if ( fi->FindByUrl(fileUrl) ) {
        Printf(">> Found on dataset %s", dsName.Data());

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
      // T = trust information (i.e. don't reset STAGED and CORRUPTED bits): the
      //     dataset manager must be configured to allow so (it is by default)
      if (!gProof->RegisterDataSet(dsName.Data(), fc, "OT")) {
        regErrors++;
      }
    }

    delete fc;
  }

  delete uris;

  if (regErrors > 0) {
    Printf("%d error(s) doing RegisterDataSet() encountered, check permissions",
      regErrors);
  }
  else {
    Printf("Success, no errors");
  }
}
