#include "AfDataSetSrc.h"
#include "AfLog.h"

#include <stdlib.h>
#include <sys/stat.h>

#include <TDataSetManagerFile.h>
#include <TFileInfo.h>
#include <TFileCollection.h>
#include <THashList.h>
#include <TObjString.h>
#include <TFileStager.h>

AfDataSetSrc::AfDataSetSrc(string &url, string &mss, string &opts, bool suid,
  string &redirHost, unsigned short redirPort) {
  fUrl  = url;
  fMss  = mss;
  fOpts = opts;
  fSuid = suid;
  fUnpUid = 0;
  fUnpGid = 0;
  fRedirHost = redirHost;
  fRedirPort = redirPort;
  //AfLogInfo("Created a new dataset source: dir:%s mss:%s opt:%s", ds.c_str(),
  //  mss.c_str(), opts.c_str());
  fManager = new TDataSetManagerFile(NULL, NULL, Form("dir:%s", fUrl.c_str()) );
}

AfDataSetSrc::~AfDataSetSrc() {
  delete fManager;
}

// Processes all the datasets in this dataset source
void AfDataSetSrc::process() {
  
  // Creates a flattened list of ds uris
  flattenDsList();

  // Clears the list of files to stage this time
  fToStage.clear();

  vector<string>::iterator i;
  for (i=fDsUris.begin(); i!=fDsUris.end(); i++) {
    processDs( i->c_str() );
  }

  // Processes the staging queue by using the appropriate method (AsyncOpen or
  // simply Stage) <-- TODO || only stage works now
  int nQueued = putIntoStageQueue();
  AfLogInfo("%d file(s) put into stage queue", nQueued);

}

int AfDataSetSrc::putIntoStageQueue() {

  TFileStager *stager = TFileStager::Open(Form("root://%s:%d",
    fRedirHost.c_str(), fRedirPort));

  if (fToStage.size() == 0) {
    AfLogInfo("No files need to be staged");
    return 0;
  }

  if (!stager) {
    AfLogError("Can't open the data stager for redirector root://%s:%d",
      fRedirHost.c_str(), fRedirPort);
    return -1;
  }
  //else {
  //  AfLogOk("Stager instantiated");
  //}

  int nQueued = 0;
  vector<TFileInfo *>::iterator i;

  for (i=fToStage.begin(); i!=fToStage.end(); i++) {
    TUrl url( *((*i)->GetCurrentUrl()) );
    url.SetAnchor("");  // we want to stage the full file
    stager->Stage( url.GetUrl() );
    AfLogInfo(">> Put into staging queue %s", url.GetUrl());
    nQueued++;
  }

  delete stager;

  return nQueued;
}

void AfDataSetSrc::processDs(const char *uri) {

  AfLogInfo("Processing dataset %s...", uri);

  TFileCollection *fc = fManager->GetDataSet(uri);

  // Preliminar verification of dataset: for marking as staged files that
  // already are
  fManager->ScanDataSet(fc, TDataSetManager::kReopen);

  // ScanDataSet actually modifies the list
  TFileInfo *fi;
  TIter i( fc->GetList() );

  int countChanged = 0;

  while (fi = dynamic_cast<TFileInfo *>(i.Next())) {
    bool isStaged = fi->TestBit(TFileInfo::kStaged);
    bool isCorrupted = fi->TestBit(TFileInfo::kCorrupted);
    AfLogInfo(">> %c%c | %s",
      (isStaged ? 'S' : 's'),
      (isCorrupted ? 'C' : 'c'),
      fi->GetCurrentUrl()->GetUrl()
    );
    if (!isStaged) {
      int ch = translateUrl( fi );  // ch is nonzero if the object contains one
                                    // or more files that can be staged, this
                                    // means root or alien urls: staging should
                                    // not be triggered on incompatible urls
      if (ch) {
        //AfLogWarning("FILE %s NEEDS TO BE STAGED",
        //  fi->GetCurrentUrl()->GetUrl());

        // File is added into staging queue
        fToStage.push_back( fi );
      }
      countChanged += ch;
    }
  }

  // Save the modified dataset
  if (countChanged) {
    TString group, user, dsName;
    fManager->ParseUri(uri, &group, &user, &dsName);
    doSuid();
    // kFileMustExist saves ds only if it already exists: it updates it
    int r = fManager->WriteDataSet(group, user, dsName, fc, \
      TDataSetManager::kFileMustExist);
    undoSuid();
    if (r == 0) {
      AfLogError("Can't save modified dataset %s, check permissions!", uri);
    }
    else {
      AfLogOk("Modified dataset %s saved", uri);
    }
  }
  else {
    AfLogInfo("Dataset %s left unchanged", uri);
  }

}

int AfDataSetSrc::translateUrl(TFileInfo *fi) {

  fi->ResetUrl();
  TUrl *url;
  int countChanged = 0;

  while (url = fi->NextUrl()) {
    if ( strcmp(url->GetProtocol(), "alien") == 0 ) {
      url->SetProtocol("root");
      url->SetFile( Form("/alien%s", url->GetFile()) );
      url->SetHost(fRedirHost.c_str());
      url->SetPort(fRedirPort);
      //AfLogInfo(">>>> Changed: %s", url->GetUrl());
      countChanged++;
    }
    else if ( strcmp(url->GetProtocol(), "root") == 0 ) {
      url->SetHost(fRedirHost.c_str());
      url->SetPort(fRedirPort);
      //AfLogInfo(">>>> Changed: %s", url->GetUrl());
      countChanged++;
    }
    //else {
    //  AfLogInfo("No need to change: %s", url->GetUrl());
    //}
  }

  fi->ResetUrl();

  return countChanged;
}

void AfDataSetSrc::flattenDsList() {

  fDsUris.clear();

  TMap *groups = fManager->GetDataSets("/*/*", TDataSetManager::kReadShort);
  TIter gi(groups);
  TObjString *gn;

  while ( gn = dynamic_cast<TObjString *>( gi.Next() ) ) {
    //AfLogInfo(">> Group: %s", gn->String().Data());

    TMap *users = dynamic_cast<TMap *>( groups->GetValue( gn->String() ) );
    TIter ui(users);
    TObjString *un;

    while ( un = dynamic_cast<TObjString *>( ui.Next() ) ) {
      //AfLogInfo(">>>> User: %s", un->GetString().Data());

      TMap *dss = dynamic_cast<TMap *>( users->GetValue( un->String() ) );
      TIter di(dss);
      TObjString *dn;

      while ( dn = dynamic_cast<TObjString *>( di.Next() ) ) {

        //AfLogInfo(">>>>>> Dataset: %s", dn->String().Data());

        // This should give an URI in the form /GROUP/USER/dataset
        TString dsUriTmp = TDataSetManager::CreateUri( gn->String(),
          un->String(), dn->String() );
        string dsUri = dsUriTmp.Data();
        fDsUris.push_back( dsUri );

        //AfLogInfo(">>>>>> Dataset URI: %s", dsUri.c_str());

      } // while over datasets
    } // while over users
  } // while over groups

}

void AfDataSetSrc::doSuid() {
  if (!fSuid) {
    return;
  }
  fUnpUid = geteuid();
  fUnpGid = getegid();
  if (!((seteuid(0) == 0) && (setegid(0) == 0))) {
    AfLogFatal("Failed obtaining privileges");
    exit(1);
  }
}

void AfDataSetSrc::undoSuid() {
  if (!fSuid) {
    return;
  }
  if (!((setegid(fUnpUid) == 0) && (seteuid(fUnpGid) == 0))) {
    AfLogFatal("Can't drop privileges!");
    exit(1);
  }
}