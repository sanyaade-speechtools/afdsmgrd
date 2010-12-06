/**
 * readds.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * Prints out the list of datasets.
 *
 */
#include <stdio.h>
#include <iostream>

#include <list>

#include <TDataSetManagerFile.h>
#include <TMap.h>
#include <TObjString.h>

#include <TFileCollection.h>
#include <THashList.h>
#include <TFileInfo.h>

#include <TMap.h>

#include "afDataSetList.h"
#include "afOpQueue.h"

//typedef std::list<af::queueEntry *> hash_tbl;
//typedef hash_tbl::iterator hash_tbl_iter;

/** See the content of a given dataset.
 */
void print_dataset(TDataSetManager &dsm, const char *ds_name) {

  // Codice del disagio: un cavolo di malloc() per ogni chiamata, devo trovare
  // il sistema di allocare fc in pool perché i dataset sono tantissimi!
  TFileCollection *fc = dsm.GetDataSet(ds_name);

  if (!fc) {
    printf(">> Can't open dataset %s\n", ds_name);
    return;
  }

  TIter i(fc->GetList());
  TFileInfo *fi;
  while (( fi = dynamic_cast<TFileInfo *>(i.Next()) )) {
    TUrl *url = fi->GetFirstUrl();
    printf(">> %s\n", url->GetUrl());
    // TUrls are owned by TFileInfo
    // TFileInfos are owned by TFileCollection
  }

  delete fc;

}

/** Put found URLs in a list.
 */
void dataset_to_opqueue(TDataSetManager &dsm, const char *ds_name,
  af::opQueue &opq) {

  TFileCollection *fc = dsm.GetDataSet(ds_name);

  if (!fc) {
    printf(">> Can't open dataset %s\n", ds_name);
    return;
  }

  TIter i(fc->GetList());
  TFileInfo *fi;
  while (( fi = dynamic_cast<TFileInfo *>(i.Next()) )) {
    TUrl *url = fi->GetFirstUrl();
    opq.insert(url->GetUrl());
  }

  delete fc;
}

/** Waits for user input (debug)
 */
void wait_user() {
#ifdef WAIT_USER
  printf("*** waiting for user input ***");
  string fuffa;
  std::cin >> fuffa;
#endif
}

/** Entry point.
 */
int main(int argc, char *argv[]) {

  unsigned int n_entries = 20;

  af::opQueue opq;

  printf("\n=== INSERT ===\n");

  char urlbuf[300];
  for (unsigned int i=1; i<=n_entries; i++) {
    snprintf(urlbuf, 300, "http://www.google.it/url/estremamente/lungo/solo/per/provare/quel/che/%09d/succede/con/il/database/root_archive.zip#AliESDs.root", i);
    opq.insert(urlbuf);
  }

  opq.dump();

  // Update
  int n = opq.update("http://www.google.it/url/estremamente/lungo/solo/per/provare/quel/che/000000016/succede/con/il/database/root_archive.zip#AliESDs.root", af::qstat_failed);
  n += opq.update("http://www.google.it/url/estremamente/lungo/solo/per/provare/quel/che/000000009/succede/con/il/database/root_archive.zip#AliESDs.root", af::qstat_failed);
  printf("\n=== UPDATE (%d) ===\n", n);
  opq.dump();

  // Delete finished/failed
  n = opq.flush();
  printf("\n=== FLUSH (%d) ===\n", n);
  opq.dump();

  return 0;
}
