/**
 * readds.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * Prints out the list of datasets.
 *
 */
#include <stdio.h>

#include <TDataSetManagerFile.h>
#include <TMap.h>
#include <TObjString.h>

#include "afDataSetList.h"

/** Entry point.
 */
int main(int argc, char *argv[]) {

  TDataSetManagerFile *dsm = new TDataSetManagerFile(NULL, NULL,
    "dir:/home/volpe/storage/datasets");

  af::dataSetList dsl(dsm);

  const char *buf;

  try { dsl.init(); }
  catch (std::exception &e) {
    printf("oww! %s\n", e.what());
    delete dsm;
    return 1;
  }

  while (buf = dsl.next()) printf("++ %s ++\n", buf);
  dsl.free();

  delete dsm;

  return 0;
}











  //TMap *map_g = dsm->GetDataSets(NULL, NULL, NULL); //<-- mappe di mappe...
  // TMap *map_g = dsm->GetDataSets("/*/*");  <-- lista piatta
  /*map_g->SetOwnerKeyValue();

  if (map_g) {

    TIter iter_g(map_g);
    TObjString *key_g;

    char buf[150];

    while (( key_g = dynamic_cast<TObjString *>(iter_g.Next()) )) {

      TMap *map_u = dynamic_cast<TMap *>(map_g->GetValue(key_g));
      map_u->SetOwnerKeyValue();

      TIter iter_u(map_u);
      TObjString *key_u;

      while (( key_u = dynamic_cast<TObjString *>(iter_u.Next()) )) {

        TMap *map_n = dynamic_cast<TMap *>(map_u->GetValue(key_u));
        map_n->SetOwnerKeyValue();

        TIter iter_n(map_n);
        TObjString *key_n;

        while (( key_n = dynamic_cast<TObjString *>(iter_n.Next()) )) {

          snprintf(buf, 150, "/%s/%s/%s",
            key_g->String().Data(),
            key_u->String().Data(),
            key_n->String().Data()
          );

          printf("*** %s ***\n", buf);
        } // n

      } // u

    } // g

    delete map_g;

  }
  else {
    printf("no datasets found\n");
  }*/
