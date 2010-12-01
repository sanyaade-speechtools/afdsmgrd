/**
 * VerifyRootFile.C -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * ROOT macro to verify a ROOT file given as argument. Verify means that the
 * file is opened, the default tree (second argument, optional) is read and the
 * number of events is counted.
 *
 * If the default tree is not given, the first valid tree found in the file is
 * read.
 *
 * In case of success, on stdout it returns:
 *
 * OK <endpoint_url> <tree_name> <num_events>
 *
 * In case of any failure, on stdout it returns:
 *
 * FAIL <error_message>
 */
#include <stdio.h>
#include <TFile.h>
#include <TTree.h>
#include <TKey.h>
#include <TGrid.h>
#include <TUrl.h>

/** Entry point
 */
void VerifyRootFile(const char *url, const char *default_tree_name = NULL) {

  if (strncmp(url, "alien://", 8) == 0) {
    if (!TGrid::Connect("alien:")) {
      printf("FAIL Can't connect to AliEn!\n");
    }
  }

  TFile *f = TFile::Open(url);

  if (!f) {
    printf("FAIL Can't open file %s!\n", url);
    return;
  }

  const char *endpoint_url = url;

  if (strncmp(url, "root://", 7) == 0) {
    const TUrl *endpoint_url_obj = f->GetEndpointUrl();
    if (endpoint_url_obj) {
      endpoint_url = endpoint_url_obj->GetUrl();
    }
  }

  // Search for the specified default tree
  if (default_tree_name != NULL) {
    TKey *key = f->FindKey(default_tree_name);
    if (key && TClass::GetClass(key->GetClassName())->InheritsFrom("TTree")) {

      TTree *tree = dynamic_cast<TTree *>(key->ReadObj());
      if (tree) {
        printf("OK %s %s %lld\n", endpoint_url, default_tree_name,
          tree->GetEntries());
      }

      f->Close();
      delete f;
      return;
    }
  }

  // Search for all the keys and stop on first valid key
  TIter it(f->GetListOfKeys());
  TKey *key;
  bool found = false;

  while (( key = dynamic_cast<TKey *>(it.Next()) )) {

    if (TClass::GetClass(key->GetClassName())->InheritsFrom("TTree")) {

      TTree *tree = dynamic_cast<TTree *>(key->ReadObj());

      if (tree) {
        printf("OK %s %s %lld\n", endpoint_url, key->GetName(),
          tree->GetEntries());
        found = true;
        break;
      }

    }

  }

  if (!found) {
    printf("FAIL Can't find a valid tree in %s!\n", url);
  }

  f->Close();
  delete f;

}
