/**
 * StageXrd.C -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * ROOT macro to perform both staging via xrootd and verification via ROOT.
 * Output is on stdout and mimics the output of xrdstagetool.
 *
 * Arguments are:
 *
 *  - url      : the root:// URL of the file to be staged
 *  - def_tree : the tree name to search for (either with or without leading /)
 *
 * If the default tree is not given, the first valid tree found in the file is
 * read.
 *
 * This macro works also as a non-compiled macro: just call it via:
 *
 *   root.exe -b -q StageXrd.C'("root://server:port//dir/file.zip#esd.root", \
 *     "/tree")'
 *
 * Remember to call root.exe and not just root to have afdsmgrd handling process
 * control correctly.
 *
 * In case of success, on stdout it returns a format like this:
 *
 * OK <orig_url_no_anchor> Size: <size_bytes> \
 *   EndpointUrl: <endpoint_url_w_anchor> Tree: <tree_name> Events: <n_events>
 *
 * Please note that "Tree:" reports the tree name with a leading /.
 *
 * If the file has been downloaded but no tree is present, the output is like:
 *
 * OK <orig_url_no_anchor> Size: <size_bytes> \
 *   EndpointUrl: <endpoint_url_w_anchor>
 *
 * In case of failure, on stdout it prints a format like this:
 *
 * FAIL <orig_url_no_anchor> <other_stuff...>
 *
 * Different reasons may lead to staging failures. If no reason is given, the
 * staging itself failed. Elsewhere, in <other_stuff...> there is a "Reason:"
 * string that may assume the following values:
 *
 *  - cant_open        : file can not be accessed
 *  - tree_disappeared : the tree exists in keys list but can't be read
 *  - no_such_tree     : the specified tree does not exist
 *
 * So, if the status is FAIL but we have a "Reason:", we conclude that the file
 * is staged but corrupted.
 *
 * In case of failure, if "Staged: 1" is reported, it means that the file has
 * been successfully staged, but it is corrupted. If no Staged: field is
 * reported, in case of failure the file is both unstaged and corrupted.
 */

/** Auxiliary function that finds a tree name given the specified TFile. If no
 *  default tree is found, an empty string is saved in def_tree.
 */
void DefaultTree(TFile *file, TString &def_tree) {

  TIter it(file->GetListOfKeys());
  TKey *key;
  Bool_t found = kFALSE;

  while (( key = dynamic_cast<TKey *>(it.Next()) )) {
    if (TClass::GetClass(key->GetClassName())->InheritsFrom("TTree")) {
      def_tree = key->GetName();  // without initial slash
      found = kTRUE;
      break;
    }
  }

  if (!found) def_tree = "";

}

/** The main function of this ROOT macro.
 */
void StageXrd(const char *url, TString def_tree = "") {

  TUrl turl(url);
  TString anchor = turl.GetAnchor();

  turl.SetAnchor("");

  //////////////////////////////////////////////////////////////////////////////
  // Stage
  //////////////////////////////////////////////////////////////////////////////

  FILE *pipe = gSystem->OpenPipe(
    Form("xrdstagetool -d 4 %s 2>&1", turl.GetUrl()), "r");
  char buf[2000];

  Bool_t stageFail = kFALSE;
  Bool_t verifyFail = kFALSE;

  ULong_t size = 0;

  while (fgets(buf, 2000, pipe)) {
    TString line = buf;
    if (line.BeginsWith("FAIL ")) {
      stageFail = kTRUE;
      break;
    }
    else if (line.BeginsWith("OK ")) {
      TString sz_str;
      Long_t pos = line.Index("Size: ");
      if (pos >= 0) {
        pos += 6;
        sz_str = line(pos, 1e9);
        size = (ULong_t)sz_str.Atoll();
      }
      break;
    }
  }

  gSystem->ClosePipe(pipe);

  if (stageFail) {
    // File was not staged correctly: mimic output of xrdstagetool
    Printf("FAIL %s", turl.GetUrl());
    return;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Verify
  //////////////////////////////////////////////////////////////////////////////

  TFile *file = TFile::Open(url);  // open full file with anchor

  if (!file) {
    Printf("FAIL %s Staged: 1 Reason: cant_open", url);
    return;
  }

  // Get endpoint URL -- defaults to redirector URL
  const char *endp_url = url;
  const TUrl *endp_url_obj = file->GetEndpointUrl();
  if (endp_url_obj) {
    endp_url_obj->SetAnchor(anchor);
    endp_url = endp_url_obj->GetUrl();
  }

  // Remove initial slash
  if (def_tree.BeginsWith("/")) def_tree = def_tree(1,1e9);

  // No default tree specified? Search for one
  if (def_tree == "") DefaultTree(file, def_tree);

  // Search for the specified default tree
  if (def_tree != "") {

    TKey *key = file->FindKey(def_tree.Data());
    if (key && TClass::GetClass(key->GetClassName())->InheritsFrom("TTree")) {

      TTree *tree = dynamic_cast<TTree *>(key->ReadObj());
      if (tree) {

        // All OK
        Printf("OK %s Size: %lu EndpointUrl: %s Tree: /%s Events: %lld",
          turl.GetUrl(),      // without anchor (mimic xrdstagetool)
          size,               // in bytes
          endp_url,           // with anchor
          def_tree.Data(),    // tree name (initial slash is added in fmt str)
          tree->GetEntries()  // events in tree
        );

      }
      else {

        // FAIL because the specified tree disappeared from key: do not report
        // tree name and events. This should not happen and it is a strong
        // indicator of file corruption. Since file has been staged, Staged=1
        Printf("FAIL %s Size: %lu EndpointUrl: %s Staged: 1 "
          "Reason: tree_disappeared", turl.GetUrl(), size, endp_url);

      }

    }
    else {

      // FAIL because the specified tree does not exist: this is a weak
      // indicator of file corruption. Since file has been staged, Staged=1
      Printf("FAIL %s Size: %lu EndpointUrl: %s Staged: 1 Reason: no_such_tree",
        turl.GetUrl(), size, endp_url);

    }

  }
  else {

    // OK but do not report number of events or tree name, because no tree has
    // been found
    Printf("OK %s Size: %lu EndpointUrl: %s", turl.GetUrl(), size, endp_url);

  }

  file->Close();
  delete file;

}
