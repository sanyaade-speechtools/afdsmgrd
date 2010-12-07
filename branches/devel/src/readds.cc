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
#include <stdexcept>

#include <TDataSetManagerFile.h>

#include "afDataSetList.h"
#include "afOpQueue.h"

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

  af::opQueue opq(2);

  printf("\n=== INSERT ===\n");

  char urlbuf[300];
  for (unsigned int i=1; i<=n_entries; i++) {
    snprintf(urlbuf, 300, "root://www.google.it/num%09d/root_archive.zip#AliESDs.root", i);
    opq.insert(urlbuf);
  }

  opq.dump();

  // Update
  for (unsigned int i=1; i<=3; i++) {
    opq.failed("root://www.google.it/num000000007/root_archive.zip#AliESDs.root");
    opq.failed("root://www.google.it/num000000011/root_archive.zip#AliESDs.root");
    printf("\n=== FAILED_%u ===\n", i);
    opq.flush();
    opq.dump();
  }

  // Delete finished/failed
  /*n = opq.flush();
  printf("\n=== FLUSH (%d) ===\n", n);
  opq.dump();*/

  // Query a caso
  /*try {
    printf("\n=== ARBITRARY QUERY ===\n");
    opq.arbitrary_query(
      "UPDATE queue SET status = CASE"
      "  WHEN (n_failures < 1) THEN 'S'"
      "  ELSE 'F'"
      "END");
    opq.dump();
  }
  catch (std::exception &e) { puts(e.what()); }*/

  return 0;
}
