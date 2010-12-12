/**
 * readds.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * Prints out the list of datasets.
 *
 */
#include <stdio.h>
#include <libgen.h>

#include <iostream>
#include <stdexcept>

#include <TDataSetManagerFile.h>

#include "afDataSetList.h"
#include "afOpQueue.h"
#include "afExtCmd.h"

/** Waits for user input (debug)
 */
//#define WAIT_USER
void wait_user() {
#ifdef WAIT_USER
  printf("*** waiting for user input ***");
  std::string fuffa;
  std::cin >> fuffa;
#endif
}

/** Returns a string containing the base path (without trailing slashes) of the
 *  command executed as "cmd" from current working directory. The returned
 *  pointer is dynamically allocated and must be freed by the caller. If some
 *  memory allocation fails, it returns a NULL pointer.
 */
char *exec_path(const char *cmd) {

  char *base = dirname((char *)cmd);  // dirname() returns a ptr to static buf
  char *path;
  size_t maxlen;

  if (base[0] == '/') {
    path = strdup(base);
    if (!path) return NULL;
  }
  else {
    path = getcwd(NULL, 0);  // getcwd() uses malloc()
    if (!path) return NULL;
    if (strcmp(base, ".") != 0) {
      maxlen = strlen(path) + strlen(base) + 2;  // one slash plus one '\0'
      path = (char *)realloc(path, maxlen);
      if (!path) {
        free(path);  // realloc() leaves prev memory unchanged upon failure
        return NULL;
      }
      strncat(path, "/", maxlen);
      strncat(path, base, maxlen);
    }
  }

  return path;
}

/** Entry point.
 */
int main(int argc, char *argv[]) {

  /*unsigned int n_entries = 20;

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
  }*/

  // Delete finished/failed
  /*n = opq.flush();
  printf("\n=== FLUSH (%d) ===\n", n);
  opq.dump();*/

  // Query a caso
  /*try {
    printf("\n=== ARBITRARY QUERY ===\n");
    opq.arbitrary_query("SELECT COUNT(*) FROM queue WHERE main_url = 'prova'");
    opq.arbitrary_query(
      "UPDATE queue SET status = CASE"
      "  WHEN (n_failures < 1) THEN 'S'"
      "  ELSE 'F'"
      "END");
    opq.dump();
  }
  catch (std::exception &e) { puts(e.what()); }*/
  
  /*const af::queueEntry *qe =
    opq.get_entry("root://www.google.it/num000000001/root_archive.zip#AliESDs.root");

  if (qe) qe->print();
  else printf("entry not found\n");*/

  //af::extCmd::helper_path(argv[0]);

  // Find path of current executable (where to find helper executables)
  {
    char *path = exec_path(argv[0]);
    const char *helper_exec = "/afdsmgrd-exec-wrapper";
    path = (char *)realloc(path, strlen(path)+strlen(helper_exec)+1);
    strcat(path, helper_exec);
    af::extCmd::set_helper_path(path);
    af::extCmd::set_temp_path("/tmp/afdsmgrd");
    free(path);
  }

  // Spawns an external test program
  af::extCmd prog("/Users/volpe/Fisica/ALICE/alz118wx_backup/afdsmgrd/"
    "devel/src/donothing.sh");
  prog.run();

  unsigned long iter = 0;
  while (true) {
    printf("main loop: iteration #%lu\n", ++iter);
    if (prog.is_running()) {
      printf("+++ program is running with pid %d\n", prog.get_pid());
    }
    else {
      printf("+++ program terminated, output follows\n");
      prog.get_output();
      prog.print_fields();
      break;
    }
    sleep(1);
  }

  return 0;
}
