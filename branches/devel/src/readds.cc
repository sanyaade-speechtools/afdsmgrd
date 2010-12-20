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
#include "afConfig.h"
#include "afLog.h"

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

bool quit_requested = false;

//af::log stdlog

/** Manages the stop signal (kill -15)
 */
void signal_quit_callback(int signum) {
  printf("quit requested with signal %d\n", signum);
  quit_requested = true;
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

/** A callback function to test the bind_callback() feature of af::config.
 */
void directive_callback(const char *val, void *args) {
  void **array = (void **)args;

  if (val == NULL) {
    printf("\033[1;31mcallback for default value called\033[m\n");
  }
  else {
    printf("\033[1;31mdirective callback called:\033[m directive is {%s}, "
      "while the meaning of life the universe and everything remains %ld "
      "and a generic text to remember is \"%s\"\n",
      val, *(long *)array[0], (const char *)array[1]);
  }
}

/** Test log facility.
 */
void test_log(unsigned long max_iters = 10) {

  //af::log log(std::cout, af::log_level_normal);
  af::log log("logfile", af::log_level_normal);
  unsigned long iter = 0;

  while (true) {
    af::log::info(af::log_level_normal, "Iteration #%lu", iter++);
    sleep(1);
    if ((quit_requested) || (iter == max_iters)) break;
  }

}

/** Test configuration file facility.
 */
void test_config(unsigned int iter_limits = 20) {

  // Configuration file management
  af::config cfg("/Users/volpe/Fisica/ALICE/alz118wx_backup/afdsmgrd/devel/"
    "etc/xrootd/test.cf");

  // Directives (bound)
  long test_int = -1;
  std::string test_text;
  double test_real;
  bool test_bool;

  long tmoltuae = 42;
  const char *some_text = "don't panic";
  void *args_to_cbk[] = { &tmoltuae, (void *)some_text };

  cfg.bind_int("signed_integer.directive", &test_int, 10, AF_INT_MIN, 100);
  cfg.bind_text("directive.of.text", &test_text, "default_value");
  cfg.bind_real("real_directive", &test_real, 666, AF_REAL_MIN, AF_REAL_MAX);
  cfg.bind_callback("directive_callback", directive_callback, args_to_cbk);
  cfg.bind_bool("mybool", &test_bool, false);

  printf("\n*** registered bindings ***\n");
  cfg.print_bindings();

  unsigned long iter = 0;
  while (true) {
    printf("main loop: iteration #%lu\n", ++iter);
    if (cfg.update()) {
      printf("\n*** config file modified ***\n");
      printf("the value of test_int is \033[1;32m%ld\033[m\n", test_int);
      printf("the value of test_text is \033[1;32m%s\033[m\n",
        test_text.c_str());
      printf("the value of test_real is \033[1;32m%lf\033[m\n", test_real);
      printf("the value of test_bool is \033[1;32m%s\033[m\n",
        test_bool ? "<true>" : "<false>");
    }
    sleep(1);
    if ((quit_requested) || (iter == iter_limits)) break;
  }

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
  // This piece of code might be useful, do not remove it, leave it commented.
  /*{
    char *path = exec_path(argv[0]);
    const char *helper_exec = "/afdsmgrd-exec-wrapper";
    path = (char *)realloc(path, strlen(path)+strlen(helper_exec)+1);
    strcat(path, helper_exec);
    af::extCmd::set_helper_path(path);
    af::extCmd::set_temp_path("/tmp/afdsmgrd");
    free(path);
  }*/

  signal(SIGTERM, signal_quit_callback);
  signal(SIGINT, signal_quit_callback);

  //test_config();
  test_log();

  printf("goodbye!\n");

  return 0;
}
