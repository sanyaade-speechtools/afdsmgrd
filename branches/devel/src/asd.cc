/**
 * afdsmgrd.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * Entry point of the daemon with some extra functions. Functions that are meant
 * to be used by external plugins are demangled by means of the extern "C"
 * keyword.
 */

#include <stdio.h>
#include <unistd.h>
#include <libgen.h>
#include <signal.h>

#include <fstream>
#include <memory>

#include "afLog.h"
#include "afConfig.h"

#include <TDataSetManagerFile.h>

#define AF_ERR_LOG 1
#define AF_ERR_CONFIG 2
#define AF_ERR_MEM 3

/** Global variables.
 */
bool quit_requested = false;

/** Returns an instance of the log facility based on the given logfile. In case
 *  the logfile can't be opened, it returns NULL.
 */
af::log *set_logfile(const char *log_file) {

  af::log *log;

  if (log_file) {
    try { log = new af::log(log_file, af::log_level_normal); }
    catch (std::ios_base::failure &exc) {
      log = new af::log(std::cout, af::log_level_normal);
      af::log::fatal(af::log_level_urgent, "Can't open %s as log file",
        log_file);
      delete log;
      return NULL;
    }
  }
  else log = new af::log(std::cout, af::log_level_normal);

  return log;

}

/** Writes the given pid on pidfile. Returns true if written, false otherwise.
 */
bool write_pidfile(pid_t pid, const char *pid_file) {
  if (!pid_file) {
    af::log::warning(af::log_level_urgent, "No pidfile given (use -p)");
    return false;
  }
  std::ofstream pf(pid_file);
  if (!pf) {
    af::log::error(af::log_level_urgent, "Can't write pidfile %s", pid_file);
    return false;
  }
  pf << pid << std::endl;
  pf.close();
  return true;
}

/** Returns a pointer to std::string containing the base path (without trailing
 *  slashes) of the command executed as "cmd" from current working directory
 *  (i.e. the argv[0]). The returned pointer is dynamically allocated and must
 *  be freed by the caller.
 */
std::string *get_exec_path(const char *cmd) {

  char *base = dirname((char *)cmd);  // dirname() returns a ptr to static buf
  char *buf;
  std::string *path;
  size_t maxlen;

  if (base[0] == '/') {
    path = new std::string(base);
  }
  else {
    buf = getcwd(NULL, 0);  // getcwd() uses malloc()

    if (!buf) return NULL;
    else {
      path = new std::string(buf);
      free(buf);
    }

    if (strcmp(base, ".") != 0) {
      *path += "/";
      *path += base;
    }
  }

  return path;
}

/** Handles a typical quit signal (see signal()).
 */
void signal_quit_callback(int signum) {
  af::log::info(af::log_level_urgent, "Quit requested with signal %d", signum);
  quit_requested = true;
}

/** Callback called when directive xpd.datasetsrc changes.
 */
void config_callback_datasetsrc(const char *name, const char *val, void *args) {

  void **args_array = (void **)args;

  TDataSetManager **root_dsm = (TDataSetManager **)args_array[0];
  std::string *dsm_url = (std::string *)args_array[1];
  std::string *dsm_mss = (std::string *)args_array[2];
  std::string *dsm_opt = (std::string *)args_array[3];

  af::log::info(af::log_level_normal, "name=%s val=%s", name, val);

  char *cfgline = strdup(val);
  char *tok;

  tok = strtok(cfgline, " \t");
  while (tok != NULL) {
    if (strncmp(tok, "url:", 4) == 0) *dsm_url = &tok[4];
    else if (strncmp(tok, "mss:", 4) == 0) *dsm_mss = &tok[4];
    else if (strncmp(tok, "opt:", 4) == 0) *dsm_opt = &tok[4];
    tok = strtok(NULL, " \t");
  }

  // url:
  // mss:
  // opt:



}

/** The main loop. The loop breaks when the external variable quit_requested is
 *  set to true.
 */
void main_loop(af::config &config) {

  static bool inited = false;

  TDataSetManager *root_dsm = NULL;
  std::string dsm_url;
  std::string dsm_mss;
  std::string dsm_opt;
  void *dsm_cbk_args[] = { &root_dsm, &dsm_url, &dsm_mss, &dsm_opt };

  if (!inited) {
    config.bind_callback("xpd.datasetsrc", &config_callback_datasetsrc,
      dsm_cbk_args);
  }

  while (!quit_requested) {
    af::log::info(af::log_level_normal, "This is the main loop");
    if (config.update())
      af::log::info(af::log_level_normal, "Config file modified");
    else af::log::info(af::log_level_low, "Config file unmodified");
    sleep(1);
  }

}

/*void AfDataSetsManager::DoSuid() {
  if (!fSuid) {
    return;
  }
  fUnpUid = geteuid();
  fUnpGid = getegid();
  if (!((seteuid(0) == 0) && (setegid(0) == 0))) {
    AfLogFatal("Failed to obtain superuser privileges");
    gSystem->Exit(51);
  }
}

void AfDataSetsManager::UndoSuid() {
  if (!fSuid) {
    return;
  }
  if (!((setegid(fUnpGid) == 0) && (seteuid(fUnpUid) == 0))) {
    AfLogFatal("Can't drop privileges!");
    gSystem->Exit(51);
  }
}*/

/** Entry point of afdsmgrd.
 */
int main(int argc, char *argv[]) {

  int c;
  const char *config_file = NULL;
  const char *log_file = NULL;
  const char *pid_file = NULL;
  bool daemon = false;

  opterr = 0;

  // c <config>
  // l <logfile>
  // b
  // p <pidfile>
  // d <low|normal|high|urgent> --> log level
  while ((c = getopt(argc, argv, ":c:l:p:d:b")) != -1) {

    switch (c) {
      case 'c': config_file = optarg; break;
      case 'l': log_file = optarg; break;
      case 'p': pid_file = optarg; break;
      case 'd': /* TODO */ break;
      case 'b': daemon = true; break;
    }

  }

  std::auto_ptr<af::log> log( set_logfile(log_file) );
  if (!log.get()) return AF_ERR_LOG;

  /* fork() */

  pid_t pid = getpid();
  write_pidfile(pid, pid_file);

  af::log::ok(af::log_level_normal, "afdsmgrd started with pid=%d", pid);

  std::auto_ptr<std::string> exec_path( get_exec_path(argv[0]) );
  if (!exec_path.get()) {
    af::log::fatal(af::log_level_urgent, "Memory error");
    return AF_ERR_MEM;
  }
  else {
    af::log::info(af::log_level_normal, "Executable path (unnormalized): %s",
      exec_path->c_str());
  }

  if (!config_file) {
    af::log::fatal(af::log_level_urgent, "No config file given (use -c)");
    return AF_ERR_CONFIG;
  }

  af::config config(config_file);

  signal(SIGTERM, signal_quit_callback);
  signal(SIGINT, signal_quit_callback);

  main_loop(config);

  return 0;

}
