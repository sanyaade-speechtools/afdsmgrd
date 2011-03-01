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
#include <pwd.h>
#include <grp.h>

#include <fstream>
#include <memory>

#include "afLog.h"
#include "afConfig.h"

#include <TDataSetManagerFile.h>

#define AF_ERR_LOG 1
#define AF_ERR_CONFIG 2
#define AF_ERR_MEM 3
#define AF_ERR_LOGLEVEL 4
#define AF_ERR_FORK 5
#define AF_ERR_CWD 6
#define AF_ERR_SETSID 7
#define AF_ERR_DROP_IMPOSSIBLE 8
#define AF_ERR_DROP_FAILED 9
#define AF_ERR_DROP_INVALID 10
#define AF_ERR_SUID_NORMAL_GID 11
#define AF_ERR_SUID_NORMAL_UID 12
#define AF_ERR_SUID_ROOT_GID 13
#define AF_ERR_SUID_ROOT_UID 14

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
 *
 * TODO TODO TODO TODO TODO TODO TODO TODO TODO !! ERRORS !! ERRORS !!
 */
/*std::string *get_exec_path(const char *cmd) {

  // HERE LIES THE ERROR: dirname() modifies the input string!!!!!
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
}*/

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

  char *cfgline = NULL;
  char *tok = NULL;
  bool rw = false;
  bool invalid = false;

  // If val == NULL, directive is invalid
  if (!val) {
    invalid = true;
  }
  else {

    cfgline = strdup(val);
    tok = strtok(cfgline, " \t");

    if (strncmp(tok, "file", 4) != 0) invalid = true;
    else {
      while (tok != NULL) {
        if (strncmp(tok, "url:", 4) == 0) *dsm_url = &tok[4];
        else if (strncmp(tok, "mss:", 4) == 0) *dsm_mss = &tok[4];
        else if (strncmp(tok, "opt:", 4) == 0) *dsm_opt = &tok[4];
        else if (strncmp(tok, "rw=1", 4) == 0) rw = true;
        tok = strtok(NULL, " \t");
      }
    }

    if (dsm_opt->empty()) *dsm_opt = (rw ? "Av:Ar" : "-Av:-Ar");
    if (dsm_mss->empty() || dsm_url->empty()) invalid = true;

    free(cfgline);

  }

  if (invalid) {

    // Error message for the masses
    if (!val) {
      af::log::error(af::log_level_urgent,
        "Mandatory directive %s is missing", name);
    }
    else {
      af::log::error(af::log_level_urgent,
        "Invalid directive \"%s\" value: %s", name, val);
    }

    // Destroy previous TDataSetManagerFile: other parts of the program must
    // not assume that it is !NULL
    if (*root_dsm) {
      delete *root_dsm;
      *root_dsm = NULL;
    }
  }
  else {
    *root_dsm = new TDataSetManagerFile(NULL, NULL,
      Form("dir:%s opt:%s", dsm_url->c_str(), dsm_opt->c_str()));
    af::log::ok(af::log_level_urgent, "ROOT dataset manager reinitialized");
    af::log::ok(af::log_level_normal, ">> Local path: %s", dsm_url->c_str());
    af::log::ok(af::log_level_normal, ">> MSS: %s", dsm_mss->c_str());
    af::log::ok(af::log_level_normal, ">> Options: %s", dsm_opt->c_str());
  }

}

/** The main loop. The loop breaks when the external variable quit_requested is
 *  set to true.
 */
void main_loop(af::config &config) {

  // These are static variables: they are initialized only at the first call of
  // this function, and their value stays intact between calls
  static bool inited = false;

  static TDataSetManager *root_dsm = NULL;
  static std::string dsm_url;
  static std::string dsm_mss;
  static std::string dsm_opt;
  static void *dsm_cbk_args[] = { &root_dsm, &dsm_url, &dsm_mss, &dsm_opt };

  // Bound to dsmgrd.sleepsecs
  static long sleep_secs = 1;

  if (!inited) {

    // Exotic directive (from PROOF): xpd.datasetsrc
    config.bind_callback("xpd.datasetsrc", &config_callback_datasetsrc,
      dsm_cbk_args);

    // Bind dsmgrd.sleepsecs
    config.bind_int("dsmgrd.sleepsecs", &sleep_secs, 30, 5, AF_INT_MAX);

    inited = true;

  }

  // The actual loop
  while (!quit_requested) {

    af::log::info(af::log_level_low, "Inside main loop");
    if (config.update()) {
      af::log::info(af::log_level_urgent, "Config file modified");
    }
    else af::log::info(af::log_level_low, "Config file unmodified");

    af::log::info(af::log_level_low, "Sleeping %ld seconds", sleep_secs);
    sleep(sleep_secs);

  }

}

/** Toggles between unprivileged user and super user. Saves the unprivileged UID
 *  and GID inside static variables.
 *
 *  Group must be changed before user! See [1] for further details.
 *
 *  [1] http://stackoverflow.com/questions/3357737/dropping-root-privileges
 */
void toggle_suid() {

  static uid_t unp_uid = 0;
  static gid_t unp_gid = 0;

  if (geteuid() == 0) {

    // Revert to normal user
    if (setegid(unp_gid) != 0) {
      af::log::fatal(af::log_level_urgent,
        "Failed to revert to unprivileged gid %d", unp_gid);
      exit(AF_ERR_SUID_NORMAL_GID);
    }
    else if (seteuid(unp_uid) != 0) {
      af::log::fatal(af::log_level_urgent,
        "Failed to revert to unprivileged uid %d", unp_uid);
      exit(AF_ERR_SUID_NORMAL_UID);
    }
    else {
      af::log::ok(af::log_level_low, "Reverted to unprivileged uid=%d gid=%d",
        unp_uid, unp_gid);
    }

  }
  else {

    // Try to become root

    unp_uid = geteuid();
    unp_gid = getegid();

    if (setegid(0) != 0) {
      af::log::fatal(af::log_level_urgent,
        "Failed to escalate to privileged gid");
      exit(AF_ERR_SUID_ROOT_GID);
    }
    else if (seteuid(0) != 0) {
      af::log::fatal(af::log_level_urgent,
        "Failed to escalate to privileged uid");
      exit(AF_ERR_SUID_ROOT_UID);
    }
    else {
      af::log::ok(af::log_level_low, "Superuser privileges obtained");
    }

  }

}

/** Entry point of afdsmgrd.
 */
int main(int argc, char *argv[]) {

  int c;
  const char *config_file = NULL;
  const char *log_file = NULL;
  const char *pid_file = NULL;
  const char *log_level = NULL;
  const char *drop_user = NULL;
  bool daemonize = false;

  opterr = 0;

  // c <config>
  // l <logfile>
  // b --> TODO: fork to background
  // p <pidfile>
  // d <low|normal|high|urgent> --> log level
  while ((c = getopt(argc, argv, ":c:l:p:bu:d:")) != -1) {

    switch (c) {
      case 'c': config_file = optarg; break;
      case 'l': log_file = optarg; break;
      case 'p': pid_file = optarg; break;
      case 'd': log_level = optarg; break;
      case 'b': daemonize = true; break;
      case 'u': drop_user = optarg; break;
    }

  }

  // Fork must be done before everything else: in particular, before getting the
  // PID (because it changes!) and before opening any file (because fds are not
  // inherited)
  if (daemonize) {

    pid_t pid = fork();

    if (pid < 0) exit(AF_ERR_FORK);  // in parent process: cannot fork
    if (pid > 0) exit(0);  // in parent proc: terminate because fork succeeded
    if (setsid() < 0) exit(AF_ERR_SETSID);
    if (chdir("/") != 0) exit(AF_ERR_CWD);

    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }

  std::auto_ptr<af::log> log( set_logfile(log_file) );
  if (!log.get()) return AF_ERR_LOG;

  // Set the log level amongst low, normal, high and urgent
  if (log_level) {
    if (strcmp(log_level, "low") == 0)
      log->set_level(af::log_level_low);
    else if (strcmp(log_level, "normal") == 0)
      log->set_level(af::log_level_normal);
    else if (strcmp(log_level, "high") == 0)
      log->set_level(af::log_level_high);
    else if (strcmp(log_level, "urgent") == 0)
      log->set_level(af::log_level_urgent);
    else {
      // Invalid log level is fatal for the daemon
      af::log::fatal(af::log_level_urgent,
        "Invalid log level \"%s\": specify with -d one amongst: "
        "low, normal, high, urgent", log_level);
      return AF_ERR_LOGLEVEL;
    }
    af::log::info(af::log_level_normal, "Log level set to %s", log_level);
  }
  else {
    af::log::warning(af::log_level_normal, "No log level specified (with -d): "
      "defaulted to normal");
  }

  // Report current PID on log facility
  pid_t pid = getpid();
  write_pidfile(pid, pid_file);

  af::log::ok(af::log_level_normal, "afdsmgrd started with pid=%d", pid);

  // Drop current effective user to an unprivileged one
  if (drop_user) {

    if (geteuid() != 0) {
      af::log::fatal(af::log_level_urgent,
        "You cannot drop privileges to user \"%s\" if you are not root",
        drop_user);
      return AF_ERR_DROP_IMPOSSIBLE;
    }

    struct passwd *pwd = getpwnam(drop_user);
    if (pwd) {
      // See http://stackoverflow.com/questions/3357737/dropping-root-privileges
      if ((setegid(pwd->pw_gid) == 0) && (seteuid(pwd->pw_uid) == 0)) {
        struct group *grp = getgrgid(pwd->pw_gid);
        af::log::ok(af::log_level_urgent,
          "Dropped privileges to user \"%s\" (%d), group \"%s\" (%d)",
          drop_user, pwd->pw_uid, grp->gr_name, pwd->pw_gid);
      }
      else {
        af::log::fatal(af::log_level_urgent,
          "Failed to drop privileges to user \"%s\"", drop_user);
        return AF_ERR_DROP_FAILED;
      }
    }
    else {
      af::log::fatal(af::log_level_urgent,
        "Can't become user \"%s\" because it appears to not exist", drop_user);
      return AF_ERR_DROP_INVALID;
    }
  }
  else if (geteuid() == 0) {
    // Running as root, privileges undropped
    af::log::warning(af::log_level_urgent,
      "Running as user root: this is potentially dangerous");
  }
  else {
    // Running as unprivileged, privileges undropped
    struct passwd *pwd = getpwuid(geteuid());
    af::log::warning(af::log_level_urgent,
      "Running as unprivileged user \"%s\": this may prevent dataset writing",
      pwd->pw_name);
  }

  /* IMPORTANT -- Get Executable Path -- dirname() error!!!

  std::auto_ptr<std::string> exec_path( get_exec_path(argv[0]) );
  if (!exec_path.get()) {
    af::log::fatal(af::log_level_urgent, "Memory error");
    return AF_ERR_MEM;
  }
  else {
    af::log::info(af::log_level_normal, "Executable path (unnormalized): %s",
      exec_path->c_str());
  }*/

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
