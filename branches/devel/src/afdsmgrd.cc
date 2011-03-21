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
//#include <sys/resource.h>

#include <fstream>
#include <memory>

#include "afLog.h"
#include "afConfig.h"
#include "afDataSetList.h"
#include "afRegex.h"
#include "afExtCmd.h"
#include "afOpQueue.h"

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
#define AF_ERR_LIBEXEC 15

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

/** Handles a typical quit signal (see signal()).
 */
void signal_quit_callback(int signum) {
  af::log::info(af::log_level_urgent, "Quit requested with signal %d", signum);
  quit_requested = true;
}

/** Callback called when directive dsmgrd.urlregex changes. Remember that val is
 *  NULL if no value was specified (i.e., directive is missing).
 */
void config_callback_urlregex(const char *name, const char *val, void *args) {

  af::regex *url_regex = (af::regex *)args;

  if (!val) url_regex->unset_regex_subst();
  else {
    char *ptn = strdup(val);
    char *subst = strchr(ptn, ' ');
    if (!subst) url_regex->unset_regex_subst();
    else {
      *subst = '\0';
      subst++;
      af::log::info(af::log_level_low, "Regex to match: <%s>", ptn);
      af::log::info(af::log_level_low, "Substitute pattern: <%s>", subst);
      url_regex->set_regex_subst(ptn, subst);
    }
    free(ptn);
  }

}

/** Callback called when directive xpd.datasetsrc changes. Remember that val is
 *  NULL if no value was specified (i.e., directive is missing).
 */
void config_callback_datasetsrc(const char *name, const char *val, void *args) {

  void **args_array = (void **)args;

  //TDataSetManagerFile **root_dsm = (TDataSetManagerFile **)args_array[0];
  af::dataSetList *dsm = (af::dataSetList *)args_array[0];
  std::string *dsm_url = (std::string *)args_array[1];
  std::string *dsm_mss = (std::string *)args_array[2];
  std::string *dsm_opt = (std::string *)args_array[3];

  af::log::info(af::log_level_normal, "(To Be Removed) name=%s val=%s", name,
    val);

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

    // Unset TDataSetManagerFile (owned by instance of af::dataSetList)
    dsm->set_dataset_mgr(NULL);

  }
  else {
    TDataSetManagerFile *root_dsm = new TDataSetManagerFile(NULL, NULL,
      Form("dir:%s opt:%s", dsm_url->c_str(), dsm_opt->c_str()));
    dsm->set_dataset_mgr(root_dsm);
    af::log::ok(af::log_level_urgent, "ROOT dataset manager reinitialized");
    af::log::ok(af::log_level_normal, ">> Local path: %s", dsm_url->c_str());
    af::log::ok(af::log_level_normal, ">> MSS: %s", dsm_mss->c_str());
    af::log::ok(af::log_level_normal, ">> Options: %s", dsm_opt->c_str());
  }

}

/** Toggles between unprivileged user and super user. Saves the unprivileged UID
 *  and GID inside static variables.
 *
 *  By default, calls to toggle_suid() are ineffective. To enable suid toggling
 *  one must first call toggle_suid(true).
 *
 *  Group must be changed before user! See [1] for further details.
 *
 *  [1] http://stackoverflow.com/questions/3357737/dropping-root-privileges
 */
void toggle_suid(bool enable = false) {

  static bool enabled = false;
  static uid_t unp_uid = 0;
  static gid_t unp_gid = 0;

  if (enable) {
    enabled = true;
    return;
  }

  if (!enabled) return;

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

/**
 */
void print_daemon_status(pid_t pid = 0) {
  static pid_t daemon_pid = 0;
  static char cmdline[50];
  static char outp[200];

  if (pid != 0) {
    daemon_pid = pid;
    return;
  }

  snprintf(cmdline, 50, "ps ax -o pid,size,vsize | grep '^\\s*%d'", daemon_pid);

  FILE *ps_file = popen(cmdline, "r");
  unsigned int size = 0;   // KiB
  unsigned int vsize = 0;  // KiB

  if (ps_file) {
    if (fgets(outp, 200, ps_file)) {
      sscanf(outp, "%*u %u %u", &size, &vsize);
    }
    pclose(ps_file);
  }

  if ((size != 0) && (vsize != 0)) {
    af::log::info(af::log_level_normal,
      "Daemon memory occupation: size: %u KiB, vsize: %u KiB", size, vsize);
  }
  else {
    af::log::error(af::log_level_normal,
      "Can't fetch daemon memory occupation");
  }
}

/** The main loop. The loop breaks when the external variable quit_requested is
 *  set to true.
 */
void main_loop(af::config &config) {

  // These are static variables: they are initialized only at the first call of
  // this function, and their value stays intact between calls
  static bool inited = false;

  //static TDataSetManagerFile *root_dsm = NULL;
  static af::dataSetList dsm;
  static af::regex url_regex;
  static std::string dsm_url;
  static std::string dsm_mss;
  static std::string dsm_opt;
  static void *dsm_cbk_args[] = { &dsm, &dsm_url, &dsm_mss, &dsm_opt };

  // The operations queue (the most important piece of the daemon)
  static af::opQueue opq;

  // The staging queue
  static std::vector<af::extCmd *> cmdq;

  // Directives in configuration files are bound to the following variables
  static long sleep_secs = 0;  // dsmgrd.sleepsecs
  static long scan_ds_every_loops = 0;  // dsmgrd.scandseveryloops
  static long max_concurrent_xfrs = 0;  // dsmgrd.parallelxfrs
  static long max_stage_retries = 0;    // dsmgrd.corruptafterfails
  static std::string stage_cmd;

  if (!inited) {

    // Exotic directive (from PROOF): xpd.datasetsrc
    config.bind_callback("xpd.datasetsrc", &config_callback_datasetsrc,
      dsm_cbk_args);

    config.bind_int("dsmgrd.sleepsecs", &sleep_secs, 30, 5, AF_INT_MAX);
    config.bind_int("dsmgrd.scandseveryloops", &scan_ds_every_loops, 10, 1,
      AF_INT_MAX);
    config.bind_int("dsmgrd.parallelxfrs", &max_concurrent_xfrs, 8, 1, 1000);
    config.bind_text("dsmgrd.stagecmd", &stage_cmd, "/bin/false");
    config.bind_int("dsmgrd.corruptafterfails", &max_stage_retries, 0, 0, 1000);

    config.bind_callback("dsmgrd.urlregex", &config_callback_urlregex,
      &url_regex);

    inited = true;

  }

  // The actual loop
  while (!quit_requested) {

    af::log::info(af::log_level_low, "*** Inside main loop ***");

    // Check and load updates from the configuration file
    if (config.update()) {
      af::log::info(af::log_level_urgent, "Config file modified");
    }
    else af::log::info(af::log_level_low, "Config file unmodified");

    //
    // The operations queue
    //

    opq.set_max_failures((unsigned int)max_stage_retries);
    af::log::info(af::log_level_normal, "/\\/\\/\\ Queue Dump /\\/\\/\\");
    opq.dump(true);
    af::log::info(af::log_level_normal, "\\/\\/\\/ Queue Dump \\/\\/\\/");

    //
    // Big Loop over datasets
    //

    const char *ds;
    dsm.fetch_datasets();
    unsigned int count_ds = 0;
    while (ds = dsm.next_dataset()) {

      af::log::info(af::log_level_low, "Processing dataset <%s>", ds);

      TFileInfo *fi;
      dsm.fetch_files(NULL, "sc");  // sc == not staged and not corrupted
      int count_changes = 0;
      int count_files = 0;
      while (fi = dsm.next_file()) {

        // Debug: we just count the retrieved files
        count_files++;

        // Originating URL is the last one; redirector URL is the last but one
        TUrl *orig_url = dsm.get_url(-1);
        TUrl *redir_url = dsm.get_url(-2);

        if (!orig_url) continue;  // no URLs in entry (should not happen)

        const char *inp_url = orig_url->GetUrl();
        const char *out_url = url_regex.subst(inp_url);

        if (!out_url) continue;
          // orig_url not matched regex (i.e., originating URL is unsupported)

        if ((redir_url) && (strcmp(out_url, redir_url->GetUrl()) == 0)) {

          //
          // The translated redirector URL already exists in this entry: let us
          // keep only the last two
          //

          switch (dsm.del_urls_but_last(2)) {
            case af::ds_manip_err_ok_mod:
              //af::log::info(af::log_level_low, "del_urls_but_last(2)");
              count_changes++;
            break;
            case af::ds_manip_err_ok_noop:
              // nothing to do
            break;
            case af::ds_manip_err_fail:  // should not happen
              af::log::error(af::log_level_normal,
                "Error in dataset %s at entry %s (last URL) when pruning all "
                  "URLs but last two", ds, inp_url);
            break;
          }

        }
        else {

          //
          // We have to add the translated redirector URL
          //

          // Conserve only last URL of the given entry
          switch (dsm.del_urls_but_last()) {
            case af::ds_manip_err_ok_mod:
              //af::log::info(af::log_level_low, "del_urls_but_last(1)");
              count_changes++;
            break;
            case af::ds_manip_err_ok_noop:
              // nothing to do
            break;
            case af::ds_manip_err_fail: // should not happen
              af::log::error(af::log_level_normal,
                "Error in dataset %s at entry %s (last URL) when pruning all "
                  "URLs but last", ds, inp_url);
            break;
          }

          // Add redirector URL
          if ( fi->AddUrl(out_url, true) ) {
            //af::log::info(af::log_level_low, "AddUrl()");
            count_changes++;
          }
          else {  // should not happen
            af::log::error(af::log_level_normal,
              "Error in dataset %s at entry %s (last URL) when adding "
                "redirector URLs %s", ds, inp_url, out_url);
          }

        }

        //
        // URL of redirector is added in queue
        //

        unsigned int unique_id = opq.insert(out_url);
        if (unique_id) {

          // URL is not yet in queue: let's prepare the external command
          af::log::ok(af::log_level_low, "In queue: %s (id=%u)", out_url,
            unique_id);

          const char *var_names[] = { "URLTOSTAGE", "TREENAME" };
          const char *var_values[2];
          const char *empty_tree = "";

          var_values[0] = out_url;
          var_values[1] = dsm.get_default_tree();
          if (!var_values[1]) var_values[1] = empty_tree;

          // NULL is never returned in this case
          //std::string *url_cmd = af::regex::dollar_subst(stage_cmd.c_str(),
          //  2, var_names, var_values);
          std::string *url_cmd = new std::string("ciao");

          af::log::ok(af::log_level_low,
            "Stage command: <<%s>>", url_cmd->c_str());

          delete url_cmd;

        }
        else {

          // URL already in queue
          af::log::error(af::log_level_low, "In queue: %s", out_url);

        }

      }  // end loop over dataset entries

      // Save only if necessary
      if (count_changes > 0) {

        toggle_suid();
        bool save_ok = dsm.save_dataset();
        toggle_suid();

        if (save_ok) {
          af::log::ok(af::log_level_normal, "Dataset <%s> saved: %d entries, %d modifications", ds, count_files, count_changes);
        }
        else {
          af::log::error(af::log_level_normal,
            "Dataset <%s> not saved (check permissions)", ds);
        }
      }
      else {
        af::log::info(af::log_level_low, "Dataset <%s> not modified", ds);
      }

      dsm.free_files();

      count_ds++;

    }
    dsm.free_datasets();

    af::log::info(af::log_level_low, "Number of datasets processed: %u",
      count_ds);

    // Report resources
    print_daemon_status();

    // Report resources -- apparently it works only on BSD-like systems
    // http://stackoverflow.com/questions/5120861/how-to-measure-memory-usage-from-inside-a-c-program
    /*struct rusage rusg;
    if (getrusage(RUSAGE_SELF, &rusg) == 0) {
      af::log::info(af::log_level_low, "Mem usage (KiB) >> "
        "Resident: %ld | Shared: %ld | Unsh. data: %ld | Unsh. stack: %ld",
        rusg.ru_maxrss, rusg.ru_ixrss, rusg.ru_idrss, rusg.ru_isrss);
    }
    else {
      af::log::warning(af::log_level_normal,
        "Cannot get resource usage of current process");
    }*/

    //long   ru_maxrss;        /* maximum resident set size */
    //long   ru_ixrss;         /* integral shared memory size */
    //long   ru_idrss;         /* integral unshared data size */
    //long   ru_isrss;         /* integral unshared stack size */

    /* */

    // Sleep until the next loop
    af::log::info(af::log_level_low, "Sleeping %ld seconds", sleep_secs);
    sleep(sleep_secs);

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
  const char *libexec_path = NULL;
  bool daemonize = false;

  opterr = 0;

  // c <config>
  // l <logfile>
  // b --> TODO: fork to background
  // p <pidfile>
  // d <low|normal|high|urgent> --> log level
  while ((c = getopt(argc, argv, ":c:l:p:bu:d:e:")) != -1) {

    switch (c) {
      case 'c': config_file = optarg; break;
      case 'l': log_file = optarg; break;
      case 'p': pid_file = optarg; break;
      case 'd': log_level = optarg; break;
      case 'b': daemonize = true; break;
      case 'u': drop_user = optarg; break;
      case 'e': libexec_path = optarg; break;
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
    af::log::ok(af::log_level_normal, "Log level set to %s", log_level);
  }
  else {
    af::log::warning(af::log_level_normal, "No log level specified (with -d): "
      "defaulted to normal");
  }

  // "libexec" path
  if (!libexec_path) {
    af::log::fatal(af::log_level_urgent, "No path for auxiliary binaries "
      "specified: specify one with -e");
    return AF_ERR_LIBEXEC;
  }
  else {
    af::log::ok(af::log_level_normal, "Path for auxiliary binaries: %s",
      libexec_path);
  }

  // Report current PID on log facility
  pid_t pid = getpid();
  write_pidfile(pid, pid_file);
  print_daemon_status(pid);

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
        toggle_suid(true);  // makes toggle_suid() calls effective
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

  if (!config_file) {
    af::log::fatal(af::log_level_urgent, "No config file given (use -c)");
    return AF_ERR_CONFIG;
  }

  af::config config(config_file);

  // Init external command facility paths
  {
    std::string exec_wrapper_path = libexec_path;
    exec_wrapper_path += "/afdsmgrd-exec-wrapper";
    af::extCmd::set_helper_path(exec_wrapper_path.c_str());

    //std::string extcmd_temp_path = "/tmp/afdsmgrd-" + pid;
    //af::extCmd::set_temp_path(extcmd_temp_path.c_str());
    af::extCmd::set_temp_path("/tmp/afdsmgrd");
  }

  // Trap some signals to terminate gently
  signal(SIGTERM, signal_quit_callback);
  signal(SIGINT, signal_quit_callback);

  main_loop(config);

  return 0;

}
