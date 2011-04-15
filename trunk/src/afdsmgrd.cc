/**
 * afdsmgrd.cc -- by Dario Berzano <dario.berzano@cern.ch>
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

// TODO
#include <sys/resource.h>
#include <time.h>

#include <fstream>
#include <memory>
#include <list>

#include "afLog.h"
#include "afConfig.h"
#include "afDataSetList.h"
#include "afRegex.h"
#include "afExtCmd.h"
#include "afOpQueue.h"
#include "afNotify.h"
#include "afOptions.h"
#include "afResMon.h"

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

/** Set of variables in configuration file.
 */
typedef struct {

  long sleep_secs;           // dsmgrd.sleepsecs
  long scan_ds_every_loops;  // dsmgrd.scandseveryloops
  long max_concurrent_xfrs;  // dsmgrd.parallelxfrs
  long max_stage_retries;    // dsmgrd.corruptafterfails
  std::string stage_cmd;     // dsmgrd.stagecmd
  af::regex url_regex;       // dsmgrd.urlregex
  af::notify *notif;

} afdsmgrd_vars_t;

/** Custom resources holder.
 */
typedef struct {
  unsigned int size_kib;
  unsigned int vsize_kib;
  float total_pcpu;
} afdsmgrd_res_t;

/** Global variables.
 */
bool quit_requested = false;

/** Returns an instance of the log facility based on the given logfile. In case
 *  the logfile can't be opened, it returns NULL.
 */
af::log *set_logfile(const char *log_file) {

  af::log *log;
  std::string banner = AF_VERSION_BANNER;

  if (log_file) {
    try {
      log = new af::log(log_file, af::log_level_normal, banner);
    }
    catch (std::ios_base::failure &exc) {
      log = new af::log(std::cout, af::log_level_normal, banner);
      af::log::fatal(af::log_level_urgent, "Can't open %s as log file",
        log_file);
      delete log;
      return NULL;
    }
  }
  else log = new af::log(std::cout, af::log_level_normal, banner);

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

  af::log::info(af::log_level_debug, "Full dataset source directive: "
    "name=%s value=%s", name, val);

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

/** Callback called when directive dsmgrd.notify changes: it loads and unloads
 *  external libraries for notification. Remember that val is NULL if no value
 *  was specified (i.e., directive is missing).
 */
void config_callback_notify(const char *name, const char *val, void *args) {

  void **args_array = (void **)args;

  af::notify **notif = (af::notify **)args_array[0];
  af::config *cfg = (af::config *)args_array[1];

  // Since directive is called only if something has changed, let's delete
  // any previously loaded library, if present
  if (*notif) {
    af::notify::unload(*notif);
    *notif = NULL;
    af::log::info(af::log_level_high, "Notification plugin unloaded");
  }

  // Then, if a right value is specified, load a new one and report status
  if (!val) return;

  *notif = af::notify::load(val, *cfg);
  if (*notif) {
    af::log::ok(af::log_level_high, "Plugin loaded: %s (%s)",
      val, (*notif)->whoami());
  }
  else {
    af::log::error(af::log_level_high, "Can't load notification plugin: %s",
      val);
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

/** Transfer queue is processed: check if slots are freed, then insert elements
 *  from opq in free slots of cmdq. Handle successes and failures by syncing
 *  info between cmdq and opq
 */
void process_transfer_queue(af::opQueue &opq, std::list<af::extCmd *> &cmdq,
  afdsmgrd_vars_t &vars) {

  const af::queueEntry *qent;

  // Variables to substitute in stage command
  static af::varmap_t stagecmd_vars;
  if (stagecmd_vars.empty()) {
    stagecmd_vars.insert( af::varpair_t("URLTOSTAGE", "") );
    stagecmd_vars.insert( af::varpair_t("TREENAME", "") );
  }

  af::log::info(af::log_level_normal, "*** Processing transfer queue ***");

  opq.set_max_failures((unsigned int)vars.max_stage_retries);

  //
  // Query on "running" to update their status if needed
  //

  opq.init_query_by_status(af::qstat_running);
  while ( qent = opq.next_query_by_status() ) {

    af::log::info(af::log_level_debug, "Searching in command queue for uiid=%u",
      qent->get_instance_id());

    // Check status in transfer queue
    for (std::list<af::extCmd *>::iterator it=cmdq.begin();
      it!=cmdq.end(); it++) {

      if ( (*it)->get_id() == qent->get_instance_id() ) {

        af::log::ok(af::log_level_debug, "Found uuid=%u in command queue",
          qent->get_instance_id());

        if ((*it)->is_running()) {
          af::log::info(af::log_level_debug, "Still downloading: %s (uiid=%u)",
            qent->get_main_url(), qent->get_instance_id());
          break;
        }

        // Download has finished

        (*it)->get_output();

        if ( (*it)->is_ok() ) {
          
          //
          // Download OK
          //

          af::log::ok(af::log_level_high, "Success: %s", qent->get_main_url());

          // The strings are owned by (*it); note that these fields are not
          // mandatory for the external command, thus they might be NULL or 0!
          const char *tree_name = (*it)->get_field_text("Tree");
          const char *endp_url = (*it)->get_field_text("EndpointUrl");
          unsigned int size_bytes = (*it)->get_field_uint("Size");
          unsigned int n_events = (*it)->get_field_uint("Events");

          opq.success(qent->get_main_url(), endp_url, tree_name, n_events,
            size_bytes);

        }
        else {

          //
          // Download failed
          //

          // Check if it was staged nevertheless
          bool was_staged = (*it)->get_field_uint("Staged");
          const char *reason = (*it)->get_field_text("Reason");

          // Stage command reported a failure
          af::log::error(af::log_level_high, "Failed: %s "
            "(reason: %s, staged: %s)",
            qent->get_main_url(), (reason ? reason : "unknown"),
            (was_staged ? "yes" : "no"));

          opq.failed(qent->get_main_url(), was_staged);

        }

        //(*it)->print_fields(true);

        // Success or failure: remove it from command queue in either case
        delete *it;
        cmdq.erase(it);
        
        break;

      }

    } // end loop over command queue

  }
  opq.free_query_by_status();

  //
  // Query on "queued", limited to the number of free download slots
  //

  int free_cmd_slots = vars.max_concurrent_xfrs - cmdq.size();
  af::log::info(af::log_level_debug, "Staging slots free: %d", free_cmd_slots);

  if (free_cmd_slots > 0) {

    opq.init_query_by_status(af::qstat_queue, free_cmd_slots);

    while ( qent = opq.next_query_by_status() ) {

      // Prepare command

      af::varmap_iter_t it;

      it = stagecmd_vars.find("URLTOSTAGE");
      it->second = qent->get_main_url();  // it is the translated (redir) one

      it = stagecmd_vars.find("TREENAME");
      const char *def_tree = qent->get_tree_name();
      it->second = def_tree ? def_tree : "";

      std::string url_cmd = af::regex::dollar_subst(vars.stage_cmd.c_str(),
        stagecmd_vars);

      af::log::info(af::log_level_debug, "Preparing staging command: %s",
        url_cmd.c_str());

      // Launch command

      af::extCmd *ext_stage_cmd = new af::extCmd(url_cmd.c_str(),
        qent->get_instance_id());
      int r = ext_stage_cmd->run();
      if (r == 0) {

        // Command started successfully
        af::log::ok(af::log_level_normal, "Staging started: %s "
          "(uiid=%u, tree=%s)", qent->get_main_url(), qent->get_instance_id(),
          qent->get_tree_name());

        // Turn status to "running"
        opq.set_status(qent->get_main_url(), af::qstat_running);

        // Enqueue in command queue
        cmdq.push_back(ext_stage_cmd);

      }
      else {
        af::log::error(af::log_level_high, "Error running staging command, "
          "wrapper returned %d: check permissions on %s. Command issued: %s",
          af::extCmd::get_temp_path(), vars.stage_cmd.c_str());
      }

    }
    opq.free_query_by_status();

  }

  //
  // Summary (also notification)
  //

  unsigned int n_queued, n_runn, n_success, n_fail, n_total;
  opq.summary(n_queued, n_runn, n_success, n_fail);
  n_total = n_queued + n_runn + n_success + n_fail;
  af::log::info(af::log_level_normal, "Total elements in queue: %u || "
    "Queued: %u | Downloading: %u | Success: %u | Failed: %u",
    n_total, n_queued, n_runn, n_success, n_fail);
  if (vars.notif)
    vars.notif->queue(n_queued, n_runn, n_success, n_fail, n_total);

  //af::log::info(af::log_level_normal, "========== Begin Of Queue ==========");
  //opq.dump(true);
  //af::log::info(af::log_level_normal, "========== End Of Queue ==========");

}

/** Scan over all datasets handled by the dataset manager wrapper dsm: proper
 *  files are inserted in opq, while finished files in opq are synced in dsm
 */
void process_datasets(af::opQueue &opq, af::dataSetList &dsm,
  afdsmgrd_vars_t &vars) {

  const af::queueEntry *qent;

  af::log::info(af::log_level_normal, "*** Processing datasets ***");

  const char *ds;
  dsm.fetch_datasets();
  unsigned int count_ds = 0;

  while (ds = dsm.next_dataset()) {

    af::log::info(af::log_level_low, "Scanning dataset %s", ds);

    TFileInfo *fi;
    dsm.fetch_files(NULL, "sc");  // sc == not staged AND not corrupted
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
      const char *out_url = vars.url_regex.subst(inp_url);

      if (!out_url) continue;
      // orig_url not matched regex (i.e., originating URL is unsupported)

      if ((redir_url) && (strcmp(out_url, redir_url->GetUrl()) == 0)) {

        //
        // The translated redirector URL already exists in this entry: let
        // us keep only the last two
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
              "In dataset %s at entry %s (last URL): problems when pruning "
                "all URLs but last two", ds, inp_url);
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
            // OK and modified
            count_changes++;
          break;
          case af::ds_manip_err_ok_noop:
            // OK but nothing changed
            af::log::ok(af::log_level_debug, "In dataset %s at entry %s: "
              "last URL not removed", ds, inp_url);
          break;
          case af::ds_manip_err_fail:
            // Should not happen, except for bugs (maybe there is one)
            af::log::error(af::log_level_normal,
              "In dataset %s at entry %s (last URL): problems when pruning "
                "all URLs but last", ds, inp_url);
          break;
        }

        // Add redirector URL: AddUrl() fails (returns false) if the same
        // URL is already in the list
        if ( fi->AddUrl(out_url, true) ) count_changes++;
        else {
          // URL already in the list: duplicates are not allowed
          af::log::warning(af::log_level_debug,
            "In dataset %s: at entry %s, AddUrl() failed while adding "
              "redirector URL %s", ds, inp_url, out_url);
        }

      }

      //
      // URL of redirector is added in queue (if not existant)
      //

      unsigned int unique_id;
      qent = opq.cond_insert(out_url, dsm.get_default_tree(), &unique_id);
        // NULL value for get_default_tree() is accepted

      if (!qent) {

        // URL is not yet in queue: cond_insert() has already appended it
        af::log::ok(af::log_level_low, "Queued: %s (id=%u)", out_url,
          unique_id);

      }
      else {

        // URL already in queue
        af::log::info(af::log_level_debug, "Already queued "
          "(status=%c, failures=%u): %s", qent->get_status(),
          qent->get_n_failures(), out_url);

        //
        // Check the status of this URL in opq and eventually update, if needed
        //

        af::qstat_t stat = qent->get_status();

        if ((stat == af::qstat_success) || (stat == af::qstat_failed)) {

          // Removes all metadata (if present)
          TList *mdl = fi->GetMetaDataList();
          if ((mdl) && (mdl->GetEntries() > 0)) {
            fi->RemoveMetaData();
            count_changes++;
          }

          // Add endpoint URL
          if (qent->get_endp_url()) {
            if (!fi->AddUrl(qent->get_endp_url(), true)) {
              af::log::warning(af::log_level_debug, "In dataset %s, "
                "endpoint URL %s is a duplicate", ds,
                qent->get_endp_url());
            }
            else count_changes++;
          }

          // Tree name: set for all TFileCollection and metadata for this
          // TFileInfo, but only on success
          if ((stat == af::qstat_success) && (qent->get_tree_name())) {

            // Default tree for TFileColleciton
            dsm.set_default_tree(qent->get_tree_name());

            // Metadata for current TFileInfo
            TFileInfoMeta *meta = new TFileInfoMeta(qent->get_tree_name());
            meta->SetEntries(qent->get_n_events());
            fi->AddMetaData(meta);
              // meta owned by TFileInfo: do not delete

            // Sets size
            fi->SetSize(qent->get_size_bytes());

            af::log::ok(af::log_level_debug, "URL in opq %s claimed by %s",
              out_url, ds);

            count_changes++;

          }

          // Current staged/corrupted status;
          bool cur_s = fi->TestBit( TFileInfo::kStaged );
          bool cur_c = fi->TestBit( TFileInfo::kCorrupted );

          // Set the staged bit only if changes are needed (even if
          // corrupted, if command returned a "Staged: 1" field in the FAIL
          // stdout line)
          if ((stat == af::qstat_success) || (qent->is_staged())) {
            if (!cur_s) {
              fi->SetBit( TFileInfo::kStaged );
              count_changes++;
            }
          }
          else {
            if (cur_s) {
              fi->ResetBit( TFileInfo::kStaged );
                // it seems not to be needed, but in the future filter on
                // dataset files may be also on files other than "sc"..
              count_changes++;
            }
          }

          // Set the corrupted bit (only if changes are needed)
          if (stat == af::qstat_failed) {
            if (!cur_c) {
              fi->SetBit( TFileInfo::kCorrupted );
              count_changes++;
            }
          }
          else {
            if (cur_c) {
              fi->ResetBit( TFileInfo::kCorrupted );
                // it seems not to be needed... see above
              count_changes++;
            }
          }

        }  // end if success or failed

      }  // end if entry already in queue

    }  // end loop over dataset entries

    //
    // Save only if needed
    //

    if (count_changes > 0) {

      toggle_suid();
      bool save_ok = dsm.save_dataset();
      toggle_suid();

      if (save_ok) {
        af::log::ok(af::log_level_high, "Dataset %s saved: %d entries", ds,
          count_files);
      }
      else {
        af::log::error(af::log_level_high,
          "Dataset %s not saved: check permissions", ds);
      }
    }
    else {
      af::log::info(af::log_level_low, "Dataset %s not modified", ds);
    }

    if (vars.notif) {
      const TFileCollection *fc = dsm.get_fc();
      const char *tree_name = dsm.get_default_tree();
      int n_events;
      long long total_size = fc->GetTotalSize();

      if (tree_name) {
        n_events = fc->GetTotalEntries(tree_name);
        if (n_events < 0) n_events = 0;
      }
      else {
        tree_name = "";
        n_events = 0;
      }

      if (total_size < 0LL) total_size = 0LL;

      vars.notif->dataset(
        ds,                             // const char *ds_name
        (int)fc->GetNFiles(),           // int n_files
        (int)fc->GetNStagedFiles(),     // int n_taged
        (int)fc->GetNCorruptFiles(),    // int n_corrupted
        (char *)tree_name,              // const char *tree_name
        n_events,                       // int n_events
        (unsigned long long)total_size  // unsigned ll total_size_bytes
      );

    }

    dsm.free_files();
    count_ds++;

  }  // end loop over datasets

  dsm.free_datasets();

  af::log::info(af::log_level_low, "Number of datasets processed: %u",
    count_ds);

  //
  // Clean up transfer queue
  //

  int n_flushed = opq.flush();
  af::log::ok(af::log_level_normal,
    "%d elements removed from the transfer queue", n_flushed);

}

/** The main loop. The loop breaks when the external variable quit_requested is
 *  set to true.
 */
void main_loop(af::config &config) {

  // Resources monitoring facility
  af::resMon resmon;

  // The dataset manager wrapper, used by process_datasets()
  af::dataSetList dsm;
  std::string dsm_url;
  std::string dsm_mss;
  std::string dsm_opt;
  void *dsm_cbk_args[] = { &dsm, &dsm_url, &dsm_mss, &dsm_opt };

  // The operations queue, used by process_transfer_queue() and
  // process_datasets()
  af::opQueue opq;

  // The staging queue, used by process_transfer_queue() only
  std::list<af::extCmd *> cmdq;

  // Variables in configuration files in a handy struct
  afdsmgrd_vars_t vars;
  vars.sleep_secs = 0;
  vars.scan_ds_every_loops = 0;
  vars.max_concurrent_xfrs = 0;
  vars.max_stage_retries = 0;
  vars.notif = NULL;

  // Variables for notify plugin loader/unloader (through callback)
  void *notif_cbk_args[] = { &vars.notif, &config };

  // Bind directives to either variables or special callbacks
  config.bind_callback("xpd.datasetsrc", &config_callback_datasetsrc,
    dsm_cbk_args);
  config.bind_int("dsmgrd.sleepsecs", &vars.sleep_secs, 30, 5, AF_INT_MAX);
  config.bind_int("dsmgrd.scandseveryloops", &vars.scan_ds_every_loops, 10, 1,
    AF_INT_MAX);
  config.bind_int("dsmgrd.parallelxfrs", &vars.max_concurrent_xfrs, 8, 1, 1000);
  config.bind_text("dsmgrd.stagecmd", &vars.stage_cmd, "/bin/false");
  config.bind_int("dsmgrd.corruptafterfails", &vars.max_stage_retries, 0, 0,
    1000);
  config.bind_callback("dsmgrd.urlregex", &config_callback_urlregex,
    &vars.url_regex);
  config.bind_callback("dsmgrd.notifyplugin", &config_callback_notify,
    notif_cbk_args);

  // The loop counter
  long count_loops = -1;

  // The actual loop
  while (!quit_requested) {

    af::log::info(af::log_level_low, "*** Inside main loop ***");

    //
    // Check and load updates from the configuration file
    //

    if (config.update()) {
      af::log::info(af::log_level_high, "Config file modified");
    }
    else af::log::info(af::log_level_low, "Config file unmodified");

    //
    // Loop counter: we do not use MOD operator to take into account config
    // file modifications of directive dsmgrd.scandseveryloops
    //

    count_loops++;
    if (count_loops >= vars.scan_ds_every_loops) count_loops = 0;
    af::log::info(af::log_level_debug, "Iteration: %ld/%ld",
      count_loops, vars.scan_ds_every_loops);

    //
    // Transfer queue
    //

    process_transfer_queue(opq, cmdq, vars);

    //
    // Process datasets (every X loops)
    //

    if (count_loops == 0) {
      process_datasets(opq, dsm, vars);
    }
    else {
      int diff_loops = vars.scan_ds_every_loops - count_loops;
      if (diff_loops == 1) {
        af::log::info(af::log_level_low, "Not processing datasets now: they "
          "will be processed in the next loop");
      }
      else {
        af::log::info(af::log_level_low, "Not processing datasets now: "
          "%d loops remaining before processing", diff_loops);
      }
    }

    //
    // Report resources and sleep until next loop
    //

    af::res_timing_t &rtd = resmon.get_delta_timing();
    af::res_timing_t &rtc = resmon.get_cumul_timing();
    af::res_mem_t    &rm  = resmon.get_mem_usage();

    if ((rm.virt_kib != 0) && (rtd.user_sec >= 0.) && (rtc.user_sec >= 0.)) {

      float pcpu_delta = rtd.user_sec / rtd.real_sec;
      float pcpu_avg   = rtc.user_sec / rtc.real_sec;

      af::log::info(af::log_level_normal,
        "Usage statistics: uptime: %.0f s, CPU: %.2f%%, avg CPU: %.2f%%, "
        "virt: %lu KiB, rss: %lu KiB",
        rtc.real_sec, pcpu_delta, pcpu_avg,
        rm.virt_kib, rm.rss_kib);

      if (vars.notif) {
        vars.notif->resources(
          rm.rss_kib, rm.virt_kib,
          rtc.real_sec, rtc.user_sec, rtc.sys_sec,
          rtd.real_sec, rtd.user_sec, rtd.sys_sec
        );
        vars.notif->commit();
      }
    }
    else {
      af::log::error(af::log_level_normal,
        "Can't fetch daemon resources usage");
    }

    af::log::info(af::log_level_low, "Sleeping %ld seconds", vars.sleep_secs);
    sleep(vars.sleep_secs);

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
  // b fork to background
  // p <pidfile>
  // d <debug|low|normal|high|urgent> --> log level
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
    if (strcmp(log_level, "debug") == 0)
      log->set_level(af::log_level_debug);
    else if (strcmp(log_level, "low") == 0)
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
