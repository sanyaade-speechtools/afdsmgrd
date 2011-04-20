/**
 * verifier.cc -- by Dario Berzano <dario.berzano@cern.ch>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * Program to verify the endpoint URL of staged files and update it if needed.
 */

#include <stdio.h>
#include <unistd.h>
#include <libgen.h>
#include <signal.h>
#include <pwd.h>
#include <grp.h>

#include <fstream>
#include <memory>
#include <list>

#include "afLog.h"
#include "afConfig.h"
#include "afDataSetList.h"
#include "afRegex.h"
#include "afExtCmd.h"
#include "afOpQueue.h"
#include "afOptions.h"
#include "afResMon.h"
#include "afOptions.h"

#define AF_ERR_CONFIG 2
#define AF_ERR_LIBEXEC 15

/** Program name that goes in version banner.
 */
#define AF_PROG_NAME "afverifier"

/** Set of variables in configuration file.
 */
typedef struct {

  long sleep_secs;           // verifier.sleepsecs
  long scan_ds_every_loops;  // verifier.scandseveryloops
  long parallel_verifies;    // verifier.parallelverifies
  long max_failures;         // verifier.maxfailures
  std::string verify_cmd;    // verifier.verifycmd
  af::regex url_regex;       // verifier.urlregex

} verifier_vars_t;

/** Global variables.
 */
bool quit_requested = false;

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

/** All datasets are scanned for staged and uncorrupted files: for these
 *  entries:
 *
 *   - the originating URL is considered;
 *   - the redirector URL is regenerated;
 *   - the endpoint URL is eliminated.
 *
 * After that, the redirector's URL is put into the processing queue.
 */
void process_datasets_enqueue(af::opQueue &opq, af::dataSetList &dsm,
  verifier_vars_t &vars) {

  af::log::info(af::log_level_normal,
    "*** Scanning datasets for files to verify ***");

  const af::queueEntry *qent;
  const char *ds;
  dsm.fetch_datasets();
  unsigned int count_ds = 0;

  while (ds = dsm.next_dataset()) {

    af::log::info(af::log_level_low, "Scanning dataset %s", ds);

    TFileInfo *fi;
    dsm.fetch_files(NULL, "Sc");  // Sc == staged AND not corrupted
    int count_changes = 0;
    int count_files = 0;

    while (fi = dsm.next_file()) {

      count_files++;

      // Originating URL is the last one; redirector URL is the last but one
      TUrl *orig_url = dsm.get_url(-1);
      TUrl *redir_url = dsm.get_url(-2);

      if (!orig_url) continue;  // no URLs in entry (should not happen)

      const char *inp_url = orig_url->GetUrl();
      const char *out_url = vars.url_regex.subst(inp_url);

      if (!out_url) continue;
      // orig_url not matched regex (i.e., originating URL is unsupported)

      //
      // Redirector's URL is re-added
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

      //
      // Enqueue redirector's URL (cond_insert treats duplicates kindly)
      //

      unsigned int unique_id;
      qent = opq.cond_insert(out_url, NULL, &unique_id);
        // NULL value for get_default_tree() is accepted

      if (!qent) {
        // URL is not yet in queue: cond_insert() has already appended it
        af::log::ok(af::log_level_low, "Queued: %s (id=%u)", out_url,
          unique_id);
      }
      else {
        // URL is not yet in queue: cond_insert() has already appended it
        af::log::info(af::log_level_low, "Already queued: %s", out_url);
      }

    } // end loop over dataset entries (TFileInfos)

    //
    // Save only if URLs were translated or added
    //

    if (count_changes > 0) {
      bool save_ok = dsm.save_dataset();  // no toggle_suid here
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

    dsm.free_files();
    count_ds++;

  }  // end loop over datasets (TFileCollections)

  dsm.free_datasets();

  af::log::info(af::log_level_low, "Number of datasets scanned: %u",
    count_ds);

}

/** Operations queue is processed: check if slots are freed, then insert
 *  elements from opq in free slots of cmdq. Handle successes and failures by
 *  syncing info between cmdq and opq
 */
void process_opqueue(af::opQueue &opq, std::list<af::extCmd *> &cmdq,
  verifier_vars_t &vars) {

  const af::queueEntry *qent;

  // Variables to substitute in stage command
  static af::varmap_t extcmd_vars;
  if (extcmd_vars.empty()) {
    extcmd_vars.insert( af::varpair_t("REDIRURL", "") );
    extcmd_vars.insert( af::varpair_t("TREENAME", "") );
  }

  af::log::info(af::log_level_normal, "*** Processing operations queue ***");

  opq.set_max_failures((unsigned int)vars.max_failures);

  //
  // Query on "running" to update their status if needed
  //

  opq.init_query_by_status(af::qstat_running);
  while ( qent = opq.next_query_by_status() ) {

    af::log::info(af::log_level_debug, "Searching in command queue for uiid=%u",
      qent->get_instance_id());

    // Check status in operations queue
    for (std::list<af::extCmd *>::iterator it=cmdq.begin();
      it!=cmdq.end(); it++) {

      if ( (*it)->get_id() == qent->get_instance_id() ) {

        af::log::ok(af::log_level_debug, "Found uuid=%u in command queue",
          qent->get_instance_id());

        if ((*it)->is_running()) {
          af::log::info(af::log_level_debug, "Still processing: %s (uiid=%u)",
            qent->get_main_url(), qent->get_instance_id());
          break;
        }

        // Processing (download, stage, verify...) has finished

        (*it)->get_output();

        if ( (*it)->is_ok() ) {
          
          //
          // Operation OK
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
          // Operation failed
          //

          // Get the reason: if the reason is *explicitly* not_staged, then the
          // file will be marked as not staged in processing datasets. File is
          // staged by default.
          const char *reason = (*it)->get_field_text("Reason");
          bool staged = true;
          if ((reason) && (strcmp(reason, "not_staged") == 0)) staged = false;

          // External command reported a failure: this could mean, during
          // verification, that either the file is not staged or another error
          // occured
          if (staged) { 
            af::log::error(af::log_level_high, "Failed: %s",
              qent->get_main_url());
          }
          else {
            af::log::warning(af::log_level_high, "Not staged: %s",
              qent->get_main_url());
          }

          opq.failed(qent->get_main_url(), staged);

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
  // Query on "queued", limited to the number of free processing slots
  //

  int free_cmd_slots = vars.parallel_verifies - cmdq.size();
  af::log::info(af::log_level_debug, "Operation slots free: %d",
    free_cmd_slots);

  if (free_cmd_slots > 0) {

    opq.init_query_by_status(af::qstat_queue, free_cmd_slots);

    while ( qent = opq.next_query_by_status() ) {

      // Prepare command

      af::varmap_iter_t it;

      it = extcmd_vars.find("REDIRURL");
      it->second = qent->get_main_url();  // it is the translated (redir) one

      it = extcmd_vars.find("TREENAME");
      const char *def_tree = qent->get_tree_name();
      it->second = def_tree ? def_tree : "";

      std::string url_cmd = af::regex::dollar_subst(vars.verify_cmd.c_str(),
        extcmd_vars);

      af::log::info(af::log_level_debug, "Preparing operation command: %s",
        url_cmd.c_str());

      // Launch command

      af::extCmd *ext_op_cmd = new af::extCmd(url_cmd.c_str(),
        qent->get_instance_id());
      int r = ext_op_cmd->run();
      if (r == 0) {

        // Command started successfully
        af::log::ok(af::log_level_normal, "Operation started: %s (uiid=%u)",
          qent->get_main_url(), qent->get_instance_id());

        // Turn status to "running"
        opq.set_status(qent->get_main_url(), af::qstat_running);

        // Set timeout of 1000 seconds on each command (TODO)
        ext_op_cmd->set_timeout_secs(1000);

        // Enqueue in command queue
        cmdq.push_back(ext_op_cmd);

      }
      else {
        af::log::error(af::log_level_high, "Error running external command, "
          "wrapper returned %d: check permissions on %s. Command issued: %s",
          af::extCmd::get_temp_path(), vars.verify_cmd.c_str());
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
    "Queued: %u | Processing: %u | Success: %u | Failed: %u",
    n_total, n_queued, n_runn, n_success, n_fail);

  //af::log::info(af::log_level_normal, "========== Begin Of Queue ==========");
  //opq.dump(true);
  //af::log::info(af::log_level_normal, "========== End Of Queue ==========");

}

/** Scan over all datasets handled by the dataset manager wrapper dsm to save
 *  back finished elements from opq
 */
void process_datasets_save(af::opQueue &opq, af::dataSetList &dsm,
  verifier_vars_t &vars) {

  const af::queueEntry *qent;

  af::log::info(af::log_level_normal,
    "*** Saving entries back on datasets ***");

  const char *ds;
  dsm.fetch_datasets();
  unsigned int count_ds = 0;

  while (ds = dsm.next_dataset()) {

    af::log::info(af::log_level_low, "Scanning dataset %s", ds);

    TFileInfo *fi;
    dsm.fetch_files(NULL, "Sc");  // Sc == staged AND not corrupted
    int count_changes = 0;
    int count_files = 0;

    while (fi = dsm.next_file()) {

      // We count the retrieved files
      count_files++;

      // Originating URL is the last one; redirector URL is the last but one
      TUrl *orig_url = dsm.get_url(-1);

      if (!orig_url) continue;  // no URLs in entry (should not happen)

      const char *inp_url = orig_url->GetUrl();
      const char *out_url = vars.url_regex.subst(inp_url);

      if (!out_url) continue;
      // orig_url not matched regex (i.e., originating URL is unsupported)

      //
      // At this point, we have redir URL on out_url
      //

      qent = opq.get_cond_entry(out_url);

      if (qent) {

        const char *endp_url = qent->get_endp_url();

        if ((qent->get_status() == af::qstat_success) && (endp_url)) {

          //
          // OK reported --> file correctly staged; if we used extended
          // verification, we can also update metadata.
          //

          bool meta_upd = false;

          // Staged and Not Corrupted
          fi->SetBit( TFileInfo::kStaged );
          fi->ResetBit( TFileInfo::kCorrupted );

          if (!fi->AddUrl(qent->get_endp_url(), kTRUE)) {
            af::log::warning(af::log_level_debug, "In dataset %s, "
              "endpoint URL %s is a duplicate", ds, endp_url);
          }

          // Update file size (only if meaningful)
          if (qent->get_size_bytes() > 0) {
            fi->SetSize(qent->get_size_bytes());
          }

          // Update tree name and events (only if meaningful)
          if ((qent->get_tree_name()) && (qent->get_n_events() > 0)) {

            meta_upd = true;

            // All metadata is first removed...
            TList *mdl = fi->GetMetaDataList();
            if ((mdl) && (mdl->GetEntries() > 0)) fi->RemoveMetaData();

            // ...then new metadata is added
            TFileInfoMeta *meta = new TFileInfoMeta(qent->get_tree_name());
            meta->SetEntries(qent->get_n_events());
            fi->AddMetaData(meta);

          }

          af::log::ok(af::log_level_normal,
            "File %s is staged as %s (metadata updated: %s)",
            out_url, endp_url, (meta_upd ? "yes" : "no"));

          count_changes++;

        }  // end if success
        else if (qent->get_status() == af::qstat_failed) {

          //
          // FAIL reported --> file not staged (Reason: not_staged) or another
          // error
          //

          // File is always staged when FAIL occurs, except when Reason is
          // not_staged: in this very case we change the staged and the
          // corrupted bit, elsewhere we don't touch anything
          if (!qent->is_staged()) {
            // Reason: not_staged
            // Not Staged and Corrupted --> why?
            // Because we have the Real Status (not staged) but we don't trigger
            // restaging through daemon (corrupted)
            fi->ResetBit( TFileInfo::kStaged );
            fi->SetBit( TFileInfo::kCorrupted );

            af::log::warning(af::log_level_normal, "File %s is not staged: "
              "marked as not staged and corrupted", out_url);

            count_changes++;

          }
          else {
            // File is not staged: mark as unstaged
            af::log::error(af::log_level_normal, "File %s is not verifiable: "
              "staged and corrupted bits not touched", out_url);
          }

        }  // end if failed

      } // end if qent

    }  // end loop over dataset entries (TFileInfos)

    //
    // Save only if needed
    //

    if (count_changes > 0) {
      bool save_ok = dsm.save_dataset();  // no toggle_suid here
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

    dsm.free_files();
    count_ds++;

  }  // end loop over datasets

  dsm.free_datasets();

  af::log::info(af::log_level_low, "Number of datasets processed: %u",
    count_ds);

  //
  // Clean up operations queue
  //

  int n_flushed = opq.flush();
  af::log::ok(af::log_level_normal,
    "%d elements removed from the operations queue", n_flushed);

}

/** The main loop. The loop breaks when the external variable quit_requested is
 *  set to true.
 */
void main_loop(af::config &config) {

  // Resources monitoring
  af::resMon resmon;

  // Variables in configuration file
  verifier_vars_t vars;

  // The dataset manager wrapper, used by process_datasets_*()
  af::dataSetList dsm;
  std::string dsm_url;
  std::string dsm_mss;
  std::string dsm_opt;
  void *dsm_cbk_args[] = { &dsm, &dsm_url, &dsm_mss, &dsm_opt };

  // The operations queue, used by process_opqueue() and process_datasets_*()
  af::opQueue opq;

  // The staging queue, used by process_opqueue() only
  std::list<af::extCmd *> cmdq;

  // Bind directives to either variables or special callbacks
  config.bind_callback("xpd.datasetsrc", &config_callback_datasetsrc,
    dsm_cbk_args);
  config.bind_callback("verifier.urlregex", &config_callback_urlregex,
    &vars.url_regex);
  config.bind_int("verifier.sleepsecs", &vars.sleep_secs, 30, 5, AF_INT_MAX);
  config.bind_int("verifier.scandseveryloops", &vars.scan_ds_every_loops, 10, 1,
    AF_INT_MAX);
  config.bind_int("verifier.parallelverifies", &vars.parallel_verifies,
    8, 1, 1000);
  config.bind_text("verifier.verifycmd", &vars.verify_cmd, "/bin/false");
  config.bind_int("verifier.maxfailures", &vars.max_failures, 0, 0,
    1000);

  //
  // Put files in queue, reading them from the datasets
  //

  // Load configuration at first place
  config.update();

  // Put files in queue
  process_datasets_enqueue(opq, dsm, vars);

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
    // Operations queue
    //

    process_opqueue(opq, cmdq, vars);

    //
    // Process datasets (every X loops)
    //

    if (count_loops == 0) {
      process_datasets_save(opq, dsm, vars);
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
    // Report resources
    //

    af::res_timing_t &rtd = resmon.get_delta_timing();
    af::res_timing_t &rtc = resmon.get_cumul_timing();
    af::res_mem_t    &rm  = resmon.get_mem_usage();

    if ((rm.virt_kib != 0) && (rtd.user_sec >= 0.) && (rtc.user_sec >= 0.)) {

      double pcpu_delta = 100. * rtd.user_sec / rtd.real_sec;
      double pcpu_avg   = 100. * rtc.user_sec / rtc.real_sec;

      af::log::info(af::log_level_normal,
        "Usage statistics: uptime: %.1lf s, CPU: %.1lf%%, avg CPU: %.1lf%%, "
        "virt: %lu KiB, rss: %lu KiB",
        rtc.real_sec, pcpu_delta, pcpu_avg,
        rm.virt_kib, rm.rss_kib);

    }
    else {
      af::log::error(af::log_level_normal,
        "Can't fetch daemon resources usage");
    }

    //
    // End of loop: do we still have something to do? Check queue...
    //

    unsigned int n_queued, n_runn, n_success, n_fail, n_total;
    opq.summary(n_queued, n_runn, n_success, n_fail);
    n_total = n_queued + n_runn + n_success + n_fail;

    if (n_total == 0) {
      af::log::ok(af::log_level_urgent,
        "Every operation has completed, let's quit");
      quit_requested = true;
    }
    else {
      af::log::info(af::log_level_low, "Sleeping %ld seconds", vars.sleep_secs);
      sleep(vars.sleep_secs);
    }

  }  // while

  //
  // Report final statistics
  //

  af::res_timing_t &rtc = resmon.get_cumul_timing();

  if (rtc.user_sec >= 0.) {
    double pcpu_avg   = 100. * rtc.user_sec / rtc.real_sec;
    af::log::info(af::log_level_high,
      "Completed in %.1lf s (average CPU: %.1lf%%)", rtc.real_sec, pcpu_avg);
  }

}

/** Entry point of the verifier
 */
int main(int argc, char *argv[]) {

  int c;
  const char *config_file = NULL;
  const char *libexec_path = NULL;

  opterr = 0;

  // c <config>
  // e <libexec_path>
  while ((c = getopt(argc, argv, ":c:e:")) != -1) {

    switch (c) {
      case 'c': config_file = optarg; break;
      case 'e': libexec_path = optarg; break;
    }

  }

  std::string banner = AF_VERSION_BANNER;
  std::auto_ptr<af::log> global_log(
    new af::log(std::cout, af::log_level_low, banner) );

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

  // Configuration file is mandatory
  if (!config_file) {
    af::log::fatal(af::log_level_urgent, "No config file given (use -c)");
    return AF_ERR_CONFIG;
  }

  // Warning if not running as root
  else if (geteuid() != 0) {
    struct passwd *pwd = getpwuid(geteuid());
    af::log::warning(af::log_level_urgent,
      "Running as unprivileged user \"%s\": this may prevent dataset writing",
      pwd->pw_name);
  }

  af::config config(config_file);

  // Init external command facility paths
  {
    std::string exec_wrapper_path = libexec_path;
    exec_wrapper_path += "/afdsmgrd-exec-wrapper";
    af::extCmd::set_helper_path(exec_wrapper_path.c_str());

    //std::string extcmd_temp_path = "/tmp/afdsmgrd-" + pid;
    //af::extCmd::set_temp_path(extcmd_temp_path.c_str());
    af::extCmd::set_temp_path("/tmp/afverifier");
  }

  // Trap some signals to terminate gently
  signal(SIGTERM, signal_quit_callback);
  signal(SIGINT, signal_quit_callback);

  main_loop(config);

  return 0;

}
