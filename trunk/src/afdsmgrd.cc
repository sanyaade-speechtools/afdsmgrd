/* Exit codes
 * 
 * Ok          :  0
 * ----------------
 * Arguments   : 1?
 * Files       : 2?
 * Fork        : 3?
 * Permissions : 4?
 * Suid        : 5?
 *
 */

// Standard includes
#include <cstdlib>
#include <pwd.h>
#include <grp.h>

// This project's includes
#include "AfLog.h"
#include "AfConfReader.h"
#include "AfDataSetsManager.h"

// ROOT includes
#include <TSystem.h>
#include <TString.h>
#include <TError.h>

AfLog *gLog = NULL;

int main(int argc, char *argv[]) {

  Int_t c;

  opterr = 0;  // getopt lib: do not show standard errors

  AfLog::Init();

  TString logFile;
  TString confFile;
  char *dropUser = NULL;
  char *dropGroup = NULL;
  bool bkg = kFALSE;
  bool resetDs = kFALSE;
  bool showRootMsg = kFALSE;
  bool debugMsg = kFALSE;

  while ((c = getopt(argc, argv, "bl:c:R:ortd")) != -1) {
    switch (c) {
      case 'b':
        bkg = kTRUE;
      break;

      case 'c':
        confFile = optarg;
      break;

      case 'l':
        logFile = optarg;
      break;

      case 'R':
        dropUser = optarg;
      break;

      case 'r':
        resetDs = kTRUE;
      break;

      case 't':
        showRootMsg = kTRUE;
      break;

      case 'd':
        debugMsg = kTRUE;
      break;

      case '?':
        switch (optopt) {
          case 'l':
          case 'c':
          case 'R':
            AfLogFatal("Option '-%c' requires an argument", optopt);
            return 11;
          break;

          default:
            AfLogFatal("Unknown option: '-%c'", optopt);
            return 12;
          break;
        }
      break;
    } // switch
  } // while

  // Show debug messages?
  gLog->SetDebug(debugMsg);

  // Check if log filename is absolute
  if ((!logFile.IsNull()) && (!logFile.BeginsWith("/"))) {
    logFile = Form("%s/%s", gSystem->WorkingDirectory(), logFile.Data());
  }

  // Open logfile just to check if it can be opened
  if (!logFile.IsNull()) {
    /*FILE *tmpLogFp = fopen(logFile, "a");
    if (!tmpLogFp) {
      AfLogFatal("Can't open logfile \"%s\" for writing", logFile);
      exit(1);
    }
    else {
      fclose(tmpLogFp);
    }*/ // TODO
  }
  else if (bkg) {
    AfLogFatal("Logfile is compulsory if daemonizing");
    return 13;
  }

  // Eventually forking
  if (bkg) {
    pid_t pid = fork();

    if (pid < 0) { // Cannot fork
      gSystem->Exit(31);
    }

    if (pid > 0) { // This is the parent process, and pid is the child's pid
      gSystem->Exit(0);
    }

    pid_t sid = setsid();
    if (sid < 0) {
      gSystem->Exit(32);
    }

    //umask(0);
    chdir("/");

    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }

  // Files should be opened after fork() (because terminating the parent
  // would close the fd of the child too)
  if (!logFile.IsNull()) {
    if (!gLog->SetFile(logFile.Data())) {
      return 21;  // no error message can be seen at this point, useless to print
    }
  }

  // After having the logfile, we should check the rest!

  // Configuration file checks
  if (confFile.IsNull()) {
    AfLogFatal("Configuration file is compulsory");
    return 14;
  }
  else if (!confFile.BeginsWith("/")) {
    confFile = Form("%s/%s", gSystem->WorkingDirectory(), confFile.Data());
    char *buf = realpath(confFile.Data(), NULL);
    if (buf) {
      confFile = buf;
      delete[] buf;
    }
    else {
      AfLogWarning("Cannot get real path for %s", confFile.Data());
    }
  }

  // From this point on, configuration file and logfile are set

  AfLogOk("##### afdsmgrd rev. %d started its operations #####", SVNREV);

  // Drop current effective user: it can revert back to the former at any time,
  // and only when needed
  if (dropUser) {
    struct passwd *p = getpwnam(dropUser);
    if (p) {
      if ((setegid(p->pw_gid) != -1) && (seteuid(p->pw_uid) != -1)) {
          struct group *g = getgrgid(p->pw_gid);
          AfLogOk("Dropped user privileges: now running as user %s (%d), " \
            "group %s (%d)", dropUser, p->pw_uid, g->gr_name, p->pw_gid);
      }
      else {
        AfLogFatal("Can't drop privileges to user %s", dropUser);
        return 41;
      }
    }
    else {
      AfLogFatal("Can't become user %s because it does not exist", dropUser);
      return 42;
    }
  }
  else if (geteuid() == 0) {
    // We are root
    AfLogWarning("Running as user root: this is potentially dangerous!");
  }
  else {
    // We are not root
    AfLogWarning("Running as an unprivileged user: this may impair " \
      "dataset writing");
  }

  // ROOT messages above the threshold are shown
  if (showRootMsg) {
    gErrorIgnoreLevel = 0;
  }
  else {
    gErrorIgnoreLevel = 10000;
  }

  //AfLogOk("If you are here, everything went fine");

  AfDataSetsManager *dsm = new AfDataSetsManager();

  if (dropUser) {
    dsm->SetSuid(kTRUE);
  }

  if ( !dsm->ReadConf(confFile.Data()) ) {
    delete dsm;
    return 22;
  }

  if (resetDs) {
    dsm->Reset();
  }
  else {
    dsm->Loop();
  }

  delete dsm;

  return 0;
}
