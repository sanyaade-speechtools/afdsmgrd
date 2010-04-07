#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pwd.h>
#include <grp.h>

#include "AfLog.h"
#include "AfConfReader.h"
#include "AfDataSetManager.h"

#include <TError.h>

FILE *logFp;

int main(int argc, char *argv[]) {

  int c;

  opterr = 0;  // getopt lib: do not show standard errors

  logFp = stderr;

  char *logFile = NULL;
  char *confFile = NULL;
  char *dropUser = NULL;
  char *dropGroup = NULL;
  bool bkg = false;
  bool runOnce = false;
  bool resetDs = false;
  bool showRootMsg = false;

  uid_t uidDrop = 0;
  gid_t gidDrop = 0;

  while ((c = getopt(argc, argv, "bl:c:R:ort")) != -1) {
    switch (c) {
      case 'b':
        bkg = true;
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
        resetDs = true;
        runOnce = true;  // implied
      break;

      case 't':
        showRootMsg = true;
      break;

      case 'o':
        runOnce = true;
      break;

      case '?':
        switch (optopt) {
          case 'l':
          case 'c':
          case 'R':
            AfLogFatal("Option '-%c' requires an argument", optopt);
            exit(1);
          break;

          default:
            AfLogFatal("Unknown option: '-%c'", optopt);
            exit(1);
          break;
        }
      break;
    } // switch
  } // while

  // Check if filenames are absolute
  if ((logFile) && (*logFile != '/')) {
    AfLogFatal("Log file name must be an absolute path");
    exit(1);
  }

  // Open logfile just to check if it can be opened
  if (logFile) {
    FILE *tmpLogFp = fopen(logFile, "a");
    if (!logFp) {
      AfLogFatal("Can't open logfile \"%s\" for writing", logFile);
      exit(1);
    }
    else {
      fclose(tmpLogFp);
    }
  }
  else if (bkg) {
    AfLogFatal("Logfile is compulsory if daemonizing");
    exit(1);
  }

  // Eventually forking
  if (bkg) {
    pid_t pid = fork();

    if (pid < 0) { // Cannot fork
      exit(1);
    }

    if (pid > 0) { // This is the parent process, and pid is the child's pid
      exit(0);
    }

    pid_t sid = setsid();
    if (sid < 0) {
      exit(1);
    }

    //umask(0);
    chdir("/");

    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }

  // Files should be opened after fork() (because terminating the parent
  // would close the fd of the child too)
  if (logFile) {
    logFp = fopen(logFile, "a");
    if (!logFp) {
      exit(2);  // no error message can be seen at this point, useless to print
    }
  }

  // After having the logfile, we should check the rest!

  // Configuration file checks
  if (!confFile) {
    AfLogFatal("Configuration file is compulsory");
    exit(1);
  }
  else if (*confFile != '/') {
    AfLogFatal("Configuration file name must be an absolute path");
    exit(1);
  }

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
        exit(1);
      }
    }
    else {
      AfLogFatal("Can't become user %s because it does not exist", dropUser);
      exit(1);
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

  AfDataSetManager *dsm = new AfDataSetManager();

  if (dropUser) {
    dsm->setSuid(true);
  }

  if (resetDs) {
    dsm->setResetDataSets(true);
  }

  if ( !dsm->readCf(confFile) ) {
    //AfLogFatal("Cannot read configuration file: %s", confFile);
    delete dsm;
    return 1;
    //exit(1);
  }

  if (runOnce) {
    dsm->loop(1);
  }
  else {
    dsm->loop();
  }

  delete dsm;

  return 0;
}
