#include <regex.h>
#include <string.h>
#include <dirent.h>
#include <stdlib.h>

#include "AfDataSetManager.h"
#include "AfDataSetSrc.h"
#include "AfLog.h"

AfDataSetManager::AfDataSetManager() {}

AfDataSetManager::~AfDataSetManager() {
  //AfLogInfo("# of datasets: %d", (int)ds.size());
  vector<AfDataSetSrc *>::iterator i;
  for (i=fSrcList.begin(); i!=fSrcList.end(); i++) {
    //AfLogInfo(">> Deleting dataset with src: %s", (*i)->getDsUrl().c_str());
    delete *i;
  }
}

bool AfDataSetManager::readCf(const char *cf) {

  AfConfReader *cfr = AfConfReader::open(cf);

  //cfr->printVarsAndDirs();

  if (!cfr) {
    AfLogFatal("Cannot read configuration file: %s", cf);
    return false;
  }

  regmatch_t reMatch[3];

  // Loop pause
  const char *loopSleep_s = cfr->getDir("dsmgrd.sleepsecs");
  if (!loopSleep_s) {
    AfLogWarning("Variable dsmgrd.sleepsecs not set, using default (%d)",
      fDefaultLoopSleep_s);
    fLoopSleep_s = fDefaultLoopSleep_s;
  }
  else if ( (fLoopSleep_s = atoi(loopSleep_s)) <= 0 )  {
    AfLogWarning("Invalid value for dsmgrd.sleepsecs (%s), using default (%d)",
      loopSleep_s, fDefaultLoopSleep_s);
    fLoopSleep_s = fDefaultLoopSleep_s;
  }
  else {
    AfLogInfo("Sleep between scans set to %d seconds", fLoopSleep_s);
  }

  // Parse redirector (which is supposed to be the same hostname as all.manager)

  regex_t reAdm;
  regcomp(&reAdm, "^[^ \t:]+", REG_EXTENDED);

  const char *redir = cfr->getDir("all.manager");

  if ((redir) && (regexec(&reAdm, redir, 1, reMatch, 0) == 0)) {
    int len = reMatch->rm_eo - reMatch->rm_so;
    char *buf = new char[len+1];
    memcpy(buf, &redir[ reMatch->rm_so ], len);
    buf[len] = '\0';
    fRedirHost = buf;
    delete[] buf;
  }
  else {
    AfLogWarning("No redirector found (scanning for all.manager directive), " \
      "using localhost as default");
    fRedirHost = "localhost";
  }

  regfree(&reAdm);

  fRedirPort = 1095;  // TODO: read from configuration file

  AfLogInfo("Redirector: root://%s:%d", fRedirHost.c_str(), fRedirPort);

  // Parse dataset sources

  regex_t reMss;  // regex that matches mss:
  regex_t reUrl;  // regex that matches url:
  regex_t reOpt;  // regex that matches opt:
  regex_t reRw;   // regex that matches rw=1

  regcomp(&reMss, "([ \t]|^)mss:([^ \t]*)", REG_EXTENDED);
  regcomp(&reUrl, "([ \t]|^)url:([^ \t]*)", REG_EXTENDED);
  regcomp(&reOpt, "([ \t]|^)opt:([^ \t]*)", REG_EXTENDED);
  regcomp(&reRw,  "([ \t]|^)rw=1([^ \t]*)", REG_EXTENDED);

  vector<const char *> *dsSrc = cfr->getDirs("xpd.datasetsrc");
  vector<const char *>::iterator i;

  for (i=dsSrc->begin(); i!=dsSrc->end(); i++) {

    AfLogInfo("Found dataset configuration: %s", *i);

    const char *dir = *i;
    bool dsValid = true;
    string mssUrl;
    string dsUrl;
    string opts;
    bool rw = false;

    if ( regexec(&reMss, dir, 3, reMatch, 0) == 0 ) {
      int len = reMatch[2].rm_eo - reMatch[2].rm_so;
      char *buf = new char[ len+1 ];
      memcpy(buf, &dir[reMatch[2].rm_so], len);
      buf[len] = '\0';
      mssUrl = buf;
      delete[] buf;
      AfLogInfo(">> MSS: %s", mssUrl.c_str());
    }
    else {
      AfLogError(">> MSS URL not set");
      dsValid = false;
    }

    if ( regexec(&reUrl, dir, 3, reMatch, 0) == 0 ) {
      int len = reMatch[2].rm_eo - reMatch[2].rm_so;
      char *buf = new char[ len+1 ];
      memcpy(buf, &dir[reMatch[2].rm_so], len);
      buf[len] = '\0';
      dsUrl = buf;
      delete[] buf;
      AfLogInfo(">> URL: %s", dsUrl.c_str());
    }
    else {
      AfLogError(">> Dataset local path not set");
      dsValid = false;
    }

    if ( regexec(&reRw, dir, 0, NULL, 0) == 0 ) {
      rw = true;
    }

    if ( regexec(&reOpt, dir, 3, reMatch, 0) == 0 ) {
      int len = reMatch[2].rm_eo - reMatch[2].rm_so;
      char *buf = new char[ len+1 ];
      memcpy(buf, &dir[reMatch[2].rm_so], len);
      buf[len] = '\0';
      opts = buf;
      delete[] buf;
      AfLogInfo(">> Opt: %s", opts.c_str());
    }
    else {
      // Default options: do not allow register and verify if readonly
      opts = rw ? "Ar:Av:" : "-Ar:-Av:";
    }

    if (dsValid) {
      AfDataSetSrc *dsSrc = new AfDataSetSrc(dsUrl, mssUrl, opts, fSuid,
        fRedirHost, fRedirPort);
      fSrcList.push_back(dsSrc);
    }
    else {
      AfLogError(">> Invalid dataset configuration, ignoring", *i);
    }

  }

  regfree(&reUrl);
  regfree(&reMss);
  regfree(&reOpt);

  delete cfr;

  if (fSrcList.size() == 0) {
    AfLogFatal("No valid dataset source found!");
    return false;
  }

  return true;
}

void AfDataSetManager::loop(unsigned int nLoops) {

  vector<AfDataSetSrc *>::iterator i;
  unsigned int countLoops = 0;

  while (true) {
    AfLogInfo("++++ Started processing loop over all dataset sources ++++");

    for (i=fSrcList.begin(); i!=fSrcList.end(); i++) {
      (*i)->process(fReset);
    }

    AfLogInfo("++++ Loop %d completed ++++", countLoops++);

    if ((nLoops > 0) && (countLoops == nLoops)) {
      break;
    }

    AfLogInfo("Sleeping %d seconds before new loop", fLoopSleep_s),

    sleep(fLoopSleep_s);
  }
}
