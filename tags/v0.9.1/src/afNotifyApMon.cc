/**
 * afNotifyApMon.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afNotifyApMon.h"

using namespace af;

/** Non-member function with C-style name binding (i.e., no mangling) to
 *  allow for classes in libraries through polymorphism.
 */
extern "C" notify *create(config &_cfg) {
  return new notifyApMon(_cfg);
}

/** Non-member destructor with C-style name binding. It is to be used instead of
 *  delete from the caller: the caller _has_ indeed the knowledge to walk
 *  through the destruction functions (because of virtual dtors), but when it's
 *  time to actually _free_ the memory (after dtors have been called), the
 *  caller has no effective knowledge on the size of the class! So, it is
 *  totally not sufficient to declare a virtual destructor: the destructor must
 *  be invoked with "delete" but from a function *within* this library!
 */
extern "C" void destroy(notify *notif) {
  delete notif;
}

/** Returns a string identifier for this plugin.
 */
const char *notifyApMon::whoami() {
  return "ApMon (MonALISA) notification plugin";
}

/** Default constructor.
 */
notifyApMon::notifyApMon(config &_cfg) : notify(_cfg) {
  printf("this is constructor -- we set config here\n");
}

/** Destructor.
 */
notifyApMon::~notifyApMon() {
  printf("this is destructor\n");
}

/** Initialization function: called right after plugin is loaded.
 */
void notifyApMon::init() {

  apmon = NULL;
  apmon_pool = malloc( sizeof(ApMon) );  // pool-allocating memory for ApMon

  apmon_params.port[0] = -1;
  apmon_params.host[0] = NULL; //(char *)0x2;
  apmon_params.pwd[0]  = NULL; //(char *)0x3;

  cbk_args[0] = &apmon;         // ApMon **
  cbk_args[1] = apmon_pool;     // void *
  cbk_args[2] = &apmon_params;  // apmon_params_t *

  printf("[ini] &apmon        = 0x%016lx\n", &apmon);
  printf("[ini] &apmon_pool   = 0x%016lx\n", apmon_pool);
  printf("[ini] &apmon_params = 0x%016lx\n", &apmon_params);
  printf("[ini]        ->port = 0x%016lx\n", apmon_params.port);
  printf("[ini]        ->host = 0x%016lx\n", apmon_params.host);
  printf("[ini]        ->pwd  = 0x%016lx\n", apmon_params.pwd);

  cfg.bind_callback("dsmgrd.apmonurl", notifyApMon::config_apmonurl_callback,
    cbk_args);
}

/** Notifies to MonALISA the presence of a dataset.
 */
void notifyApMon::dataset(const char *ds_name, int n_files, int n_staged,
  int n_corrupted, const char *tree_name, int n_events,
  unsigned long long total_size_bytes) {


}

/**
 */
void notifyApMon::config_apmonurl_callback(const char *dir_name,
  const char *dir_val, void *args) {

  void **args_array  = (void **)args;
  ApMon **apmon_ptr = (ApMon **)args_array[0];
  void *apmon_mempool = (void *)args_array[1];
  apmon_params_t *apmon_params_ptr = (apmon_params_t *)args_array[2];

  printf("[cbk] Callback invoked\n");
  printf("[cbk] name=%s\n", dir_name);
  printf("[cbk] val=%s\n", dir_val);

  printf("[cbk] apmon_ptr        = 0x%016lx --> 0x%016lx\n", apmon_ptr, *apmon_ptr);
  printf("[cbk] apmon_mempool    = 0x%016lx\n", apmon_mempool);
  printf("[cbk] apmon_params_ptr = 0x%016lx\n", apmon_params_ptr);
  printf("[cbk]           ->port = 0x%016lx\n", apmon_params_ptr->port);
  printf("[cbk]           ->host = 0x%016lx --> 0x%016lx\n", apmon_params_ptr->host, apmon_params_ptr->host[0]);
  printf("[cbk]           ->pwd  = 0x%016lx --> 0x%016lx\n", apmon_params_ptr->pwd, apmon_params_ptr->pwd[0]);

  //
  // Delete previous instance of ApMon, if there is one
  //

  printf("[cbk] apmon_ptr = %016lx\n", *apmon_ptr);
  if (*apmon_ptr) {
    (*apmon_ptr)->~ApMon();  // don't free, just destroy: it's a pool
    *apmon_ptr = NULL; 
  }

  if ( apmon_params_ptr->host[0] ) {
    free( apmon_params_ptr->host[0] );
    apmon_params_ptr->host[0] = NULL;
  }

  if ( apmon_params_ptr->pwd[0] ) {
    free( apmon_params_ptr->pwd[0] );
    apmon_params_ptr->pwd[0] = NULL;
  }

  apmon_params_ptr->port[0] = -1;

  //
  // URL parsing: if directive is null, exit here leaving with no valid ApMon
  //

  if (dir_val == NULL) return;

  const char *reurl_ptn =
    "^([a-z]+)://(([^:@]+)?(:([^:@]*))?@)?([^:/]+)(:([0-9]+))?(.*)$";
  string url = dir_val;

  regex_t *re_compd = new regex_t;
  regcomp(re_compd, reurl_ptn, REG_EXTENDED);

  // Parameters for ApMon
  std::string proto, pwd, host;
  int port = -1;

  const unsigned int nmatch = 10; // 9 + 1

  regmatch_t match[nmatch];
  if (regexec(re_compd, url.c_str(), nmatch, match, 0) == 0) {

    // URL is valid

    for (unsigned int i=0; i<nmatch; i++) {
      int s = (int)match[i].rm_so;
      int e = (int)match[i].rm_eo;
      if (s > -1) {
        std::string sub = url.substr(s, e-s);
        //printf("#%d : [%3d, %3d) : {%s}\n", i, s, e, sub.c_str());
        switch (i) {
          case 1: proto = sub; break;
          case 5: pwd   = sub; break;
          case 6: host  = sub; break;
          case 8:
            printf("[cbk] port is %s\n", sub.c_str());
            port = (int)strtol(sub.c_str(), NULL, 0);
          break;
        }
      }
    }

    //
    // Now we have proto, pwd, host and port: create a new ApMon instance
    //

    // Memory is allocated in pool and zeroed because ApMon is deeply bugged!
    printf("[cbk] Setting to zero %u bytes\n", sizeof(ApMon));
    memset(apmon_mempool, 0, sizeof(ApMon));

    if (proto == "apmon") {

      // Check port and assign default (8884) if needed
      if ((port <= 0) || (port > 65535)) port = 8884;

      // ApMon(int nDestinations, char **destAddresses, int *destPorts,
      //   char **destPasswds)
      printf("[cbk] protocol is apmon\n");
      printf("[cbk] pwd=%s host=%s port=%d\n", pwd.c_str(), host.c_str(),
        port);

      apmon_params_ptr->host[0] = strdup(host.c_str());
      apmon_params_ptr->pwd[0]  = strdup(pwd.c_str());
      apmon_params_ptr->port[0] = port;

      // In memory pool
      *apmon_ptr = new(apmon_mempool) ApMon(1,
        apmon_params_ptr->host,
        apmon_params_ptr->port,
        apmon_params_ptr->pwd);

    }
    else {
      // ApMon(char *initsource);
      printf("[cbk] protocol is any\n");
      printf("[cbk] init with url=%s\n", url.c_str());

      apmon_params_ptr->host[0] = strdup(url.c_str());

      // In memory pool
      *apmon_ptr = new(apmon_mempool) ApMon(1, apmon_params_ptr->host);
    }

    printf("[cbk] *apmon_ptr = %016lx, apmon_pool = %016lx\n",
      *apmon_ptr, apmon_mempool);

  }

  // If URL is not valid, nothing is done: no valid ApMon will be available

  // Free regex
  regfree(re_compd);
  delete re_compd;

}

/*

void AfDataSetsManager::NotifyDataSetStatus(UInt_t uniqueId, const char *dsName,
  Int_t nFiles, Int_t nStaged, Int_t nCorrupted, const char *treeName,
  Int_t nEvts, Long64_t totalSizeBytes) {

  #ifdef WOTH_APMON

  if (!fApMon) {
    return;
  }

  Float_t pctStaged = 100. * nStaged / nFiles;
  Float_t pctCorrupted = 100. * nCorrupted / nFiles;

  char *paramNames[] = {
    (char *)"dsname",
    (char *)"filescount",
    (char *)"stagecount",
    (char *)"corruptedcount",
    (char *)"stagedpct",
    (char *)"corruptedpct",
    (char *)"treename",
    (char *)"nevts",
    (char *)"totsizemb" // MB = 1024*1024 bytes
  };

  Int_t valueTypes[] = {
    XDR_STRING,
    XDR_INT32,
    XDR_INT32,
    XDR_INT32,
    XDR_REAL32,
    XDR_REAL32,
    XDR_STRING,
    XDR_INT32,
    XDR_INT32
  };

  Int_t totalSizeMB = (Int_t)(totalSizeBytes / 1048576L);

  char *paramValues[] = {
    (char *)dsName,
    (char *)&nFiles,
    (char *)&nStaged,
    (char *)&nCorrupted,
    (char *)&pctStaged,
    (char *)&pctCorrupted,
    (char *)treeName,
    (char *)&nEvts,
    (char *)&totalSizeMB
  };

  Int_t nParams = sizeof(paramNames) / sizeof(char *);

  char buf[10];
  snprintf(buf, 10, "%08x", uniqueId);

  try {

    const UInt_t maxRetries = 5;
    Int_t r;

    for (UInt_t i=0; i<maxRetries; i++) {
      r = fApMon->sendParameters( (char *)fApMonDsPrefix.Data(),
        (char *)buf, nParams, paramNames, valueTypes, paramValues);

      if (r != RET_NOT_SENT) break;
    }

    // Report error only if fails to send message after maxRetries retries
    if (r == RET_NOT_SENT) {
      AfLogDebug(10, "MonALISA notification skipped after %u tries: maximum "
        "number of datagrams per second exceeded", maxRetries);
    }

  }
  catch (runtime_error &e) {
    AfLogError("Error sending information to MonALISA");
  }

  #endif // WOTH_APMON
}

void AfDataSetsManager::CreateApMon(TUrl *monUrl) {

  #ifdef WOTH_APMON

  try {
    if (strcmp(monUrl->GetProtocol(), "apmon") == 0) {
      char *host = (char *)monUrl->GetHost();
      Int_t port = monUrl->GetPort();
      char *pwd = (char *)monUrl->GetPasswd();
      fApMon = new ApMon(1, &host, &port, &pwd);
    }
    else {
      fApMon = new ApMon((char *)monUrl->GetUrl());
    }
  }
  catch (runtime_error &e) {
    AfLogError("Can't create ApMon object from URL %s, error was: %s",
      monUrl->GetUrl(), e.what());
    delete fApMon;
    fApMon = NULL;
  }

  #endif // WOTH_APMON
}

*/
