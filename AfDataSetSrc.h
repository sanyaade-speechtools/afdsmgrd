#ifndef AFDATASETSRC_H
#define AFDATASETSRC_H

using namespace std;

#include <cstdio>
#include <string>
#include <TDataSetManagerFile.h>
#include <TFileInfo.h>

extern FILE *logFp;

class AfDataSetSrc {

  public:

    AfDataSetSrc(string &ds, string &mss, string &o, bool canSuid,
      string &redirHost, unsigned short redirPort);
    string &getDsUrl() { return fUrl; }
    string &getMssUrl() { return fMss; }
    void process();
    ~AfDataSetSrc();

  private:

    // Private methods
    void flattenDsList();
    void processDs(const char *uri);
    int  translateUrl(TFileInfo *ti);
    int  putIntoStageQueue();
    void fixDsDirPerms();
    void doSuid();
    void undoSuid();

    // Private variables
    TDataSetManagerFile *fManager;
    vector<string>       fDsUris;
    vector<TFileInfo *>  fToStage;
    string               fUrl;
    string               fMss;
    string               fOpts;
    bool                 fSuid;
    uid_t                fUnpUid;
    gid_t                fUnpGid;
    string               fRedirHost;
    unsigned short       fRedirPort;
};

#endif // AFDATASETSRC_H
