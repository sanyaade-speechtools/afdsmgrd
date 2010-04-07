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
    void process(bool resetBits = false);
    ~AfDataSetSrc();

  private:

    // Private methods
    void flattenDsList();
    void processDs(const char *uri);
    void resetDs(const char *uri);
    void listDs(const char *uri);
    int  translateUrl(TFileInfo *ti,
      int whichUrls = kTranslateROOT | kTranslateAliEn);
    int  putIntoStageQueue();
    void fixDsDirPerms();
    void doSuid();
    void undoSuid();
    int keepOnlyFirstUrl(TFileInfo *fi);

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

    // Some constants
    static const int     kTranslateROOT = 1;
    static const int     kTranslateAliEn = 2;
};

#endif // AFDATASETSRC_H
