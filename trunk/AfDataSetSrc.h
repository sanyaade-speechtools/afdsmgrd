#ifndef AFDATASETSRC_H
#define AFDATASETSRC_H

using namespace std;

#include <cstdio>
#include <string>
#include <TDataSetManagerFile.h>
#include <TFileInfo.h>
#include <TUrl.h>

class AfDataSetSrc {

  public:

    AfDataSetSrc(const char *url, TUrl *redirUrl, const char *opts,
      bool suid);
    string &getDsUrl() { return fUrl; }
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
    TUrl                *fRedirUrl;
    vector<TFileInfo *>  fToStage;
    string               fUrl;
    string               fOpts;
    bool                 fSuid;
    uid_t                fUnpUid;
    gid_t                fUnpGid;

    // Some constants
    static const int     kTranslateROOT = 1;
    static const int     kTranslateAliEn = 2;
};

#endif // AFDATASETSRC_H
