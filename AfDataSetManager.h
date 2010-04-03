#ifndef AFDATASETMANAGER_H
#define AFDATASETMANAGER_H

#include "AfConfReader.h"
#include "AfDataSetSrc.h"
#include <cstdio>

using namespace std;

extern FILE *logFp;

class AfDataSetManager {

  public:

    AfDataSetManager();
    ~AfDataSetManager();
    void loop();
    bool readCf(const char *cf);
    void setSuid(bool suid = true) { fSuid = suid; }

  private:

    // Private methods

    // Private variables
    vector<AfDataSetSrc *> fSrcList;
    bool                   fSuid;
    string                 fRedirHost;
    unsigned short         fRedirPort;
    int                    fLoopSleep_s;
    static const int       fDefaultLoopSleep_s = 3600;
};

#endif // AFDATASETMANAGER_H
