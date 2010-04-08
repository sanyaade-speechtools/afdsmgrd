#ifndef AFDATASETMANAGER_H
#define AFDATASETMANAGER_H

#include "AfConfReader.h"
#include "AfDataSetSrc.h"
#include <cstdio>

using namespace std;

class AfDataSetManager {

  public:

    AfDataSetManager();
    ~AfDataSetManager();
    void loop(unsigned int nLoops = 0);
    bool readCf(const char *cf);
    void setSuid(bool suid = true) { fSuid = suid; }
    void setResetDataSets(bool suid = true) { fReset = suid; }

  private:

    // Private methods

    // Private variables
    vector<AfDataSetSrc *> fSrcList;
    bool                   fSuid;
    bool                   fReset;
    string                 fRedirHost;
    unsigned short         fRedirPort;
    int                    fLoopSleep_s;
    static const int       fDefaultLoopSleep_s = 3600;
};

#endif // AFDATASETMANAGER_H
