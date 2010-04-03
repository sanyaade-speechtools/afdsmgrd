#ifndef AFCONFREADER_H
#define AFCONFREADER_H

#include <string>
#include <vector>
#include <fstream>
#include <map>

using namespace std;

extern FILE *logFp;

class AfConfReader {

  public:

    ~AfConfReader();
    const char *getVar(const char *vn);
    const char *getDir(const char *dn);
    vector<const char *> *getDirs(const char *dn);
    static AfConfReader *open(const char *cffn, bool subVars = true);
    void printVarsAndDirs();

  private:

    // Private methods
    AfConfReader();  // constructor is private
    void substVar();
    void readCf();

    // Private variables
    ifstream                       fp;
    vector< pair<string, string> > dirs;
    map<string, string>            vars;

};

#endif // AFCONFREADER_H
