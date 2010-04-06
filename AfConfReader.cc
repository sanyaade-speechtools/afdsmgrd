#include "AfConfReader.h"
#include "AfLog.h"

#include <fstream>
#include <utility>
#include <cstring>
#include <map>
#include <regex.h>
#include <vector>
#include <cstdlib>

AfConfReader::AfConfReader() {}

void AfConfReader::printVarsAndDirs() {
  map<string, string>::iterator i;
  for (i=vars.begin(); i != vars.end(); i++) {
    AfLogInfo("Variable: %s = %s", (i->first).c_str(), (i->second).c_str());
  }

  vector< pair<string, string> >::iterator j;
  for (j=dirs.begin(); j != dirs.end(); j++) {
    AfLogInfo("Directive: %s = %s", (j->first).c_str(), (j->second).c_str());
  }
}

const char *AfConfReader::getVar(const char *vn) {
  map<string, string>::iterator i;
  for (i=vars.begin(); i != vars.end(); i++) {
    if (strcmp((i->first).c_str(), vn) == 0) {
      return (i->second).c_str();
    }
  }
  return NULL;
}

const char *AfConfReader::getDir(const char *dn) {
  vector< pair<string, string> >::iterator i;
  for (i=dirs.begin(); i != dirs.end(); i++) {
    if (strcmp((i->first).c_str(), dn) == 0) {
      return (i->second).c_str();
    }
  }
  return NULL;
}

vector<const char *> *AfConfReader::getDirs(const char *dn) {
  vector< pair<string, string> >::iterator i;
  vector<const char *> *res = new vector<const char *>();
  for (i=dirs.begin(); i != dirs.end(); i++) {
    if (strcmp((i->first).c_str(), dn) == 0) {
      res->push_back( (i->second).c_str() );
    }
  }
  return res;
}

void AfConfReader::readCf() {

  fp.clear();
  fp.seekg(0, ios::beg);

  char *buf = new char[400];

  regex_t reCom;  // regex that matches a comment
  regex_t reVar;  // regex that matches a variable
  regex_t reDir;  // regex that matches a directive

  regcomp(&reCom, "^[ \t]*#|^[ \t]*$", REG_EXTENDED);
  regcomp(&reVar, "^[ \t]*set[ \t]+([A-Za-z0-9]+)[ \t]+=[ \t]+(.+)$",
    REG_EXTENDED);
  regcomp(&reDir, "^[ \t]*([^ \t]+)[ \t]+(.+?)$", REG_EXTENDED);

  regmatch_t reMatch[3];

  while (fp.getline(buf, 400)) {

    if ( regexec(&reCom, buf, 0, NULL, 0) != 0 ) {

      if ( regexec(&reVar, buf, 3, reMatch, 0) == 0 ) {
        // We've got a variable here
        char *vn = &buf[reMatch[1].rm_so];
        buf[reMatch[1].rm_eo] = '\0';
        char *vv = &buf[reMatch[2].rm_so];
        buf[reMatch[2].rm_eo] = '\0';
        vars.insert( make_pair(vn, vv) ); // <-- XXX
        //AfLogInfo("[V] {%s} {%s}", vn, vv);
        //vars.push_back( make_pair(vn, vv) ); // <-- XXX
      }
      else if ( regexec(&reDir, buf, 3, reMatch, 0) == 0 ) {
        // We've got a directive here
        char *dn = &buf[reMatch[1].rm_so];
        buf[reMatch[1].rm_eo] = '\0';
        char *dv = &buf[reMatch[2].rm_so];
        buf[reMatch[2].rm_eo] = '\0';
        //dirs.insert( make_pair(dn, dv) ); // <-- XXX
        //AfLogInfo("[D] {%s} {%s}", dn, dv);
        dirs.push_back( make_pair(dn, dv) );
      }
      //else {}

    }

  }

  regfree(&reCom);
  regfree(&reVar);
  regfree(&reDir);

  delete[] buf;
}

void AfConfReader::substVar() {

  //map<string,string>::iterator i;
  vector< pair<string, string> >::iterator i;

  regex_t reSub;
  regcomp(&reSub, "(\\$[A-Za-z0-9]+)", REG_EXTENDED);
  regmatch_t reMatch;

  for (i=dirs.begin(); i != dirs.end(); i++) {

    char *ptr = const_cast<char *>( (i->second).c_str() );
    int off = 0;
    vector< pair<int,int> > v;

    // Finds all variables and put match results into a vector
    while (regexec(&reSub, ptr, 1, &reMatch, 0) == 0) {
      v.push_back( make_pair(reMatch.rm_so + off, reMatch.rm_eo + off) );
      ptr = &ptr[ reMatch.rm_eo ];
      off += reMatch.rm_eo;
    }

    // If variables are found we try to substitute them in *reverse* order,
    // because matching indexes change the other way around
    if (v.size() > 0) {
      //AfLogInfo("{%s} <-- old string", (i->second).c_str());
      for (int j=v.size()-1; j>=0; j--) {
        int len = v[j].second-v[j].first-1;
        char *vn = new char[ len+2 ];
        memcpy( vn, &((i->second).c_str())[ v[j].first+1 ], len );
        vn[len] = '\0';

        map<string,string>::iterator itVar = vars.find(vn);
        if (itVar != vars.end()) {
          // If variable is found, substitute it; elsewhere, leave it alone
          (i->second).replace( v[j].first, v[j].second-v[j].first,
            itVar->second );
        }

        delete[] vn;
      }
      //AfLogInfo("{%s} <-- new string", (i->second).c_str());
    }

  } // end for over directives

  regfree(&reSub);
}

AfConfReader *AfConfReader::open(const char *cffn, bool subVars) {
  AfConfReader *afcf = new AfConfReader();
  (afcf->fp).open(cffn);

  if (!(afcf->fp)) {
    delete afcf;
    return NULL;
  }

  afcf->readCf();
  (afcf->fp).close();

  if (subVars) {
    afcf->substVar();
  }

  return afcf;
}

AfConfReader::~AfConfReader() {
  fp.close();
}
