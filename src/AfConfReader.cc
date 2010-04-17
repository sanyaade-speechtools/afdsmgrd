#include "AfConfReader.h"

AfConfReader::AfConfReader() {
  fVars = new TMap();
  fVars->SetOwner();
  fDirs = new TList();
  fDirs->SetOwner();
}

AfConfReader::~AfConfReader() {
  delete fVars;
  delete fDirs;
}

void AfConfReader::PrintVarsAndDirs() {
  TIter i(fVars);
  TObjString *key;
  TObjString *val;
  while (key = dynamic_cast<TObjString *>(i.Next())) {
    val = dynamic_cast<TObjString *>( fVars->GetValue(key) );
    AfLogInfo("Variable: %s = %s", key->GetString().Data(),
      val->GetString().Data());
  }

  TIter j(fDirs);
  TPair *pair;
  while (pair = dynamic_cast<TPair *>(j.Next())) {
    AfLogInfo("Directive: %s = %s",
      dynamic_cast<TObjString *>(pair->Key())->GetString().Data(),
      dynamic_cast<TObjString *>(pair->Value())->GetString().Data());
  }
}

TString *AfConfReader::GetVar(const char *vn) {
  TPair *p = dynamic_cast<TPair *>( fVars->FindObject( vn ) );
  if (p) {
    TString *s =
      new TString( dynamic_cast<TObjString *>(p->Value())->GetString() );
    return s;
  }
  return NULL;
}

TString *AfConfReader::GetDir(const char *dn) {
  TIter j(fDirs);
  TPair *pair;
  while (pair = dynamic_cast<TPair *>(j.Next())) {
    TObjString *key = dynamic_cast<TObjString *>(pair->Key());
    TObjString *value = dynamic_cast<TObjString *>(pair->Value());
    if ( key->GetString() == dn ) { // == overloaded
      return new TString( value->GetString() );
    }
  }
  return NULL;
}

TList *AfConfReader::GetDirs(const char *dn) {
  TIter j(fDirs);
  TPair *pair;
  TList *res = new TList();
  res->SetOwner();
  while (pair = dynamic_cast<TPair *>(j.Next())) {
    TObjString *key = dynamic_cast<TObjString *>(pair->Key());
    TObjString *value = dynamic_cast<TObjString *>(pair->Value());
    if ( key->GetString() == dn ) { // == overloaded
      res->Add( new TObjString(*value) );
    }
  }
  return res;
}

void AfConfReader::ReadConf() {

  fFp.clear();
  fFp.seekg(0, ios::beg);

  char *buf = new char[400];

  regex_t reCom;  // regex that matches a comment
  regex_t reVar;  // regex that matches a variable
  regex_t reDir;  // regex that matches a directive

  regcomp(&reCom, "^[ \t]*#|^[ \t]*$", REG_EXTENDED);
  regcomp(&reVar, "^[ \t]*set[ \t]+([A-Za-z0-9]+)[ \t]+=[ \t]+(.+)$",
    REG_EXTENDED);
  regcomp(&reDir, "^[ \t]*([^ \t]+)[ \t]+(.+?)$", REG_EXTENDED);

  regmatch_t reMatch[3];

  while (fFp.getline(buf, 400)) {

    if ( regexec(&reCom, buf, 0, NULL, 0) != 0 ) {

      if ( regexec(&reVar, buf, 3, reMatch, 0) == 0 ) {
        // We've got a variable here
        char *vn = &buf[reMatch[1].rm_so];
        buf[reMatch[1].rm_eo] = '\0';
        char *vv = &buf[reMatch[2].rm_so];
        buf[reMatch[2].rm_eo] = '\0';
        fVars->Add( new TObjString(vn), new TObjString(vv) );
      }
      else if ( regexec(&reDir, buf, 3, reMatch, 0) == 0 ) {
        // We've got a directive here
        char *dn = &buf[reMatch[1].rm_so];
        buf[reMatch[1].rm_eo] = '\0';
        char *dv = &buf[reMatch[2].rm_so];
        buf[reMatch[2].rm_eo] = '\0';
        fDirs->Add( new TPair( new TObjString(dn), new TObjString(dv) ) );
      }
      //else {}

    }

  }

  regfree(&reCom);
  regfree(&reVar);
  regfree(&reDir);

  delete[] buf;
}

void AfConfReader::SubstVar() {

  regex_t reSub;
  regcomp(&reSub, "(\\$[A-Za-z0-9]+)", REG_EXTENDED);
  regmatch_t reMatch;

  TIter i(fDirs);  // fDirs is a TList of TPair
  TPair *elm;

  while ( elm = dynamic_cast<TPair *>(i.Next()) ) {

    TString val = dynamic_cast<TObjString *>(elm->Value())->GetString();
    char *ptr = const_cast<char *>( val.Data() );  // I promise not to modify it

    Int_t off = 0;
    const Int_t maxVar = 20;
    Int_t v1[maxVar];
    Int_t v2[maxVar];
    Int_t nVars = 0;

    // Finds all variables and put match results into an array
    while (regexec(&reSub, ptr, 1, &reMatch, 0) == 0) {
      //v.push_back( make_pair(reMatch.rm_so + off, reMatch.rm_eo + off) );
      v1[nVars] = reMatch.rm_so + off;
      v2[nVars] = reMatch.rm_eo + off;

      ptr = &ptr[ reMatch.rm_eo ];
      off += reMatch.rm_eo;

      if (++nVars == maxVar) {
        break;
      }
    }

    // If variables are found we try to substitute them in *reverse* order,
    // because matching indexes change the other way around
    if (nVars > 0) {
      //AfLogInfo("old --> {%s}", val.Data());
      for (Int_t j=nVars-1; j>=0; j--) {
        Int_t len = v2[j]-v1[j]-1;
        char *vn = new char[ len+2 ];
        memcpy( vn, &(val.Data())[ v1[j]+1 ], len );
        vn[len] = '\0';
        //AfLogInfo("%d, %d --> %s", v[j].second, v[j].first, vn);

        TString *var = GetVar(vn);
        if (var) {
          //AfLogInfo("Found: %s = %s", vn, var->Data());
          val.Replace( v1[j], v2[j]-v1[j], var->Data() );
          //AfLogInfo("new --> {%s}", val.Data());
          delete elm->Value();
          elm->SetValue( new TObjString(val.Data()) );
          delete var;
        }

        delete[] vn;
      }
      //AfLogInfo("new --> {%s}",
      //  dynamic_cast<TObjString *>(elm->Value())->GetString().Data());
    }

  } // end for over directives

  regfree(&reSub);
}

AfConfReader *AfConfReader::Open(const char *cffn, bool subVars) {
  AfConfReader *afcf = new AfConfReader();
  (afcf->fFp).open(cffn);

  if (!(afcf->fFp)) {
    delete afcf;
    return NULL;
  }

  afcf->ReadConf();
  (afcf->fFp).close();

  if (subVars) {
    afcf->SubstVar();
  }

  return afcf;
}

