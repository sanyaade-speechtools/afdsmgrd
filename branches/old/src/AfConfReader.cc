#include "AfConfReader.h"

AfConfReader::AfConfReader() {
  fVars = new TMap();
  fVars->SetOwnerKeyValue();
  fDirs = new TList();
  //fDirs->SetOwner();  // list of TPairs (they do not own objects): manual dtor
}

AfConfReader::~AfConfReader() {
  delete fVars;

  TPair *p;
  TIter i(fDirs);
  while (p = dynamic_cast<TPair *>(i.Next())) {
    delete p->Key();
    delete p->Value();
    delete p;
  }
  delete fDirs;
}

void AfConfReader::PrintVarsAndDirs() {
  TIter i(fVars);
  TObjString *key;
  TObjString *val;
  while (key = dynamic_cast<TObjString *>(i.Next())) {
    val = dynamic_cast<TObjString *>( fVars->GetValue(key) );
    AfLogInfo("Var: {%s} = {%s}", key->GetString().Data(),
      val->GetString().Data());
  }

  TIter j(fDirs);
  TPair *pair;
  while (pair = dynamic_cast<TPair *>(j.Next())) {
    AfLogInfo("Dir: {%s} = {%s}",
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

  char buf[400];

  TPMERegexp reCom("^[ \t]*#|^[ \t]*$");
  TPMERegexp reVar("^[ \t]*set[ \t]+([A-Za-z0-9]+)[ \t]+=[ \t]+(.+)$");
  TPMERegexp reDir("^[ \t]*([^ \t]+)[ \t]*(.*?)[ \t]*$");

  while (fFp.getline(buf, 400)) {

    if ( reCom.Match(buf) == 0 ) {
      // Not a comment

      if ( reVar.Match(buf) == 3 ) {
        // We've got a variable here: fVars is a TMap
        fVars->Add( new TObjString(reVar[1]), new TObjString(reVar[2]) );
      }
      if ( reDir.Match(buf) == 3 ) {
        // We've got a directive here -- fDirs is a TList
        fDirs->Add( new TPair( new TObjString(reDir[1]),
          new TObjString(reDir[2]) ) );
      }

    }

  }

}

void AfConfReader::SubstVar() {

  TPMERegexp reSub("\\$[A-Za-z0-9]+", "g");

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
    while (reSub.Match(ptr) == 1) {
      Int_t en = reSub.GetGlobalPosition();
      Int_t st = en - reSub[0].Length();

      v1[nVars] = st;
      v2[nVars] = en;

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

        TString *var = GetVar(vn);
        if (var) {
          val.Replace( v1[j], v2[j]-v1[j], var->Data() );
          delete elm->Value();
          elm->SetValue( new TObjString(val.Data()) );
          delete var;
        }

        delete[] vn;
      }
      //AfLogInfo("new --> {%s}",
      //  dynamic_cast<TObjString *>(elm->Value())->GetString().Data());
    }
  } // end while over directives
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
