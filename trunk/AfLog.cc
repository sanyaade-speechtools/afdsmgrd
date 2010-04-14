#include "AfLog.h"

AfLog::AfLog(bool debug) {
  kStdErr = stderr;
  fDatime = new TDatime();
  fLastRotated = NULL;
  fDebug = debug;
  SetStdErr();
}

AfLog::~AfLog() {
  if (fLastRotated) {
    delete fLastRotated;
  }
  delete fDatime;
}

void AfLog::Init(bool debug) {
  if (!gLog) {
    gLog = new AfLog(debug);
  }
  else {
    gLog->Warning("Log facility already initialized!");
  }
}

bool AfLog::SetFile(const char *fn) {
  fLogFile = fopen(fn, "a");
  if (!fLogFile) {
    SetStdErr();
    return kFALSE;
  }

  fLogFileName = fn;

  if (fLastRotated) {
    fLastRotated->Set();
  }
  else {
    fLastRotated = new TDatime();
  }
  fRotateable = kTRUE;
  return kTRUE;
}

void AfLog::SetStdErr() {
  fLogFile = kStdErr;
  fLogFileName = NULL;
  fRotateable = kFALSE;
  if (fLastRotated) {
    delete fLastRotated;
    fLastRotated = NULL;
  }
}

Int_t AfLog::CheckRotate() {

  if (!fRotateable) {
    return 0;
  }

  fDatime->Set();

  if ( fDatime->GetDate() > fLastRotated->GetDate() ) {
    char buf[200];

    // Archived logfile has yesterday's date
    fDatime->Set( fDatime->Convert() - 86400 );
    snprintf(buf, 200, "%s.%04u%02u%02u", fLogFileName, fDatime->GetYear(),
      fDatime->GetMonth(), fDatime->GetDay());

    fclose(fLogFile);
    Int_t rRen = gSystem->Rename( fLogFileName, buf );
    // TODO: add compression here

    bool rSet = SetFile(fLogFileName);

    if ((rRen == -1) || (!rSet)) {
      return -1;
    }

    return 1;
  }

}

void AfLog::Message(msgType type, const char *fmt, va_list args) {
  Int_t r = CheckRotate();
  va_list dummy = {};
  if (r == -1) {
    Format(kAfError, "Can't rotate logfile!", dummy);
  }
  else if (r == 1) {
    Format(kAfOk, "Logfile rotated", dummy);
  }
  // 0 == no need to rotate
  Format(type, fmt, args);
}

void AfLog::Format(msgType type, const char *fmt, va_list args) {

  char prefix[4];

  switch (type) {
    case kAfOk:
      strcpy(prefix, "OK!");
    break;
    case kAfWarning:
      strcpy(prefix, "WRN");
    break;
    case kAfInfo:
      strcpy(prefix, "INF");
    break;
    case kAfError:
      strcpy(prefix, "ERR");
    break;
    case kAfFatal:
      strcpy(prefix, "FTL");
    break;
    case kAfDebug:
      strcpy(prefix, "DBG");
    break;
  }
  fDatime->Set();
  fprintf(fLogFile, "[%s] *** %s *** ", fDatime->AsSQLString(), prefix);
  vfprintf(fLogFile, fmt, args);
  fputc('\n', fLogFile);
  fflush(fLogFile);
}

void AfLog::SetDebug(bool debug) {
  fDebug = debug;
}

void AfLog::Debug(const char *fmt, ...) {
  if (fDebug) {
    va_list args;
    va_start(args, fmt);
    Message(kAfDebug, fmt, args);
    va_end(args);
  }
}

void AfLog::Info(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kAfInfo, fmt, args);
  va_end(args);
}

void AfLog::Ok(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kAfOk, fmt, args);
  va_end(args);
}

void AfLog::Warning(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kAfWarning, fmt, args);
  va_end(args);
}

void AfLog::Error(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kAfError, fmt, args);
  va_end(args);
}

void AfLog::Fatal(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kAfFatal, fmt, args);
  va_end(args);
}
