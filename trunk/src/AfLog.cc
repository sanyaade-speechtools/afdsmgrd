#include "AfLog.h"

AfLog::AfLog(bool debug) {
  kStdErr = stderr;
  fDatime = new TDatime();
  fLastRotated = NULL;
  fDebug = debug;
  fLogFile = NULL;
  SetStdErr();
}

AfLog::~AfLog() {
  if (fLastRotated) {
    delete fLastRotated;
  }
  delete fDatime;
  if ((fLogFile) && (fLogFile != kStdErr)) {
    fclose(fLogFile);
  }
}

void AfLog::Init(bool debug) {
  if (!gLog) {
    gLog = new AfLog(debug);
  }
  else {
    gLog->Warning("Log facility already initialized!");
  }
}

void AfLog::Delete() {
  if (gLog) {
    delete gLog;
  }
}

bool AfLog::SetFile(const char *fn) {

  // Close previously opened file, if one
  if ((fLogFile) && (fLogFile != kStdErr)) {
    fclose(fLogFile);
  }

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
  if ((fLogFile) && (fLogFile != kStdErr)) {
    fclose(fLogFile);
  }
  fLogFile = kStdErr;
  fLogFileName = "";
  fRotateable = kFALSE;
  if (fLastRotated) {
    delete fLastRotated;
    fLastRotated = NULL;
  }
}

// Return values:
//   0 : not rotated
//   1 : rotated
//   2 : rotated but not compressed
//  -1 : can't rename, output still appended to old file
//  -2 : can't rename and can't reopen, output on stderr
//  -3 : renamed and compressed but can't open file, output on stderr
//  -4 : renamed but not compressed and can't open file, output on stderr
Int_t AfLog::CheckRotate() {

  if (!fRotateable) {
    return 0;
  }

  fDatime->Set();

  if ( fDatime->GetDate() > fLastRotated->GetDate() ) {
  //if ( fDatime->GetTime() != fLastRotated->GetTime() ) {

    // Archived logfile has yesterday's date
    fDatime->Set( fDatime->Convert() - 86400 );

    TString newFn = Form("%s.%04u%02u%02u", fLogFileName.Data(),
      fDatime->GetYear(), fDatime->GetMonth(), fDatime->GetDay());

    //TString newFn = Form("%s.%04u%02u%02u-%02u%02u%02u", fLogFileName.Data(),
    //  fDatime->GetYear(), fDatime->GetMonth(), fDatime->GetDay(),
    //  fDatime->GetHour(), fDatime->GetMinute(), fDatime->GetSecond());

    // It is not allowed to close the log: we need to switch to stderr for a
    // moment to close the opened file, but before that we save the name of the
    // opened file
    TString realLogFn = fLogFileName;
    SetStdErr();

    if (gSystem->Rename(realLogFn.Data(), newFn.Data()) == -1) {
      // Revert to the old logfile if rename failed
      if ( !SetFile(realLogFn.Data()) ) {
        SetStdErr();
        return -2;
      }
      return -1;
    }

    // bzip2 command must be in path
    Int_t rCmp = gSystem->Exec( Form("bzip2 -9 \"%s\" > /dev/null 2>&1",
      newFn.Data()) );

    // Redirect again the output on the expected original file, which is now
    // new and empty
    if ( !SetFile(realLogFn.Data()) ) {
      SetStdErr();
      if (rCmp != 0) {
        return -4;
      }
      return -3;
    }

    if (rCmp != 0) {
      return 2;
    }

    return 1;
  }

  return 0;
}

void AfLog::Message(MsgType_t type, const char *fmt, va_list args) {
  Int_t r = CheckRotate();
  va_list dummy = {};
  if (r < 0) {
    Format(kMsgError, "Errors occured rotating logfile!", dummy);
  }
  else if (r > 0) {
    Format(kMsgOk, "Logfile rotated", dummy);
  }
  // 0 == no need to rotate
  Format(type, fmt, args);
}

void AfLog::Format(MsgType_t type, const char *fmt, va_list args) {

  char prefix[4];

  switch (type) {
    case kMsgOk:
      strcpy(prefix, "OK!");
    break;
    case kMsgWarning:
      strcpy(prefix, "WRN");
    break;
    case kMsgInfo:
      strcpy(prefix, "INF");
    break;
    case kMsgError:
      strcpy(prefix, "ERR");
    break;
    case kMsgFatal:
      strcpy(prefix, "FTL");
    break;
    case kMsgDebug:
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
    Message(kMsgDebug, fmt, args);
    va_end(args);
  }
}

void AfLog::Info(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kMsgInfo, fmt, args);
  va_end(args);
}

void AfLog::Ok(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kMsgOk, fmt, args);
  va_end(args);
}

void AfLog::Warning(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kMsgWarning, fmt, args);
  va_end(args);
}

void AfLog::Error(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kMsgError, fmt, args);
  va_end(args);
}

void AfLog::Fatal(const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  Message(kMsgFatal, fmt, args);
  va_end(args);
}
