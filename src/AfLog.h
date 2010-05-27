#ifndef AFLOG_H
#define AFLOG_H

#define AfLogInfo(...) gLog->Info(__VA_ARGS__)
#define AfLogOk(...) gLog->Ok(__VA_ARGS__)
#define AfLogWarning(...) gLog->Warning(__VA_ARGS__)
#define AfLogError(...) gLog->Error(__VA_ARGS__)
#define AfLogFatal(...) gLog->Fatal(__VA_ARGS__)
#define AfLogDebug(...) gLog->Debug(__VA_ARGS__)

typedef enum { kMsgOk, kMsgInfo, kMsgWarning, kMsgError, kMsgFatal, kMsgDebug }
  MsgType_t;

#include <Riostream.h>
#include <Varargs.h>
#include <TDatime.h>
#include <TSystem.h>
#include <TThread.h>

class AfLog {

  public:
    static void Init(Int_t debugLevel = -999);
    static void Delete();
    void        SetBanner(const char *msg) { fBanner = msg; };
    void        PrintBanner();
    Int_t       GetDebugLevel() { return fDebugLevel; };
    void        SetDebugLevel(Int_t debugLevel) { fDebugLevel = debugLevel; };
    Bool_t      SetFile(const char *fn);
    void        SetStdErr();
    void        Debug(Int_t level, const char *fmt, ...);
    void        Info(const char *fmt, ...);
    void        Ok(const char *fmt, ...);
    void        Warning(const char *fmt, ...);
    void        Error(const char *fmt, ...);
    void        Fatal(const char *fmt, ...);

  private:

    // Methods
    AfLog(Int_t debugLevel = -999);
	~AfLog();
    void CheckRotateAndFormat(MsgType_t type, const char *fmt,
      va_list args);
    void Format(MsgType_t type, const char *fmt, va_list args);
    Int_t CheckRotate();

    // Constants
    FILE *kStdErr;

    // Variables
    FILE       *fLogFile;
    TDatime    *fDatime;
    TDatime    *fLastRotated;
    TString     fLogFileName;
    Bool_t      fRotateable;
    TString     fBanner;
    Int_t       fDebugLevel;

};

extern AfLog *gLog;

#endif // AFLOG_H
