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

class AfLog {

  public:
    static void Init(Bool_t debug = kFALSE);
    static void Delete();
    void        SetDebug(Bool_t debug);
    Bool_t      GetDebug() { return fDebug; };
    Bool_t      SetFile(const char *fn);
    void        SetStdErr();
    void        Debug(const char *fmt, ...);
    void        Info(const char *fmt, ...);
    void        Ok(const char *fmt, ...);
    void        Warning(const char *fmt, ...);
    void        Error(const char *fmt, ...);
    void        Fatal(const char *fmt, ...);

  private:

    // Methods
    AfLog(Bool_t debug = kFALSE);
	~AfLog();
    void        Message(MsgType_t type, const char *fmt, va_list args);
    void        Format(MsgType_t type, const char *fmt, va_list args);
    Int_t       CheckRotate();

    // Constants
    FILE *kStdErr;

    // Variables
    FILE       *fLogFile;
    TDatime    *fDatime;
    TDatime    *fLastRotated;
    TString     fLogFileName;
    Bool_t      fRotateable;
    Bool_t      fDebug;

};

extern AfLog *gLog;

#endif // AFLOG_H
