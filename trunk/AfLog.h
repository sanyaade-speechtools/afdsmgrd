#ifndef AFLOG_H
#define AFLOG_H

#define AfLogInfo(...) gLog->Info(__VA_ARGS__)
#define AfLogOk(...) gLog->Ok(__VA_ARGS__)
#define AfLogWarning(...) gLog->Warning(__VA_ARGS__)
#define AfLogError(...) gLog->Error(__VA_ARGS__)
#define AfLogFatal(...) gLog->Fatal(__VA_ARGS__)

enum msgType { kAfOk, kAfInfo, kAfWarning, kAfError, kAfFatal };

#include <TDatime.h>

class AfLog {

  public:
    static void Init();
    bool SetFile(const char *fn);
    void SetStdErr();
    void Info(const char *fmt, ...);
    void Ok(const char *fmt, ...);
    void Warning(const char *fmt, ...);
    void Error(const char *fmt, ...);
    void Fatal(const char *fmt, ...);

  private:

    // Methods
    void Message(msgType type, const char *fmt, va_list args);
    void Format(msgType type, const char *fmt, va_list args);
    int CheckRotate();
    AfLog();
	~AfLog();

    // Constants
    FILE *kFallbackLogFile;
    static const int kRotateEvery_s = 20; //86400;

    // Variables
    FILE       *fLogFile;
    TDatime    *fDatime;
    TDatime    *fLastRotated;
    const char *fLogFileName;
    bool        fRotateable;

};

extern AfLog *gLog;

#endif // AFLOG_H
