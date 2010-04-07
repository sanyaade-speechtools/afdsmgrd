#ifndef AFLOG_H
#define AFLOG_H

// Global log file pointer
//FILE *logFp;

// Macro for logging
#if defined(__GNUC__) || defined(__ICC) || defined(__ECC) || defined(__APPLE__)
#define THISFUNC() __FUNCTION__
#elif defined(__HP_aCC) || defined(__alpha) || defined(__DECCXX)
#define FUNCTIONNAME() __FUNC__
#else
#define THISFUNC() "???"
#endif

#define AfLogInfo(...)    fputs("** INF ** ", logFp); AfLog(__VA_ARGS__)
#define AfLogOk(...)      fputs("** OK! ** ", logFp); AfLog(__VA_ARGS__)
#define AfLogWarning(...) fputs("** WRN ** ", logFp); AfLog(__VA_ARGS__)
#define AfLogError(...)   fputs("** ERR ** ", logFp); AfLog(__VA_ARGS__)
#define AfLogFatal(...)   fputs("** FTL ** ", logFp); AfLog(__VA_ARGS__)

#define AfLog(...) fprintf(logFp, __VA_ARGS__); fputc('\n', logFp); \
  fflush(logFp)

//#define FullLog(STR) if (fDebugLevel >= kFull) std::cout \
//  << "XrdDownloadManager::" << THISFUNC() << "() " << (STR) << std::endl

#endif // AFLOG_H
