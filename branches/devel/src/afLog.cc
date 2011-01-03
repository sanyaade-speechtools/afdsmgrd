/**
 * afLog.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afLog.h"

using namespace af;

/** Static pointer to the standard log facility.
 */
log *log::stdlog = NULL;

/** Constructor. It takes an ostream (NOT ofstream!) as the only argument. Log
 *  rotation is obviously not supported by a generic ostream, only by files.
 */
log::log(std::ostream &out_stream, log_level_t min_level) :
  out(&out_stream), out_file(NULL), min_log_level(min_level), rotated_time(0),
  secs_rotate(0.) {
  if (!stdlog) stdlog = this;
}

/** Constructor. It takes an file name as the only argument. Remember that this
 *  class throws an exception any time a file open call fails (because disk is
 *  full or no permissions on output file or whatever).
 *
 *  See http://www.cplusplus.com/reference/iostream/ios/exceptions/
 */
log::log(const char *log_file, log_level_t min_level) :
  file_name(log_file), min_log_level(min_level), rotated_time(time(NULL)),
  secs_rotate(86400.) {

  out_file = new std::ofstream();
  out_file->exceptions(std::ios::failbit);
  out_file->open(log_file, std::ios::app);
  out = out_file;

  if (!stdlog) stdlog = this;
}

/** Destructor. Sets to NULL the default facility if it equals to the current
 *  one.
 */
log::~log() {
  if (out_file) {
    out_file->close();
    delete out_file;
  }
  if (this == stdlog) stdlog = NULL;
}

/** Says a log message with the given type and level. This is the main function
 *  of the class.
 */
void log::say(log_type_t type, log_level_t level, const char *fmt, ...) {
  va_list vargs;
  va_start(vargs, fmt);
  rotate_say(type, level, fmt, vargs);
  va_end(vargs);
};

/** Rotates the logfile and returns a value of type rotate_err_t. Keep in mind
 *  that if file open fails an exception is thrown and must be caught, elsewhere
 *  the program aborts. See the constructor (for files, not generic ostreams)
 *  for more information.
 */
rotate_err_t log::rotate() {
  struct tm *rotated_tm = localtime(&rotated_time);
  strcpy(strbuf, "bzip2 -9 \"");
  size_t pos = strlen(strbuf);
  char *old_log_file_name = &strbuf[pos];
  strftime(old_log_file_name, AF_LOG_BUFSIZE-pos, "logfile-%Y%m%d-%H%M%S",
    rotated_tm);

  out_file->close();

  rotate_err_t ret = rotate_err_ok;

  if (rename(file_name.c_str(), old_log_file_name)) ret = rotate_err_rename;
  else {
    strncat(strbuf, "\" > /dev/null 2>&1", AF_LOG_BUFSIZE);
    if (system(strbuf)) ret = rotate_err_compress;
  }

  out_file->open(file_name.c_str(), std::ios::app);

  return ret;
}

/** Private function that checks if the current stream is rotateable and should
 *  be rotated, rotates it in such a case, then says the message to the logfile
 *  with the appropriate level and type.
 */
void log::rotate_say(log_type_t type, log_level_t level, const char *fmt,
  va_list vargs) {

  if (out_file) {
    time_t cur_time = time(NULL);
    if (difftime(cur_time, rotated_time) >= secs_rotate) {

      switch (rotate()) {
        case rotate_err_rename:
          vsay(log_type_error, log_level_urgent,
            "Can't rename logfile: rotation failed", vargs);
        break;

        case rotate_err_compress:
          vsay(log_type_warning, log_level_urgent,
            "Can't compress rotated logfile", vargs);
        break;

        case rotate_err_ok:
          vsay(log_type_ok, log_level_urgent, "Logfile rotated", vargs);
        break;
      }
    }

    rotated_time = cur_time;
  }

  vsay(type, level, fmt, vargs);
}

/** Says a log message, varargs version. This function is private and used
 *  internally to format the messages.
 */
void log::vsay(log_type_t type, log_level_t level, const char *fmt,
  va_list vargs) {

  if (level < min_log_level) return;

  char pref;

  switch (type) {
    case log_type_ok:      pref = 'O'; break;
    case log_type_info:    pref = 'I'; break;
    case log_type_warning: pref = 'W'; break;
    case log_type_error:   pref = 'E'; break;
    case log_type_fatal:   pref = 'F'; break;
  }

  time_t cur_time = time(NULL);
  struct tm *cur_tm = localtime(&cur_time);
  strftime(strbuf, AF_LOG_BUFSIZE, "-[%Y%m%d-%H%M%S] ", cur_tm);
  *out << pref << strbuf;

  vsnprintf(strbuf, AF_LOG_BUFSIZE, fmt, vargs);
  *out << strbuf << std::endl;
}

/** Success message of the specified log level on the default log facility.
 */
void log::ok(log_level_t level, const char *fmt, ...) {
  if (!stdlog) return;
  va_list vargs;
  va_start(vargs, fmt);
  stdlog->rotate_say(log_type_ok, level, fmt, vargs);
  va_end(vargs);
};

/** Info message of the specified log level on the default log facility.
 */
void log::info(log_level_t level, const char *fmt, ...) {
  if (!stdlog) return;
  va_list vargs;
  va_start(vargs, fmt);
  stdlog->rotate_say(log_type_info, level, fmt, vargs);
  va_end(vargs);
};

/** Warning message of the specified log level on the default log facility.
 */
void log::warning(log_level_t level, const char *fmt, ...) {
  if (!stdlog) return;
  va_list vargs;
  va_start(vargs, fmt);
  stdlog->rotate_say(log_type_warning, level, fmt, vargs);
  va_end(vargs);
};

/** Error message of the specified log level on the default log facility.
 */
void log::error(log_level_t level, const char *fmt, ...) {
  if (!stdlog) return;
  va_list vargs;
  va_start(vargs, fmt);
  stdlog->rotate_say(log_type_error, level, fmt, vargs);
  va_end(vargs);
};

/** Fatal error message of the specified log level on the default log facility.
 */
void log::fatal(log_level_t level, const char *fmt, ...) {
  if (!stdlog) return;
  va_list vargs;
  va_start(vargs, fmt);
  stdlog->rotate_say(log_type_fatal, level, fmt, vargs);
  va_end(vargs);
};
