/**
 * afConfig.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * This class is a collection of key/value pairs associated to a configuration
 * file. If the configuration file changes the values are updated dynamically.
 */

#ifndef AFCONFIG_H
#define AFCONFIG_H

#define AF_CONFIG_BUFSIZE 1000

#include <fstream>
#include <string>
#include <map>
#include <stdexcept>

namespace af {

  /** Identifies a data type of a directive.
   */
  typedef enum {
    dir_type_int,
    dir_type_uint,
    dir_type_real,
    dir_type_text
  } dir_type_t;

  /** Holds different data types upon need.
   */
  typedef union {
    double d;
    unsigned long ui;
    long i;
    std::string *s;
  } mixed_t;

  /** A single directive, with the corresponding binding, default value, maximum
   *  and minimum allowed value, and directive name.
   */
  class cfg_binding {

    public:
      void *dest;
      dir_type_t type;
      mixed_t min;
      mixed_t max;
      mixed_t def_val;
      cfg_binding(long *_dest, long _min, long _max, long _def_val);

  };

  /** Useful typedefs.
   */
  typedef std::multimap<std::string,cfg_binding> conf_dirs_t;
  typedef std::pair<std::string,cfg_binding> dir_t;
  typedef conf_dirs_t::const_iterator conf_dirs_iter_t;

  /** Manages a configuration file.
   */
  class config {

    public:

      config(const char *config_file);
      //virtual ~config();
      void print_bindings();
      void read_file();
      void bind_int(const char *directive, long *dest, long min_eq, long max_eq,
        bool has_default, long def_val);

    private:

      char *ltrim(char *str);
      char *rtrim(char *str);

      conf_dirs_t directives;
      std::string file_name;
      char strbuf[AF_CONFIG_BUFSIZE];

  };

};

#endif // AFCONFIG_H
