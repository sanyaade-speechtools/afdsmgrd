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

#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <stdexcept>

/** Constants for directive limits.
 */
#define AF_INT_MAX  ( std::numeric_limits<long>::max() )
#define AF_INT_MIN  ( std::numeric_limits<long>::min() )
#define AF_UINT_MAX ( std::numeric_limits<unsigned long>::max() )
#define AF_UINT_MIN ( 0 )
#define AF_REAL_MAX ( std::numeric_limits<double>::max() )
#define AF_REAL_MIN ( std::numeric_limits<double>::min() )

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
    unsigned long u;
    long i;
    std::string *s;
  } mixed_t;

  /** A single directive, with the corresponding binding, default value, maximum
   *  and minimum allowed value, and directive name.
   */
  class cfg_binding {

    public:
      cfg_binding(long *_dest_ptr, long _def_val, long _min, long _max);
      cfg_binding(std::string *_dest_ptr, const char *_def_val);
      virtual ~cfg_binding();
      inline void *get_dest() const { return dest; };
      inline dir_type_t get_type() const { return type; };
      void check_assign(long value) const;
      void check_assign(unsigned long value) const;
      void check_assign(double value) const;
      void assign(const char *value) const;
      void print() const;

    private:
      template<typename T>
        static inline bool inside_limits(T val, T inf, T sup) {
          return ((val >= inf) && (val <= sup));
        };
      template<typename T>
        void ctor_helper(T _def_val, T _min, T _max);
      void *dest;
      dir_type_t type;
      mixed_t min;
      mixed_t max;
      mixed_t def_val;

  };

  /** Useful typedefs.
   */
  typedef std::multimap<std::string,cfg_binding *> conf_dirs_t;
  typedef std::pair<std::string,cfg_binding *> dir_t;
  typedef conf_dirs_t::const_iterator conf_dirs_iter_t;

  /** Manages a configuration file.
   */
  class config {

    public:

      config(const char *config_file);
      virtual ~config();
      void print_bindings();
      void read_file();

      /** Bindings.
       */
      void bind_int(const char *dir_name, long *dest_ptr, long def_val,
        long min_eq, long max_eq);
      void bind_text(const char *dir_name, std::string *dest_ptr,
        const char *def_val);

    private:

      inline char *ltrim(char *str) const;
      inline char *rtrim(char *str) const;

      conf_dirs_t directives;
      std::string file_name;
      char strbuf[AF_CONFIG_BUFSIZE];

  };

};

#endif // AFCONFIG_H
