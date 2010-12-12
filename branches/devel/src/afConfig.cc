/**
 * afConfig.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afConfig.h"

using namespace af;

/** Constructor.
 */
config::config(const char *config_file) : file_name(config_file) {}

/** Prints fields (used for debug).
 */
void config::print_bindings() {
  for (conf_dirs_iter_t it=directives.begin(); it!=directives.end(); it++)
    printf("%s->%016lx\n", (*it).first.c_str(),
      (unsigned long)((*it).second.dest));
}

/**
 */
cfg_binding::cfg_binding(long *_dest, long _min, long _max, long _def_val) :
  dest(_dest) {

  min.i = _min;
  max.i = _max;
  def_val.i = _def_val;
  type = dir_type_int;
}

/**
 */
void config::bind_int(const char *dir_name, long *dest, long min_eq,
  long max_eq, bool has_default, long def_val) {

  printf("binding directive %s to long ptr %lx\n", dir_name,
    (unsigned long)dest);

  cfg_binding b(dest, min_eq, max_eq, def_val);
  directives.insert( dir_t(dir_name, b) );

  // testtesttest
  strcpy(strbuf, "");
  printf("[%s]\n", rtrim(strbuf));

}

/** Returns a pointer to the first non-blank char of the input string. The
 *  original string is modified.
 */
char *config::ltrim(char *str) {
  char *tr;
  for (tr=str; *tr!='\0'; tr++) {
    if ((*tr != '\t') && (*tr != ' ')) break;
  }
  return tr;
}

/** Truncates the string at the first blank character. The original string is
 *  modified.
 */
char *config::rtrim(char *str) {
  char *p;
  for (p=str; *p!='\0'; p++) {}
  for (p--; p>=str; p--) {
    if ((*p == '\t') || (*p == ' ')) *p = '\0';
    else break;
  }
  return str;
}

/** Reads configuration from file. Only directives bound to a value are parsed,
 *  the others are ignored.
 */
void config::read_file() {

  std::ifstream file(file_name.c_str());

  if (!file) throw std::runtime_error("Can't read configuration file");

  char *dir;
  char *val;

  while ( file.getline(strbuf, AF_CONFIG_BUFSIZE) ) {

    dir = ltrim(strbuf);
    if ((*dir == '#') || (*dir == '\0')) continue;

    for (val=dir; *val!='\0'; val++) {
      if ((*val == '\t') || (*val == ' ')) {
        *val++ = '\0';
        break;
      }
    }

    val = rtrim(ltrim(val));

    printf("--{%s}-{%s}--\n", dir, val);

    // HERE: check if dirname is bound; elsewhere, continue;
  }

  file.close();

}
