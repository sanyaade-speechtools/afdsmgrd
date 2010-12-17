/**
 * afConfig.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afConfig.h"

using namespace af;

/** Constructor of the directive binding for numeric type int.
 */
cfg_binding::cfg_binding(long *_dest_ptr, long _def_val, long _min, long _max) :
  dest(_dest_ptr) {
  ctor_helper(_def_val, _min, _max);
  type = dir_type_int;
}

/** Constructor of the directive binding for text type.
 */
cfg_binding::cfg_binding(std::string *_dest_ptr, const char *_def_val) :
  dest(_dest_ptr) {
  type = dir_type_text;
  def_val.s = new std::string(_def_val);
  printf("cted, default value is {%s}\n", def_val.s->c_str());
  //std::string *def_val_ptr = new std::string(_def_val);
}

/** Destructor. It deletes the pointer to the string, if directive type is such.
 */
cfg_binding::~cfg_binding() {
  printf("dtor: %016lx\n", (unsigned long)dest);
  if (type == dir_type_text) delete def_val.s;
}

/** Constructor helper (templatized).
 */
template<typename T> void cfg_binding::ctor_helper(T _def_val, T _min, T _max) {
  *(T *)(&min) = _min;
  *(T *)(&max) = _max;
  *(T *)(&def_val) = _def_val;
}

/** Prints on stdout the directive type and the pointer.
 */
void cfg_binding::print() const {
  printf("type:");
  switch (type) {
    case dir_type_int:  printf("int");  break;
    case dir_type_uint: printf("uint"); break;
    case dir_type_real: printf("real"); break;
    case dir_type_text: printf("text"); break;
  }
  printf(" ptr:0x%016lx\n", (unsigned long)dest);
}

/** Checks if given value is inside limits: in this case assigns it to the
 *  pointer; elsewhere the default value is assigned. This works for the Int
 *  type (other specializations follow).
 */
void cfg_binding::check_assign(long value) const {
  if (inside_limits(value, min.i, max.i)) *(long *)(dest) = value;
  else *(long *)(dest) = def_val.i;
}

/** Same, but for Uint type.
 */
void cfg_binding::check_assign(unsigned long value) const {
  if (inside_limits(value, min.u, max.u)) *(unsigned long *)(dest) = value;
  else *(unsigned long *)(dest) = def_val.u;
}

/** Same, but for Real type.
 */
void cfg_binding::check_assign(double value) const {
  if (inside_limits(value, min.d, max.d)) *(double *)(dest) = value;
  else *(double *)(dest) = def_val.d;
}

/** Assign a string value.
 */
void cfg_binding::assign(const char *value) const {
  *(std::string *)(dest) = value;
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/** Constructor.
 */
config::config(const char *config_file) : file_name(config_file) {}

/** Destructor. It destructs every pointer to cfg_binding: these directives are
 *  saved as pointers to avoid multiple and useless ctor/copy ctor/dtor calls
 *  when pushing in the list.
 */
config::~config() {
  printf("config dtor\n");
  for (conf_dirs_iter_t it=directives.begin(); it!=directives.end(); it++)
    delete (*it).second;
  directives.clear();
}

/** Prints fields (used for debug).
 */
void config::print_bindings() {
  for (conf_dirs_iter_t it=directives.begin(); it!=directives.end(); it++) {
    printf("%s: ", (*it).first.c_str());
    (*it).second->print();
  }
}

/** Binds an "integer" directive to a pointer.
 */
void config::bind_int(const char *dir_name, long *dest_ptr, long def_val,
  long min_eq, long max_eq) {
  cfg_binding *binding = new cfg_binding(dest_ptr, def_val, min_eq, max_eq);
  directives.insert( dir_t(dir_name, binding) );
}

/** Binds a "text" directive to a STL string pointer.
 */
void config::bind_text(const char *dir_name, std::string *dest_ptr,
  const char *def_val) {
  cfg_binding *binding = new cfg_binding(dest_ptr, def_val);
  directives.insert( dir_t(dir_name, binding) );
}

/** Returns a pointer to the first non-blank char of the input string. The
 *  original string is not modified.
 */
char *config::ltrim(char *str) const {
  char *tr;
  for (tr=str; *tr!='\0'; tr++) {
    if ((*tr != '\t') && (*tr != ' ')) break;
  }
  return tr;
}

/** Truncates the string at the first blank character. The original string is
 *  modified.
 */
char *config::rtrim(char *str) const {
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

    conf_dirs_iter_t it = directives.find(dir);
    if (it == directives.end()) continue;  // directive does not exist

    const cfg_binding &binding = *((*it).second);

    val = rtrim(ltrim(val));
    printf("--{%s}-{%s}-{0x%016lx}-\n", dir, val,
      (unsigned long)binding.get_dest());

    switch (binding.get_type()) {

      case dir_type_int:
        binding.check_assign( strtol(val, NULL, 0) );
      break;

      case dir_type_uint:
        binding.check_assign( strtoul(val, NULL, 0) );
      break;

      case dir_type_real:
        binding.check_assign( strtod(val, NULL) );
      break;

      case dir_type_text:
        binding.assign(val);
      break;

    }

  }

  file.close();

}
