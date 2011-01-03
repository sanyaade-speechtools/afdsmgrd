/**
 * afExtCmd.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afExtCmd.h"

using namespace af;

/** Static values used by every instance of this class.
 */
std::string extCmd::helper_path;
std::string extCmd::temp_path;
const char *extCmd::errf_pref = "err";
const char *extCmd::outf_pref = "out";
const char *extCmd::pidf_pref = "pid";

/** Constructor. The instance_id is chosen automatically if not given or if
 *  equal to zero.
 */
extCmd::extCmd(const char *exec_cmd, unsigned long instance_id) :
  cmd(exec_cmd), id(instance_id), ok(false), already_started(false), pid(-1) {

  if ((helper_path.empty()) || (temp_path.empty()))
    throw std::runtime_error("Helper path and temp path must be defined");

  // Choose a random id (avoiding collisions)
  if (id == 0) {
    struct stat file_info;
    while (true) {
      if ((id = rand()) == 0) continue;
      snprintf(strbuf, AF_EXTCMD_BUFSIZE, "%s/%s-%lu", temp_path.c_str(),
        pidf_pref, id);
      if (stat(strbuf, &file_info) != 0) break;
    }
  }

  // Creates temporary empty pidfile
  snprintf(strbuf, AF_EXTCMD_BUFSIZE, "%s/%s-%lu", temp_path.c_str(),
    pidf_pref, id);
  std::ofstream of(strbuf);
  of.close();

}

/** Spawns the program in background using the helper. Returns false if program
 *  was not spawned successfully.
 */
bool extCmd::run() {

  if (already_started) return false;
  already_started = true;

  // Assembles the command line
  snprintf(strbuf, AF_EXTCMD_BUFSIZE,
    "\"%s\" -p \"%s/%s-%lu\" -o \"%s/%s-%lu\" -e \"%s/%s-%lu\" %s",
    helper_path.c_str(),
    temp_path.c_str(), pidf_pref, id,
    temp_path.c_str(), outf_pref, id,
    temp_path.c_str(), errf_pref, id,
    cmd.c_str());

  // Runs the program
  int r = system(strbuf);
  if (r != 0) return false;

  // Gets the pid
  snprintf(strbuf, AF_EXTCMD_BUFSIZE, "%s/%s-%lu",
    temp_path.c_str(), pidf_pref, id);
  struct stat pidfile_info;
  while (true) {
    usleep(1000);  // sleeps 1/1000 of a second
    if ((stat(strbuf, &pidfile_info) == 0) && (pidfile_info.st_size != 0))
      break;
  }
  std::ifstream pidfile(strbuf);  // TODO: throw except if pidf not readable
  pidfile >> pid;
  pidfile.close();

  return true;
}

/** Checks if the spawned program is still running using the trick of sending
 *  the signal 0 (noop) to the process.
 */
bool extCmd::is_running() {
  if (kill(pid, 0) == -1) return false;
  else return true;
}

/** Searches for a line on stdout that begins either with FAIL or with OK and
 *  parses all the fields, space-separated. If the program did not give any
 *  output, a FAIL status is triggered by default. Fields must be unique.
 */
void extCmd::get_output() {

  const char *delims = " \t";
  bool found = false;

  if (!fields_map.empty()) fields_map.clear();

  snprintf(strbuf, AF_EXTCMD_BUFSIZE, "%s/%s-%lu",
    temp_path.c_str(), outf_pref, id);

  std::ifstream outfile(strbuf);

  while ( outfile.getline(strbuf, AF_EXTCMD_BUFSIZE) ) {
    //printf("line={%s}\n", strbuf);
    char *tok = strtok(strbuf, delims);

    if (( strcmp(tok, "OK") == 0 ) || ( strcmp(tok, "FAIL") == 0 )) {

      bool expect_key = false;
      std::string key;
      std::string val;

      if (*tok == 'O') ok = true;
      else ok = false;

      while ((tok = strtok(NULL, delims))) {
        //printf("  tok={%s}\n", tok);
        if (expect_key) {
          size_t len = strlen(tok);
          if (tok[len-1] == ':') {
            tok[len-1] = '\0';
            key = tok;
            expect_key = false;
          }
        }
        else {
          val = tok;
          //printf("    pair={%s},{%s}\n", key.c_str(), val.c_str());
          // See http://www.cplusplus.com/reference/stl/map/insert/
          fields_map.insert( key_val_t(key, val) );
          expect_key = true;
        }
      }

      found = true;
      break;

    }
  }

  outfile.close();

  if (!found) ok = false;

}

/** Gets a field from output formatted as an unsigned integer. 0 is returned if
 *  field does not exist or it is not a number. The base is guessed from the
 *  number prefix (i.e., 0 means octal and 0x means hex): for more information
 *  see http://www.cplusplus.com/reference/clibrary/cstdlib/strtoul/.
 */
unsigned long extCmd::get_field_uint(const char *key) {
  const char *strval = get_field_text(key);
  if (!strval) return 0L;
  return strtoul(strval, NULL, 0);
}

/** Gets a field from output formatted as a signed integer. 0 is returned if
 *  field does not exist or it is not a number. The base is guessed from the
 *  number prefix (i.e., 0 means octal and 0x means hex): for more information
 *  see http://www.cplusplus.com/reference/clibrary/cstdlib/strtol/.
 */
long extCmd::get_field_int(const char *key) {
  const char *strval = get_field_text(key);
  if (!strval) return 0L;
  return strtol(strval, NULL, 0);
}

/** Gets a field from output formatted as a real (floating point). 0.0 is
 *  returned if field does not exist or it is not a number. For more information
 *  see http://www.cplusplus.com/reference/clibrary/cstdlib/strtod/.
 */
double extCmd::get_field_real(const char *key) {
  const char *strval = get_field_text(key);
  if (!strval) return 0.0;
  return strtod(strval, NULL);
}

/** Gets a field from output as a string. A NULL pointer is returned if field
 *  does not exist. The returned pointer belongs to the class instance.
 */
const char *extCmd::get_field_text(const char *key) {
  fields_iter_t keyval = fields_map.find(key);
  if (keyval == fields_map.end()) return NULL;
  return (*keyval).second.c_str();
}

/** Prints out key/value pairs gathered during latest get_output() call; used
 *  mostly for debug.
 */
void extCmd::print_fields() {
  for (fields_iter_t it=fields_map.begin(); it!=fields_map.end(); it++)
    printf("{%s}={%s}\n", (*it).first.c_str(), (*it).second.c_str());
}

/** Sets the helper path. The given string is copied in an internal buffer.
 */
void extCmd::set_helper_path(const char *path) {
  helper_path = path;
}

/** Sets the temporary directory and creates it. The given string is copied in
 *  an internal buffer.
 */
void extCmd::set_temp_path(const char *path) {
  temp_path = path;
  mkdir(path, S_IRWXU|S_IRGRP|S_IXGRP);
}
