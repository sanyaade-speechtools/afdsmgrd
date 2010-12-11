#include "afExtCmd.h"

using namespace af;

/**
 */
std::string extCmd::helper_path;
std::string extCmd::temp_path;
const char *extCmd::errf_pref = "err";
const char *extCmd::outf_pref = "out";
const char *extCmd::pidf_pref = "pid";

/** Constructor
 */
extCmd::extCmd(const char *_cmd, unsigned long _id) : cmd(_cmd), id(_id) {

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

  // Creates temporary files (lock)
  snprintf(strbuf, AF_EXTCMD_BUFSIZE, "%s/%s-%lu", temp_path.c_str(),
    pidf_pref, id);
  std::ofstream of(strbuf);
  of.close();

}

/** Spawns the program in background using the helper. Returns false if program
 *  was not spawned successfully.
 */
bool extCmd::run() {

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
  std::ifstream pidfile(strbuf);
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
 *  parses all the fields, space-separated. Fields must be unique.
 */
void extCmd::get_output() {

  snprintf(strbuf, AF_EXTCMD_BUFSIZE, "%s/%s-%lu",
    temp_path.c_str(), outf_pref, id);

  std::ifstream outfile(strbuf);

  while ( outfile.getline(strbuf, AF_EXTCMD_BUFSIZE) ) {
    printf("line={%s}\n", strbuf);
  }

  outfile.close();

}

/** Sets the helper path. Path must be a string allocated with malloc().
 */
void extCmd::set_helper_path(const char *path) {
  helper_path = path;
}

/** Sets the temporary directory. Path must be a string allocated with malloc().
 */
void extCmd::set_temp_path(const char *path) {
  temp_path = path;
  mkdir(path, S_IRWXU|S_IRGRP|S_IXGRP);
}
