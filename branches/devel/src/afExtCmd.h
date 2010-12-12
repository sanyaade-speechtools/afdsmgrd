/**
 * afExtCmd.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * An instance of this class represents and manages an external program that
 * independently runs in background and returns its status on stdout on a single
 * line with separated fields.
 *
 * The class is capable of checking if the program is still running and parses
 * the output in memory.
 */

#ifndef AFEXTCMD_H
#define AFEXTCMD_H

#define AF_EXTCMD_BUFSIZE 1000

#include <map>
#include <string>
#include <fstream>
#include <stdio.h>
#include <stdexcept>
#include <signal.h>
#include <sys/stat.h>

namespace af {

  typedef std::map<std::string,std::string> fields_t;
  typedef std::pair<std::string,std::string> key_val_t;
  typedef fields_t::iterator fields_iter_t;

  class extCmd {

    public:
      extCmd(const char *command, unsigned long id = 0);
      //virtual ~extCmd();
      bool run();
      bool is_running();
      pid_t get_pid() { return pid; };
      void get_output();
      void print_fields();
      bool is_ok() { return ok; };

      static void set_helper_path(const char *path);
      static void set_temp_path(const char *path);

    private:
      char strbuf[AF_EXTCMD_BUFSIZE];
      pid_t pid;
      unsigned long id;
      std::string cmd;
      fields_t fields_map;
      bool ok;

      static std::string helper_path;
      static std::string temp_path;
      static const char *errf_pref;
      static const char *outf_pref;
      static const char *pidf_pref;

  };

};

#endif // AFEXTCMD_H
