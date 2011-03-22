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
 * the output, made of fields and values, in memory.
 */

#ifndef AFEXTCMD_H
#define AFEXTCMD_H

#define AF_EXTCMD_BUFSIZE 1000

#include "afLog.h"

#include <map>
#include <string>
#include <fstream>
#include <stdexcept>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/stat.h>

namespace af {

  typedef std::map<std::string,std::string> fields_t;
  typedef std::pair<std::string,std::string> key_val_t;
  typedef fields_t::const_iterator fields_iter_t;

  class extCmd {

    public:

      extCmd(const char *command, unsigned int id = 0);
      //virtual ~extCmd();
      bool run();
      bool is_running();
      pid_t get_pid() { return pid; };
      void get_output();
      void print_fields(bool log = false);
      bool is_ok() { return ok; };
      unsigned int get_id() { return id; };

      unsigned long get_field_uint(const char *key);
      long get_field_int(const char *key);
      double get_field_real(const char *key);
      const char *get_field_text(const char *key);

      static void set_helper_path(const char *path);
      static void set_temp_path(const char *path);
      static const char *get_helper_path() { return helper_path.c_str(); };
      static const char *get_temp_path() { return temp_path.c_str(); };

    private:

      char strbuf[AF_EXTCMD_BUFSIZE];
      pid_t pid;
      unsigned int id;
      std::string cmd;
      fields_t fields_map;
      bool ok;
      bool already_started;

      static std::string helper_path;
      static std::string temp_path;
      static const char *errf_pref;
      static const char *outf_pref;
      static const char *pidf_pref;

  };

};

#endif // AFEXTCMD_H
