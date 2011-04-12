/**
 * afNotify.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * This class acts as a wrapper around an external generic notification library
 * with expected function names in a flattened namespace.
 */

#ifndef AFNOTIFY_H
#define AFNOTIFY_H

#include <dlfcn.h>

#include <exception>

#include "afConfig.h"
#include "afLog.h"

namespace af {

  /** The main class of this file.
   */
  class notify {

    typedef notify *(*create_t)(config &);
    typedef void (*destroy_t)(notify *);

    public:

      /** Notification functions
       */
      /*
      void notify_resources(unsigned int vsize_kib, unsigned int size_kib);
      void send();
      */

      /** Functions that *must* be implemented by subclasses (pure virtual).
       */
      virtual void dataset(const char *ds_name, int n_files, int n_staged,
        int n_corrupted, const char *tree_name, int n_events,
        unsigned long long total_size_bytes) = 0;
      virtual const char *whoami() = 0;
      virtual void init() = 0;

      /** Plugin creation and destruction.
       */
      notify(config &_cfg) : cfg(_cfg) {};
      static notify *load(const char *libpath, config &cfg);
      static void unload(notify *notif);
      virtual ~notify() {}

    protected:
      config   &cfg;

    private:
      void     *lib_handler;
      create_t  lib_create;
      destroy_t lib_destroy;

  };

}

#endif // AFNOTIFY_H
