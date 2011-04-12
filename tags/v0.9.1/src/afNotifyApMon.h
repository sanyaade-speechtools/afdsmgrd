/**
 * afNotifyApMon.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * Plugin that allows for MonALISA notification of datasets and used resources.
 */

#ifndef AFNOTIFYAPMON_H
#define AFNOTIFYAPMON_H

#include <stdio.h>

#include "ApMon.h"

#include "afNotify.h"

namespace af {

  class notifyApMon : public notify {

    typedef struct {
      char *host[1];
      char *pwd[1];
      int   port[1];
    } apmon_params_t;

    public:
      notifyApMon(config &_cfg);
      virtual const char *whoami();
      virtual void dataset(const char *ds_name, int n_files, int n_staged,
        int n_corrupted, const char *tree_name, int n_events,
        unsigned long long total_size_bytes);
      virtual void init();
      virtual ~notifyApMon();

      static void config_apmonurl_callback(const char *dir_name,
        const char *dir_val, void *args);

    private:
      ApMon          *apmon;
      void           *apmon_pool;
      apmon_params_t  apmon_params;
      void           *cbk_args[2];

  };

}

#endif // AFNOTIFYAPMON_H
