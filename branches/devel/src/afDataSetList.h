/**
 * afDataSetList.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * This class implements a list of datasets and it is a wrapper around calls on
 * the TDataSetManager class (and subclasses). The class is defined inside the
 * namespace "af".
 */

#ifndef AFDATASETLIST_H
#define AFDATASETLIST_H

#define AF_DSNAME_BUFSIZE 300

#include <stdexcept>

#include <TDataSetManager.h>
#include <TObjString.h>

namespace af {

  class dataSetList {

    public:
      dataSetList(TDataSetManager *_ds_mgr);
      virtual ~dataSetList();
      void fetch();
      void set_dataset_mgr(TDataSetManager *_ds_mgr);
      void free();
      void rewind();
      const char *next();

    private:
      TDataSetManager *ds_mgr;
      TMap            *ds_list;
      TIter           *ds_iter;
      TObjString      *ds_os_name;
      char             buf[AF_DSNAME_BUFSIZE];
      bool             inited;

  };

}

#endif // AFDATASETLIST_H
