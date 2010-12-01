/**
 * adfasdf -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * Prints out the list of datasets.
 *
 */
#ifndef AFDATASETLIST_H
#define AFDATASETLIST_H

#define AF_DSNAME_BUFSIZE 200

#include <stdexcept>

#include <TDataSetManager.h>
#include <TObjString.h>

namespace af {

  class dataSetList {

    public:
      dataSetList(TDataSetManager *dsm);
      ~dataSetList();
      void init();
      void set_dataset_mgr(TDataSetManager *dsm);
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
