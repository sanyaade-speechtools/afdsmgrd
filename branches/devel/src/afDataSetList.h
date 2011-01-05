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
#include <TFileCollection.h>
#include <THashList.h>
#include <TFileInfo.h>
#include <TObjString.h>

namespace af {

  class dataSetList {

    public:

      dataSetList(TDataSetManager *_ds_mgr);
      virtual ~dataSetList();

      // Browse dataset names
      void fetch_datasets();
      void free_datasets();
      void rewind_datasets();
      const char *next_dataset();

      // Browse entries of a dataset
      bool fetch_files(const char *ds_name = NULL);
      void free_files();
      void rewind_files();
      TFileInfo *next_file();

      void set_dataset_mgr(TDataSetManager *_ds_mgr);

    private:

      TDataSetManager *ds_mgr;

      TMap            *ds_list;
      TIter           *ds_iter;
      TObjString      *ds_objstr_name;
      bool             ds_inited;
      char             ds_curr[AF_DSNAME_BUFSIZE];

      TFileCollection *fi_coll;
      TIter           *fi_iter;
      TFileInfo       *fi_curr;
      bool             fi_inited;

  };

}

#endif // AFDATASETLIST_H
