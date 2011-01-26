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

#define AF_DATASETLIST_BUFSIZE 400

#define AF_STAGED        1
#define AF_NOTSTAGED     2
#define AF_CORRUPTED     4
#define AF_NOTCORRUPTED  8
#define AF_HASEVENTS    16
#define AF_HASNOEVENTS  32
#define AF_EVERYFILE    63

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
      bool fetch_files(const char *ds_name = NULL,
        unsigned short filter = AF_EVERYFILE);
      void free_files();
      void rewind_files();
      TFileInfo *next_file();

      void set_dataset_mgr(TDataSetManager *_ds_mgr);
      inline TDataSetManager *get_dataset_mgr() const { return ds_mgr; };

    private:

      TDataSetManager *ds_mgr;

      TMap            *ds_list;
      TIter           *ds_iter;
      TObjString      *ds_objstr_name;
      bool             ds_inited;
      char             ds_curr[AF_DATASETLIST_BUFSIZE];

      TFileCollection *fi_coll;
      TIter           *fi_iter;
      TFileInfo       *fi_curr;
      bool             fi_inited;
      unsigned short   fi_filter;

  };

}

#endif // AFDATASETLIST_H
