/**
 * afDataSetList.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * This class implements a list of datasets and it is a wrapper around calls on
 * the TDataSetManagerFile class. The class is defined inside the namespace af.
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

#include "afLog.h"

#include <stdexcept>

#include <TDataSetManagerFile.h>
#include <TFileCollection.h>
#include <THashList.h>
#include <TFileInfo.h>
#include <TObjString.h>

namespace af {

  /** Dataset manipulation errors.
   */
  typedef enum {
    ds_manip_err_ok_mod = 0,   // success, data modified
    ds_manip_err_ok_noop = 1,  // success, data not modified
    ds_manip_err_fail = 2      // failure
  } ds_manip_err_t;

  class dataSetList {

    public:

      dataSetList(TDataSetManagerFile *_ds_mgr);
      virtual ~dataSetList();

      // Browse dataset names
      void fetch_datasets();
      const char *next_dataset();
      void rewind_datasets();
      void free_datasets();
      bool save_dataset();
      // TODO: void set_toggle_suid();

      // Browse entries of a dataset
      bool fetch_files(const char *ds_name = NULL,
        unsigned short filter = AF_EVERYFILE);
      TFileInfo *next_file();
      void rewind_files();
      void free_files();

      // Browse and manipulate URLs of a TFileInfo (entry)
      TUrl *get_url(int idx);
      ds_manip_err_t del_urls_but_last();

      void set_dataset_mgr(TDataSetManagerFile *_ds_mgr);
      inline TDataSetManagerFile *get_dataset_mgr() const { return ds_mgr; };

    private:

      TDataSetManagerFile        *ds_mgr;
      std::vector<std::string *>  ds_list;
      int                         ds_cur_idx;
      std::string                 ds_cur_name;
      bool                        ds_inited;

      TFileCollection            *fi_coll;
      TIter                      *fi_iter;
      TFileInfo                  *fi_curr;
      bool                        fi_inited;
      unsigned short              fi_filter;

  };

}

#endif // AFDATASETLIST_H
