/**
 * afDataSetList.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afDataSetList.h"

using namespace af;

/** The only constructor for the class. It takes as an argument a pointer to the
 *  dataset manager used for the datasets requests. The class does not own the
 *  dataset manager! The dataset manager is required to be non-null: a runtime
 *  exception is thrown elsewhere.
 */
dataSetList::dataSetList(TDataSetManagerFile *_ds_mgr) :
  ds_mgr(_ds_mgr), ds_inited(false), fi_inited(false) {
  if (!ds_mgr) throw std::runtime_error("Invalid dataset manager");
}

/** The destructor. It frees the memory taken by requests.
 */
dataSetList::~dataSetList() {
  free_datasets();
  free_files();
}

/** Frees the resources used by the former dataset manager and sets the new one.
 *  The dataset manager is required to be non-null: a runtime exception is
 *  thrown elsewhere.
 */
void dataSetList::set_dataset_mgr(TDataSetManagerFile *_ds_mgr) {
  if (!_ds_mgr) throw std::runtime_error("Invalid dataset manager");
  free_datasets();
  free_files();
  ds_mgr = _ds_mgr;
}

/** Initializes the list: this is the first function to call if you want to
 *  browse a list, and it is the function which actually performs the request to
 *  the dataset manager and takes memory.
 *
 *  All the memory allocations must be subsequently freed with the member
 *  free_datasets() function, or by deleting the instance.
 *
 *  This function is protected against erroneous double calls: if called a 2nd
 *  time, it does nothing.
 */
void dataSetList::fetch_datasets() {

  if (ds_inited) return;

  TMap *groups = ds_mgr->GetDataSets("/*/*", TDataSetManager::kReadShort);

  groups->SetOwnerKeyValue();  // important to avoid leaks!
  TIter gi(groups);
  TObjString *gn;

  while ((gn = dynamic_cast<TObjString *>(gi.Next()))) {

    TMap *users = dynamic_cast<TMap *>( groups->GetValue( gn->String() ) );
    users->SetOwnerKeyValue();
    TIter ui(users);
    TObjString *un;

    while ((un = dynamic_cast<TObjString *>(ui.Next()))) {

      TMap *dss = dynamic_cast<TMap *>( users->GetValue( un->String() ) );
      dss->SetOwnerKeyValue();
      TIter di(dss);
      TObjString *dn;

      while ((dn = dynamic_cast<TObjString *>(di.Next()))) {

        // No COMMON user/group mapping is done here...
        TString dsUri = TDataSetManager::CreateUri( gn->String(),
          un->String(), dn->String() );

        ds_list.push_back( new std::string(dsUri.Data()) );

      }
    }
  }

  delete groups;

  ds_inited = true;
  ds_cur_idx = -1;
}

/** Frees the memory taken by previously querying the dataset manager for a
 *  datasets list. It is safe to call it even if no previous request has been
 *  performed.
 */
void dataSetList::free_datasets() {
  if (!ds_inited) return;
  unsigned int sz = ds_list.size();
  for (unsigned int i=0; i<sz; i++)
    delete ds_list[i];
  ds_list.clear();
  ds_inited = false;
}

/** Rewind the list pointer to the first element without re-performing the
 *  dataset request. It is safe to call it even if no previous request has been
 *  performed.
 */
void dataSetList::rewind_datasets() {
  if (!ds_inited) return;
  ds_cur_idx = -1;
}

/** Gets the next dataset name in the list. Returns NULL if list is not inited
 *  or if last element was reached. Elsewhere it returns a pointer to a buffer
 *  that contains the dataset name: the class owns buffer's memory which is
 *  overwritten by the next call of next(), so if you want to manipulate or
 *  store the dataset name you must make a copy of the returned buffer. Returned
 *  data is not ordered to avoid performance issues.
 */
const char *dataSetList::next_dataset() {
  if ((!ds_inited) || (++ds_cur_idx >= ds_list.size())) return NULL;
  return ds_list[ds_cur_idx]->c_str();
}

/** Asks for the list of files (TFileInfo objs) for a given dataset name in
 *  current dataset manager. If ds_name is NULL then the last dataset name
 *  obtained via next_dataset() is used, if one. If reading of dataset fails for
 *  whatever reason, it returns false; if dataset was read successfully, it
 *  returns true. This function has to be called at the beginning of TFileInfos
 *  reading. It is safe to double call it - 2nd call does nothing (and returns
 *  true for success).
 *
 *  The filter argument tells the class to return only files that match some
 *  criteria (staged, corrupted, has event count...) via next_file() method.
 *  See header file for possible constants to combine via OR operator. Default
 *  is to show all files.
 */
bool dataSetList::fetch_files(const char *ds_name, unsigned short filter) {

  if (fi_inited) return true;

  if (!ds_name) {
    if ((ds_inited) && (ds_cur_idx < ds_list.size()))
      ds_cur_name = *ds_list[ds_cur_idx];
    else return false;
  }
  else ds_cur_name = ds_name;

  fi_coll = ds_mgr->GetDataSet(ds_cur_name.c_str());
  if (!fi_coll) return false;

  fi_iter = new TIter(fi_coll->GetList());
  fi_filter = filter;
  fi_inited = true;
  fi_curr = NULL;

  return true;
}

/** Frees the resources taken by the dataset list reading. This funcion must be
 *  called at the end of TFileInfos reading. It is safe to call it if
 *  fetch_files() has not been called yet: it does nothing in such a case.
 */
void dataSetList::free_files() {
  if (!fi_inited) return;
  delete fi_coll;
  delete fi_iter;
  fi_inited = false;
  fi_curr = NULL;
}

/** Resets the pointer so that the next call of next_file() will point to the
 *  first element in the list without the need to re-fetch it. It is safe to
 *  call it if fetch_files() has not been called yet: it does nothing in such a
 *  case.
 */
void dataSetList::rewind_files() {
  if (!fi_inited) return;
  fi_iter->Reset();
  fi_curr = NULL;
}

/** Returns a pointer to the next TFileInfo in the current dataset. If entry
 *  reading has not been prepared yet or if we reached the end of the list, NULL
 *  is returned instead.
 */
TFileInfo *dataSetList::next_file() {

  bool s, c, e;

  if (!fi_inited) return NULL;

  while (true) {
    fi_curr = dynamic_cast<TFileInfo *>(fi_iter->Next());
    if (fi_curr == NULL) break;

    TFileInfoMeta *meta = fi_curr->GetMetaData(NULL);
    e = ((meta) && (meta->GetEntries() >=0));

    s = fi_curr->TestBit(TFileInfo::kStaged);
    c = fi_curr->TestBit(TFileInfo::kCorrupted);

    if ((s && (fi_filter & AF_STAGED)) ||
        (!s && (fi_filter & AF_NOTSTAGED)) ||
        (c && (fi_filter & AF_CORRUPTED)) ||
        (!c && (fi_filter & AF_NOTCORRUPTED)) ||
        (e && (fi_filter & AF_HASEVENTS)) ||
        (!e && (fi_filter & AF_HASNOEVENTS))) {
      break;
    }
  }

  return fi_curr;  // may be NULL
}

/** Fetches the URL at the specified index from the currently selected
 *  TFileInfo. Index can either be from the beginning (if positive) or from the
 *  end (if negative); indexes start from 1 (or -1). If for any reason the URL
 *  can not be retireved (no TFileInfo selected, no such index...), NULL is
 *  returned.
 */
TUrl *dataSetList::get_url(int idx) {
  if (!fi_curr) return NULL;

  int nurls = fi_curr->GetNUrls();

  if (idx < 0) idx = nurls + 1 + idx;
  if ((idx <= 0) || (idx > nurls)) return NULL;

  TUrl *curl;

  fi_curr->ResetUrl();
  for (int i=1; i<=idx; i++) curl = fi_curr->NextUrl();

  fi_curr->ResetUrl();
  return curl;
}

/** Deletes all URLs except the last one from the currently selected entry.
 *
 *  Returns an error code of type ds_manip_err_t (see) which indicates if there
 *  is an error or not, and in the latter case it indicates if data has been
 *  changed or not.
 */
ds_manip_err_t dataSetList::del_urls_but_last() {

  if (!fi_curr) return ds_manip_err_fail;

  int nurls = fi_curr->GetNUrls();
  if ((nurls == 0) || (nurls == 1)) return ds_manip_err_ok_noop;
  else nurls--;

  std::vector<std::string *> lurls;
  bool all_ok = true;

  TUrl *curl;

  fi_curr->ResetUrl();
  for (int i=0; i<nurls; i++) {
    curl = fi_curr->NextUrl();
    if (!curl) {
      all_ok = false;
      break;
    }
    lurls.push_back( new std::string(curl->GetUrl()) );
  }

  unsigned int sz = lurls.size();
  for (unsigned int i=0; i<sz; i++) {
    if (all_ok) {
      if ( !(fi_curr->RemoveUrl(lurls[i]->c_str())) ) all_ok = false;
    }
    delete lurls[i];
  }

  return (all_ok ? ds_manip_err_ok_mod : ds_manip_err_fail);
}

/**
 */
bool dataSetList::save_dataset() {

  if (!fi_inited) return false;

  TString group;
  TString user;
  TString name;
  ds_mgr->ParseUri( ds_cur_name.c_str(), &group, &user, &name);

  int r = ds_mgr->WriteDataSet(group, user, name, fi_coll);

  af::log::info(af::log_level_low,
    "WriteDataSet() on group=%s user=%s name=%s returned %d", group.Data(),
    user.Data(), name.Data(), r);

  if (r != 0) return true;

  //if (ds_mgr->RegisterDataSet(ds_cur_name.c_str(), fi_coll, "O") == 0)
  //  return true;

  return false;
}
