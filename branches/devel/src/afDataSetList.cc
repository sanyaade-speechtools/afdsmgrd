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
dataSetList::dataSetList(TDataSetManager *_ds_mgr) :
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
void dataSetList::set_dataset_mgr(TDataSetManager *_ds_mgr) {
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

  ds_list = ds_mgr->GetDataSets("/*/*");
  if (!ds_list) throw std::runtime_error("Can't obtain the list of datasets");

  ds_list->SetOwnerKeyValue();
  ds_iter = new TIter(ds_list);
  *ds_curr = '\0';

  ds_inited = true;
}

/** Frees the memory taken by previously querying the dataset manager for a
 *  datasets list. It is safe to call it even if no previous request has been
 *  performed.
 */
void dataSetList::free_datasets() {
  if (!ds_inited) return;
  delete ds_list;
  delete ds_iter;
  *ds_curr = '\0';
  ds_inited = false;
}

/** Rewind the list pointer to the first element without re-performing the
 *  dataset request. It is safe to call it even if no previous request has been
 *  performed.
 */
void dataSetList::rewind_datasets() {
  if (!ds_inited) return;
  ds_iter->Reset();
  *ds_curr = '\0';
}

/** Gets the next dataset name in the list. Returns NULL if list is not inited
 *  or if last element was reached. Elsewhere it returns a pointer to a buffer
 *  that contains the dataset name: the class owns buffer's memory which is
 *  overwritten by the next call of next(), so if you want to manipulate or
 *  store the dataset name you must make a copy of the returned buffer. Returned
 *  data is not ordered to avoid performance issues.
 */
const char *dataSetList::next_dataset() {
  if (!ds_inited) return NULL;

  ds_objstr_name = dynamic_cast<TObjString *>(ds_iter->Next());
  if (!ds_objstr_name) {
    *ds_curr = '\0';
    return NULL;
  }

  strncpy(ds_curr, ds_objstr_name->String().Data(), AF_DATASETLIST_BUFSIZE);
  return ds_curr;
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
    if ((ds_inited) && (*ds_curr != '\0')) ds_name = ds_curr;
    else return false;
  }

  fi_coll = ds_mgr->GetDataSet(ds_name);
  if (!fi_coll) return false;

  fi_iter = new TIter(fi_coll->GetList());
  fi_filter = filter;
  fi_inited = true;

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
}

/** Resets the pointer so that the next call of next_file() will point to the
 *  first element in the list without the need to re-fetch it. It is safe to
 *  call it if fetch_files() has not been called yet: it does nothing in such a
 *  case.
 */
void dataSetList::rewind_files() {
  if (!fi_inited) return;
  fi_iter->Reset();
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
