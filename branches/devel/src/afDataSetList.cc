/**
 * afDataSetList.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * This class implements a list of datasets and it is a wrapper around calls on
 * the TDataSetManager class (and subclasses). The class is defined inside the
 * namespace "af".
 */

#include "afDataSetList.h"

using namespace af;

/** The only constructor for the class. It takes as an argument a pointer to the
 *  dataset manager used for the datasets requests. The class does not own the
 *  dataset manager!
 */
dataSetList::dataSetList(TDataSetManager *dsm) : ds_mgr(dsm), inited(false) {
  if (!dsm) throw std::runtime_error("Invalid dataset manager");
}

/** The destructor. It frees the memory taken by requests.
 */
dataSetList::~dataSetList() {
  free();
}

/** Frees the resources used by the former dataset manager and sets the new one.
 */
void dataSetList::set_dataset_mgr(TDataSetManager *dsm) {
  free();
  ds_mgr = dsm;
}

/** Initializes the list: this is the first function to call if you want to
 *  browse a list, and it is the function which actually performs the request to
 *  the dataset manager and takes memory.
 *
 *  All the memory allocations must be subsequently freed with the member free()
 *  function, or by destroying the class.
 */
void dataSetList::init() {
  if (inited) return;

  ds_list = ds_mgr->GetDataSets("/*/*");
  if (!ds_list) throw std::runtime_error("Can't obtain the list of datasets");

  ds_list->SetOwnerKeyValue();
  ds_iter = new TIter(ds_list);

  inited = true;
}

/** Frees the memory taken by previously querying the dataset manager for a
 *  datasets list.
 */
void dataSetList::free() {
  if (!inited) return;
  delete ds_list;
  delete ds_iter;
  inited = false;
}

/** Rewind the list pointer to the first element without re-performing the
 *  dataset request.
 */
void dataSetList::rewind() {
  if (!inited) return;
  ds_iter->Reset();
}

/** Gets the next dataset name in the list. Returns NULL if list is not inited
 *  or if last element was reached. Elsewhere it returns a pointer to a buffer
 *  that contains the dataset name: the class owns buffer's memory which is
 *  overwritten by the next call of next(), so if you want to manipulate or
 *  store the dataset name you must make a copy of the returned buffer.
 */
const char *dataSetList::next() {
  if (!inited) return NULL;

  ds_os_name = dynamic_cast<TObjString *>(ds_iter->Next());
  if (!ds_os_name) return NULL;

  strncpy(buf, ds_os_name->String().Data(), AF_DSNAME_BUFSIZE);
  return buf;
}
