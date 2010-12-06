/**
 * afQueueEntry.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afQueueEntry.h"

using namespace af;

/** Hash function from http://stackoverflow.com/questions/98153/whats-the-best-h
 *  ashing-algorithm-to-use-on-a-stl-string-when-using-hash-map
 */
hash_t queueEntry::hash(const char *s) {
  unsigned int h = 0;
  while (*s) h = h * 101 + *s++;
  return h;
}

/** Constructs a queue entry from the URL to process.
 */
queueEntry::queueEntry(const char *url) {
  main_url = url;
  main_url_hash = hash(main_url.c_str());
}
