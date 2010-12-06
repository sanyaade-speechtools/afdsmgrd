/**
 * afOpQueue.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * Queue entry for opQueue (unused for now, but may be useful in future).
 */

#ifndef AFQUEUEENTRY_H
#define AFQUEUEENTRY_H

#include <string>

namespace af {

  typedef unsigned int hash_t;

  class queueEntry {

    public:
      queueEntry(const char *url);
      //~queueEntry();
      //queueEntry(const queueEntry &src);
      const char *get_main_url() const { return main_url.c_str(); }
      hash_t get_hash() const { return main_url_hash; }
      static hash_t hash(const char *s);

    private:
      std::string main_url;
      std::string endp_url;
      hash_t main_url_hash;

  };

}

#endif // AFQUEUEENTRY_H
