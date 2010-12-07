/**
 * afOpQueue.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * A queue that holds the files to be processed with their status. It is
 * implemented as an hash table for faster retrieval.
 */

#ifndef AFOPQUEUE_H
#define AFOPQUEUE_H

#include <stdio.h>
#include <string.h>

#include "sqlite3.h"

#include <stdexcept>

#define AF_STRBUFSIZE 1000

namespace af {

  typedef enum { qstat_queue   = 'Q',
                 qstat_running = 'R',
                 qstat_success = 'D',
                 qstat_failed  = 'F' } qstat_t;

  class opQueue {

    public:
      opQueue(unsigned int max_failures = 0);
      virtual ~opQueue();
      bool insert(const char *url);
      int flush();
      bool set_status(const char *url, qstat_t qstat);
      bool failed(const char *url);
      void arbitrary_query(const char *query);
      void dump();

    private:
      sqlite3 *db;
      char strbuf[AF_STRBUFSIZE];
      char *sql_err;
      static int query_callback(void *, int argc, char *argv[], char **colname);
      unsigned long last_queue_rowid;
      unsigned int fail_threshold;

  };

};

#endif // AFOPQUEUE_H
