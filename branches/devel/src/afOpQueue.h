/**
 * afOpQueue.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * A queue that holds the files to be processed with their status. It is
 * implemented as an SQLite database for holding large amounts of data without
 * eating up the memory.
 */

#ifndef AFOPQUEUE_H
#define AFOPQUEUE_H

#include <stdio.h>
#include <string.h>

#include "sqlite3.h"

#include <stdexcept>

#define AF_OPQUEUE_BUFSIZE 1000

namespace af {

  typedef enum { qstat_queue   = 'Q',
                 qstat_running = 'R',
                 qstat_success = 'D',
                 qstat_failed  = 'F' } qstat_t;

  /** In-memory representation of an entry of the opQueue. It is a very simple
   *  class with direct access to members (no getters or setters).
   */
  class queueEntry {

    public:
      const char *main_url;
      const char *endp_url;
      const char *tree_name;
      unsigned long n_events;
      unsigned int n_failures;
      unsigned long size_bytes;
      qstat_t status;
      void print() const {
        printf("main_url:   %s\n",  main_url);
        printf("endp_url:   %s\n",  endp_url);
        printf("tree_name:  %s\n",  tree_name);
        printf("n_events:   %lu\n", n_events);
        printf("n_failures: %u\n",  n_failures);
        printf("size_bytes: %lu\n", size_bytes);
      };

  };

  /** The actual operation queue.
   */
  class opQueue {

    public:
      opQueue(unsigned int max_failures = 0);
      virtual ~opQueue();
      bool insert(const char *url);
      int flush();
      bool set_status(const char *url, qstat_t qstat);
      void set_max_failures(unsigned int max_failures) {
        fail_threshold = max_failures;
      };
      bool failed(const char *url);
      void arbitrary_query(const char *query);
      void dump();
      bool exists(const char *url);
      const queueEntry *get_entry(const char *url);

    private:
      sqlite3 *db;
      char strbuf[AF_OPQUEUE_BUFSIZE];
      char *sql_err;
      static int query_callback(void *, int argc, char *argv[], char **colname);
      unsigned long last_queue_rowid;
      unsigned int fail_threshold;

      sqlite3_stmt *query_exists;
      sqlite3_stmt *query_get_entry;

      queueEntry qentry_buf;
  };

};

#endif // AFOPQUEUE_H
