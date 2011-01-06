/**
 * afOpQueue.h -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * A queue that holds the files to be processed with their status. It is
 * implemented as a SQLite database for holding large amounts of data without
 * eating up the memory.
 */

#ifndef AFOPQUEUE_H
#define AFOPQUEUE_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "sqlite3.h"

#include <stdexcept>

#define AF_NULL_STR(STR) ((STR) ? (STR) : "#null#")
#define AF_OPQUEUE_BUFSIZE 1000

namespace af {

  typedef enum { qstat_queue   = 'Q',
                 qstat_running = 'R',
                 qstat_success = 'D',
                 qstat_failed  = 'F' } qstat_t;

  /** In-memory representation of an entry of the opQueue. It can own its
   *  members or not.
   */
  class queueEntry {

    public:

      // Constructors/destructors
      queueEntry(bool _own);
      queueEntry(const char *_main_url, const char *_endp_url,
        const char *_tree_name, unsigned long _n_events,
        unsigned int _n_failures, unsigned long _size_bytes, bool _own);
      virtual ~queueEntry();
    
      // Getters
      inline const char *get_main_url() const { return main_url; };
      inline const char *get_endp_url() const { return endp_url; };
      inline const char *get_tree_name() const { return tree_name; };
      inline unsigned long get_n_events() const { return n_events; };
      inline unsigned int get_n_failures() const { return n_failures; };
      inline unsigned long get_size_bytes() const { return size_bytes; };
      inline qstat_t get_status() const { return status; };

      // Setters
      inline void set_main_url(const char *_main_url);
      inline void set_endp_url(const char *_endp_url);
      inline void set_tree_name(const char *_tree_name);
      inline void set_n_events(unsigned long _n_events) {
        n_events = _n_events; };
      inline void set_n_failures(unsigned int _n_failures) {
        n_failures = _n_failures;
      };
      inline void set_size_bytes(unsigned long _size_bytes) {
        size_bytes = _size_bytes;
      };
      inline void set_status(qstat_t _status) { status = _status; }

      void print() const;
      void reset();

    private:

      void set_str(char **dest, const char *src);

      bool own;
      char *main_url;
      char *endp_url;
      char *tree_name;
      unsigned long n_events;
      unsigned int n_failures;
      unsigned long size_bytes;
      qstat_t status;

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
      //bool exists(const char *url);
      const queueEntry *get_full_entry(const char *url);
      const queueEntry *get_status(const char *url);
      const queueEntry *get_entry(const char *url);

    private:
      sqlite3 *db;
      char strbuf[AF_OPQUEUE_BUFSIZE];
      char *sql_err;
      static int query_callback(void *, int argc, char *argv[], char **colname);
      unsigned long last_queue_rowid;
      unsigned int fail_threshold;

      //sqlite3_stmt *query_exists;
      sqlite3_stmt *query_get_full_entry;
      sqlite3_stmt *query_get_status;

      queueEntry qentry_buf;
  };

};

#endif // AFOPQUEUE_H
