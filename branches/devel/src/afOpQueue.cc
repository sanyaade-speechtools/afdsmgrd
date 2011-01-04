/**
 * afOpQueue.cc -- by Dario Berzano <dario.berzano@gmail.com>
 *
 * This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
 *
 * See header file for a description of the class.
 */

#include "afOpQueue.h"

using namespace af;

////////////////////////////////////////////////////////////////////////////////
// Member functions for the af::queueEntry class
////////////////////////////////////////////////////////////////////////////////

/** Default constructor. Constructs an empty instance of this class with the
 *  defined ownership.
 */
queueEntry::queueEntry(bool _own) : main_url(NULL), endp_url(NULL),
  tree_name(NULL), n_events(0L), n_failures(0), size_bytes(0L),
  status(qstat_queue), own(_own) {};

/** Constructor that assigns passed values to the members. The _own parameter
 *  decides if this class should dispose the strings when destroying. NULL
 *  values for strings are permitted and properly dealt with.
 */
queueEntry::queueEntry(const char *_main_url, const char *_endp_url,
  const char *_tree_name, unsigned long _n_events, unsigned int _n_failures,
  unsigned long _size_bytes, bool _own) :
  main_url(NULL), endp_url(NULL), tree_name(NULL), n_events(_n_events),
  n_failures(_n_failures), size_bytes(_size_bytes), status(qstat_queue),
  own(_own) {
  set_str(&main_url, _main_url);
  set_str(&endp_url, _endp_url);
  set_str(&tree_name, _tree_name);
};

/** Destructor. Frees the space of owned strings.
 */
queueEntry::~queueEntry() {
  if (own) {
    if (main_url) free(main_url);
    if (endp_url) free(endp_url);
    if (tree_name) free(tree_name);
  }
}

/** Resets the data members to initial values.
 */
void queueEntry::reset() {
  n_events = 0L;
  n_failures = 0;
  size_bytes = 0L;
  status = qstat_queue;
  set_main_url(NULL);
  set_endp_url(NULL);
  set_tree_name(NULL);
}

/** Private auxiliary function to assign a value to a string depending on the
 *  ownership bit. NULL values for strings to assign are allowed and properly
 *  dealt with.
 */
void queueEntry::set_str(char **dest, const char *src) {
  if (!own) *dest = (char *)src;
  else {
    if (src) {
      *dest = (char *)realloc(*dest, strlen(src)+1);  // TODO: bad alloc!
      strcpy(*dest, src);
    }
    else {
      if (*dest) free(*dest);
      *dest = NULL;
    }
  }
}

/** Setter for main_url. See set_str().
 */
void queueEntry::set_main_url(const char *_main_url) {
  set_str(&main_url, _main_url);
};

/** Setter for endp_url. See set_str().
 */
void queueEntry::set_endp_url(const char *_endp_url) {
  set_str(&endp_url, _endp_url);
};

/** Setter for tree_name. See set_str().
 */
void queueEntry::set_tree_name(const char *_tree_name) {
  set_str(&tree_name, _tree_name);
};

/** Debug function to print on stdout the members of this class.
 */
void queueEntry::print() const {
  printf("is owner:   %s\n", (own ? "yes" : "no"));
  printf("main_url:   %s\n", AF_NULL_STR(main_url));
  printf("endp_url:   %s\n", AF_NULL_STR(endp_url));
  printf("tree_name:  %s\n", AF_NULL_STR(tree_name));
  printf("n_events:   %lu\n", n_events);
  printf("n_failures: %u\n", n_failures);
  printf("size_bytes: %lu\n", size_bytes);
  printf("status:     %c\n", status);
};

////////////////////////////////////////////////////////////////////////////////
// Member functions for the af::opQueue class
////////////////////////////////////////////////////////////////////////////////

/** Queue constructor: it associates the queue to a SQLite object, and it
 *  creates the database in memory. SQLite takes care of creating (and
 *  immediately unlinking) a swap file for it.
 *
 *  The argument max_failures sets the maximum number of failures before
 *  removing ("flushing") the element from the queue. Zero means never flush it.
 */
opQueue::opQueue(unsigned int max_failures) :
  fail_threshold(max_failures), qentry_buf(false) {

  const char *db_filename = ":memory:";

  if (sqlite3_open(db_filename, &db)) {
    throw std::runtime_error("Can't create SQLite database.");
  }

  int r;
  char *sql_err;

  // See http://www.sqlite.org/c3ref/exec.html
  r = sqlite3_exec(db,
    "CREATE TEMPORARY TABLE queue ("
    "  rank INTEGER PRIMARY KEY NOT NULL,"
    "  status CHAR( 1 ) NOT NULL DEFAULT 'Q',"
    "  main_url VARCHAR( 200 ) NOT NULL,"
    "  endp_url VARCHAR( 200 ),"
    "  tree_name VARCHAR( 50 ),"
    "  n_events BIGINT UNSIGNED,"
    "  n_failures INTEGER UNSIGNED NOT NULL DEFAULT 0,"
    "  size_bytes BIGINT UNSIGNED,"
    "  UNIQUE (main_url)"
    ")",
  NULL, NULL, &sql_err);

  // See http://www.sqlite.org/c_interface.html#callback_returns_nonzero
  if (r != SQLITE_OK) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE, "Error in SQL CREATE query: %s\n",
      sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

  // Query for exists()
  /*r = sqlite3_prepare_v2(db,
    "SELECT COUNT(rank) FROM queue WHERE main_url=?", -1, &query_exists, NULL);
  if (r != SQLITE_OK) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE,
      "Error #%d while preparing SQL query for exists(): %s\n", r, sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }*/

  // Query for get_full_entry()
  r = sqlite3_prepare_v2(db,
    "SELECT main_url,endp_url,tree_name,n_events,n_failures,size_bytes,status"
    "  FROM queue WHERE main_url=? LIMIT 1", -1, &query_get_full_entry, NULL);
  if (r != SQLITE_OK) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE,
      "Error #%d while preparing query_get_full_entry: %s\n", r,
      sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

  // Query for get_status()
  r = sqlite3_prepare_v2(db,
    "SELECT status FROM queue WHERE main_url=? LIMIT 1", -1,
    &query_get_status, NULL);
  if (r != SQLITE_OK) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE,
      "Error #%d while preparing query_get_status: %s\n", r,
      sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

}

/** Removes from queue elements that are in status Success (D) or Failed (F).
 *  Returns the number of elements flushed (TODO: prepare query).
 */
int opQueue::flush() {

  int r = sqlite3_exec(db,
    "DELETE FROM queue WHERE ( status='D' OR status='F' )",
    NULL, NULL, &sql_err);

  if (r != SQLITE_OK) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE, "Error #%d in SQL DELETE query: %s\n",
      r, sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

  // See http://www.sqlite.org/c3ref/changes.html
  return sqlite3_changes(db);
}

/** Dumps the content of the database, ordered by insertion date.
 */
void opQueue::dump() {

  // Simplified implementation
  //sqlite3_exec(db, 
  //  "SELECT * FROM queue WHERE 1 ORDER BY rank ASC",
  //  dump_callback, NULL, &sql_err);*/

  int r;
  sqlite3_stmt *comp_query;

  r = sqlite3_prepare_v2(db,
    "SELECT rank,status,main_url,n_failures "
    "  FROM queue "
    "  ORDER BY rank ASC",
    -1, &comp_query, NULL);

  if (r != SQLITE_OK) {
    throw std::runtime_error("Error while preparing SQL SELECT query");
  }

  int count = 0;
  while ((r = sqlite3_step(comp_query)) == SQLITE_ROW) {
    int                  rank            = sqlite3_column_int(comp_query,   0);
    const unsigned char *status          = sqlite3_column_text(comp_query,  1);
    const unsigned char *main_url        = sqlite3_column_text(comp_query,  2);
    unsigned int         n_failures      = sqlite3_column_int64(comp_query, 3);
    printf("%04d | %c | %d | %s\n", rank, *status, n_failures, main_url);
  }

  // Free resources
  sqlite3_finalize(comp_query);
}

/** Executes an arbitrary query on the database. Beware: it throws exception on
 *  failure, that must be caught or else the execution of the program stops!
 */
void opQueue::arbitrary_query(const char *query) {

  int r = sqlite3_exec(db, query, query_callback, NULL, &sql_err);

  if (r != SQLITE_OK) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE, "Error in SQL arbitrary query: %s\n",
      sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

}

/** Generic callback function for a SELECT SQLite query that dumps results
 *  on screen. It is declared as static.
 */
int opQueue::query_callback(void *, int argc, char *argv[], char **colname) {
  printf("Query response contains %d field(s):\n", argc);
  for (int i=0; i<argc; i++) {
    printf("   %2d %s={%s}\n", i+1, colname[i], argv[i] ? argv[i] : "undefined");
  }
  return 0;
}

/** Change status queue. Returns true on success, false if update failed. To
 *  properly deal with Failed (F) status, i.e. to increment error count for the
 *  entry and move the element to the end of the queue (highest rank), use
 *  member function failed().
 */
bool opQueue::set_status(const char *url, qstat_t qstat) {

  if (!url) return false;

  snprintf(strbuf, AF_OPQUEUE_BUFSIZE,
    "UPDATE queue SET status='%c' WHERE main_url='%s'", qstat, url);

  int r = sqlite3_exec(db, strbuf, NULL, NULL, &sql_err);

  if (r != SQLITE_OK) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE, "Error in SQL UPDATE query: %s\n",
      sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

  return true;
}

/** Manages failed operations on the given URL: increments the failure counter
 *  and places the URL at the end of the queue (biggest rank), and if the number
 *  of failures is above threshold, sets the status to failed (F). Everything is
 *  done in a single UPDATE SQL query for efficiency reasons. It returns true on
 *  success, false on failure.
 */
bool opQueue::failed(const char *url) {

  if (!url) return false;

  if (fail_threshold != 0) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE,
      "UPDATE queue SET"
      "  n_failures=n_failures+1,rank=%lu,status=CASE"
      "    WHEN n_failures>=%u THEN 'F'"
      "    ELSE 'Q'"
      "  END"
      "  WHERE main_url='%s'",
      ++last_queue_rowid, fail_threshold-1, url);
  }
  else {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE,
      "UPDATE queue SET"
      "  n_failures=n_failures+1,rank=%lu,status='Q'"
      "  WHERE main_url='%s'",
      ++last_queue_rowid, url);
  }

  int r = sqlite3_exec(db, strbuf, NULL, NULL, &sql_err);

  if (r != SQLITE_OK) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE, "Error in SQL UPDATE query: %s\n",
      sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

  return true;
}

/** Queue destructor: it closes the connection to the opened SQLite db.
 */
opQueue::~opQueue() {
  sqlite3_close(db);
  //sqlite3_finalize(query_exists);
}

/** Enqueue URL. Returns true on success, false on failure.
 */
bool opQueue::insert(const char *url) {

  snprintf(strbuf, AF_OPQUEUE_BUFSIZE,
    "INSERT INTO queue "
    "(main_url) "
    "VALUES ('%s')", url);

  int r = sqlite3_exec(db, strbuf, NULL, NULL, &sql_err);

  if (r == SQLITE_CONSTRAINT) {
    sqlite3_free(sql_err);
    return false;
  }
  else if (r != SQLITE_OK) {
    snprintf(strbuf, AF_OPQUEUE_BUFSIZE, "Error #%d in SQL INSERT query: %s",
      r, sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

  last_queue_rowid = sqlite3_last_insert_rowid(db);

  return true;
}

/** Returns true if a given URL is in the queue, elsewhere it returns false. The
 *  query is prepared in the constructor, and finalized in the destructor.
 */
/*bool opQueue::exists(const char *url) {

  if (!url) return false;

  // See http://www.sqlite.org/capi3ref.html#sqlite3_bind_blob
  sqlite3_bind_text(query_exists, 1, url, -1, SQLITE_STATIC);

  sqlite3_step(query_exists);
  unsigned long count = sqlite3_column_int64(query_exists, 0);

  sqlite3_reset(query_exists);
  sqlite3_clear_bindings(query_exists);

  if (count) return true;
  return false;
}*/

/** Searches for an entry and returns its status inside a queueEntry class.
 *  The other members of the class are left intacts. This method can be used
 *  to check if an element exists.
 */
const queueEntry *opQueue::get_status(const char *url) {

  if (!url) return NULL;

  sqlite3_reset(query_get_status);
  sqlite3_clear_bindings(query_get_status);

  sqlite3_bind_text(query_get_status, 1, url, -1, SQLITE_STATIC);

  int r = sqlite3_step(query_get_status);

  // See http://www.sqlite.org/c3ref/c_abort.html for SQLite3 constants
  if (r == SQLITE_ROW) {
    qentry_buf.reset();
    qentry_buf.set_main_url(url);
    qentry_buf.set_status(
      (qstat_t)*sqlite3_column_text(query_get_status, 0) );
    return &qentry_buf;
  }

  return NULL;
}

/** Returns the full entry if it has finished processing (success or failed
 *  status), and a partial entry containing only the main_url and status if
 *  it hasn't. If entry does not exist or the given URL is NULL, it returns
 *  NULL. This function avoids unnecessarily allocating memory for elements that
 *  haven't finished processing yet.
 */
const queueEntry *opQueue::get_entry(const char *url) {

  if (!url) return NULL;

  const queueEntry *qe = get_status(url);

  if (qe) {
    qstat_t status = qe->get_status();
    if ((status == qstat_success) || (status == qstat_failed))
      return get_full_entry(url);
  }

  return qe;
}

/** Finds an entry with the given main URL and returns it; it returns NULL if
 *  the URL was not found in the list.
 */
const queueEntry *opQueue::get_full_entry(const char *url) {

  if (!url) return NULL;

  sqlite3_reset(query_get_full_entry);
  sqlite3_clear_bindings(query_get_full_entry);

  sqlite3_bind_text(query_get_full_entry, 1, url, -1, SQLITE_STATIC);

  int r = sqlite3_step(query_get_full_entry);

  // See http://www.sqlite.org/c3ref/c_abort.html for SQLite3 constants
  if (r == SQLITE_ROW) {
    // 0:main_url, 1:endp_url, 2:tree_name, 3:n_events
    // 4:n_failures, 5:size_bytes, 6:status

    qentry_buf.set_main_url(
      (char*)sqlite3_column_text(query_get_full_entry, 0) );
    qentry_buf.set_endp_url(
      (char*)sqlite3_column_text(query_get_full_entry, 1) );
    qentry_buf.set_tree_name(
      (char*)sqlite3_column_text(query_get_full_entry, 2) );
    qentry_buf.set_n_events( sqlite3_column_int64(query_get_full_entry, 3) );
    qentry_buf.set_n_failures( sqlite3_column_int64(query_get_full_entry, 4) );
    qentry_buf.set_size_bytes( sqlite3_column_int64(query_get_full_entry, 5) );
    qentry_buf.set_status(
      (qstat_t)*sqlite3_column_text(query_get_full_entry, 6) );

    return &qentry_buf;
  }

  return NULL;
}
