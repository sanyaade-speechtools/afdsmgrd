#include "afOpQueue.h"

using namespace af;

/** Queue constructor: it associates the queue to a SQLite object, and it
 *  creates the database in memory. SQLite takes care of creating (and
 *  immediately unlinking) a swap file for it.
 */
opQueue::opQueue() {

  const char *db_filename = ":memory:";

  if (sqlite3_open(db_filename, &db)) {
    throw std::runtime_error("Can't create SQLite database.");
  }

  int rc;
  char *sql_err;

  // See http://www.sqlite.org/c3ref/exec.html
  rc = sqlite3_exec(db,
    "CREATE TEMPORARY TABLE  queue ("
    "  rank INTEGER PRIMARY KEY NOT NULL,"
    "  status CHAR( 1 ) NOT NULL,"
    "  main_url VARCHAR( 200 ) NOT NULL,"
    "  endp_url VARCHAR( 200 ),"
    "  tree_name VARCHAR( 50 ),"
    "  n_events BIGINT UNSIGNED,"
    "  file_size_bytes BIGINT UNSIGNED,"
    "  UNIQUE (main_url)"
    ")",
  NULL, NULL, &sql_err);

  // See http://www.sqlite.org/c_interface.html#callback_returns_nonzero
  if (rc != SQLITE_OK) {
    snprintf(strbuf, AF_STRBUFSIZE, "Error in SQL CREATE query: %s\n",
      sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

}

/** Generic callback function for a SELECT SQLite query that dumps results
 *  on screen. It is declared as static
 */
int opQueue::dump_callback(void *, int argc, char *argv[], char **colname) {
  printf("!! callback !! argc=%d\n", argc);
  for (int i=0; i<argc; i++) {
    printf("   %02d %s={%s}\n", i, colname[i], argv[i] ? argv[i] : "undefined");
  }
  return 0;
}

/** Removes from queue elements that are in status Success(D) or Failed(F).
 */
int opQueue::flush() {

  int rc = sqlite3_exec(db,
    "DELETE FROM queue WHERE ( status='D' OR status='F' )",
    NULL, NULL, &sql_err);

  if (rc != SQLITE_OK) {
    snprintf(strbuf, AF_STRBUFSIZE, "Error in SQL DELETE query: %s\n",
      sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

  return sqlite3_changes(db);

}

/** Dumps the content of the database, ordered by insertion date.
 */
void opQueue::dump() {

  /*sqlite3_exec(db, 
    "SELECT * FROM queue WHERE 1 ORDER BY rank ASC",
    dump_callback, NULL, &sql_err);*/

  int r;
  sqlite3_stmt *comp_query;

  r = sqlite3_prepare_v2(db, "SELECT * FROM queue WHERE 1 ORDER BY rank ASC",
    -1, &comp_query, NULL);

  if (r != SQLITE_OK) {
    throw std::runtime_error("Error while preparing SQL SELECT query");
  }

  int count = 0;
  while ((r = sqlite3_step(comp_query)) == SQLITE_ROW) {
    int                  rank            = sqlite3_column_int(comp_query,   0);
    const unsigned char *status          = sqlite3_column_text(comp_query,  1);
    const unsigned char *main_url        = sqlite3_column_text(comp_query,  2);
    const unsigned char *endp_url        = sqlite3_column_text(comp_query,  3);
    const unsigned char *tree_name       = sqlite3_column_text(comp_query,  4);
    unsigned long        n_events        = sqlite3_column_int64(comp_query, 5);
    unsigned long        file_size_bytes = sqlite3_column_int64(comp_query, 6);
    printf("%04d [%c] %s\n", rank, *status, main_url);
  }

  // Free resources
  sqlite3_finalize(comp_query);

}

/** Change status queue. Returns true on success, false on failure.
 */
int opQueue::update(const char *url, qstat_t qstat) {

  if (!url) return false;

  snprintf(strbuf, AF_STRBUFSIZE,
    "UPDATE queue SET status='%c' WHERE main_url='%s'",
    qstat, url);

  int r = sqlite3_exec(db, strbuf, NULL, NULL, &sql_err);

  if (r != SQLITE_OK) {
    snprintf(strbuf, AF_STRBUFSIZE, "Error in SQL UPDATE query: %s\n",
      sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

  // See http://www.sqlite.org/c3ref/changes.html
  return sqlite3_changes(db);
}

/** Queue destructor: it closes the connection to the opened SQLite db.
 */
opQueue::~opQueue() {
  sqlite3_close(db);
}

/** Enqueue URL. Returns true on success, false on failure.
 */
bool opQueue::insert(const char *url) {

  snprintf(strbuf, AF_STRBUFSIZE,
    "INSERT INTO queue "
    "(main_url,status) "
    "VALUES ('%s','Q')", url);

  int r = sqlite3_exec(db, strbuf, NULL, NULL, &sql_err);

  if (r == SQLITE_CONSTRAINT) {
    sqlite3_free(sql_err);
    return false;
  }
  else if (r != SQLITE_OK) {
    snprintf(strbuf, AF_STRBUFSIZE, "Error in SQL INSERT query (code=%d): %s",
      r, sql_err);
    sqlite3_free(sql_err);
    throw std::runtime_error(strbuf);
  }

  return true;
}

/** Finds an entry with the given main URL and returns it; it returns NULL if
 *  the URL was not found in the list.
 */
/*queueEntry *opQueue::find(const char *url) {
  return NULL;
}*/
