#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"
#include "dupin_date.h"
#include "dupin_db.h"

#include <stdlib.h>
#include <string.h>

#define DUPIN_DB_SQL_MAIN_CREATE \
  "CREATE TABLE IF NOT EXISTS Dupin (\n" \
  "  seq      INTEGER PRIMARY KEY AUTOINCREMENT,\n" \
  "  id       CHAR(255) NOT NULL,\n" \
  "  rev      INTEGER NOT NULL DEFAULT 1,\n" \
  "  hash     CHAR(255) NOT NULL,\n" \
  "  type     CHAR(255) DEFAULT NULL,\n" \
  "  obj      TEXT,\n" \
  "  deleted  BOOL DEFAULT FALSE,\n" \
  "  tm       INTEGER NOT NULL,\n" \
  "  rev_head BOOL DEFAULT TRUE,\n" \
  "  UNIQUE   (id, rev)\n" \
  ");"

#define DUPIN_DB_SQL_CREATE_INDEX \
  "CREATE INDEX IF NOT EXISTS DupinId ON Dupin (id);\n" \
  "CREATE INDEX IF NOT EXISTS DupinIdRev ON Dupin (id,rev);\n" \
  "CREATE INDEX IF NOT EXISTS DupinType ON Dupin (type);\n" \
  "CREATE INDEX IF NOT EXISTS DupinObj ON Dupin (obj);\n" \
  "CREATE INDEX IF NOT EXISTS DupinIdRevHead ON Dupin (id,rev_head);"

#define DUPIN_DB_SQL_DESC_CREATE \
  "CREATE TABLE IF NOT EXISTS DupinDB (\n" \
  "  total_doc_ins   INTEGER NOT NULL DEFAULT 0,\n" \
  "  total_doc_del   INTEGER NOT NULL DEFAULT 0,\n" \
  "  compact_id      CHAR(255) NOT NULL DEFAULT '0',\n" \
  "  creation_time   CHAR(255) NOT NULL DEFAULT '0'\n" \
  ");\n" \
  "PRAGMA user_version = 3"

#define DUPIN_DB_SQL_DESC_UPGRADE_FROM_VERSION_1 \
  "ALTER TABLE Dupin   ADD COLUMN seq INTEGER PRIMARY KEY AUTOINCREMENT;\n" \
  "ALTER TABLE DupinDB ADD COLUMN creation_time CHAR(255) NOT NULL DEFAULT '0';\n" \
  "PRAGMA user_version = 3"

#define DUPIN_DB_SQL_DESC_UPGRADE_FROM_VERSION_2 \
  "PRAGMA user_version = 3"

#define DUPIN_DB_SQL_USES_OLD_ROWID \
        "SELECT seq FROM Dupin"

#define DUPIN_DB_SQL_TOTAL \
        "SELECT count(*) AS c FROM Dupin AS d WHERE d.rev_head = 'TRUE' "

#define DUPIN_DB_COMPACT_COUNT 100

gchar **
dupin_get_databases (Dupin * d)
{
  guint i;
  gsize size;
  gchar **ret;
  gpointer key;
  GHashTableIter iter;

  g_return_val_if_fail (d != NULL, NULL);

  g_rw_lock_reader_lock (d->rwlock);

  if (!(size = g_hash_table_size (d->dbs)))
    {
      g_rw_lock_reader_unlock (d->rwlock);
      return NULL;
    }

  ret = g_malloc (sizeof (gchar *) * (g_hash_table_size (d->dbs) + 1));

  i = 0;
  g_hash_table_iter_init (&iter, d->dbs);
  while (g_hash_table_iter_next (&iter, &key, NULL) == TRUE)
    ret[i++] = g_strdup (key);

  ret[i] = NULL;

  g_rw_lock_reader_unlock (d->rwlock);

  return ret;
}

gboolean
dupin_database_exists (Dupin * d, gchar * db_name)
{
  gboolean ret;

  g_rw_lock_reader_lock (d->rwlock);
  DupinDB * db = g_hash_table_lookup (d->dbs, db_name);
  ret = ((db != NULL) && db->todelete == FALSE) ? TRUE : FALSE;
  g_rw_lock_reader_unlock (d->rwlock);

  return ret;
}

DupinDB *
dupin_database_open (Dupin * d, gchar * db, GError ** error)
{
  DupinDB *ret;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (db != NULL, NULL);

  g_rw_lock_reader_lock (d->rwlock);

  if (!(ret = g_hash_table_lookup (d->dbs, db)) || ret->todelete == TRUE)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		 "Database '%s' doesn't exist.", db);

      g_rw_lock_reader_unlock (d->rwlock);
      return NULL;
    }
  else
    {
      ret->ref++;

#if DEBUG
      fprintf(stderr,"dupin_database_open: (%p) name=%s \t ref++=%d\n", g_thread_self (), db, (gint) ret->ref);
#endif
    }

  g_rw_lock_reader_unlock (d->rwlock);

  return ret;
}

DupinDB *
dupin_database_new (Dupin * d, gchar * dbname, GError ** error)
{
  DupinDB *ret;

  gchar *path;
  gchar *name;
  gchar * str;
  gchar * errmsg;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (dbname != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_db_name (dbname) == TRUE, NULL);

  g_rw_lock_writer_lock (d->rwlock);

  if ((ret = g_hash_table_lookup (d->dbs, dbname)))
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Database '%s' already exist.", dbname);
      g_rw_lock_writer_unlock (d->rwlock);
      return NULL;
    }

  name = g_strdup_printf ("%s%s", dbname, DUPIN_DB_SUFFIX);
  path = g_build_path (G_DIR_SEPARATOR_S, d->path, name, NULL);
  g_free (name);

  if (!(ret = dupin_db_connect (d, dbname, path, DP_SQLITE_OPEN_CREATE, error)))
    {
      g_rw_lock_writer_unlock (d->rwlock);
      g_free (path);
      return NULL;
    }

  g_free (path);

  ret->ref++;

#if DEBUG
  fprintf(stderr,"dupin_database_new: (%p) name=%s \t ref++=%d\n", g_thread_self (), dbname, (gint) ret->ref);
#endif

  g_hash_table_insert (d->dbs, g_strdup (dbname), ret);

  if (dupin_database_begin_transaction (ret, error) < 0)
    {
      g_rw_lock_writer_unlock (d->rwlock);
      dupin_db_disconnect (ret);
      return NULL;
    }

  gchar * creation_time = g_strdup_printf ("%" G_GSIZE_FORMAT, dupin_date_timestamp_now (0));

  str = sqlite3_mprintf ("INSERT OR REPLACE INTO DupinDB (compact_id, creation_time) VALUES (0, '%q')", creation_time);

  g_free (creation_time);

  if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (d->rwlock);
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                       errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (str);
      dupin_database_rollback_transaction (ret, error);
      dupin_db_disconnect (ret);
      return NULL;
    }

  if (dupin_database_commit_transaction (ret, error) < 0)
    {
      g_rw_lock_writer_unlock (d->rwlock);
      dupin_db_disconnect (ret);
      return NULL;
    }

  /* NOTE - the respective map and reduce threads will add +1 top the these values */
  sqlite3_free (str);

  g_rw_lock_writer_unlock (d->rwlock);

  /* NOTE - create one default link base and attachment database named after the main database */

  /* TODO - default attachement db and linkbase are unchangable at the moment */

  /* NOTE - We keep two refs to the parent DB one for the default linkbase and one for the default attachment db
            so when we delete a database the linkbase and the attachment db are always deleted first
	    See dupin_database_delete () */

  if (ret->default_linkbase == NULL)
    {
      if (!(ret->default_linkbase = dupin_linkbase_new (d, ret->default_linkbase_name, dbname, TRUE, error)))
        {
          dupin_db_disconnect (ret);
          return NULL;
        }
    }
  else
    {
      g_warning ("dupin_database_new: (thread=%p) default linkbase for database %s already connected to %s\n", g_thread_self (), ret->name, dupin_linkbase_get_name (ret->default_linkbase));
    }

  if (ret->default_attachment_db == NULL)
    {
      if (!(ret->default_attachment_db = dupin_attachment_db_new (d, ret->default_attachment_db_name, dbname, error)))
        {
          dupin_db_disconnect (ret);
          return NULL;
        }
    }
  else
    {
      g_warning ("dupin_database_new: (thread=%p) default linkbase for database %s already connected to %s\n", g_thread_self (), ret->name, dupin_attachment_db_get_name (ret->default_attachment_db));
    }

  return ret;
}

void
dupin_database_ref (DupinDB * db)
{
  g_return_if_fail (db != NULL);

  g_rw_lock_writer_lock (db->rwlock);

  db->ref++;

#if DEBUG
  fprintf(stderr,"dupin_database_ref: (%p) name=%s \t ref++=%d\n", g_thread_self (), db->name, (gint) db->ref);
#endif

  g_rw_lock_writer_unlock (db->rwlock);
}

void
dupin_database_unref (DupinDB * db)
{
  Dupin *d;

  g_return_if_fail (db != NULL);

  d = db->d;

  g_rw_lock_writer_lock (db->rwlock);

  if (db->ref > 0)
    {
      db->ref--;

#if DEBUG
      fprintf(stderr,"dupin_database_unref: (%p) name=%s \t ref--=%d\n", g_thread_self (), db->name, (gint) db->ref);
#endif
    }

  g_rw_lock_writer_unlock (db->rwlock);

  if (db->todelete == TRUE &&
      dupin_database_is_compacting (db) == FALSE)
    {
      g_rw_lock_reader_lock (db->rwlock);

      if (db->ref > 0)
        {
          g_rw_lock_reader_unlock (db->rwlock);

          g_warning ("dupin_database_unref: (thread=%p) database %s flagged for deletion but can't free it due ref is %d\n", g_thread_self (), db->name, (gint) db->ref);
        }
      else
        {
          g_rw_lock_reader_unlock (db->rwlock);

	  if (db->default_linkbase != NULL)
	    dupin_linkbase_unref (db->default_linkbase);

          if (db->default_attachment_db != NULL)
            dupin_attachment_db_unref (db->default_attachment_db);

          g_rw_lock_writer_lock (d->rwlock);
          g_hash_table_remove (d->dbs, db->name);
          g_rw_lock_writer_unlock (d->rwlock);
        }
    }
}

gboolean
dupin_database_delete (DupinDB * db, GError ** error)
{
  g_return_val_if_fail (db != NULL, FALSE);

  /* NOTE - delete default link base and attachment database named after the main database due they have
	    been created in dupin_database_new () - see above */

  if (dupin_linkbase_delete (db->default_linkbase, NULL) == FALSE)
    {
      dupin_database_set_error (db, "Cannot delete default linkbase");
      return FALSE;
    }

  if (dupin_attachment_db_delete (db->default_attachment_db, NULL) == FALSE)
    {
      dupin_database_set_error (db, "Cannot delete default attachments database");
      return FALSE;
    }

  g_rw_lock_writer_lock (db->rwlock);
  db->todelete = TRUE;
  g_rw_lock_writer_unlock (db->rwlock);

  return TRUE;
}

const gchar *
dupin_database_get_name (DupinDB * db)
{
  g_return_val_if_fail (db != NULL, NULL);
  return db->name;
}

gsize
dupin_database_get_size (DupinDB * db)
{
  struct stat st;

  g_return_val_if_fail (db != NULL, 0);

  if (g_stat (db->path, &st) != 0)
    return 0;

  return (gsize) st.st_size;
}

static int
dupin_database_get_creation_time_cb (void *data, int argc, char **argv, char **col)
{
  gsize *creation_time = data;

  if (argv[0])
    *creation_time = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gboolean
dupin_database_get_creation_time (DupinDB * db, gsize * creation_time)
{
  gchar * query;
  gchar * errmsg=NULL;

  *creation_time = 0;

  g_return_val_if_fail (db != NULL, 0);

  /* get creation time out of database */
  query = "SELECT creation_time as creation_time FROM DupinDB";

  g_rw_lock_reader_lock (db->rwlock);

  if (sqlite3_exec (db->db, query, dupin_database_get_creation_time_cb, creation_time, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (db->rwlock);

      g_error("dupin_database_get_creation_time: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_rw_lock_reader_unlock (db->rwlock);

  return TRUE;
}

gchar *
dupin_database_generate_id_real (DupinDB * db,
				 GError ** error,
				 gboolean lock)
{
  g_return_val_if_fail (db != NULL, NULL);

  if (lock == TRUE)
    g_rw_lock_writer_lock (db->rwlock);

  while (TRUE)
    {
      gchar * id = NULL;

      id = dupin_util_generate_id (error);

      if (id != NULL)
        {
          if (dupin_record_exists_real (db, id, FALSE) == TRUE)
	    {
              g_free (id);
	    }
	  else
	    {
              if (lock == TRUE)
                g_rw_lock_writer_unlock (db->rwlock);

              return id;
            }
        }
      else
        break;
    }

  if (lock == TRUE)
    g_rw_lock_writer_unlock (db->rwlock);

  return NULL;
}

gchar *
dupin_database_generate_id (DupinDB * db, GError ** error)
{
  g_return_val_if_fail (db != NULL, NULL);

  return dupin_database_generate_id_real (db, error, TRUE);
}

/* Internal: */
void
dupin_db_disconnect (DupinDB * db)
{
  g_return_if_fail (db != NULL);

#if DEBUG
  g_message("dupin_db_disconnect: total number of changes for '%s' database: %d\n", db->name, (gint)sqlite3_total_changes (db->db));
#endif

  if (db->db)
    sqlite3_close (db->db);

  if (db->todelete == TRUE)
    g_unlink (db->path);

  if (db->name)
    g_free (db->name);

  if (db->path)
    g_free (db->path);

  if (db->default_linkbase_name != NULL)
    g_free (db->default_linkbase_name);

  if (db->default_attachment_db_name != NULL)
    g_free (db->default_attachment_db_name);

  if (db->rwlock)
    {
      g_rw_lock_clear (db->rwlock);
      g_free (db->rwlock);
    }

  if (db->views.views)
    g_free (db->views.views);

  if (db->linkbs.linkbs)
    g_free (db->linkbs.linkbs);

  if (db->attachment_dbs.attachment_dbs)
    g_free (db->attachment_dbs.attachment_dbs);

  if (db->error_msg)
    g_free (db->error_msg);

  if (db->warning_msg)
    g_free (db->warning_msg);

  g_free (db);
}

static int
dupin_database_get_user_version_cb (void *data, int argc, char **argv,
                                    char **col)
{
  gint *user_version = data;

  if (argv[0])
    *user_version = atoi (argv[0]);

  return 0;
}

DupinDB *
dupin_db_connect (Dupin * d, gchar * name, gchar * path,
 		  DupinSQLiteOpenType mode,
		  GError ** error)
{
  gchar *errmsg;
  DupinDB *db;

  db = g_malloc0 (sizeof (DupinDB));

  db->d = d;

  db->name = g_strdup (name);
  db->path = g_strdup (path);

  db->default_attachment_db_name = g_strdup (name);
  db->default_linkbase_name = g_strdup (name);

  db->tocompact = FALSE;
  db->topurge = FALSE;
  db->compact_processed_count = 0;

  db->rwlock = g_new0 (GRWLock, 1);
  g_rw_lock_init (db->rwlock);

  if (sqlite3_open_v2 (db->path, &db->db, dupin_util_dupin_mode_to_sqlite_mode (mode), NULL) != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Database error.");
      dupin_db_disconnect (db);
      return NULL;
    }

  sqlite3_busy_timeout (db->db, DUPIN_SQLITE_TIMEOUT);

  if (mode == DP_SQLITE_OPEN_CREATE)
    {
      if (sqlite3_exec (db->db, "PRAGMA journal_mode = WAL", NULL, NULL, &errmsg) != SQLITE_OK
          || sqlite3_exec (db->db, "PRAGMA encoding = \"UTF-8\"", NULL, NULL, &errmsg) != SQLITE_OK)
        {
          if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma journal_mode or encoding: %s",
		   errmsg);
          sqlite3_free (errmsg);
          dupin_db_disconnect (db);
          return NULL;
        }

      if (dupin_database_begin_transaction (db, error) < 0)
        {
          dupin_db_disconnect (db);
          return NULL;
        }

      if (sqlite3_exec (db->db, DUPIN_DB_SQL_MAIN_CREATE, NULL, NULL, &errmsg) != SQLITE_OK
          || sqlite3_exec (db->db, DUPIN_DB_SQL_CREATE_INDEX, NULL, NULL, &errmsg) != SQLITE_OK
          || sqlite3_exec (db->db, DUPIN_DB_SQL_DESC_CREATE, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
          sqlite3_free (errmsg);
          dupin_database_rollback_transaction (db, error);
          dupin_db_disconnect (db);
          return NULL;
        }

      if (dupin_database_commit_transaction (db, error) < 0)
        {
          dupin_db_disconnect (db);
          return NULL;
        }
    }

  /* check database version */
  gint user_version = 0;

  if (sqlite3_exec (db->db, "PRAGMA user_version", dupin_database_get_user_version_cb, &user_version, &errmsg) != SQLITE_OK)
    {
      /* default to 1 if not found or error - TODO check not SQLITE_OK error only */
      user_version = 1;
    }

  if (user_version > DUPIN_SQLITE_MAX_USER_VERSION)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "SQLite database user version (%d) is newer than I know how to work with (%d).",
			user_version, DUPIN_SQLITE_MAX_USER_VERSION);
      sqlite3_free (errmsg);
      dupin_db_disconnect (db);
      return NULL;
    }

  if (user_version <= 1)
    {
      if (sqlite3_exec (db->db, DUPIN_DB_SQL_DESC_UPGRADE_FROM_VERSION_1, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
          sqlite3_free (errmsg);
          dupin_db_disconnect (db);
          return NULL;
        }
    }
  else if (user_version == 2)
    {
      if (sqlite3_exec (db->db, DUPIN_DB_SQL_DESC_UPGRADE_FROM_VERSION_2, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
          sqlite3_free (errmsg);
          dupin_db_disconnect (db);
          return NULL;
        }
    }

  if (sqlite3_exec (db->db, DUPIN_DB_SQL_USES_OLD_ROWID, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s", errmsg);
      sqlite3_free (errmsg);

      g_warning ("dupin_db_connect: Consider to recreate your %s SQLite database and reingest your data. Since version 3 the Dupin table uses a seq column INTEGER PRIMARY KEY AUTOINCREMENT as ROWID and UNIQUE (id, rev) constraint rather then PRIMARY KEY(id, rev). See http://www.sqlite.org/autoinc.html for more information.\n", path);
    }

  gchar * cache_size = g_strdup_printf ("PRAGMA cache_size = %d", DUPIN_SQLITE_CACHE_SIZE);
  if (sqlite3_exec (db->db, "PRAGMA temp_store = memory", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (db->db, cache_size, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma temp_store: %s",
		   errmsg);
      sqlite3_free (errmsg);
      if (cache_size)
        g_free (cache_size);
      dupin_db_disconnect (db);
      return NULL;
    }

  if (cache_size)
    g_free (cache_size);

  /*
   TODO - check if the below can be optimized using NORMAL or OFF and use separated syncing thread
          see also http://www.sqlite.org/pragma.html#pragma_synchronous
   */

  if (sqlite3_exec (db->db, "PRAGMA synchronous = NORMAL", NULL, NULL, &errmsg) != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma synchronous: %s",
		   errmsg);
      sqlite3_free (errmsg);
      dupin_db_disconnect (db);
      return NULL;
    }

  /* NOTE - we know this is inefficient, but we need it till proper Elastic search or lucene used as frontend */

  sqlite3_create_function(db->db, "filterBy", 5, SQLITE_ANY, d, dupin_sqlite_json_filterby, NULL, NULL);

  return db;
}

/* NOTE - 0 = ok, 1 = already in transaction, -1 = error */

gint
dupin_database_begin_transaction (DupinDB * db, GError ** error)
{
  g_return_val_if_fail (db != NULL, -1);

  gchar *errmsg;
  gint rc = -1;

  if (db->d->bulk_transaction == TRUE)
    {
#if DEBUG
      g_message ("dupin_database_begin_transaction: database %s transaction ALREADY open", db->name);
#endif

      return 1;
    }

  rc = sqlite3_exec (db->db, "BEGIN TRANSACTION", NULL, NULL, &errmsg);

  if (rc == SQLITE_BUSY)
    {
        rc = dupin_sqlite_subs_mgr_busy_handler(db->db, "BEGIN TRANSACTION", NULL, NULL, &errmsg, rc);
    }

  if (rc != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot begin database %s transaction: %s", db->name, errmsg);

      sqlite3_free (errmsg);

      return -1;
    }

#if DEBUG
  g_message ("dupin_database_begin_transaction: database %s transaction begin", db->name);
#endif

  return 0;
}

gint
dupin_database_rollback_transaction (DupinDB * db, GError ** error)
{
  g_return_val_if_fail (db != NULL, -1);

  gchar *errmsg;
  gint rc = -1;

  rc = sqlite3_exec (db->db, "ROLLBACK", NULL, NULL, &errmsg);

  if (rc == SQLITE_BUSY)
    {
        rc = dupin_sqlite_subs_mgr_busy_handler(db->db, "ROLLBACK", NULL, NULL, &errmsg, rc);
    }

  if (rc != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot rollback database %s transaction: %s", db->name, errmsg);

      sqlite3_free (errmsg);

      return -1;
    }

#if DEBUG
  g_message ("dupin_database_rollback_transaction: database %s transaction rollback", db->name);
#endif

  return 0;
}

gint
dupin_database_commit_transaction (DupinDB * db, GError ** error)
{
  g_return_val_if_fail (db != NULL, -1);

  gchar *errmsg;
  gint rc = -1;

  if (db->d->bulk_transaction == TRUE)
    {
#if DEBUG
      g_message ("dupin_database_commit_transaction: database %s transaction commit POSTPONED", db->name);
#endif

      return 1;
    }

  rc = sqlite3_exec (db->db, "COMMIT", NULL, NULL, &errmsg);

  if (rc == SQLITE_BUSY)
    {
        rc = dupin_sqlite_subs_mgr_busy_handler(db->db, "COMMIT", NULL, NULL, &errmsg, rc);
    }

  if (rc != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot commit database %s transaction: %s", db->name, errmsg);

      sqlite3_free (errmsg);

      return -1;
    }

#if DEBUG
  g_message ("dupin_database_commit_transaction: database %s transaction commit", db->name);
#endif

  return 0;
}

gsize
dupin_database_count (DupinDB * db, DupinCountType type)
{
  g_return_val_if_fail (db != NULL, 0);

  struct dupin_record_select_total_t count;
  memset (&count, 0, sizeof (count));

  g_rw_lock_reader_lock (db->rwlock);

  if (sqlite3_exec (db->db, DUPIN_DB_SQL_GET_TOTALS, dupin_record_select_total_cb, &count, NULL) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (db->rwlock);
      return 0;
    }

  g_rw_lock_reader_unlock (db->rwlock);

  if (type == DP_COUNT_EXIST)
    {
      return count.total_doc_ins;
    }
  else if (type == DP_COUNT_DELETE)
    {
      return count.total_doc_del;
    }
  else if (type == DP_COUNT_CHANGES)
    {
      return count.total_doc_ins + count.total_doc_del;
    }
  else
    {
      return count.total_doc_ins + count.total_doc_del;
    }
}

static int
dupin_database_get_max_rowid_cb (void *data, int argc, char **argv,
                                  char **col)
{
  gsize *max_rowid = data;

  if (argv[0])
    *max_rowid = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gboolean
dupin_database_get_max_rowid (DupinDB * db, gsize * max_rowid)
{
  gchar *query;
  gchar * errmsg=NULL;

  g_return_val_if_fail (db != NULL, 0);

  *max_rowid=0;

  query = "SELECT max(ROWID) as max_rowid FROM Dupin";

  g_rw_lock_reader_lock (db->rwlock);

  if (sqlite3_exec (db->db, query, dupin_database_get_max_rowid_cb, max_rowid, &errmsg) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (db->rwlock);

      g_error("dupin_database_get_max_rowid: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_rw_lock_reader_unlock (db->rwlock);

  return TRUE;
}

static int
dupin_database_get_changes_list_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_database_get_changes_list_t *s = data;

  guint rev = 0;
  gsize tm = 0;
  gchar *id = NULL;
  gchar *hash = NULL;
  gchar *type = NULL;
  gchar *obj = NULL;
  gboolean delete = FALSE;
  gsize rowid=0;
  gint i;

  for (i = 0; i < argc; i++)
    {
      /* shouldn't this be double and use g_ascii_strtoll() ?!? */
      if (!g_strcmp0 (col[i], "rev"))
        rev = atoi (argv[i]);

      else if (!g_strcmp0 (col[i], "tm"))
        tm = (gsize) g_ascii_strtoll (argv[i], NULL, 10);

      else if (!g_strcmp0 (col[i], "type"))
        type = argv[i];

      else if (!g_strcmp0 (col[i], "hash"))
        hash = argv[i];

      else if (!g_strcmp0 (col[i], "obj"))
        obj = argv[i];

      else if (!g_strcmp0 (col[i], "deleted"))
        delete = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;

      else if (!g_strcmp0 (col[i], "rowid"))
        rowid = (gsize) g_ascii_strtoll (argv[i], NULL, 10);

      else if (!g_strcmp0 (col[i], "id"))
        id = argv[i];
    }

  if (rev && hash !=NULL)
    {
      JsonNode *change_node=json_node_new (JSON_NODE_OBJECT);
      JsonObject *change=json_object_new();
      json_node_take_object (change_node, change);

      json_object_set_int_member (change,"seq", rowid);
      json_object_set_string_member (change,"id", id);

      if (delete == TRUE)
        json_object_set_boolean_member (change, "deleted", delete);

      JsonNode *change_details_node=json_node_new (JSON_NODE_ARRAY);
      JsonArray *change_details=json_array_new();
      json_node_take_array (change_details_node, change_details);
      json_object_set_member (change, "changes", change_details_node);

      JsonNode * node = json_node_new (JSON_NODE_OBJECT);
      JsonObject * node_obj = json_object_new ();
      json_node_take_object (node, node_obj);
      json_array_add_element (change_details, node);

      gchar mvcc[DUPIN_ID_MAX_LEN];
      dupin_util_mvcc_new (rev, hash, mvcc);

      json_object_set_string_member (node_obj, "rev", mvcc);
      gchar * created = dupin_date_timestamp_to_http_date (tm);
      json_object_set_string_member (node_obj, RESPONSE_OBJ_CREATED, created);
      g_free (created);
      if (type != NULL)
        json_object_set_string_member (node_obj,RESPONSE_OBJ_TYPE, type);

      if (s->style == DP_CHANGES_MAIN_ONLY)
        {
        }
      else if (s->style == DP_CHANGES_ALL_DOCS)
        {
        }

      s->list = g_list_append (s->list, change_node);
    }

  return 0;
}

gboolean
dupin_database_get_changes_list (DupinDB *              db,
                                 guint                  count,
                                 guint                  offset,
                                 gsize                  since,
                                 gsize                  to,
         			 DupinChangesType	changes_type,
				 DupinCountType         count_type,
                                 DupinOrderByType       orderby_type,
                                 gboolean               descending,
				 gchar **               types,
                                 DupinFilterByType      types_op,
                                 GList **               list,
                                 GError **              error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;
  gchar *check_deleted="";

  struct dupin_database_get_changes_list_t s;

  g_return_val_if_fail (db != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  gint i;
  if (types != NULL
      && types_op == DP_FILTERBY_EQUALS )
    {
      for (i = 0; types[i]; i++)
        {
          g_return_val_if_fail (dupin_util_is_valid_record_type (types[i]) == TRUE, FALSE);
        }
    }

  memset (&s, 0, sizeof (s));
  s.style = changes_type;

  str = g_string_new ("SELECT id, rev, hash, type, obj, deleted, tm, ROWID AS rowid FROM Dupin as d WHERE d.rev_head = 'TRUE' ");

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  gchar * op = "AND";

  if (since > 0 && to > 0)
    {
      g_string_append_printf (str, " %s d.ROWID > %d AND d.ROWID <= %d ", op, (gint)since, (gint)to);
    }
  else if (since > 0)
    {
      g_string_append_printf (str, " %s d.ROWID > %d ", op, (gint)since);
    }
  else if (to > 0)
    {
      g_string_append_printf (str, " %s d.ROWID <= %d ", op, (gint)to);
    }

  if (g_strcmp0 (check_deleted, ""))
    {
      g_string_append_printf (str, " %s %s ", op, check_deleted);
    }

  if (types != NULL
      && types_op != DP_FILTERBY_PRESENT)
    {
      if (types[0])
        {
          gchar * tmp2 = sqlite3_mprintf (" %s ( ", op);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
        }

      for (i = 0; types[i]; i++)
        {
          gchar * tmp2;

          if (types_op == DP_FILTERBY_EQUALS)
            tmp2 = sqlite3_mprintf (" d.type = '%q' ", types[i]);
          else if (types_op == DP_FILTERBY_CONTAINS)
            tmp2 = sqlite3_mprintf (" d.type LIKE '%%%q%%' ", types[i]);
          else if (types_op == DP_FILTERBY_STARTS_WITH)
            tmp2 = sqlite3_mprintf (" d.type LIKE '%q%%' ", types[i]);

          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
          if (types[i+1])
            str = g_string_append (str, " OR ");
        }

      if (types[0])
        str = g_string_append (str, " ) ");

      op = "AND";
    }
  else
    {
      if (types_op == DP_FILTERBY_PRESENT)
        {
          gchar * tmp2 = tmp2 = sqlite3_mprintf (" %s ( d.type IS NOT NULL OR d.type != '' ) ", op);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);

          op = "AND";
        }
    }

  //str = g_string_append (str, " GROUP BY d.id ");

  str = g_string_append (str, " ORDER BY d.ROWID");

  if (descending)
    str = g_string_append (str, " DESC");

  if (count || offset)
    {
      str = g_string_append (str, " LIMIT ");

      if (offset)
        g_string_append_printf (str, "%u", offset);

      if (offset && count)
        str = g_string_append (str, ",");

      if (count)
        g_string_append_printf (str, "%u", count);
    }

  tmp = g_string_free (str, FALSE);

#if DEBUG
  g_message("dupin_database_get_changes_list() query=%s\n",tmp);
#endif

  g_rw_lock_reader_lock (db->rwlock);

  if (sqlite3_exec (db->db, tmp, dupin_database_get_changes_list_cb, &s, &errmsg) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (db->rwlock);

      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_rw_lock_reader_unlock (db->rwlock);

  g_free (tmp);

  *list = s.list;
  return TRUE;
}

void
dupin_database_get_changes_list_close
				(GList *                list)
{
  while (list)
    {
      json_node_free (list->data);
      list = g_list_remove (list, list->data);
    }
}

static int
dupin_database_get_total_changes_cb
				(void *data, int argc, char **argv, char **col)
{
  gsize *numb = data;

  if (argv[0] && *argv[0])
    *numb = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gboolean
dupin_database_get_total_changes
				(DupinDB *              db,
                                 gsize *                total,
                                 gsize                  since,
                                 gsize                  to,
			 	 DupinCountType         count_type,
                                 gboolean               inclusive_end,
				 gchar **               types,
                                 DupinFilterByType      types_op,
                                 GError **              error)
{
  g_return_val_if_fail (db != NULL, FALSE);

  gchar *errmsg;
  gchar *tmp;
  GString *str;

  gint i;
  if (types != NULL
      && types_op == DP_FILTERBY_EQUALS )
    {
      for (i = 0; types[i]; i++)
        {
          g_return_val_if_fail (dupin_util_is_valid_record_type (types[i]) == TRUE, FALSE);
        }
    }

  *total = 0;

  gchar *check_deleted="";

  str = g_string_new (DUPIN_DB_SQL_TOTAL);

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  gchar * op = "AND";

  if (since > 0 && to > 0)
    {
      g_string_append_printf (str, " %s d.ROWID > %d AND d.ROWID <= %d ", op, (gint)since, (gint)to);
    }
  else if (since > 0)
    {
      g_string_append_printf (str, " %s d.ROWID > %d ", op, (gint)since);
    }
  else if (to > 0)
    {
      g_string_append_printf (str, " %s d.ROWID <= %d ", op, (gint)to);
    }

  if (g_strcmp0 (check_deleted, ""))
    {
      g_string_append_printf (str, " %s %s ", op, check_deleted);
    }

  if (types != NULL
      && types_op != DP_FILTERBY_PRESENT)
    {
      if (types[0])
        {
          gchar * tmp2 = sqlite3_mprintf (" %s ( ", op);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
        }
  
      for (i = 0; types[i]; i++)
        {
          gchar * tmp2;

          if (types_op == DP_FILTERBY_EQUALS)
            tmp2 = sqlite3_mprintf (" d.type = '%q' ", types[i]);
          else if (types_op == DP_FILTERBY_CONTAINS)
            tmp2 = sqlite3_mprintf (" d.type LIKE '%%%q%%' ", types[i]);
          else if (types_op == DP_FILTERBY_STARTS_WITH)
            tmp2 = sqlite3_mprintf (" d.type LIKE '%q%%' ", types[i]);

          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
          if (types[i+1])
            str = g_string_append (str, " OR ");
        }

      if (types[0])
        str = g_string_append (str, " ) ");

      op = "AND";
    }
  else
    {
      if (types_op == DP_FILTERBY_PRESENT)
        {
          gchar * tmp2 = tmp2 = sqlite3_mprintf (" %s ( d.type IS NOT NULL OR d.type != '' ) ", op);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);

          op = "AND";
        }
    }

  //str = g_string_append (str, " GROUP BY d.id ");

  tmp = g_string_free (str, FALSE);

#if DEBUG
  g_message("dupin_database_get_total_changes() query=%s\n",tmp);
#endif

  g_rw_lock_reader_lock (db->rwlock);

  *total = 0;

  if (sqlite3_exec (db->db, tmp, dupin_database_get_total_changes_cb, total, &errmsg) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (db->rwlock);

      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_rw_lock_reader_unlock (db->rwlock);

  g_free (tmp);

  return TRUE;
}

static int
dupin_database_compact_cb (void *data, int argc, char **argv, char **col)
{
  gchar **compact_id = data;

  if (argv[0] && *argv[0])
    *compact_id = g_strdup (argv[0]);

  return 0;
}

gboolean
dupin_database_thread_compact (DupinDB * db, gsize count)
{
  g_return_val_if_fail (db != NULL, FALSE);

  gchar * compact_id = NULL;
  gsize rowid;
  gchar * errmsg;
  GList *results, *list;

  g_rw_lock_reader_lock (db->rwlock);
  gboolean topurge = db->topurge;
  g_rw_lock_reader_unlock (db->rwlock);

  gboolean ret = TRUE;

  gchar *str;

  /* get last position we reduced and get anything up to count after that */
  gchar * query = "SELECT compact_id as c FROM DupinDB";

  g_rw_lock_reader_lock (db->rwlock);

  if (sqlite3_exec (db->db, query, dupin_database_compact_cb, &compact_id, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (db->rwlock);

      g_error("dupin_database_thread_compact: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_rw_lock_reader_unlock (db->rwlock);

  gsize start_rowid = (compact_id != NULL) ? (gsize) g_ascii_strtoll (compact_id, NULL, 10)+1 : 1;

  if (dupin_record_get_list (db, count, 0, start_rowid, 0, NULL, NULL, NULL, TRUE, DP_COUNT_ALL, DP_ORDERBY_ROWID, FALSE, NULL, DP_FILTERBY_EQUALS,
				NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == FALSE || !results)
    {
      if (compact_id != NULL)
        g_free(compact_id);

      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

  for (list = results; list; list = list->next)
    {
      DupinRecord * record = list->data;

      gchar *tmp;

      rowid = dupin_record_get_rowid (record);

      guint last_revision = record->last->revision;

      if (dupin_record_is_deleted (record, NULL) == TRUE)
        {
	  /* remove any attachments */

	  if (dupin_attachment_record_delete_all (db->default_attachment_db, (gchar *) dupin_record_get_id (record)) == FALSE)
	    {
	      g_warning ("dupin_database_thread_compact: Cannot delete all attachments for id %s\n", (gchar *) dupin_record_get_id (record));
	      continue;
	    }

          if (topurge == TRUE)
            {
	      /* NOTE - need to decrese deleted counter */

	      g_rw_lock_reader_lock (db->rwlock);

              struct dupin_record_select_total_t t;
              memset (&t, 0, sizeof (t));

              if (sqlite3_exec (db->db, DUPIN_DB_SQL_GET_TOTALS, dupin_record_select_total_cb, &t, &errmsg) != SQLITE_OK)
                {
                  g_rw_lock_reader_unlock (db->rwlock);

                  g_error ("dupin_database_thread_compact: %s", errmsg);
                  sqlite3_free (errmsg);

                  return FALSE;
                }
              else
                {
                  g_rw_lock_reader_unlock (db->rwlock);

                  g_rw_lock_writer_lock (db->rwlock);

                  t.total_doc_del--;

                  tmp = sqlite3_mprintf (DUPIN_DB_SQL_SET_TOTALS, (gint)t.total_doc_ins, (gint)t.total_doc_del);

                  if (sqlite3_exec (db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
                    {
                      g_rw_lock_writer_unlock (db->rwlock);

                      g_error ("dupin_database_thread_compact: %s", errmsg);
                      sqlite3_free (errmsg);

                      sqlite3_free (tmp);

                      return FALSE;
                    }

                  g_rw_lock_writer_unlock (db->rwlock);
                }

              if (tmp != NULL)
                sqlite3_free (tmp);

	      /* wipe anything about ID */

              tmp = sqlite3_mprintf ("DELETE FROM Dupin WHERE id = '%q'", (gchar *) dupin_record_get_id (record));
	    }
	  else
            {
              tmp = sqlite3_mprintf ("DELETE FROM Dupin WHERE id = '%q' AND rev < %d", (gchar *) dupin_record_get_id (record), (gint)last_revision);
	    }
        }
      else
        {
          tmp = sqlite3_mprintf ("DELETE FROM Dupin WHERE id = '%q' AND rev < %d", (gchar *) dupin_record_get_id (record), (gint)last_revision);
        }

#if DEBUG
      g_message("dupin_database_thread_compact: query=%s\n", tmp);
#endif

      g_rw_lock_writer_lock (db->rwlock);

      if (sqlite3_exec (db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_rw_lock_writer_unlock (db->rwlock);

          sqlite3_free (tmp);

          g_error ("dupin_database_thread_compact: %s", errmsg);

          sqlite3_free (errmsg);

          return FALSE;
        }

      db->compact_processed_count++;

      g_rw_lock_writer_unlock (db->rwlock);

      sqlite3_free (tmp);

      if (compact_id != NULL)
        g_free(compact_id);

      compact_id = g_strdup_printf ("%i", (gint)rowid);

#if DEBUG
      g_message("dupin_database_thread_compact(%p) compact_id=%s as fetched",g_thread_self (), compact_id);
#endif
    }
  
  dupin_record_get_list_close (results);

#if DEBUG
  g_message("dupin_database_thread_compact() compact_id=%s as to be stored",compact_id);

  g_message("dupin_database_thread_compact(%p)  finished last_compact_rowid=%s - compacted %d\n", g_thread_self (), compact_id, (gint)db->compact_processed_count);
#endif

  str = sqlite3_mprintf ("UPDATE DupinDB SET compact_id = '%q'", compact_id);

  if (compact_id != NULL)
    g_free (compact_id);

  g_rw_lock_writer_lock (db->rwlock);

  if (sqlite3_exec (db->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (db->rwlock);
      sqlite3_free (str);

      g_error("dupin_database_thread_compact: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_rw_lock_writer_unlock (db->rwlock);

  sqlite3_free (str);

  return ret;
}

void
dupin_database_compact_func (gpointer data, gpointer user_data)
{
  DupinDB * db = (DupinDB*) data;
  gchar * errmsg;

  dupin_database_ref (db);

#if DEBUG
  g_message("dupin_database_compact_func(%p) started\n",g_thread_self ());
#endif

  g_rw_lock_writer_lock (db->rwlock);
  db->tocompact = TRUE;
  db->compact_thread = g_thread_self ();
  db->compact_processed_count = 0;
  g_rw_lock_writer_unlock (db->rwlock);

  while (db->todelete == FALSE)
    {
      gboolean compact_operation = dupin_database_thread_compact (db, DUPIN_DB_COMPACT_COUNT);

      if (compact_operation == FALSE)
        {
#if DEBUG
          g_message("dupin_database_compact_func(%p) Compacted TOTAL %d records\n", g_thread_self (), (gint)db->compact_processed_count);
#endif

          /* claim disk space back */

	  /* NOTE - wait till next transaction is finished */

	  if (db->d->bulk_transaction == TRUE)
            {
#if DEBUG
              g_message("dupin_database_compact_func(%p) waiting for transaction to finish\n", g_thread_self ());
#endif

	      continue;
	    }

	  /* NOTE - make sure last transaction is commited */

          g_rw_lock_writer_lock (db->rwlock);

	  if (dupin_database_commit_transaction (db, NULL) < 0)
	    {
      	      dupin_database_rollback_transaction (db, NULL);
    	    }

	  /*
		IMPORTANT: rowids may change after a VACUUM, so the cursor of views should be reset as well, eventually !
			   see http://www.sqlite.org/lang_vacuum.html
           */

#if DEBUG
          g_message("dupin_database_compact_func: VACUUM and ANALYZE\n");
#endif

          if (sqlite3_exec (db->db, "VACUUM", NULL, NULL, &errmsg) != SQLITE_OK
             || sqlite3_exec (db->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK)
            {
              g_rw_lock_writer_unlock (db->rwlock);
              g_error ("dupin_database_compact_func: %s while vacuum and analyze db", errmsg);
              sqlite3_free (errmsg);
              break;
            }

          g_rw_lock_writer_unlock (db->rwlock);

          /* NOTE - make sure last transaction is commited */

          g_rw_lock_writer_lock (db->default_attachment_db->rwlock);

          if (dupin_attachment_db_commit_transaction (db->default_attachment_db, NULL) < 0)
            {
              dupin_attachment_db_rollback_transaction (db->default_attachment_db, NULL);
            }

#if DEBUG
          g_message("dupin_database_compact_func: VACUUM and ANALYZE attachments database\n");
#endif

          if (sqlite3_exec (db->default_attachment_db->db, "VACUUM", NULL, NULL, &errmsg) != SQLITE_OK
             || sqlite3_exec (db->default_attachment_db->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK)
            {
              g_rw_lock_writer_unlock (db->default_attachment_db->rwlock);
              g_error("dupin_database_compact_func: %s while vacuum and analyze attachemtns db", errmsg);
              sqlite3_free (errmsg);
	      break;
            }

          g_rw_lock_writer_unlock (db->default_attachment_db->rwlock);

          break;
        }
    }

#if DEBUG
  g_message("dupin_database_compact_func(%p) finished and database is compacted\n",g_thread_self ());
#endif

  g_rw_lock_writer_lock (db->rwlock);
  db->tocompact = FALSE;
  db->topurge = FALSE;
  db->compact_thread = NULL;
  g_rw_lock_writer_unlock (db->rwlock);

  dupin_database_unref (db);
}

void
dupin_database_compact (DupinDB * db,
			gboolean purge)
{
  g_return_if_fail (db != NULL);

  if (dupin_database_is_compacting (db))
    {
#if DEBUG
      g_message("dupin_database_compact(%p): database is still compacting db->compact_thread=%p\n", g_thread_self (), db->compact_thread);
#endif
    }
  else
    {
#if DEBUG
      g_message("dupin_database_compact(%p): push to thread pools db->compact_thread=%p\n", g_thread_self (), db->compact_thread);
#endif

      GError * error=NULL;

      g_rw_lock_writer_lock (db->rwlock);
      db->topurge = purge;
      g_rw_lock_writer_unlock (db->rwlock);

      if (!db->compact_thread)
        {
          g_thread_pool_push(db->d->db_compact_workers_pool, db, &error);

	  if (error)
            {
              g_error("dupin_database_compact: database %s compact thread creation error: %s", db->name, error->message);
              dupin_database_set_error (db, error->message);
              g_error_free (error);
            }
        }
    }
}

gboolean
dupin_database_is_compacting (DupinDB * db)
{
  g_return_val_if_fail (db != NULL, FALSE);

  if (db->compact_thread)
    return TRUE;

  return FALSE;
}

gboolean
dupin_database_is_compacted (DupinDB * db)
{
  g_return_val_if_fail (db != NULL, FALSE);

  if (dupin_database_is_compacting (db))
    return FALSE;

  return db->tocompact ? FALSE : TRUE;
}

void
dupin_database_set_error (DupinDB * db,
			  gchar * msg)
{
  g_return_if_fail (db != NULL);
  g_return_if_fail (msg != NULL);

  dupin_database_clear_error (db);

  db->error_msg = g_strdup ( msg );

  return;
}

void
dupin_database_clear_error (DupinDB * db)
{
  g_return_if_fail (db != NULL);

  if (db->error_msg != NULL)
    g_free (db->error_msg);

  db->error_msg = NULL;

  return;
}

gchar * dupin_database_get_error (DupinDB * db)
{
  g_return_val_if_fail (db != NULL, NULL);

  return db->error_msg;
}

void dupin_database_set_warning (DupinDB * db,
				 gchar * msg)
{
  g_return_if_fail (db != NULL);
  g_return_if_fail (msg != NULL);

  dupin_database_clear_warning (db);

  db->warning_msg = g_strdup ( msg );

  return;
}

void dupin_database_clear_warning (DupinDB * db)
{
  g_return_if_fail (db != NULL);

  if (db->warning_msg != NULL)
    g_free (db->warning_msg);

  db->warning_msg = NULL;

  return;
}

gchar * dupin_database_get_warning (DupinDB * db)
{
  g_return_val_if_fail (db != NULL, NULL);

  return db->warning_msg;
}

/* NOTE - the default attachment db and linkbase are used for insert and retrieve operations
          in dupin_record API - we just set and force the dbname as default, full stop */

gchar *
dupin_database_get_default_attachment_db_name (DupinDB * db)
{
  g_return_val_if_fail (db != NULL, NULL);

  return db->name;
}

DupinAttachmentDB *
dupin_database_get_default_attachment_db (DupinDB * db)
{
  g_return_val_if_fail (db != NULL, NULL);

  return db->default_attachment_db;
}

gchar *
dupin_database_get_default_linkbase_name (DupinDB * db)
{
  g_return_val_if_fail (db != NULL, NULL);

  return db->default_linkbase_name;
}

DupinLinkB *
dupin_database_get_default_linkbase (DupinDB * db)
{
  g_return_val_if_fail (db != NULL, NULL);

  return db->default_linkbase;
}

/* EOF */
