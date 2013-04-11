#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"
#include "dupin_date.h"
#include "dupin_attachment_db.h"

#include <stdlib.h>
#include <string.h>

#define DUPIN_ATTACHMENT_DB_SQL_MAIN_CREATE \
  "CREATE TABLE IF NOT EXISTS Dupin (\n" \
  "  id        CHAR(255) NOT NULL,\n" \
  "  title     CHAR(255) NOT NULL,\n" \
  "  type      CHAR(255) DEFAULT 'application/octect-stream',\n" \
  "  length    INTEGER DEFAULT 0,\n" \
  "  hash      CHAR(255),\n" \
  "  content   BLOB NOT NULL DEFAULT '',\n" \
  "  PRIMARY KEY(id, title)\n" \
  ");"

#define DUPIN_ATTACHMENT_DB_SQL_CREATE_INDEX \
  "CREATE INDEX IF NOT EXISTS DupinIdTitle ON Dupin (id, title);"

#define DUPIN_ATTACHMENT_DB_SQL_DESC_CREATE \
  "CREATE TABLE IF NOT EXISTS DupinAttachmentDB (\n" \
  "  parent          CHAR(255) NOT NULL,\n" \
  "  creation_time   CHAR(255) NOT NULL DEFAULT '0'\n" \
  ");\n" \
  "PRAGMA user_version = 2"

#define DUPIN_ATTACHMENT_DB_SQL_DESC_UPGRADE_FROM_VERSION_1 \
  "ALTER TABLE DupinAttachmentDB ADD COLUMN creation_time CHAR(255) NOT NULL DEFAULT '0';\n" \
  "PRAGMA user_version = 2"

gchar **
dupin_get_attachment_dbs (Dupin * d)
{
  guint i;
  gsize size;
  gchar **ret;
  gpointer key;
  GHashTableIter iter;

  g_return_val_if_fail (d != NULL, NULL);

  g_rw_lock_reader_lock (d->rwlock);

  if (!(size = g_hash_table_size (d->attachment_dbs)))
    {
      g_rw_lock_reader_unlock (d->rwlock);
      return NULL;
    }

  ret = g_malloc (sizeof (gchar *) * (g_hash_table_size (d->attachment_dbs) + 1));

  i = 0;
  g_hash_table_iter_init (&iter, d->attachment_dbs);
  while (g_hash_table_iter_next (&iter, &key, NULL) == TRUE)
    ret[i++] = g_strdup (key);

  ret[i] = NULL;

  g_rw_lock_reader_unlock (d->rwlock);

  return ret;
}

gboolean
dupin_attachment_db_exists (Dupin * d, gchar * attachment_db)
{
  gboolean ret;

  g_rw_lock_reader_lock (d->rwlock);
  ret = g_hash_table_lookup (d->attachment_dbs, attachment_db) != NULL ? TRUE : FALSE;
  g_rw_lock_reader_unlock (d->rwlock);

  return ret;
}

DupinAttachmentDB *
dupin_attachment_db_open (Dupin * d, gchar * attachment_db, GError ** error)
{
  DupinAttachmentDB *ret;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (attachment_db != NULL, NULL);

  g_rw_lock_reader_lock (d->rwlock);

  if (!(ret = g_hash_table_lookup (d->attachment_dbs, attachment_db)) || ret->todelete == TRUE)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		 "Attachment DB '%s' doesn't exist.", attachment_db);

      g_rw_lock_reader_unlock (d->rwlock);
      return NULL;
    }
  else
    {
      ret->ref++;

#if DEBUG
      fprintf(stderr,"dupin_attachment_db_open: (%p) name=%s \t ref++=%d\n", g_thread_self (), attachment_db, (gint) ret->ref);
#endif
    }

  g_rw_lock_reader_unlock (d->rwlock);

  return ret;
}

DupinAttachmentDB *
dupin_attachment_db_new (Dupin * d, gchar * attachment_db,
                         gchar * parent,
		         GError ** error)
{
  DupinAttachmentDB *ret;
  gchar *path;
  gchar *name;

  gchar *str;
  gchar *errmsg;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (attachment_db != NULL, NULL);
  g_return_val_if_fail (parent != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_attachment_db_name (attachment_db) == TRUE, NULL);

  g_return_val_if_fail (dupin_database_exists (d, parent) == TRUE, NULL);

  g_rw_lock_writer_lock (d->rwlock);

  if ((ret = g_hash_table_lookup (d->attachment_dbs, attachment_db)))
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Attachment DB '%s' already exist.", attachment_db);
      g_rw_lock_writer_unlock (d->rwlock);
      return NULL;
    }

  name = g_strdup_printf ("%s%s", attachment_db, DUPIN_ATTACHMENT_DB_SUFFIX);
  path = g_build_path (G_DIR_SEPARATOR_S, d->path, name, NULL);
  g_free (name);

  if (!(ret = dupin_attachment_db_connect (d, attachment_db, path, DP_SQLITE_OPEN_CREATE, error)))
    {
      g_rw_lock_writer_unlock (d->rwlock);
      g_free (path);
      return NULL;
    }

  ret->parent = g_strdup (parent);

  g_free (path);

  ret->ref++;

#if DEBUG
  fprintf(stderr,"dupin_attachment_db_new: (%p) name=%s \t ref++=%d\n", g_thread_self (), attachment_db, (gint) ret->ref);
#endif

  if (dupin_attachment_db_begin_transaction (ret, error) < 0)
    {
      g_rw_lock_writer_unlock (d->rwlock);
      dupin_attachment_db_disconnect (ret);
      return NULL;
    }

  gchar * creation_time = g_strdup_printf ("%" G_GSIZE_FORMAT, dupin_date_timestamp_now (0));

  str =
    sqlite3_mprintf ("INSERT OR REPLACE INTO DupinAttachmentDB "
		     "(parent, creation_time) "
		     "VALUES('%q', '%q')", parent, creation_time);

  g_free (creation_time);

  if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (d->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (str);
      dupin_attachment_db_rollback_transaction (ret, error);
      dupin_attachment_db_disconnect (ret);
      return NULL;
    }

  if (dupin_attachment_db_commit_transaction (ret, error) < 0)
    {
      g_rw_lock_writer_unlock (d->rwlock);
      dupin_attachment_db_disconnect (ret);
      return NULL;
    }

  sqlite3_free (str);

  g_rw_lock_writer_unlock (d->rwlock);

  if (dupin_attachment_db_p_update (ret, error) == FALSE)
    {
      dupin_attachment_db_disconnect (ret);
      return NULL;
    }

  g_rw_lock_writer_lock (d->rwlock);
  g_hash_table_insert (d->attachment_dbs, g_strdup (attachment_db), ret);
  g_rw_lock_writer_unlock (d->rwlock);

  return ret;
}

static int
dupin_attachment_db_p_update_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_attachment_db_p_update_t *update = data;

  if (argv[0] && *argv[0])
    update->parent = g_strdup (argv[0]);

  return 0;
}

#define DUPIN_ATTACHMENT_DB_P_SIZE	64

static void
dupin_attachment_db_p_update_real (DupinAttachmentDBP * p, DupinAttachmentDB * attachment_db)
{
  g_rw_lock_reader_lock (attachment_db->rwlock);
  gboolean todelete = attachment_db->todelete;
  g_rw_lock_reader_unlock (attachment_db->rwlock);

  if (todelete == TRUE)
    {
      if (p->attachment_dbs != NULL)
        {
          /* NOTE - need to remove pointer from parent if linkb is "hot deleted" */

          DupinAttachmentDB ** attachment_dbs = g_malloc (sizeof (DupinAttachmentDB *) * p->size);

          gint i;
          gint current_numb = p->numb;
          p->numb = 0;
          for (i=0; i < current_numb ; i++)
            {
              if (p->attachment_dbs[i] != attachment_db)
                {
                  attachment_dbs[p->numb] = p->attachment_dbs[i];
                  p->numb++;
                }
            }
          g_free (p->attachment_dbs);
          p->attachment_dbs = attachment_dbs;
        }

      return;
    }

  if (p->attachment_dbs == NULL)
    {
      p->attachment_dbs = g_malloc (sizeof (DupinAttachmentDB *) * DUPIN_ATTACHMENT_DB_P_SIZE);
      p->size = DUPIN_ATTACHMENT_DB_P_SIZE;
    }

  else if (p->numb == p->size)
    {
      p->size += DUPIN_ATTACHMENT_DB_P_SIZE;
      p->attachment_dbs = g_realloc (p->attachment_dbs, sizeof (DupinAttachmentDB *) * p->size);
    }

  p->attachment_dbs[p->numb] = attachment_db;
  p->numb++;
}

gboolean
dupin_attachment_db_p_update (DupinAttachmentDB * attachment_db, GError ** error)
{
  gchar *errmsg;
  struct dupin_attachment_db_p_update_t update;
  gchar *query = "SELECT parent FROM DupinAttachmentDB LIMIT 1";

  memset (&update, 0, sizeof (struct dupin_attachment_db_p_update_t));

  g_rw_lock_writer_lock (attachment_db->d->rwlock);

  if (sqlite3_exec (attachment_db->db, query, dupin_attachment_db_p_update_cb, &update, &errmsg)
      != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (attachment_db->d->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      return FALSE;
    }

  g_rw_lock_writer_unlock (attachment_db->d->rwlock);

  if (!update.parent)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Internal error.");
      return FALSE;
    }

  DupinDB *db;

  if (!(db = dupin_database_open (attachment_db->d, update.parent, error)))
    {
      g_free (update.parent);
      return FALSE;
    }

  g_rw_lock_writer_lock (db->rwlock);
  dupin_attachment_db_p_update_real (&db->attachment_dbs, attachment_db);
  g_rw_lock_writer_unlock (db->rwlock);

  dupin_database_unref (db);

  /* make sure parameters are set after dupin server restart on existing database */

  if (attachment_db->parent == NULL)
    attachment_db->parent = update.parent;
  else
    g_free (update.parent);

  return TRUE;
}

void
dupin_attachment_db_ref (DupinAttachmentDB * attachment_db)
{
  Dupin *d;

  g_return_if_fail (attachment_db != NULL);

  d = attachment_db->d;

  g_rw_lock_writer_lock (d->rwlock);

  attachment_db->ref++;

#if DEBUG
  fprintf(stderr,"dupin_attachment_db_ref: (%p) name=%s \t ref++=%d\n", g_thread_self (), attachment_db->name, (gint) attachment_db->ref);
#endif

  g_rw_lock_writer_unlock (d->rwlock);
}

void
dupin_attachment_db_unref (DupinAttachmentDB * attachment_db)
{
  Dupin *d;

  g_return_if_fail (attachment_db != NULL);

  d = attachment_db->d;

  g_rw_lock_writer_lock (d->rwlock);

  if (attachment_db->ref > 0)
    {
      attachment_db->ref--;

#if DEBUG
      fprintf(stderr,"dupin_attachment_db_new: (%p) name=%s \t ref--=%d\n", g_thread_self (), attachment_db->name, (gint) attachment_db->ref);
#endif
    }

  if (attachment_db->todelete == TRUE)
    {
      if (attachment_db->ref > 0)
        {
          g_warning ("dupin_attachment_db_unref: (thread=%p) attachment database %s flagged for deletion but can't free it due ref is %d\n", g_thread_self (), attachment_db->name, (gint) attachment_db->ref);
        }
      else
        {
          g_hash_table_remove (d->attachment_dbs, attachment_db->name);
        }
    }

  g_rw_lock_writer_unlock (d->rwlock);
}

gboolean
dupin_attachment_db_delete (DupinAttachmentDB * attachment_db, GError ** error)
{
  Dupin *d;

  g_return_val_if_fail (attachment_db != NULL, FALSE);

  d = attachment_db->d;

  g_rw_lock_writer_lock (d->rwlock);
  attachment_db->todelete = TRUE;
  g_rw_lock_writer_unlock (d->rwlock);

  if (dupin_attachment_db_p_update (attachment_db, error) == FALSE)
    {
      dupin_attachment_db_disconnect (attachment_db);
      return FALSE;
    }

  return TRUE;
}

const gchar *
dupin_attachment_db_get_name (DupinAttachmentDB * attachment_db)
{
  g_return_val_if_fail (attachment_db != NULL, NULL);
  return attachment_db->name;
}

const gchar *
dupin_attachment_db_get_parent (DupinAttachmentDB * attachment_db)
{
  g_return_val_if_fail (attachment_db != NULL, NULL);

  return attachment_db->parent;
}

gsize
dupin_attachment_db_get_size (DupinAttachmentDB * attachment_db)
{
  struct stat st;

  g_return_val_if_fail (attachment_db != NULL, 0);

  if (g_stat (attachment_db->path, &st) != 0)
    return 0;

  return (gsize) st.st_size;
}

static int
dupin_attachment_db_get_creation_time_cb (void *data, int argc, char **argv, char **col)
{
  gsize *creation_time = data;

  if (argv[0])
    *creation_time = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gboolean
dupin_attachment_db_get_creation_time (DupinAttachmentDB * attachment_db, gsize * creation_time)
{
  gchar * query;
  gchar * errmsg=NULL;

  *creation_time = 0;

  g_return_val_if_fail (attachment_db != NULL, 0);

  /* get creation time out of attachment db */
  query = "SELECT creation_time as creation_time FROM DupinAttachmentDB";
  g_rw_lock_reader_lock (attachment_db->rwlock);

  if (sqlite3_exec (attachment_db->db, query, dupin_attachment_db_get_creation_time_cb, creation_time, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (attachment_db->rwlock);

      g_error("dupin_attachment_db_get_creation_time: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_rw_lock_reader_unlock (attachment_db->rwlock);

  return TRUE;
}

/* Internal: */
void
dupin_attachment_db_disconnect (DupinAttachmentDB * attachment_db)
{
#if DEBUG
  g_message("dupin_attachment_db_disconnect: total number of changes for '%s' attachments database: %d\n", attachment_db->name, (gint)sqlite3_total_changes (attachment_db->db));
#endif

  if (attachment_db->db)
    sqlite3_close (attachment_db->db);

  if (attachment_db->todelete == TRUE)
    g_unlink (attachment_db->path);

  if (attachment_db->name)
    g_free (attachment_db->name);

  if (attachment_db->path)
    g_free (attachment_db->path);

  if (attachment_db->parent)
    g_free (attachment_db->parent);

  if (attachment_db->rwlock)
    {
      g_rw_lock_clear (attachment_db->rwlock);
      g_free (attachment_db->rwlock);
    }

  if (attachment_db->error_msg)
    g_free (attachment_db->error_msg);

  if (attachment_db->warning_msg)
    g_free (attachment_db->warning_msg);

  g_free (attachment_db);
}

static int
dupin_attachment_db_connect_cb (void *data, int argc, char **argv, char **col)
{
  DupinAttachmentDB *attachment_db = data;

  if (argc == 1)
    {
      attachment_db->parent = g_strdup (argv[0]);
    }

  return 0;
}

static int
dupin_attachment_db_get_user_version_cb (void *data, int argc, char **argv,
                                    char **col)
{
  gint *user_version = data;

  if (argv[0])
    *user_version = atoi (argv[0]);

  return 0;
}

DupinAttachmentDB *
dupin_attachment_db_connect (Dupin * d, gchar * name, gchar * path,
			     DupinSQLiteOpenType mode,
			     GError ** error)
{
  gchar *query;
  gchar *errmsg;
  DupinAttachmentDB *attachment_db;

  attachment_db = g_malloc0 (sizeof (DupinAttachmentDB));

  attachment_db->d = d;

  attachment_db->name = g_strdup (name);
  attachment_db->path = g_strdup (path);

  if (sqlite3_open_v2 (attachment_db->path, &attachment_db->db, dupin_util_dupin_mode_to_sqlite_mode (mode), NULL) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Attachment DB error.");
      dupin_attachment_db_disconnect (attachment_db);
      return NULL;
    }

  sqlite3_busy_timeout (attachment_db->db, DUPIN_SQLITE_TIMEOUT);

  if (mode == DP_SQLITE_OPEN_CREATE)
    {
      if (sqlite3_exec (attachment_db->db, "PRAGMA journal_mode = WAL", NULL, NULL, &errmsg) != SQLITE_OK
          || sqlite3_exec (attachment_db->db, "PRAGMA encoding = \"UTF-8\"", NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma journal_mode or encoding: %s",
		   errmsg);
          sqlite3_free (errmsg);
          dupin_attachment_db_disconnect (attachment_db);
          return NULL;
        }

      if (dupin_attachment_db_begin_transaction (attachment_db, error) < 0)
        {
          dupin_attachment_db_disconnect (attachment_db);
          return NULL;
        }

      if (sqlite3_exec (attachment_db->db, DUPIN_ATTACHMENT_DB_SQL_MAIN_CREATE, NULL, NULL, &errmsg) != SQLITE_OK
          || sqlite3_exec (attachment_db->db, DUPIN_ATTACHMENT_DB_SQL_DESC_CREATE, NULL, NULL, &errmsg) != SQLITE_OK
          || sqlite3_exec (attachment_db->db, DUPIN_ATTACHMENT_DB_SQL_CREATE_INDEX, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
          sqlite3_free (errmsg);
	  dupin_attachment_db_rollback_transaction (attachment_db, error);
          dupin_attachment_db_disconnect (attachment_db);
          return NULL;
        }

      if (dupin_attachment_db_commit_transaction (attachment_db, error) < 0)
        {
          dupin_attachment_db_disconnect (attachment_db);
          return NULL;
        }
    }

  /* check attachment_db version */
  gint user_version = 0;

  if (sqlite3_exec (attachment_db->db, "PRAGMA user_version", dupin_attachment_db_get_user_version_cb, &user_version, &errmsg) != SQLITE_OK)
    {
      /* default to 1 if not found or error - TODO check not SQLITE_OK error only */
      user_version = 1;
    }

  if (user_version > DUPIN_SQLITE_MAX_USER_VERSION)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "SQLite attachment db user version (%d) is newer than I know how to work with (%d).",
				user_version, DUPIN_SQLITE_MAX_USER_VERSION);
      sqlite3_free (errmsg);
      dupin_attachment_db_disconnect (attachment_db);
      return NULL;
    }

  if (user_version <= 1)
    {
      if (sqlite3_exec (attachment_db->db, DUPIN_ATTACHMENT_DB_SQL_DESC_UPGRADE_FROM_VERSION_1, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                   errmsg);
          sqlite3_free (errmsg);
          dupin_attachment_db_disconnect (attachment_db);
          return NULL;
        }
    }

  gchar * cache_size = g_strdup_printf ("PRAGMA cache_size = %d", DUPIN_SQLITE_CACHE_SIZE); 
  if (sqlite3_exec (attachment_db->db, "PRAGMA temp_store = memory", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (attachment_db->db, cache_size, NULL, NULL, &errmsg) != SQLITE_OK)
    {   
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma temp_store: %s",
                   errmsg);
      sqlite3_free (errmsg);
      if (cache_size)
        g_free (cache_size); 
      dupin_attachment_db_disconnect (attachment_db);
      return NULL;
    }

  if (cache_size)
    g_free (cache_size);

  /*
   TODO - check if the below can be optimized using NORMAL or OFF and use separated syncing thread
          see also http://www.sqlite.org/pragma.html#pragma_synchronous
   */

  if (sqlite3_exec (attachment_db->db, "PRAGMA synchronous = NORMAL", NULL, NULL, &errmsg) != SQLITE_OK)
    {   
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma synchronous: %s",
                   errmsg);
      sqlite3_free (errmsg);
      dupin_attachment_db_disconnect (attachment_db);
      return NULL;
    }

  query =
    "SELECT parent FROM DupinAttachmentDB LIMIT 1";

  if (sqlite3_exec (attachment_db->db, query, dupin_attachment_db_connect_cb, attachment_db, &errmsg) !=
      SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      dupin_attachment_db_disconnect (attachment_db);
    }

  attachment_db->rwlock = g_new0 (GRWLock, 1);
  g_rw_lock_init (attachment_db->rwlock);

  return attachment_db;
}

/* NOTE - 0 = ok, 1 = already in transaction, -1 = error */

gint
dupin_attachment_db_begin_transaction (DupinAttachmentDB * attachment_db, GError ** error)
{
  g_return_val_if_fail (attachment_db != NULL, -1);

  gchar *errmsg;
  gint rc = -1;

  if (attachment_db->d->bulk_transaction == TRUE)
    {
//g_message ("dupin_attachment_db_begin_transaction: attachment database %s transaction ALREADY open", attachment_db->name);

      return 1;
    }

  rc = sqlite3_exec (attachment_db->db, "BEGIN TRANSACTION", NULL, NULL, &errmsg);

  if (rc == SQLITE_BUSY)
    {
        rc = dupin_sqlite_subs_mgr_busy_handler(attachment_db->db, "BEGIN TRANSACTION", NULL, NULL, &errmsg, rc);
    }

  if (rc != SQLITE_OK)
    {
      if (error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot begin attachment database %s transaction: %s", attachment_db->name, errmsg);

      sqlite3_free (errmsg);

      return -1;
    }

//g_message ("dupin_attachment_db_begin_transaction: attachment database %s transaction begin", attachment_db->name);

  return 0;
}

gint
dupin_attachment_db_rollback_transaction (DupinAttachmentDB * attachment_db, GError ** error)
{
  g_return_val_if_fail (attachment_db != NULL, -1);

  gchar *errmsg;
  gint rc = -1;

  rc = sqlite3_exec (attachment_db->db, "ROLLBACK", NULL, NULL, &errmsg);

  if (rc == SQLITE_BUSY)
    {
        rc = dupin_sqlite_subs_mgr_busy_handler(attachment_db->db, "ROLLBACK", NULL, NULL, &errmsg, rc);
    }

  if (rc != SQLITE_OK)
    {
      if (error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot rollback attachment database %s transaction: %s", attachment_db->name, errmsg);

      sqlite3_free (errmsg);

      return -1;
    }

//g_message ("dupin_attachment_db_rollback_transaction: attachment database %s transaction rollback", attachment_db->name);

  return 0;
}

gint
dupin_attachment_db_commit_transaction (DupinAttachmentDB * attachment_db, GError ** error)
{
  g_return_val_if_fail (attachment_db != NULL, -1);

  gchar *errmsg;
  gint rc = -1;

  if (attachment_db->d->bulk_transaction == TRUE)
    {
//g_message ("dupin_attachment_db_commit_transaction: attachment database %s transaction commit POSTPONED", attachment_db->name);

      return 1;
    }

  rc = sqlite3_exec (attachment_db->db, "COMMIT", NULL, NULL, &errmsg);

  if (rc == SQLITE_BUSY)
    {
        rc = dupin_sqlite_subs_mgr_busy_handler(attachment_db->db, "COMMIT", NULL, NULL, &errmsg, rc);
    }

  if (rc != SQLITE_OK)
    {
      if (error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot commit attachment database %s transaction: %s", attachment_db->name, errmsg);

      sqlite3_free (errmsg);

      return -1;
    }

//g_message ("dupin_attachment_db_commit_transaction: attachment database %s transaction commit", attachment_db->name);

  return 0;
}

static int
dupin_attachment_db_count_cb (void *data, int argc, char **argv, char **col)
{
  gsize *size = data;

  if (argv[0] && *argv[0])
    *size = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gsize
dupin_attachment_db_count (DupinAttachmentDB * attachment_db)
{
  gsize size;
  gchar *query;

  g_return_val_if_fail (attachment_db != NULL, 0);

  query = "SELECT count(*) as c FROM Dupin";

  g_rw_lock_reader_lock (attachment_db->rwlock);

  if (sqlite3_exec (attachment_db->db, query, dupin_attachment_db_count_cb, &size, NULL) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (attachment_db->rwlock);
      return 0;
    }

  g_rw_lock_reader_unlock (attachment_db->rwlock);
  return size;
}

void
dupin_attachment_db_set_error (DupinAttachmentDB * attachment_db,
                          gchar * msg)
{
  g_return_if_fail (attachment_db != NULL);
  g_return_if_fail (msg != NULL);

  dupin_attachment_db_clear_error (attachment_db);

  attachment_db->error_msg = g_strdup ( msg );

  return;
}

void
dupin_attachment_db_clear_error (DupinAttachmentDB * attachment_db)
{
  g_return_if_fail (attachment_db != NULL);

  if (attachment_db->error_msg != NULL)
    g_free (attachment_db->error_msg);

  attachment_db->error_msg = NULL;

  return;
}

gchar * dupin_attachment_db_get_error (DupinAttachmentDB * attachment_db)
{
  g_return_val_if_fail (attachment_db != NULL, NULL);

  return attachment_db->error_msg;
}

void dupin_attachment_db_set_warning (DupinAttachmentDB * attachment_db,
                                 gchar * msg)
{
  g_return_if_fail (attachment_db != NULL);
  g_return_if_fail (msg != NULL);

  dupin_attachment_db_clear_warning (attachment_db);

  attachment_db->warning_msg = g_strdup ( msg );

  return;
}

void dupin_attachment_db_clear_warning (DupinAttachmentDB * attachment_db)
{
  g_return_if_fail (attachment_db != NULL);

  if (attachment_db->warning_msg != NULL)
    g_free (attachment_db->warning_msg);

  attachment_db->warning_msg = NULL;

  return;
}

gchar * dupin_attachment_db_get_warning (DupinAttachmentDB * attachment_db)
{
  g_return_val_if_fail (attachment_db != NULL, NULL);

  return attachment_db->warning_msg;
}

/* EOF */
