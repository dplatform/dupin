#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"
#include "dupin_db.h"

#include <stdlib.h>
#include <string.h>

#define DUPIN_DB_SQL_MAIN_CREATE \
  "CREATE TABLE IF NOT EXISTS Dupin (\n" \
  "  id      CHAR(255) NOT NULL,\n" \
  "  rev     INTEGER NOT NULL DEFAULT 1,\n" \
  "  obj     TEXT,\n" \
  "  deleted BOOL DEFAULT FALSE,\n" \
  "  PRIMARY KEY(id, rev)\n" \
  ");"

gchar **
dupin_get_databases (Dupin * d)
{
  guint i;
  gsize size;
  gchar **ret;
  gpointer key;
  GHashTableIter iter;

  g_return_val_if_fail (d != NULL, NULL);

  g_mutex_lock (d->mutex);

  if (!(size = g_hash_table_size (d->dbs)))
    {
      g_mutex_unlock (d->mutex);
      return NULL;
    }

  ret = g_malloc (sizeof (gchar *) * (g_hash_table_size (d->dbs) + 1));

  i = 0;
  g_hash_table_iter_init (&iter, d->dbs);
  while (g_hash_table_iter_next (&iter, &key, NULL) == TRUE)
    ret[i++] = g_strdup (key);

  ret[i] = NULL;

  g_mutex_unlock (d->mutex);

  return ret;
}

gboolean
dupin_database_exists (Dupin * d, gchar * db)
{
  gboolean ret;

  g_mutex_lock (d->mutex);
  ret = g_hash_table_lookup (d->dbs, db) != NULL ? TRUE : FALSE;
  g_mutex_unlock (d->mutex);

  return ret;
}

DupinDB *
dupin_database_open (Dupin * d, gchar * db, GError ** error)
{
  DupinDB *ret;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (db != NULL, NULL);

  g_mutex_lock (d->mutex);

  if (!(ret = g_hash_table_lookup (d->dbs, db)) || ret->todelete == TRUE)
    g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		 "Database '%s' doesn't exist.", db);

  else {
    /* fprintf(stderr,"ref++\n"); */
    ret->ref++;
	};

  g_mutex_unlock (d->mutex);

  return ret;
}

DupinDB *
dupin_database_new (Dupin * d, gchar * db, GError ** error)
{
  DupinDB *ret;
  gchar *path;
  gchar *name;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (db != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_db_name (db) == TRUE, NULL);

  g_mutex_lock (d->mutex);

  if ((ret = g_hash_table_lookup (d->dbs, db)))
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Database '%s' already exist.", db);
      g_mutex_unlock (d->mutex);
      return NULL;
    }

  name = g_strdup_printf ("%s%s", db, DUPIN_DB_SUFFIX);
  path = g_build_path (G_DIR_SEPARATOR_S, d->path, name, NULL);
  g_free (name);

  if (!(ret = dupin_db_create (d, db, path, error)))
    {
      g_mutex_unlock (d->mutex);
      g_free (path);
      return NULL;
    }

  g_free (path);

  /* fprintf(stderr,"ref++\n"); */
  ret->ref++;

  g_hash_table_insert (d->dbs, g_strdup (db), ret);

  g_mutex_unlock (d->mutex);

  return ret;
}

void
dupin_database_ref (DupinDB * db)
{
  Dupin *d;

  g_return_if_fail (db != NULL);

  d = db->d;

  g_mutex_lock (d->mutex);
  /* fprintf(stderr,"ref++\n"); */
  db->ref++;
  g_mutex_unlock (d->mutex);
}

void
dupin_database_unref (DupinDB * db)
{
  Dupin *d;

  g_return_if_fail (db != NULL);

  d = db->d;
  g_mutex_lock (d->mutex);

  if (db->ref >= 0) {
    /* fprintf(stderr,"ref--\n"); */
    db->ref--;
    };

  if (db->ref == 0 && db->todelete == TRUE)
    g_hash_table_remove (d->dbs, db->name);

  g_mutex_unlock (d->mutex);
}

gboolean
dupin_database_delete (DupinDB * db, GError ** error)
{
  Dupin *d;

  g_return_val_if_fail (db != NULL, FALSE);

  d = db->d;

  g_mutex_lock (d->mutex);
  db->todelete = TRUE;
  g_mutex_unlock (d->mutex);

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

static void
dupin_database_generate_id_create (DupinDB * db, gchar id[255])
{
  do
    {
      dupin_util_generate_id (id);
    }
  while (dupin_record_exists_real (db, id, FALSE) == TRUE);
}

gchar *
dupin_database_generate_id_real (DupinDB * db, GError ** error, gboolean lock)
{
  gchar id[255];

  if (lock == TRUE)
    g_mutex_lock (db->mutex);

  dupin_database_generate_id_create (db, id);

  if (lock == TRUE)
    g_mutex_unlock (db->mutex);

  return g_strdup (id);
}

gchar *
dupin_database_generate_id (DupinDB * db, GError ** error)
{
  g_return_val_if_fail (db != NULL, NULL);

  return dupin_database_generate_id_real (db, error, TRUE);
}

/* Internal: */
void
dupin_db_free (DupinDB * db)
{
  if (db->db)
    sqlite3_close (db->db);

  if (db->todelete == TRUE)
    g_unlink (db->path);

  if (db->name)
    g_free (db->name);

  if (db->path)
    g_free (db->path);

  if (db->mutex)
    g_mutex_free (db->mutex);

  if (db->views.views)
    g_free (db->views.views);

  g_free (db);
}

DupinDB *
dupin_db_create (Dupin * d, gchar * name, gchar * path, GError ** error)
{
  gchar *errmsg;
  DupinDB *db;

  db = g_malloc0 (sizeof (DupinDB));

  db->d = d;

  db->name = g_strdup (name);
  db->path = g_strdup (path);

  if (sqlite3_open (db->path, &db->db) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Database error.");
      dupin_db_free (db);
      return NULL;
    }

  if (sqlite3_exec (db->db, DUPIN_DB_SQL_MAIN_CREATE, NULL, NULL, &errmsg) !=
      SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      dupin_db_free (db);
      return NULL;
    }

  db->mutex = g_mutex_new ();

  return db;
}

struct dp_count_t
{
  gsize ret;
  DupinCountType type;
};

static int
dupin_database_count_cb (void *data, int argc, char **argv, char **col)
{
  struct dp_count_t *count = data;

  if (argv[0] && *argv[0])
    {
      switch (count->type)
	{
	case DP_COUNT_EXIST:
	  if (!strcmp (argv[0], "FALSE"))
	    count->ret++;
	  break;

	case DP_COUNT_DELETE:
	  if (!strcmp (argv[0], "TRUE"))
	    count->ret++;
	  break;

	case DP_COUNT_ALL:
	default:
	  count->ret++;
	  break;
	}
    }

  return 0;
}

gsize
dupin_database_count (DupinDB * db, DupinCountType type)
{
  struct dp_count_t count;
  gchar *query;

  g_return_val_if_fail (db != NULL, 0);

  count.ret = 0;
  count.type = type;

  query = "SELECT deleted, max(rev) as rev FROM Dupin GROUP BY id";

  g_mutex_lock (db->mutex);

  if (sqlite3_exec (db->db, query, dupin_database_count_cb, &count, NULL) !=
      SQLITE_OK)
    {
      g_mutex_unlock (db->mutex);
      return 0;
    }

  g_mutex_unlock (db->mutex);
  return count.ret;
}

/* EOF */
