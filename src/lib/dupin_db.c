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
  "  hash    CHAR(255) NOT NULL,\n" \
  "  obj     TEXT,\n" \
  "  deleted BOOL DEFAULT FALSE,\n" \
  "  PRIMARY KEY(id, rev, hash)\n" \
  ");"

#define DUPIN_DB_SQL_CREATE_INDEX \
  "CREATE INDEX IF NOT EXISTS DupinId ON Dupin (id);"

#define DUPIN_DB_SQL_TOTAL \
        "SELECT count(*) AS c FROM Dupin AS d"

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
dupin_database_generate_id_create (DupinDB * db, gchar id[DUPIN_ID_MAX_LEN])
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
  gchar id[DUPIN_ID_MAX_LEN];

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

  if (db->attachment_dbs.attachment_dbs)
    g_free (db->attachment_dbs.attachment_dbs);

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

  if (sqlite3_exec (db->db, DUPIN_DB_SQL_MAIN_CREATE, NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (db->db, DUPIN_DB_SQL_CREATE_INDEX, NULL, NULL, &errmsg) != SQLITE_OK)
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
	  if (!g_strcmp0 (argv[0], "FALSE"))
	    count->ret++;
	  break;

	case DP_COUNT_DELETE:
	  if (!g_strcmp0 (argv[0], "TRUE"))
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

static int
dupin_database_get_max_rowid_cb (void *data, int argc, char **argv,
                                  char **col)
{
  gsize *max_rowid = data;

  if (argv[0])
    *max_rowid = atoi (argv[0]);

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

  g_mutex_lock (db->mutex);

  if (sqlite3_exec (db->db, query, dupin_database_get_max_rowid_cb, max_rowid, &errmsg) !=
      SQLITE_OK)
    {
      g_mutex_unlock (db->mutex);

      g_error("dupin_database_get_max_rowid: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (db->mutex);

  return TRUE;
}

struct dupin_database_get_changes_list_t
{
  DupinChangesType style;
  GList *list;
};

static int
dupin_database_get_changes_list_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_database_get_changes_list_t *s = data;

  guint rev = 0;
  gchar *id = NULL;
  gchar *hash = NULL;
  gchar *obj = NULL;
  gboolean delete = FALSE;
  gsize rowid=0;
  gint i;

  for (i = 0; i < argc; i++)
    {
      /* shouldn't this be double and use atof() ?!? */
      if (!g_strcmp0 (col[i], "rev"))
        rev = atoi (argv[i]);

      else if (!g_strcmp0 (col[i], "hash"))
        hash = argv[i];

      else if (!g_strcmp0 (col[i], "obj"))
        obj = argv[i];

      else if (!g_strcmp0 (col[i], "deleted"))
        delete = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;

      else if (!g_strcmp0 (col[i], "rowid"))
        rowid = atoi(argv[i]);

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

      JsonArray *change_details=json_array_new();
      json_object_set_array_member (change, "changes", change_details);

      JsonObject * node_obj = json_object_new ();

      gchar mvcc[DUPIN_ID_MAX_LEN];
      dupin_util_mvcc_new (rev, hash, mvcc);

      json_object_set_string_member (node_obj, "rev", mvcc);

      if (s->style == DP_CHANGES_MAIN_ONLY)
        {
        }
      else if (s->style == DP_CHANGES_ALL_DOCS)
        {
          if (delete == TRUE)
            json_object_set_boolean_member (node_obj, "deleted", delete);
        }

      json_array_add_object_element (change_details, node_obj);

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

  memset (&s, 0, sizeof (s));
  s.style = changes_type;

  str = g_string_new ("SELECT id, rev, hash, obj, deleted, ROWID AS rowid FROM Dupin as d");

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  if (since > 0 && to > 0)
    g_string_append_printf (str, " WHERE %s %s d.ROWID >= %d AND d.ROWID <= %d ", check_deleted, (g_strcmp0 (check_deleted, "")) ? "AND" : "", (gint)since, (gint)to);
  else if (since > 0)
    g_string_append_printf (str, " WHERE %s %s d.ROWID >= %d ", check_deleted, (g_strcmp0 (check_deleted, "")) ? "AND" : "", (gint)since);
  else if (to > 0)
    g_string_append_printf (str, " WHERE %s %s d.ROWID <= %d ", check_deleted, (g_strcmp0 (check_deleted, "")) ? "AND" : "", (gint)to);
  else if (g_strcmp0 (check_deleted, ""))
    g_string_append_printf (str, " WHERE %s ", check_deleted);

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

//g_message("dupin_database_get_changes_list() query=%s\n",tmp);

  g_mutex_lock (db->mutex);

  if (sqlite3_exec (db->db, tmp, dupin_database_get_changes_list_cb, &s, &errmsg) !=
      SQLITE_OK)
    {
      g_mutex_unlock (db->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (db->mutex);

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

  if (argv[0])
    *numb = atoi (argv[0]);

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
                                 GError **              error)
{
  g_return_val_if_fail (db != NULL, FALSE);

  gchar *errmsg;
  gchar *tmp;
  GString *str;

  *total = 0;

  gchar *check_deleted="";

  str = g_string_new (DUPIN_DB_SQL_TOTAL);

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  if (since > 0 && to > 0)
    g_string_append_printf (str, " WHERE %s %s d.ROWID >= %d AND d.ROWID <= %d ", check_deleted, (g_strcmp0 (check_deleted, "")) ? "AND" : "", (gint)since, (gint)to);
  else if (since > 0)
    g_string_append_printf (str, " WHERE %s %s d.ROWID >= %d ", check_deleted, (g_strcmp0 (check_deleted, "")) ? "AND" : "", (gint)since);
  else if (to > 0)
    g_string_append_printf (str, " WHERE %s %s d.ROWID <= %d ", check_deleted, (g_strcmp0 (check_deleted, "")) ? "AND" : "", (gint)to);
  else if (g_strcmp0 (check_deleted, ""))
    g_string_append_printf (str, " WHERE %s ", check_deleted);

  tmp = g_string_free (str, FALSE);

//g_message("dupin_database_get_total_changes() query=%s\n",tmp);

  g_mutex_lock (db->mutex);

  if (sqlite3_exec (db->db, tmp, dupin_database_get_total_changes_cb, total, &errmsg) !=
      SQLITE_OK)
    {
      g_mutex_unlock (db->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (db->mutex);

  g_free (tmp);

  return TRUE;
}

/* EOF */
