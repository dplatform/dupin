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
  "  tm      INTEGER NOT NULL,\n" \
  "  PRIMARY KEY(id, rev)\n" \
  ");"

#define DUPIN_DB_SQL_CREATE_INDEX \
  "CREATE INDEX IF NOT EXISTS DupinId ON Dupin (id);\n" \
  "CREATE INDEX IF NOT EXISTS DupinIdRev ON Dupin (id,rev);"

#define DUPIN_DB_SQL_DESC_CREATE \
  "CREATE TABLE IF NOT EXISTS DupinDB (\n" \
  "  total_doc_ins   INTEGER NOT NULL DEFAULT 0,\n" \
  "  total_doc_del   INTEGER NOT NULL DEFAULT 0,\n" \
  "  compact_id      CHAR(255)\n" \
  ");"

#define DUPIN_DB_SQL_TOTAL \
        "SELECT count(*) AS c, max(rev) as rev FROM Dupin AS d"

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
     //fprintf(stderr,"dupin_database_open: (%p) ref++\n", g_thread_self ());
    ret->ref++;
     //fprintf(stderr,"dupin_database_open: (%p) \t ref=%d\n", g_thread_self (), (gint) ret->ref);
	};

  g_mutex_unlock (d->mutex);

  return ret;
}

DupinDB *
dupin_database_new (Dupin * d, gchar * db, GError ** error)
{
  DupinDB *ret;
  DupinLinkB *linkb;
  DupinAttachmentDB *attachment_db;

  gchar *path;
  gchar *name;
  gchar * str;
  gchar * errmsg;

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

    //fprintf(stderr,"dupin_database_new: (%p) ref++\n", g_thread_self ());
  ret->ref++;
    //fprintf(stderr,"dupin_database_new: (%p) \t ref=%d\n", g_thread_self (), (gint) ret->ref);

  g_hash_table_insert (d->dbs, g_strdup (db), ret);

  
  /* NOTE - the respective map and reduce threads will add +1 top the these values */
  str = sqlite3_mprintf ("INSERT INTO DupinDB (compact_id) VALUES (0)");

  if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (d->mutex);
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                       errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (str);
      dupin_db_free (ret);
      return NULL;
    }

  sqlite3_free (str);

  g_mutex_unlock (d->mutex);

  /* NOTE - create one default link base and attachment database named after the main database */

  if (!  (linkb = dupin_linkbase_new (d, ret->default_linkbase_name, db, TRUE, NULL)))
    return NULL;

  // keep a ref to default linkbase so it can not be accidentally deleted
  //dupin_linkbase_unref (linkb);

  if (!  (attachment_db =
       dupin_attachment_db_new (d, ret->default_attachment_db_name, db, NULL)))
    return NULL;

  // keep a ref to default attachments db so it can not be accidentally deleted
  //dupin_attachment_db_unref (attachment_db);

  return ret;
}

void
dupin_database_ref (DupinDB * db)
{
  Dupin *d;

  g_return_if_fail (db != NULL);

  d = db->d;

  g_mutex_lock (d->mutex);
   //fprintf(stderr,"dupin_database_ref: (%p) ref++\n", g_thread_self ());
  db->ref++;
   //fprintf(stderr,"dupin_database_ref: (%p) \t ref=%d\n", g_thread_self (), (gint) db->ref);
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
     //fprintf(stderr,"dupin_database_unref: (%p) ref--\n", g_thread_self ());
    db->ref--;
     //fprintf(stderr,"dupin_database_unref: (%p) \t ref=%d\n", g_thread_self (), (gint) db->ref);
    };

  if (db->ref != 0 && db->todelete == TRUE)
    g_warning ("dupin_database_unref: (thread=%p) database %s flagged for deletion but can't free it due ref is %d\n", g_thread_self (), db->name, (gint) db->ref);

  if (db->ref == 0 && db->todelete == TRUE)
    g_hash_table_remove (d->dbs, db->name);

  g_mutex_unlock (d->mutex);
}

gboolean
dupin_database_delete (DupinDB * db, GError ** error)
{
  Dupin *d;
  DupinLinkB *linkb;
  DupinAttachmentDB *attachment_db;

  g_return_val_if_fail (db != NULL, FALSE);

  d = db->d;

  /* NOTE - delete default link base and attachment database named after the main database */

  /* NOTE - the following "repeats" of "unref" must be left due we kept a ref to those since we first created this DB
            of course, this will not survive stop/start of server - we will need to prob have
            linkbases and attachment dbs to be only deleted via the main DB 
	    Note we do the second unref after open to make sure the linkbase and 
	    attachment dbs can be open, and the later deleted, and garbage collection can be done correctly */

  if (!
      (linkb =
       dupin_linkbase_open (d, db->default_linkbase_name, NULL)))
    {
      dupin_database_set_error (db, "Cannot connect to default linkbase");
      return FALSE;
    }

  dupin_linkbase_unref (linkb); // default ref (see dupin_database_new() above)

  if (dupin_linkbase_delete (linkb, NULL) == FALSE)
    {
      dupin_database_set_error (db, "Cannot delete default linkbase");
      return FALSE;
    }

  dupin_linkbase_unref (linkb);

  if (!
      (attachment_db =
       dupin_attachment_db_open (d, db->default_attachment_db_name, NULL)))
    {
      dupin_database_set_error (db, "Cannot connect to default attachments database");
      return FALSE;
    }

  dupin_attachment_db_unref (attachment_db); // default ref (see dupin_database_new() above)

  if (dupin_attachment_db_delete (attachment_db, NULL) == FALSE)
    {
      dupin_database_set_error (db, "Cannot delete default attachments database");
      return FALSE;
    }

  dupin_attachment_db_unref (attachment_db);

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
  g_message("dupin_db_free: total number of changes for '%s' database: %d\n", db->name, (gint)sqlite3_total_changes (db->db));

  if (db->db)
    sqlite3_close (db->db);

  if (db->todelete == TRUE)
    g_unlink (db->path);

  if (db->name)
    g_free (db->name);

  if (db->default_attachment_db_name)
    g_free (db->default_attachment_db_name);

  if (db->default_linkbase_name)
    g_free (db->default_linkbase_name);

  if (db->path)
    g_free (db->path);

  if (db->mutex)
    g_mutex_free (db->mutex);

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

DupinDB *
dupin_db_create (Dupin * d, gchar * name, gchar * path, GError ** error)
{
  gchar *errmsg;
  DupinDB *db;

  db = g_malloc0 (sizeof (DupinDB));

  db->d = d;

  db->name = g_strdup (name);
  db->path = g_strdup (path);

  db->tocompact = FALSE;
  db->compact_processed_count = 0;

  /* NOTE - default attachement db and linkbase - unchangable at the moment */
  db->default_attachment_db_name = g_strdup (name);
  db->default_linkbase_name = g_strdup (name);

  if (sqlite3_open (db->path, &db->db) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Database error.");
      dupin_db_free (db);
      return NULL;
    }

  if (sqlite3_exec (db->db, DUPIN_DB_SQL_MAIN_CREATE, NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (db->db, DUPIN_DB_SQL_CREATE_INDEX, NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (db->db, DUPIN_DB_SQL_DESC_CREATE, NULL, NULL, &errmsg) != SQLITE_OK)
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

gsize
dupin_database_count (DupinDB * db, DupinCountType type)
{
  g_return_val_if_fail (db != NULL, 0);

  struct dupin_record_select_total_t count;
  memset (&count, 0, sizeof (count));

  g_mutex_lock (db->mutex);

  if (sqlite3_exec (db->db, DUPIN_DB_SQL_GET_TOTALS, dupin_record_select_total_cb, &count, NULL) !=
      SQLITE_OK)
    {
      g_mutex_unlock (db->mutex);
      return 0;
    }

  g_mutex_unlock (db->mutex);

  if (type == DP_COUNT_EXIST)
    {
      return count.total_doc_ins;
    }
  else if (type == DP_COUNT_DELETE)
    {
      return count.total_doc_del;
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
  gsize tm = 0;
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

      else if (!g_strcmp0 (col[i], "tm"))
        tm = (gsize)atof(argv[i]);

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

      if (delete == TRUE)
        json_object_set_boolean_member (change, "deleted", delete);

      gchar * created = dupin_util_timestamp_to_iso8601 (tm);
      json_object_set_string_member (change, RESPONSE_OBJ_CREATED, created);
      g_free (created);

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

  str = g_string_new ("SELECT id, rev, hash, obj, deleted, tm, ROWID AS rowid FROM Dupin as d WHERE d.rev = (select max(rev) as rev FROM Dupin WHERE id=d.id) ");

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  if (since > 0 && to > 0)
    g_string_append_printf (str, " AND %s %s d.ROWID >= %d AND d.ROWID <= %d ", check_deleted, (g_strcmp0 (check_deleted, "")) ? "AND" : "", (gint)since, (gint)to);
  else if (since > 0)
    g_string_append_printf (str, " AND %s %s d.ROWID >= %d ", check_deleted, (g_strcmp0 (check_deleted, "")) ? "AND" : "", (gint)since);
  else if (to > 0)
    g_string_append_printf (str, " AND %s %s d.ROWID <= %d ", check_deleted, (g_strcmp0 (check_deleted, "")) ? "AND" : "", (gint)to);
  else if (g_strcmp0 (check_deleted, ""))
    g_string_append_printf (str, " AND %s ", check_deleted);

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
    *numb += 1;

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

  str = g_string_append (str, " GROUP BY d.id ");

  tmp = g_string_free (str, FALSE);

//g_message("dupin_database_get_total_changes() query=%s\n",tmp);

  g_mutex_lock (db->mutex);

  *total = 0;

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

  gboolean ret = TRUE;

  gchar *str;

  /* get last position we reduced and get anything up to count after that */
  gchar * query = "SELECT compact_id as c FROM DupinDB";
  g_mutex_lock (db->mutex);

  if (sqlite3_exec (db->db, query, dupin_database_compact_cb, &compact_id, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (db->mutex);

      g_error("dupin_database_thread_compact: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (db->mutex);

  gsize start_rowid = (compact_id != NULL) ? atoi(compact_id)+1 : 1;

  if (dupin_record_get_list (db, count, 0, start_rowid, 0, NULL, NULL, TRUE, DP_COUNT_ALL, DP_ORDERBY_ROWID, FALSE, &results, NULL) ==
      FALSE || !results)
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

      guint last_revision = record->last->revision;

      gchar *tmp = sqlite3_mprintf ("DELETE FROM Dupin WHERE id = '%q' AND rev < %d", (gchar *) dupin_record_get_id (record), (gint)last_revision);

//g_message("dupin_database_thread_compact: query=%s\n", tmp);

      g_mutex_lock (db->mutex);

      if (sqlite3_exec (db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_mutex_unlock (db->mutex);

          sqlite3_free (tmp);

          g_error ("dupin_database_thread_compact: %s", errmsg);

          sqlite3_free (errmsg);

          return FALSE;
        }

      db->compact_processed_count++;

      g_mutex_unlock (db->mutex);

      sqlite3_free (tmp);

      rowid = dupin_record_get_rowid (record);

      if (compact_id != NULL)
        g_free(compact_id);

      compact_id = g_strdup_printf ("%i", (gint)rowid);

//g_message("dupin_database_thread_compact(%p) compact_id=%s as fetched",g_thread_self (), compact_id);
    }
  
  dupin_record_get_list_close (results);

//g_message("dupin_database_thread_compact() compact_id=%s as to be stored",compact_id);

//g_message("dupin_database_thread_compact(%p)  finished last_compact_rowid=%s - compacted %d\n", g_thread_self (), compact_id, (gint)db->compact_processed_count);

  str = sqlite3_mprintf ("UPDATE DupinDB SET compact_id = '%q'", compact_id);

  if (compact_id != NULL)
    g_free (compact_id);

  g_mutex_lock (db->mutex);

  if (sqlite3_exec (db->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (db->mutex);
      sqlite3_free (str);

      g_error("dupin_database_thread_compact: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (db->mutex);

  sqlite3_free (str);

  return ret;
}

void
dupin_database_compact_func (gpointer data, gpointer user_data)
{
  DupinDB * db = (DupinDB*) data;
  gchar * errmsg;

  dupin_database_ref (db);

  g_mutex_lock (db->mutex);
  db->compact_thread = g_thread_self ();
  g_mutex_unlock (db->mutex);

//g_message("dupin_database_compact_func(%p) started\n",g_thread_self ());

  g_mutex_lock (db->mutex);
  db->compact_processed_count = 0;
  g_mutex_unlock (db->mutex);

  while (db->todelete == FALSE)
    {
      gboolean compact_operation = dupin_database_thread_compact (db, DUPIN_DB_COMPACT_COUNT);

      if (compact_operation == FALSE)
        {
//g_message("dupin_database_compact_func(%p) Compacted TOTAL %d records\n", g_thread_self (), (gint)db->compact_processed_count);

          /* claim disk space back */

//g_message("dupin_database_compact_func: VACUUM and ANALYZE\n");

          g_mutex_lock (db->mutex);

          if (sqlite3_exec (db->db, "VACUUM", NULL, NULL, &errmsg) != SQLITE_OK
             || sqlite3_exec (db->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK)
            {
              g_mutex_unlock (db->mutex);
              g_error ("dupin_database_compact_func: %s", errmsg);
              sqlite3_free (errmsg);
              break;
            }

          g_mutex_unlock (db->mutex);

          break;
        }
    }

//g_message("dupin_database_compact_func(%p) finished and database is compacted\n",g_thread_self ());

  g_mutex_lock (db->mutex);
  db->tocompact = FALSE;
  g_mutex_unlock (db->mutex);

  g_mutex_lock (db->mutex);
  db->compact_thread = NULL;
  g_mutex_unlock (db->mutex);

  dupin_database_unref (db);
}

void
dupin_database_compact (DupinDB * db)
{
  g_return_if_fail (db != NULL);

  if (dupin_database_is_compacting (db))
    {
      g_mutex_lock (db->mutex);
      db->tocompact = TRUE;
      g_mutex_unlock (db->mutex);

//g_message("dupin_database_compact(%p): database is still compacting db->compact_thread=%p\n", g_thread_self (), db->compact_thread);
    }
  else
    {
//g_message("dupin_database_compact(%p): push to thread pools db->compact_thread=%p\n", g_thread_self (), db->compact_thread);

      if (!db->compact_thread)
        {
          g_thread_pool_push(db->d->db_compact_workers_pool, db, NULL);
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

/* TODO - select current attachment db and linkbase to use for insert and retrieve operations
          in dupin_record API - we just set and force the dbname as default, full stop */

gboolean        dupin_database_set_default_attachment_db_name
                                        (DupinDB *      db,
                                         gchar *        attachment_db_name)
{
  g_return_val_if_fail (db != NULL, FALSE);
  g_return_val_if_fail (!g_strcmp0 (attachment_db_name, db->name), FALSE);

  /* TODO */
 
  return TRUE;
}

gchar *         dupin_database_get_default_attachment_db_name
                                        (DupinDB *      db)
{
  g_return_val_if_fail (db != NULL, NULL);

  return db->default_attachment_db_name;
}

gboolean        dupin_database_set_default_linkbase_name
                                        (DupinDB *      db,
                                         gchar *        linkbase_name)
{
  g_return_val_if_fail (db != NULL, FALSE);
  g_return_val_if_fail (!g_strcmp0 (linkbase_name, db->name), FALSE);

  /* TODO */

  return TRUE;
}

gchar *         dupin_database_get_default_linkbase_name
                                        (DupinDB *      db)
{
  g_return_val_if_fail (db != NULL, NULL);

  return db->default_linkbase_name;
}

/* EOF */
