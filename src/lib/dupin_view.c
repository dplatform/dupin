#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"
#include "dupin_view.h"

#include <stdlib.h>
#include <string.h>

/*

See http://wiki.apache.org/couchdb/Introduction_to_CouchDB_views

-> SORT Dupin table by key as primary key and pid as secondary key

*/

#define DUPIN_VIEW_SQL_MAIN_CREATE \
  "CREATE TABLE IF NOT EXISTS Dupin (\n" \
  "  id          CHAR(255) NOT NULL,\n" \
  "  pid         TEXT NOT NULL,\n" \
  "  key         TEXT NOT NULL COLLATE dupincmp,\n" \
  "  obj         TEXT COLLATE dupincmp,\n" \
  "  PRIMARY KEY(id)\n" \
  ");"

#define DUPIN_VIEW_SQL_CREATE_INDEX \
  "CREATE INDEX IF NOT EXISTS DupinId ON Dupin (id);\n" \
  "CREATE INDEX IF NOT EXISTS DupinPid ON Dupin (pid);\n" \
  "CREATE INDEX IF NOT EXISTS DupinKey ON Dupin (key);\n" \
  "CREATE INDEX IF NOT EXISTS DupinObj ON Dupin (obj);\n" \
  "CREATE INDEX IF NOT EXISTS DupinKeyObj ON Dupin (key, obj);"

#define DUPIN_VIEW_SQL_DESC_CREATE \
  "CREATE TABLE IF NOT EXISTS DupinView (\n" \
  "  parent                    CHAR(255) NOT NULL,\n" \
  "  isdb                      BOOL DEFAULT TRUE,\n" \
  "  islinkb                   BOOL DEFAULT FALSE,\n" \
  "  map                       TEXT,\n" \
  "  map_lang                  CHAR(255),\n" \
  "  reduce                    TEXT,\n" \
  "  reduce_lang               CHAR(255),\n" \
  "  sync_map_id               CHAR(255),\n" \
  "  sync_reduce_id            CHAR(255),\n" \
  "  sync_rereduce             BOOL DEFAULT FALSE\n" \
  ");"

#define DUPIN_VIEW_SQL_INSERT \
	"INSERT INTO Dupin (id, pid, key, obj) " \
        "VALUES('%q', '%q', '%q', '%q')"

#define DUPIN_VIEW_SQL_TOTAL_REREDUCE \
	"SELECT key AS inner_key, count(*) AS inner_count FROM Dupin GROUP BY inner_key HAVING inner_count > 1 "

#define DUPIN_VIEW_SQL_COUNT \
	"SELECT count(id) as c FROM Dupin"

#define VIEW_SYNC_COUNT	100

static gchar *dupin_view_generate_id (DupinView * view);

gchar **
dupin_get_views (Dupin * d)
{
  guint i;
  gsize size;
  gchar **ret;
  gpointer key;
  GHashTableIter iter;

  g_return_val_if_fail (d != NULL, NULL);

  g_mutex_lock (d->mutex);

  if (!(size = g_hash_table_size (d->views)))
    {
      g_mutex_unlock (d->mutex);
      return NULL;
    }

  ret = g_malloc (sizeof (gchar *) * (g_hash_table_size (d->views) + 1));

  i = 0;
  g_hash_table_iter_init (&iter, d->views);
  while (g_hash_table_iter_next (&iter, &key, NULL) == TRUE)
    ret[i++] = g_strdup (key);

  ret[i] = NULL;

  g_mutex_unlock (d->mutex);

  return ret;
}

gboolean
dupin_view_exists (Dupin * d, gchar * view)
{
  gboolean ret;

  g_mutex_lock (d->mutex);
  ret = g_hash_table_lookup (d->views, view) != NULL ? TRUE : FALSE;
  g_mutex_unlock (d->mutex);

  return ret;
}

DupinView *
dupin_view_open (Dupin * d, gchar * view, GError ** error)
{
  DupinView *ret;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (view != NULL, NULL);

  g_mutex_lock (d->mutex);

  if (!(ret = g_hash_table_lookup (d->views, view)) || ret->todelete == TRUE)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		 "View '%s' doesn't exist.", view);

      g_mutex_unlock (d->mutex);
      return NULL;
    }
  else
    ret->ref++;

  g_mutex_unlock (d->mutex);

  return ret;
}

DupinView *
dupin_view_new (Dupin * d, gchar * view, gchar * parent, gboolean is_db, gboolean is_linkb,
		gchar * map, DupinMRLang map_language, gchar * reduce,
		DupinMRLang reduce_language, GError ** error)
{
  DupinView *ret;
  gchar *path;
  gchar *name;

  gchar *str;
  gchar *errmsg;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (view != NULL, NULL);
  g_return_val_if_fail (parent != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_view_name (view) == TRUE, NULL);

  if (is_db == TRUE)
    g_return_val_if_fail (dupin_database_exists (d, parent) == TRUE, NULL);
  else if (is_linkb == TRUE)
    g_return_val_if_fail (dupin_linkbase_exists (d, parent) == TRUE, NULL);
  else
    g_return_val_if_fail (dupin_view_exists (d, parent) == TRUE, NULL);

  g_mutex_lock (d->mutex);

  if ((ret = g_hash_table_lookup (d->views, view)))
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "View '%s' already exist.", view);
      g_mutex_unlock (d->mutex);
      return NULL;
    }

  name = g_strdup_printf ("%s%s", view, DUPIN_VIEW_SUFFIX);
  path = g_build_path (G_DIR_SEPARATOR_S, d->path, name, NULL);
  g_free (name);

  if (!(ret = dupin_view_connect (d, view, path, DP_SQLITE_OPEN_CREATE, error)))
    {
      g_mutex_unlock (d->mutex);
      g_free (path);
      return NULL;
    }

  ret->map = g_strdup (map);
  ret->map_lang = map_language;

  if (reduce != NULL && g_strcmp0(reduce,"(NULL)") && g_strcmp0(reduce,"null") )
    {
      ret->reduce = g_strdup (reduce);
      ret->reduce_lang = reduce_language;
    }

  ret->parent = g_strdup (parent);
  ret->parent_is_db = is_db;
  ret->parent_is_linkb = is_linkb;

  g_free (path);
  ret->ref++;

  str =
    sqlite3_mprintf ("INSERT INTO DupinView "
		     "(parent, isdb, islinkb, map, map_lang, reduce, reduce_lang) "
		     "VALUES('%q', '%s', '%s', '%q', '%q', '%q' ,'%q')", parent,
		     is_db ? "TRUE" : "FALSE",
		     is_linkb ? "TRUE" : "FALSE",
		     map,
		     dupin_util_mr_lang_to_string (map_language), reduce,
		     dupin_util_mr_lang_to_string (reduce_language));

  if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (d->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (str);
      dupin_view_disconnect (ret);
      return NULL;
    }

  sqlite3_free (str);

  /* NOTE - the respective map and reduce threads will add +1 top the these values */
  str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '0', sync_reduce_id = '0' ");

  if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (d->mutex);
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		       errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (str);
      dupin_view_disconnect (ret);
      return NULL;
    }

  sqlite3_free (str);

  g_mutex_unlock (d->mutex);

  if (dupin_view_p_update (ret, error) == FALSE)
    {
      dupin_view_disconnect (ret);
      return NULL;
    }

  g_mutex_lock (d->mutex);
  g_hash_table_insert (d->views, g_strdup (view), ret);
  g_mutex_unlock (d->mutex);

  dupin_view_sync (ret);
  return ret;
}

struct dupin_view_p_update_t
{
  gchar *parent;
  gchar *map;
  DupinMRLang map_lang;
  gchar *reduce;
  DupinMRLang reduce_lang;
  gboolean isdb;
  gboolean islinkb;
};

static int
dupin_view_p_update_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_view_p_update_t *update = data;

  if (argc == 7)
    {
      if (argv[0] && *argv[0])
        update->parent = g_strdup (argv[0]);

      if (argv[1] && *argv[1])
        update->isdb = !g_strcmp0 (argv[1], "TRUE") ? TRUE : FALSE;

      if (argv[2] && *argv[2])
        update->islinkb = !g_strcmp0 (argv[2], "TRUE") ? TRUE : FALSE;

      if (argv[3] && *argv[3])
        update->map = g_strdup (argv[3]);

      if (argv[4] && *argv[4])
        update->map_lang = dupin_util_mr_lang_to_enum (argv[4]);

      if (argv[5] != NULL && g_strcmp0(argv[5],"(NULL)") && g_strcmp0(argv[5],"null") )
        {
          update->reduce = g_strdup (argv[5]);
        }

      if (argv[6] != NULL && g_strcmp0(argv[6],"(NULL)") && g_strcmp0(argv[6],"null") )
        {
          update->reduce_lang = dupin_util_mr_lang_to_enum (argv[6]);
        }
    }

  return 0;
}

#define DUPIN_VIEW_P_SIZE	64

static void
dupin_view_p_update_real (DupinViewP * p, DupinView * view)
{
  if (p->views == NULL)
    {
      p->views = g_malloc (sizeof (DupinView *) * DUPIN_VIEW_P_SIZE);
      p->size = DUPIN_VIEW_P_SIZE;
    }

  else if (p->numb == p->size)
    {
      p->size += DUPIN_VIEW_P_SIZE;
      p->views = g_realloc (p->views, sizeof (DupinView *) * p->size);
    }

  p->views[p->numb] = view;
  p->numb++;
}

gboolean
dupin_view_p_update (DupinView * view, GError ** error)
{
  gchar *errmsg;
  struct dupin_view_p_update_t update;
  gchar *query = "SELECT parent, isdb, islinkb, map, map_lang, reduce, reduce_lang FROM DupinView";

  memset (&update, 0, sizeof (struct dupin_view_p_update_t));

  g_mutex_lock (view->d->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_p_update_cb, &update, &errmsg)
      != SQLITE_OK)
    {
      g_mutex_unlock (view->d->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      return FALSE;
    }

  g_mutex_unlock (view->d->mutex);

  if (!update.parent)
    {
      if (update.map != NULL)
        g_free (update.map);

      if (update.reduce != NULL)
        g_free (update.reduce);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Internal error.");
      return FALSE;
    }

  if (update.isdb == TRUE)
    {
      DupinDB *db;

      if (!(db = dupin_database_open (view->d, update.parent, error)))
	{
	  g_free (update.parent);
	  return FALSE;
	}

      g_mutex_lock (db->mutex);
      dupin_view_p_update_real (&db->views, view);
      g_mutex_unlock (db->mutex);

      dupin_database_unref (db);
    }
  else if (update.islinkb == TRUE)
    {
      DupinLinkB *linkb;

      if (!(linkb = dupin_linkbase_open (view->d, update.parent, error)))
	{
	  g_free (update.parent);
	  return FALSE;
	}

      g_mutex_lock (linkb->mutex);
      dupin_view_p_update_real (&linkb->views, view);
      g_mutex_unlock (linkb->mutex);

      dupin_linkbase_unref (linkb);
    }
  else
    {
      DupinView *v;

      if (!(v = dupin_view_open (view->d, update.parent, error)))
	{
	  g_free (update.parent);
	  return FALSE;
	}

      g_mutex_lock (v->mutex);
      dupin_view_p_update_real (&v->views, view);
      g_mutex_unlock (v->mutex);

      dupin_view_unref (view);
    }

  /* make sure parameters are set after dupin server restart on existing view */

  if (view->parent == NULL)
    view->parent = update.parent;
  else
    g_free (update.parent);

  view->parent_is_db = update.isdb;
  view->parent_is_linkb = update.islinkb;

  if (view->map == NULL)
    view->map = update.map;
  else
    g_free (update.map);

  view->map_lang = update.map_lang;

  if (update.reduce != NULL && g_strcmp0(update.reduce,"(NULL)") && g_strcmp0(update.reduce,"null") )
    {
      if (view->reduce == NULL)
        view->reduce = update.reduce;
      else
        g_free (update.reduce);

      view->reduce_lang = update.reduce_lang;
    }

  return TRUE;
}

void
dupin_view_p_record_insert (DupinViewP * p, gchar * id,
			    JsonObject * obj)
{
  gsize i;

  for (i = 0; i < p->numb; i++)
    {
      DupinView *view = p->views[i];

      /* NOTE - we do not sync any insertion - it is done on deman at first access of view, restart or explicit view/_sync call */

      /* see also http://wiki.apache.org/couchdb/Regenerating_views_on_update */

      dupin_view_p_record_insert (&view->views, id, obj);
    }
}

void
dupin_view_p_record_delete (DupinViewP * p, gchar * pid)
{
  gsize i;

  for (i = 0; i < p->numb; i++)
    {
      DupinView *view = p->views[i];
      dupin_view_p_record_delete (&view->views, pid);
      dupin_view_record_delete (view, pid);
    }
}

void
dupin_view_record_save_map (DupinView * view, JsonNode * pid_node, JsonNode * key_node, JsonNode * node)
{
  g_return_if_fail (view != NULL);
  g_return_if_fail (pid_node != NULL);
  g_return_if_fail (json_node_get_node_type (pid_node) == JSON_NODE_ARRAY);
  g_return_if_fail (key_node != NULL);
  g_return_if_fail (node != NULL);

  const gchar *id = NULL;
  gchar *tmp, *errmsg, *node_serialized=NULL, *key_serialized=NULL, *pid_serialized=NULL;

  if (!(id = dupin_view_generate_id (view)))
    {
      return;
    }

//DUPIN_UTIL_DUMP_JSON ("dupin_view_record_save_map: KEY", key_node);
//DUPIN_UTIL_DUMP_JSON ("dupin_view_record_save_map: PID", pid_node);
//DUPIN_UTIL_DUMP_JSON ("dupin_view_record_save_map: OBJ", node);

  g_mutex_lock (view->mutex);

  /* serialize the node */

  node_serialized = dupin_util_json_serialize (node);

  if (node_serialized == NULL)
    {
      g_mutex_unlock (view->mutex);
      g_free ((gchar *)id);
      return;
    }

  /* serialize the key */

  if (key_node != NULL)
    {
      key_serialized = dupin_util_json_serialize (key_node);

      if (key_serialized == NULL)
        {
          g_mutex_unlock (view->mutex);
          g_free ((gchar *)id);
          g_free (node_serialized);
          return;
        }
    }

  if (pid_node != NULL)
    {
      pid_serialized = dupin_util_json_serialize (pid_node);

      if (pid_serialized == NULL)
        {
          g_mutex_unlock (view->mutex);
          g_free ((gchar *)id);
          g_free (node_serialized);
          if (key_serialized)
            g_free (key_serialized);
          return;
        }
    }

  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_INSERT, id, pid_serialized, key_serialized, node_serialized);

//g_message("query: %s\n",tmp);

  if (sqlite3_exec (view->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_error("dupin_view_record_save_map: %s", errmsg);
      sqlite3_free (errmsg);
    }

  g_mutex_unlock (view->mutex);

  sqlite3_free (tmp);
  g_free (node_serialized);
  if (key_serialized)
    g_free (key_serialized);
  if (pid_serialized)
    g_free (pid_serialized);
  g_free ((gchar *)id);
}

static void
dupin_view_generate_id_create (DupinView * view, gchar id[DUPIN_ID_MAX_LEN])
{
  do
    {
      dupin_util_generate_id (id);
    }
  while (dupin_view_record_exists_real (view, id, FALSE) == TRUE);
}

static gchar *
dupin_view_generate_id (DupinView * view)
{
  gchar id[DUPIN_ID_MAX_LEN];

  dupin_view_generate_id_create (view, id);
  return g_strdup (id);
}

void
dupin_view_record_delete (DupinView * view, gchar * pid)
{
  gchar *query;
  gchar *errmsg;

  /* NOTE - hack to avoid to keep another table and be able to delete entries
            from a view generated from multiple input documents */
     
  query = sqlite3_mprintf ("DELETE FROM Dupin WHERE pid LIKE '%%\"%q\"%%' ;", pid); /* TODO - might need double %% to escape % for mprintf */

//g_message("dupin_view_record_delete() query=%s\n",query);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_error("dupin_view_record_delete: %s", errmsg);
      sqlite3_free (errmsg);
    }

  g_mutex_unlock (view->mutex);

  sqlite3_free (query);

  /* NOTE - delete operations do not need re-index and call map/reduce due we use PIDs of DB
            or views to delete record in view which where accefted by record delete */
}

void
dupin_view_ref (DupinView * view)
{
  Dupin *d;

  g_return_if_fail (view != NULL);

  d = view->d;

  g_mutex_lock (d->mutex);
  view->ref++;
  g_mutex_unlock (d->mutex);
}

void
dupin_view_unref (DupinView * view)
{
  Dupin *d;

  g_return_if_fail (view != NULL);

  d = view->d;
  g_mutex_lock (d->mutex);

  if (view->ref >= 0)
    view->ref--;

  if (view->ref != 0 && view->todelete == TRUE)
    g_warning ("dupin_view_unref: (thread=%p) view %s flagged for deletion but can't free it due ref is %d\n", g_thread_self (), view->name, (gint) view->ref);

  if (view->ref == 0 && view->todelete == TRUE)
    g_hash_table_remove (d->views, view->name);

  g_mutex_unlock (d->mutex);
}

gboolean
dupin_view_delete (DupinView * view, GError ** error)
{
  Dupin *d;

  g_return_val_if_fail (view != NULL, FALSE);

  d = view->d;

  g_mutex_lock (d->mutex);
  view->todelete = TRUE;
  g_mutex_unlock (d->mutex);

  return TRUE;
}

gboolean
dupin_view_force_quit (DupinView * view, GError ** error)
{
  Dupin *d;

  g_return_val_if_fail (view != NULL, FALSE);

  d = view->d;

  g_mutex_lock (d->mutex);
  view->sync_toquit = TRUE;
  g_mutex_unlock (d->mutex);

  return TRUE;
}

const gchar *
dupin_view_get_name (DupinView * view)
{
  g_return_val_if_fail (view != NULL, NULL);
  return view->name;
}

const gchar *
dupin_view_get_parent (DupinView * view)
{
  g_return_val_if_fail (view != NULL, NULL);

  return view->parent;
}

gboolean
dupin_view_get_parent_is_db (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  return view->parent_is_db;
}

gboolean
dupin_view_get_parent_is_linkb (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  return view->parent_is_linkb;
}

const gchar *
dupin_view_get_map (DupinView * view)
{
  g_return_val_if_fail (view != NULL, NULL);

  return view->map;
}

DupinMRLang
dupin_view_get_map_language (DupinView * view)
{
  g_return_val_if_fail (view != NULL, 0);

  return view->map_lang;
}

const gchar *
dupin_view_get_reduce (DupinView * view)
{
  g_return_val_if_fail (view != NULL, NULL);

  return view->reduce;
}

DupinMRLang
dupin_view_get_reduce_language (DupinView * view)
{
  g_return_val_if_fail (view != NULL, 0);

  return view->reduce_lang;
}

gsize
dupin_view_get_size (DupinView * view)
{
  struct stat st;

  g_return_val_if_fail (view != NULL, 0);

  if (g_stat (view->path, &st) != 0)
    return 0;

  return (gsize) st.st_size;
}

/* Internal: */
void
dupin_view_disconnect (DupinView * view)
{
  g_message("dupin_view_disconnect: total number of changes for '%s' view database: %d\n", view->name, (gint)sqlite3_total_changes (view->db));

  if (view->db)
    sqlite3_close (view->db);

  if (view->todelete == TRUE)
    g_unlink (view->path);

  g_cond_free(view->sync_map_has_new_work);

  if (view->name)
    g_free (view->name);

  if (view->path)
    g_free (view->path);

  if (view->parent)
    g_free (view->parent);

  if (view->mutex)
    g_mutex_free (view->mutex);

  if (view->views.views)
    g_free (view->views.views);

  if (view->map)
    g_free (view->map);

  if (view->reduce)
    g_free (view->reduce);

  if (view->error_msg)
    g_free (view->error_msg);

  if (view->warning_msg)
    g_free (view->warning_msg);

  if (view->collation_parser)
    g_object_unref (view->collation_parser);

  g_free (view);
}

static int
dupin_view_connect_cb (void *data, int argc, char **argv, char **col)
{
  DupinView *view = data;

  if (argc == 7)
    {
      view->map = g_strdup (argv[0]);
      view->map_lang = dupin_util_mr_lang_to_enum (argv[1]);

      if (argv[2] != NULL && g_strcmp0(argv[2],"(NULL)") && g_strcmp0(argv[2],"null") )
        {
          view->reduce = g_strdup (argv[2]);
          view->reduce_lang = dupin_util_mr_lang_to_enum (argv[3]);
        }

      view->parent = g_strdup (argv[4]);
      view->parent_is_db = g_strcmp0 (argv[5], "TRUE") == 0 ? TRUE : FALSE;
      view->parent_is_linkb = g_strcmp0 (argv[6], "TRUE") == 0 ? TRUE : FALSE;
    }

  return 0;
}

DupinView *
dupin_view_connect (Dupin * d, gchar * name, gchar * path,
		    DupinSQLiteOpenType mode,
		    GError ** error)
{
  gchar *query;
  gchar *errmsg;
  DupinView *view;

  view = g_malloc0 (sizeof (DupinView));

  view->tosync = FALSE;

  view->sync_map_processed_count = 0;
  view->sync_reduce_total_records = 0;
  view->sync_reduce_processed_count = 0;

  view->sync_map_has_new_work = g_cond_new();

  view->d = d;

  view->name = g_strdup (name);
  view->path = g_strdup (path);

  view->collation_parser = json_parser_new ();

  if (sqlite3_open_v2 (view->path, &view->db, dupin_util_dupin_mode_to_sqlite_mode (mode), NULL) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "View error.");
      dupin_view_disconnect (view);
      return NULL;
    }

  /* NOTE - set simple collation functions for views - see http://wiki.apache.org/couchdb/View_collation */

  if (sqlite3_create_collation (view->db, "dupincmp", SQLITE_UTF8,  view->collation_parser, dupin_util_collation) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "View error. Cannot create collation function 'dupincmp'");
      dupin_view_disconnect (view);
      return NULL;
    }

  if (mode == DP_SQLITE_OPEN_CREATE)
    {
      if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_MAIN_CREATE, NULL, NULL, &errmsg)
      				!= SQLITE_OK
          || sqlite3_exec (view->db, DUPIN_VIEW_SQL_DESC_CREATE, NULL, NULL,
		       &errmsg) != SQLITE_OK
          || sqlite3_exec (view->db, DUPIN_VIEW_SQL_CREATE_INDEX, NULL, NULL,
		       &errmsg) != SQLITE_OK)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          return NULL;
        }
    }

  /* NOTE - we know this is inefficient, but we need it till proper Elastic search or lucene used as frontend */

  sqlite3_create_function(view->db, "filterBy", 5, SQLITE_ANY, d, dupin_sqlite_json_filterby, NULL, NULL);

  query =
    "SELECT map, map_lang, reduce, reduce_lang, parent, isdb, islinkb FROM DupinView";

  if (sqlite3_exec (view->db, query, dupin_view_connect_cb, view, &errmsg) !=
      SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      dupin_view_disconnect (view);
    }

  view->mutex = g_mutex_new ();

  return view;
}

static int
dupin_view_count_cb (void *data, int argc, char **argv, char **col)
{
  gsize *size = data;

  if (argv[0] && *argv[0])
    *size = atoi (argv[0]);

  return 0;
}

gsize
dupin_view_count (DupinView * view)
{
  gsize size;

  g_return_val_if_fail (view != NULL, 0);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_COUNT, dupin_view_count_cb, &size, NULL) !=
      SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      return 0;
    }

  g_mutex_unlock (view->mutex);
  return size;
}

static int
dupin_view_sync_cb (void *data, int argc, char **argv, char **col)
{
  gchar **sync_id = data;

  if (argv[0] && *argv[0])
    *sync_id = g_strdup (argv[0]);

  return 0;
}

struct dupin_view_sync_t
{
  JsonNode *obj;
  gchar *id;
  JsonNode *pid; /* array or null */
  JsonNode *key; /* array or null */
};

static void
dupin_view_sync_thread_real_map (DupinView * view, GList * list)
{
  for (; list; list = list->next)
    {
      struct dupin_view_sync_t *data = list->data;
      JsonArray *array;
      JsonObject * data_obj = json_node_get_object (data->obj);

      gchar * id = g_strdup ( (gchar *)json_node_get_string ( json_array_get_element ( json_node_get_array (data->pid), 0) ) );

      if ((array = dupin_mr_record_map (view, data_obj)))
	{
	  GList *nodes, *n;
	  nodes = json_array_get_elements (array);

	  JsonParser * parser = json_parser_new ();
          GError * error = NULL;

          for (n = nodes; n != NULL; n = n->next)
            {
              JsonObject *nobj;
              JsonNode *element_node = NULL;

	      /* WARNING - json-glib bug or feature ?! we need to make sure get member does not cast
			   integers to double automatically - we need to reparse !!! why?! */

	      gchar * element_node_serialized = dupin_util_json_serialize ((JsonNode*)n->data);
	      if (element_node_serialized == NULL
		  || (!json_parser_load_from_data (parser, element_node_serialized, -1, &error)))
                {
                  if (error)
                    {
                      dupin_view_set_error (view, error->message);
                      g_error_free (error);
                    }

		  if (element_node_serialized != NULL)
		    g_free (element_node_serialized);

                  if (parser != NULL)
                    g_object_unref (parser);
		  
		  continue; // TODO - shall we fail instead?
                }

	      if (element_node_serialized != NULL)
	        g_free (element_node_serialized);

	      element_node = json_parser_get_root (parser);

              nobj = json_node_get_object (element_node);
		
	      JsonNode * key_node = json_object_get_member (nobj, DUPIN_VIEW_KEY);
	      JsonNode * node = json_object_get_member (nobj, DUPIN_VIEW_VALUE);

	      dupin_view_record_save_map (view, data->pid, key_node, node);

              g_mutex_lock (view->mutex);
              view->sync_map_processed_count++;
              g_mutex_unlock (view->mutex);

	      dupin_view_p_record_insert (&view->views, id, nobj);
            }
          g_list_free (nodes);

	  g_object_unref (parser);

	  json_array_unref (array);
	}

        g_free(id);
    }
}

static gboolean
dupin_view_sync_thread_map_db (DupinView * view, gsize count)
{
  gchar * sync_map_id = NULL;
  gsize rowid;
  gchar * errmsg;
  DupinDB *db;
  GList *results, *list;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *str;

  if (!(db = dupin_database_open (view->d, view->parent, NULL)))
    return FALSE;

  /* get last position we reduced and get anything up to count after that */
  gchar * query = "SELECT sync_map_id as c FROM DupinView";
  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_map_id, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_error("dupin_view_sync_thread_map_db: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_database_unref (db);
      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  gsize start_rowid = (sync_map_id != NULL) ? atoi(sync_map_id)+1 : 1;

  if (dupin_record_get_list (db, count, 0, start_rowid, 0, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE, NULL, DP_FILTERBY_EQUALS,
				NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == FALSE || !results)
    {
      if (sync_map_id != NULL)
        g_free(sync_map_id);
      dupin_database_unref (db);
      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

//g_message("dupin_view_sync_thread_map_db(%p)    g_list_length (results) = %d start_rowid=%d - mapped %d\n", g_thread_self (), (gint) g_list_length (results), (gint)start_rowid, (gint)view->sync_map_processed_count);

  for (list = results; list; list = list->next)
    {
      /* NOTE - we do *not* count deleted records are processed */

      struct dupin_view_sync_t *data =
	g_malloc0 (sizeof (struct dupin_view_sync_t));

      JsonNode * obj_node = dupin_record_get_revision_node (list->data, NULL);

      if (obj_node)
        {
          /* Setting _id, _rev and _created fields - we do not store them into serialized object */
          JsonObject * obj = json_node_get_object (obj_node);
          json_object_set_string_member (obj, "_id", (gchar *) dupin_record_get_id (list->data));
          json_object_set_string_member (obj, "_rev", dupin_record_get_last_revision (list->data));

	  if (json_object_has_member (obj, "_created") == TRUE)
            json_object_remove_member (obj, "_created"); // ignore any record one if set by user, ever
          gchar * created = dupin_util_timestamp_to_iso8601 (dupin_record_get_created (list->data));
          json_object_set_string_member (obj, "_created", created);
          g_free (created);

	  if (json_object_has_member (obj, "_type") == TRUE)
            json_object_remove_member (obj, "_type"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_type", (gchar *)dupin_record_get_type (list->data));

	  /* NOTE - needed for m/r dupin.docpath() and dupin.links() methods - see dupin_js.c */

          if (json_object_has_member (obj, "_linkbase") == TRUE)
            json_object_remove_member (obj, "_linkbase"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_linkbase", dupin_database_get_default_linkbase_name (db));

          data->obj = json_node_copy (obj_node);
        }

      data->pid = json_node_new (JSON_NODE_ARRAY);
      JsonArray *pid_array=json_array_new ();

      rowid = dupin_record_get_rowid (list->data);

      if (sync_map_id != NULL)
        g_free(sync_map_id);
        
      sync_map_id = g_strdup_printf ("%i", (gint)rowid);

//g_message("dupin_view_sync_thread_map_db(%p) sync_map_id=%s as fetched",g_thread_self (), sync_map_id);

      json_array_add_string_element (pid_array, (gchar *) dupin_record_get_id (list->data));
      json_node_take_array (data->pid, pid_array);

      /* NOTE - key not set for dupin_view_sync_thread_map_db() - see dupin_view_sync_thread_map_view() instead */

      l = g_list_append (l, data);
    }

  dupin_view_sync_thread_real_map (view, l);

  for (list=l; list; list = list->next)
    {
      struct dupin_view_sync_t *data = list->data;
      if (data->obj)
        json_node_free (data->obj);
      json_node_free (data->pid);
    }
  g_list_foreach (l, (GFunc) g_free, NULL);
  g_list_free (l);
  dupin_record_get_list_close (results);

//g_message("dupin_view_sync_thread_map_db() sync_map_id=%s as to be stored",sync_map_id);

//g_message("dupin_view_sync_thread_map_db(%p)  finished last_map_rowid=%s - mapped %d\n", g_thread_self (), sync_map_id, (gint)view->sync_map_processed_count);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

  if (sync_map_id != NULL)
    g_free (sync_map_id);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_database_unref (db);

      g_error("dupin_view_sync_thread_map_db: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  sqlite3_free (str);

  dupin_database_unref (db);
  return ret;
}

static gboolean
dupin_view_sync_thread_map_linkb (DupinView * view, gsize count)
{
  gchar * sync_map_id = NULL;
  gsize rowid;
  gchar * errmsg;
  DupinLinkB *linkb;
  GList *results, *list;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *str;

  if (!(linkb = dupin_linkbase_open (view->d, view->parent, NULL)))
    return FALSE;

  /* get last position we reduced and get anything up to count after that */
  gchar * query = "SELECT sync_map_id as c FROM DupinView";
  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_map_id, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_error("dupin_view_sync_thread_map_linkb: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_linkbase_unref (linkb);
      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  gsize start_rowid = (sync_map_id != NULL) ? atoi(sync_map_id)+1 : 1;

  if (dupin_link_record_get_list (linkb, count, 0, start_rowid, 0, DP_LINK_TYPE_ANY, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE,
				  NULL, NULL, DP_FILTERBY_EQUALS, NULL, DP_FILTERBY_EQUALS, NULL, DP_FILTERBY_EQUALS,
				  NULL, DP_FILTERBY_EQUALS, NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) ==
      FALSE || !results)
    {
      if (sync_map_id != NULL)
        g_free(sync_map_id);
      dupin_linkbase_unref (linkb);
      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

//g_message("dupin_view_sync_thread_map_linkb(%p)    g_list_length (results) = %d start_rowid=%d - mapped %d\n", g_thread_self (), (gint) g_list_length (results), (gint)start_rowid, (gint)view->sync_map_processed_count);

  for (list = results; list; list = list->next)
    {
      /* NOTE - we do *not* count deleted records are processed */

      struct dupin_view_sync_t *data =
	g_malloc0 (sizeof (struct dupin_view_sync_t));

      JsonNode * obj_node = dupin_link_record_get_revision_node (list->data, NULL);

      if (obj_node)
        {
          /* Setting _id, _rev and _created fields - we do not store them into serialized object */
          JsonObject * obj = json_node_get_object (obj_node);
          json_object_set_string_member (obj, "_id", (gchar *) dupin_link_record_get_id (list->data));
          json_object_set_string_member (obj, "_rev", dupin_link_record_get_last_revision (list->data));

          if (json_object_has_member (obj, "_created") == TRUE)
            json_object_remove_member (obj, "_created"); // ignore any record one if set by user, ever
	  gchar * created = dupin_util_timestamp_to_iso8601 (dupin_link_record_get_created (list->data));
          json_object_set_string_member (obj, "_created", created);
          g_free (created);

	  if (json_object_has_member (obj, "_context_id") == TRUE)
            json_object_remove_member (obj, "_context_id"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_context_id", (gchar *)dupin_link_record_get_context_id (list->data));

	  if (json_object_has_member (obj, "_href") == TRUE)
            json_object_remove_member (obj, "_href"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_href", (gchar *)dupin_link_record_get_href (list->data));

	  if (json_object_has_member (obj, "_label") == TRUE)
            json_object_remove_member (obj, "_label"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_label", (gchar *)dupin_link_record_get_label (list->data));

	  if (json_object_has_member (obj, "_rel") == TRUE)
            json_object_remove_member (obj, "_rel"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_rel", (gchar *)dupin_link_record_get_rel (list->data));

	  if (json_object_has_member (obj, "_tag") == TRUE)
            json_object_remove_member (obj, "_tag"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_tag", (gchar *)dupin_link_record_get_tag (list->data));

	  if (json_object_has_member (obj, "_is_weblink") == TRUE)
            json_object_remove_member (obj, "_is_weblink"); // ignore any record one if set by user, ever
          json_object_set_boolean_member (obj, "_is_weblink", dupin_link_record_is_weblink (list->data));

          data->obj = json_node_copy (obj_node);
        }

      data->pid = json_node_new (JSON_NODE_ARRAY);
      JsonArray *pid_array=json_array_new ();

      rowid = dupin_link_record_get_rowid (list->data);

      if (sync_map_id != NULL)
        g_free(sync_map_id);
        
      sync_map_id = g_strdup_printf ("%i", (gint)rowid);

//g_message("dupin_view_sync_thread_map_linkb(%p) sync_map_id=%s as fetched",g_thread_self (), sync_map_id);

      json_array_add_string_element (pid_array, (gchar *) dupin_link_record_get_id (list->data));
      json_node_take_array (data->pid, pid_array);

      /* NOTE - key not set for dupin_view_sync_thread_map_linkb() - see dupin_view_sync_thread_map_view() instead */

      l = g_list_append (l, data);
    }

  dupin_view_sync_thread_real_map (view, l);

  for (list=l; list; list = list->next)
    {
      struct dupin_view_sync_t *data = list->data;
      if (data->obj)
        json_node_free (data->obj);
      json_node_free (data->pid);
    }
  g_list_foreach (l, (GFunc) g_free, NULL);
  g_list_free (l);
  dupin_link_record_get_list_close (results);

//g_message("dupin_view_sync_thread_map_linkb() sync_map_id=%s as to be stored",sync_map_id);

//g_message("dupin_view_sync_thread_map_linkb(%p)  finished last_map_rowid=%s - mapped %d\n", g_thread_self (), sync_map_id, (gint)view->sync_map_processed_count);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

  if (sync_map_id != NULL)
    g_free (sync_map_id);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_linkbase_unref (linkb);

      g_error("dupin_view_sync_thread_map_linkb: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  sqlite3_free (str);

  dupin_linkbase_unref (linkb);
  return ret;
}

static gboolean
dupin_view_sync_thread_map_view (DupinView * view, gsize count)
{
  DupinView *v;
  GList *results, *list;
  gchar * sync_map_id = NULL;
  gsize rowid;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *str;
  gchar *errmsg;

  if (!(v = dupin_view_open (view->d, view->parent, NULL)))
    return FALSE;

  /* get last position we reduced and get anything up to count after that */
  gchar * query = "SELECT sync_map_id as c FROM DupinView";
  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_map_id, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_error("dupin_view_sync_thread_map_view: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_unref (v);
      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  gsize start_rowid = (sync_map_id != NULL) ? atoi(sync_map_id)+1 : 1;

  if (dupin_view_record_get_list (v, count, 0, start_rowid, 0, DP_ORDERBY_ROWID, FALSE, NULL, NULL, TRUE, NULL, NULL, TRUE,
					NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) ==
      FALSE || !results)
    {
      if (sync_map_id != NULL)
        g_free(sync_map_id);
      dupin_view_unref (v);
      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

//g_message("dupin_view_sync_thread_map_view(%p)    g_list_length (results) = %d\n", g_thread_self (), (gint) g_list_length (results) );

  for (list = results; list; list = list->next)
    {
      /* NOTE - views are read-only we do not have to skip deleted records - see dupin_view_sync_thread_map_db() instead */

      struct dupin_view_sync_t *data =
	g_malloc0 (sizeof (struct dupin_view_sync_t));

      JsonNode * obj_node = dupin_view_record_get (list->data);

      if (obj_node)
        {
          /* Setting _id field */
          JsonObject * obj = json_node_get_object (obj_node);
          json_object_set_string_member (obj, "_id", (gchar *) dupin_view_record_get_id (list->data));

          data->obj = json_node_copy (obj_node);
        }

      /* TODO - check shouldn't this be more simply json_node_copy (dupin_view_record_get_pid (list->data))  or not ?! */
      data->pid = json_node_new (JSON_NODE_ARRAY);
      JsonArray *pid_array=json_array_new ();

      /* NOTE - key not set for dupin_view_sync_thread_map_db() - see dupin_view_sync_thread_map_view() instead */

      rowid = dupin_record_get_rowid (list->data);

      if (sync_map_id != NULL)
        g_free(sync_map_id);
        
      sync_map_id = g_strdup_printf ("%i", (gint)rowid);

      json_array_add_string_element (pid_array, (gchar *) dupin_view_record_get_id (list->data));
      json_node_take_array (data->pid, pid_array);

//g_message("dupin_view_sync_thread_map_view() sync_map_id=%s as fetched",sync_map_id);

      JsonNode * key = dupin_view_record_get_key (list->data);

      if (key)
        data->key = json_node_copy (key);

      l = g_list_append (l, data);
    }

  dupin_view_sync_thread_real_map (view, l);

  for (list=l; list; list = list->next)
    {
      struct dupin_view_sync_t *data = list->data;
      if (data->obj)
        json_node_free (data->obj);
      if (data->key)
        json_node_free (data->key);
      json_node_free (data->pid);
    }
  g_list_foreach (l, (GFunc) g_free, NULL);
  g_list_free (l);
  dupin_view_record_get_list_close (results);

//g_message("dupin_view_sync_thread_map_view() sync_map_id=%s as to be stored",sync_map_id);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

  if (sync_map_id != NULL)
    g_free (sync_map_id);

//g_message("dupin_view_sync_thread_map_view() query=%s\n",str);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_view_unref (v);

      g_error("dupin_view_sync_thread_map_view: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  sqlite3_free (str);

  dupin_view_unref (v);
  return ret;
}

static gboolean
dupin_view_sync_thread_map (DupinView * view, gsize count)
{
  if (view->parent_is_db == TRUE)
    return dupin_view_sync_thread_map_db (view, count);

  /* TODO - unimplemented */
  if (view->parent_is_linkb == TRUE)
    return dupin_view_sync_thread_map_linkb (view, count);

  return dupin_view_sync_thread_map_view (view, count);
}

void
dupin_view_sync_record_update (DupinView * view, gchar * previous_rowid, gint replace_rowid,
                          gchar * key, gchar * value, gchar * pid)
{
  gchar *query, *errmsg;
  gchar *replace_rowid_str=NULL;
  replace_rowid_str = g_strdup_printf ("%d", (gint)replace_rowid);

/* TODO - escape keys due we do not catch erros below !!!!! */

  query = sqlite3_mprintf ("DELETE FROM Dupin WHERE key='%q' AND ROWID > %q AND ROWID < %q ;",
				key,
				(previous_rowid != NULL) ? previous_rowid : "0",
				replace_rowid_str);

//g_message("dupin_view_sync_record_update() delete query=%s\n",query);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (query);
      g_free (replace_rowid_str);

      g_error("dupin_view_sync_record_update: %s", errmsg);
      sqlite3_free (errmsg);

      return;
    }

  g_mutex_unlock (view->mutex);

  sqlite3_free (query);

  query = sqlite3_mprintf ("UPDATE Dupin SET key='%q', pid='%q', obj='%q' WHERE rowid=%q ;",
				key,
				pid,
				value,
				replace_rowid_str);

//g_message("dupin_view_sync_record_update() update query=%s\n",query);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (query);
      g_free (replace_rowid_str);

      g_error("dupin_view_sync_record_update: %s", errmsg);
      sqlite3_free (errmsg);

      return;
    }

  g_mutex_unlock (view->mutex);

  sqlite3_free (query);

  g_free (replace_rowid_str);
}

static gboolean
dupin_view_sync_thread_reduce (DupinView * view, gsize count, gboolean rereduce, gchar * matching_key)
{
  if (view->reduce == NULL)
    return FALSE;

//g_message("dupin_view_sync_thread_reduce(%p) count=%d\n",g_thread_self (), (gint)count);

  GList *results, *list;
  GList *nodes, *n;
  gchar * sync_reduce_id = NULL;
  gchar * previous_sync_reduce_id=NULL;

  gboolean ret = TRUE;

  gchar *str, *errmsg;

  JsonNode * key=NULL;
  JsonNode * pid=NULL;
  JsonNode * reduce_parameters_obj_key=NULL;
  JsonObject * reduce_parameters_obj_key_o=NULL;
  JsonArray * reduce_parameters_obj_key_keys=NULL;
  JsonArray * reduce_parameters_obj_key_keys_i=NULL;
  JsonArray * reduce_parameters_obj_key_values=NULL;
  JsonArray * reduce_parameters_obj_key_pids=NULL;
  JsonNode  * reduce_parameters_obj_key_rowid=NULL;
  gsize rowid;
  gchar * key_string = NULL;
  gchar * query;

  /* get last position we reduced and get anything up to count after that */
  query = "SELECT sync_reduce_id as c FROM DupinView";
  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_reduce_id, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_error("dupin_view_sync_thread_reduce: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  previous_sync_reduce_id = g_strdup (sync_reduce_id);

  g_mutex_unlock (view->mutex);

  gsize start_rowid = (sync_reduce_id != NULL) ? atoi(sync_reduce_id)+1 : 1;

  if (dupin_view_record_get_list (view, count, 0, start_rowid, 0, (rereduce) ? DP_ORDERBY_KEY : DP_ORDERBY_ROWID, FALSE,
					matching_key, matching_key, TRUE, NULL, NULL, TRUE,
					NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) ==
      FALSE || !results)
    {
      if (previous_sync_reduce_id != NULL)
        g_free(previous_sync_reduce_id);

      if (sync_reduce_id != NULL)
        g_free(sync_reduce_id);

      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

//g_message("dupin_view_sync_thread_reduce(%p)    g_list_length (results) = %d start_rowid=%d - reduced %d of total to reduce=%d\n", g_thread_self (), (gint) g_list_length (results), (gint)start_rowid, (gint)view->sync_reduce_processed_count, (gint)view->sync_reduce_total_records );

  JsonNode * reduce_parameters = json_node_new (JSON_NODE_OBJECT);
  JsonObject * reduce_parameters_obj = json_object_new ();

  for (list = results; list; list = list->next)
    {
      /* NOTE - views are read-only we do not have to skip deleted records - see dupin_view_sync_thread_map_db() instead */

      key = dupin_view_record_get_key (list->data);
      rowid = dupin_view_record_get_rowid (list->data);
      pid = dupin_view_record_get_pid (list->data);

      if (sync_reduce_id != NULL)
        g_free(sync_reduce_id);
        
      /* NOTE - we assume rowid is always defined even for bad keys or pids - due it is a record returned in the above dupin_view_record_get_list() */

      sync_reduce_id = g_strdup_printf ("%i", (gint)rowid);

//g_message("dupin_view_sync_thread_reduce(%p) sync_reduce_id=%s\n",g_thread_self (), sync_reduce_id);

      /* NOTE - silently ignore bad records for the moment (but count them - see above setting of sync_reduce_id status)
                assuming 'null' is returned as valid JSON_NODE_NULL from above call */
      if (!key)
        continue;

      key_string = dupin_util_json_serialize (key);
      if (key_string == NULL)
        {
          // TODO - log something?
	  continue;
        }

      key = json_node_copy (key);

      if (pid)
        {
          pid = json_array_get_element (json_node_get_array (pid), 0);
          if (pid)
            pid = json_node_copy (pid);
        }
 
      if (!pid)
        pid = json_node_new (JSON_NODE_NULL);

//g_message("key_string =%s\n",key_string);

      reduce_parameters_obj_key = json_object_get_member (reduce_parameters_obj, key_string);

      if (!reduce_parameters_obj_key)
        {
          reduce_parameters_obj_key = json_node_new (JSON_NODE_OBJECT);
          reduce_parameters_obj_key_o = json_object_new ();
          json_node_take_object (reduce_parameters_obj_key, reduce_parameters_obj_key_o);

          reduce_parameters_obj_key_keys = json_array_new ();
          reduce_parameters_obj_key_values = json_array_new ();
          reduce_parameters_obj_key_pids = json_array_new ();
          reduce_parameters_obj_key_rowid = json_node_new (JSON_NODE_VALUE);

          json_object_set_array_member (reduce_parameters_obj_key_o, DUPIN_VIEW_KEYS, reduce_parameters_obj_key_keys);
          json_object_set_array_member (reduce_parameters_obj_key_o, DUPIN_VIEW_VALUES, reduce_parameters_obj_key_values);
          json_object_set_array_member (reduce_parameters_obj_key_o, DUPIN_VIEW_PIDS, reduce_parameters_obj_key_pids);
          json_object_set_member (reduce_parameters_obj_key_o, "rowid", reduce_parameters_obj_key_rowid);

          /* TODO - check if we need double to gurantee larger number of rowids and use json_node_set_number () */
          json_node_set_int (reduce_parameters_obj_key_rowid, rowid);

          json_object_set_member (reduce_parameters_obj, key_string, reduce_parameters_obj_key);
        }
      else
        {
//DUPIN_UTIL_DUMP_JSON("Key did exist \n",reduce_parameters_obj_key);

          reduce_parameters_obj_key_o = json_node_get_object (reduce_parameters_obj_key);

          reduce_parameters_obj_key_keys = json_node_get_array (json_object_get_member (reduce_parameters_obj_key_o, DUPIN_VIEW_KEYS));
          reduce_parameters_obj_key_values = json_node_get_array (json_object_get_member (reduce_parameters_obj_key_o, DUPIN_VIEW_VALUES));
          reduce_parameters_obj_key_pids = json_node_get_array (json_object_get_member (reduce_parameters_obj_key_o, DUPIN_VIEW_PIDS));
          reduce_parameters_obj_key_rowid = json_object_get_member (reduce_parameters_obj_key_o, "rowid");

          if ( json_node_get_int (reduce_parameters_obj_key_rowid) < rowid )
            json_node_set_int (reduce_parameters_obj_key_rowid, rowid);
        }

      /* i-esim [k,pid] pair */
      reduce_parameters_obj_key_keys_i = json_array_new ();
      json_array_add_element (reduce_parameters_obj_key_keys_i, key);
      json_array_add_element (reduce_parameters_obj_key_keys_i, pid);
      json_array_add_array_element (reduce_parameters_obj_key_keys, reduce_parameters_obj_key_keys_i);

      /* i-esim value */
      JsonNode * reduce_parameters_obj_key_values_i = dupin_view_record_get (list->data);
      if (reduce_parameters_obj_key_values_i)
        {
          reduce_parameters_obj_key_values_i = json_node_copy (reduce_parameters_obj_key_values_i);
        }
      else
        {
          reduce_parameters_obj_key_values_i = json_node_new (JSON_NODE_NULL);
        }

      json_array_add_element (reduce_parameters_obj_key_values, reduce_parameters_obj_key_values_i);
      json_array_add_element (reduce_parameters_obj_key_pids, json_node_copy (pid));

      g_free (key_string);
    }

  json_node_take_object (reduce_parameters, reduce_parameters_obj);

//DUPIN_UTIL_DUMP_JSON ("REDUCE parameters:", reduce_parameters);

  nodes = json_object_get_members (reduce_parameters_obj);
  for (n = nodes; n != NULL; n = n->next)
    {
      gchar *member_name = (gchar *) n->data;

      /* call reduce for each group of keys */

      /* call function(keys, values, rereduce)  values = [ v1, v2... vN ] */

      JsonNode * result = dupin_mr_record_reduce (view,
						  (rereduce) ? NULL : json_node_get_array (json_object_get_member ( json_node_get_object(json_object_get_member (reduce_parameters_obj, member_name)), DUPIN_VIEW_KEYS)),
						  json_node_get_array (json_object_get_member ( json_node_get_object(json_object_get_member (reduce_parameters_obj, member_name)), DUPIN_VIEW_VALUES)),
						  rereduce);

      if (result != NULL)
        {
          gchar * pids_string = dupin_util_json_serialize (json_object_get_member ( json_node_get_object(json_object_get_member (reduce_parameters_obj, member_name)), "pids"));
          if (pids_string == NULL)
            {
	      json_node_free (result);
	      ret = FALSE;
              break;
            }

          gchar * value_string = dupin_util_json_serialize (result);
          if (value_string == NULL)
            {
	      json_node_free (result);
              g_free (pids_string);
	      ret = FALSE;
              break;
            }

//g_message ("dupin_view_sync_thread_reduce: KEY: %s", member_name);
//DUPIN_UTIL_DUMP_JSON ("dupin_view_sync_thread_reduce: PID", json_object_get_member ( json_node_get_object(json_object_get_member (reduce_parameters_obj, member_name)), "pids"));
//DUPIN_UTIL_DUMP_JSON ("dupin_view_sync_thread_reduce: OBJ", result);

	  json_node_free (result);

          /* TODO - check if we need double to gurantee larger number of rowids and use json_node_get_number () in the below */

	  /* delete all rows but last one and replace last one with result where last one is rowid */
          dupin_view_sync_record_update (view,
				         previous_sync_reduce_id,
				         (gint)json_node_get_int (json_object_get_member ( json_node_get_object(json_object_get_member (reduce_parameters_obj, member_name)), "rowid")),
                                         member_name,
                                         value_string,
				         pids_string);

          g_free (value_string);
          g_free (pids_string);

          g_mutex_lock (view->mutex);
          view->sync_reduce_processed_count++;
          g_mutex_unlock (view->mutex);
        }

      /* just append to the end for the moment - DEBUG */
    }
  g_list_free (nodes);

  if (previous_sync_reduce_id != NULL)
    g_free(previous_sync_reduce_id);
  
  json_node_free (reduce_parameters); /* it shoulf freee the whole tree of objects, arrays and value ... */

  dupin_view_record_get_list_close (results);

//g_message("dupin_view_sync_thread_reduce(%p) finished last_reduce_rowid=%s - reduced %d of total to reduce=%d\n", g_thread_self (), sync_reduce_id, (gint)view->sync_reduce_processed_count, (gint)view->sync_reduce_total_records);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_reduce_id = '%q'", sync_reduce_id); /* is the ROWID we stopped */

  if (sync_reduce_id != NULL)
    g_free(sync_reduce_id);

//g_message("dupin_view_sync_thread_reduce() query=%s\n",str);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      g_error("dupin_view_sync_thread_reduce: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  sqlite3_free (str);

  return ret;
}

static int
dupin_view_rereduce_cb (void *data, int argc, char **argv, char **col)
{
  gboolean *rereduce = data;

  if (argv[0] && *argv[0])
    *rereduce = !g_strcmp0 (argv[0], "TRUE") ? TRUE : FALSE;

  return 0;
}

struct dupin_view_sync_total_rereduce_t
{
  gsize total;
  gchar * first_matching_key;
};

static int
dupin_view_sync_total_rereduce_cb (void *data, int argc, char **argv,
                                  char **col)
{
  struct dupin_view_sync_total_rereduce_t * rere = data;
 
  if (argv[0] && *argv[0]
      && rere->first_matching_key==NULL)
    rere->first_matching_key = g_strdup (argv[0]);

  if (argv[1] && *argv[1])
    rere->total += atoi (argv[1]);

  return 0;
}

/* NOTE - bear in mind SQLite might be able to store more than gsize total records
          see also ROWID and http://www.sqlite.org/autoinc.html */

gboolean
dupin_view_sync_total_rereduce (DupinView * view, struct dupin_view_sync_total_rereduce_t * rere)
{
  g_return_val_if_fail (view != NULL, FALSE);
  g_return_val_if_fail (rere != NULL, FALSE);

  gchar *tmp, *errmsg;

  rere->total = 0;
  rere->first_matching_key = NULL;

  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_TOTAL_REREDUCE);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, tmp, dupin_view_sync_total_rereduce_cb, rere, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (tmp);

      g_error("dupin_view_sync_total_rereduce: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  sqlite3_free (tmp);

  return TRUE;
}

void
dupin_view_sync_map_func (gpointer data, gpointer user_data)
{
  gchar * errmsg;
  DupinView * view = (DupinView*) data;

  dupin_view_ref (view);

  g_mutex_lock (view->mutex);
  view->sync_map_thread = g_thread_self ();
  g_mutex_unlock (view->mutex);

//g_message("dupin_view_sync_map_func(%p) started\n",g_thread_self ());

  g_mutex_lock (view->mutex);
  view->sync_map_processed_count = 0;
  g_mutex_unlock (view->mutex);

  while (view->sync_toquit == FALSE || view->todelete == FALSE)
    {
      gboolean map_operation = dupin_view_sync_thread_map (view, VIEW_SYNC_COUNT);

      /* NOTE - make sure waiting reduce thread is started as soon as the first set of mapped results is ready 
                 the sync_map_processed_count is set to the total of mapped results so far and used for very basic IPC map -> reduce threads */

      if (view->reduce != NULL)
        {
//g_message("dupin_view_sync_map_func(%p) Mapped %d records\n", g_thread_self (), (gint)view->sync_map_processed_count);

          g_mutex_lock (view->mutex);
	  g_cond_signal(view->sync_map_has_new_work);
          g_mutex_unlock (view->mutex);
        }

      if (map_operation == FALSE)
        {
//g_message("dupin_view_sync_map_func(%p) Mapped TOTAL %d records\n", g_thread_self (), (gint)view->sync_map_processed_count);

	  if (view->tosync)
	    {
              g_mutex_lock (view->mutex);
              view->tosync = FALSE;
              g_mutex_unlock (view->mutex);
            }
          else
            {
              break;
            }
        }
    }

//g_message("dupin_view_sync_map_func(%p) finished and view map part is in sync\n",g_thread_self ());

  /* NOTE - make sure reduce thread can terminate too eventually */
  if (view->reduce != NULL)
    {
      g_mutex_lock (view->mutex);
      g_cond_signal(view->sync_map_has_new_work);
      g_mutex_unlock (view->mutex);
    }
  else if (view->sync_map_processed_count > 0)
    {
//g_message("dupin_view_sync_map_func(%p): ANALYZE\n", g_thread_self ());

      g_mutex_lock (view->mutex);
      if (sqlite3_exec (view->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_mutex_unlock (view->mutex);
          g_error ("dupin_view_sync_reduce_func: %s", errmsg);
          sqlite3_free (errmsg);
        }
      g_mutex_unlock (view->mutex);
    }

  g_mutex_lock (view->mutex);
  view->tosync = FALSE;
  g_mutex_unlock (view->mutex);

  g_mutex_lock (view->mutex);
  view->sync_map_thread = NULL;
  g_mutex_unlock (view->mutex);

  dupin_view_unref (view);
}

void
dupin_view_sync_reduce_func (gpointer data, gpointer user_data)
{
  gchar * query;
  gchar * errmsg;
  struct dupin_view_sync_total_rereduce_t rere_matching;

  DupinView * view = (DupinView*) data;

  dupin_view_ref (view);

  g_mutex_lock (view->mutex);
  view->sync_reduce_thread = g_thread_self ();
  g_mutex_unlock (view->mutex);

  g_mutex_lock (view->mutex);
  view->sync_reduce_processed_count = 0;
  view->sync_reduce_total_records = 0;
  g_mutex_unlock (view->mutex);

  gboolean rereduce = FALSE;
  gchar * rereduce_previous_matching_key = NULL;

  rere_matching.total = 0;
  rere_matching.first_matching_key = NULL;

//g_message("dupin_view_sync_reduce_func(%p) started", g_thread_self ());

  /* NOTE - if map hangs, reduce also hangs - for the moment we should make sure a _rest method is allowed on views to avoid disasters */

  /* TODO - added processing step when restarted and sync_reduce_id is set to the ID of view table latest record processed, and continue
            and when done, wait for another bunch ... */

  query = "SELECT sync_rereduce as c FROM DupinView";
  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_rereduce_cb, &rereduce, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_error("dupin_view_sync_reduce_func: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_unref (view);

      return;
    }

  g_mutex_unlock (view->mutex);

  while (view->sync_toquit == FALSE || view->todelete == FALSE)
    {
//g_message("rereduce=%d\n", rereduce);

      if (rereduce == FALSE
	  && view->sync_map_thread)
        {
          g_mutex_lock (view->mutex);
          g_cond_wait(view->sync_map_has_new_work, view->mutex);
          g_mutex_unlock (view->mutex);
        }

      if (view->sync_map_processed_count > view->sync_reduce_total_records /* got a new bunch to work on */
	  || rereduce)
        {
          g_mutex_lock (view->mutex);
          view->sync_reduce_total_records = (rereduce) ? rere_matching.total : view->sync_map_processed_count;
          g_mutex_unlock (view->mutex);

//g_message("dupin_view_sync_reduce_func(%p) got %d records to REDUCE (rereduce=%d)\n",g_thread_self (), (gint)view->sync_reduce_total_records,(gint)rereduce);

          while (dupin_view_sync_thread_reduce (view, VIEW_SYNC_COUNT, rereduce, rere_matching.first_matching_key) == TRUE);

//g_message("dupin_view_sync_reduce_func(%p) Reduced %d records of %d\n", g_thread_self (), (gint)view->sync_reduce_processed_count, (gint)view->sync_reduce_total_records);
        }

      if (!view->sync_map_thread) /* map finished */
        {
//g_message("Map was finished in meantime\n");

	  /* check if there is anything to re-reduce */
          rere_matching.total = 0;
          if (rere_matching.first_matching_key != NULL)
            g_free (rere_matching.first_matching_key);
          rere_matching.first_matching_key = NULL;
          dupin_view_sync_total_rereduce (view, &rere_matching);

//g_message("Done first round of reduce but there are still %d record to re-reduce and first key to process is '%s'\n", (gint)rere_matching.total, rere_matching.first_matching_key);

          if (rere_matching.total > 0)
            {
              /* still work to do */
              rereduce = TRUE;

//g_message("Going to re-reduce\n");

              /* NOTE - the following check allows to skip any possible bad records in re-reduce process dupin_view_sync_thread_reduce()
		and avoid infinite loop - if nothing has changed  */
              if (rere_matching.first_matching_key == NULL 
	          || rereduce_previous_matching_key == NULL
		  || g_strcmp0(rere_matching.first_matching_key, rereduce_previous_matching_key))
                {
                  query = "UPDATE DupinView SET sync_reduce_id = '0', sync_rereduce = 'TRUE'";
                }
	      else
                {
		  g_warning("dupin_view_sync_reduce_func: rereduce of key %s reported problems - bad records have been deleted\n", rere_matching.first_matching_key);

		  /* NOTE - delete records giving problems */
                  gchar * str = sqlite3_mprintf ("DELETE FROM Dupin WHERE key='%q' ;", rere_matching.first_matching_key);

                  g_mutex_lock (view->mutex);

                  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
                    {
                      g_mutex_unlock (view->mutex);
                      sqlite3_free (str);

                      g_error("dupin_view_sync_reduce_func: %s", errmsg);
                      sqlite3_free (errmsg);

                      break;
                    }

                  g_mutex_unlock (view->mutex);

                  sqlite3_free (str);

		  /* flag to carry on (for any future request to sync) but exit */

                  query = "UPDATE DupinView SET sync_rereduce = 'TRUE'";

		  break;
                }

              g_mutex_lock (view->mutex);

              if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
                {
                  g_mutex_unlock (view->mutex);

                  g_error("dupin_view_sync_reduce_func: %s", errmsg);
                  sqlite3_free (errmsg);

                  break;
                }

              g_mutex_unlock (view->mutex);

              if (rereduce_previous_matching_key != NULL)
                g_free (rereduce_previous_matching_key);

              rereduce_previous_matching_key = g_strdup (rere_matching.first_matching_key);
            }
          else
            {
//g_message("Done rereduce=%d\n", (gint)rereduce);
              rereduce = FALSE;

              query = "UPDATE DupinView SET sync_rereduce = 'FALSE'";

              g_mutex_lock (view->mutex);

              if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
                {
                  g_mutex_unlock (view->mutex);

                  g_error("dupin_view_sync_reduce_func: %s", errmsg);
                  sqlite3_free (errmsg);

                  break;
                }

              g_mutex_unlock (view->mutex);

	      break; /* both terminated, amen */
            }
        }
    }

  if (rereduce_previous_matching_key != NULL)
    g_free (rereduce_previous_matching_key);

  if (rere_matching.first_matching_key != NULL)
    g_free (rere_matching.first_matching_key);

//g_message("dupin_view_sync_reduce_func(%p) finished to reduce %d total records and view reduce part is in sync\n",g_thread_self (), (gint)view->sync_reduce_total_records);


  /* claim disk space back due reduce did actually remove rows from view table */

//g_message("dupin_view_sync_reduce_func: VACUUM and ANALYZE\n");

  g_mutex_lock (view->mutex);
  if (sqlite3_exec (view->db, "VACUUM", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (view->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      g_error ("dupin_view_sync_reduce_func: %s", errmsg);
      sqlite3_free (errmsg);
    }
  g_mutex_unlock (view->mutex);

  g_mutex_lock (view->mutex);
  view->sync_reduce_thread = NULL;
  g_mutex_unlock (view->mutex);

  dupin_view_unref (view);
}

/* NOTE- we try to spawn two threads map, reduce 
         when reduce is done we re-reduce till no map and reduce is still running,
         finished scan and only one key is left (count=1) */

void
dupin_view_sync (DupinView * view)
{
  if (dupin_view_is_syncing (view))
    {
      g_mutex_lock (view->mutex);
      view->tosync = TRUE;
      g_mutex_unlock (view->mutex);

//g_message("dupin_view_sync(%p): view is still syncing view->sync_map_thread=%p view->sync_reduce_thread=%p \n", g_thread_self (), view->sync_map_thread, view->sync_reduce_thread);
    }
  else
    {
      /* TODO - have a master sync thread which manage the all three rather than have chain of
            dependency between map, reduce and re-reduce threads */

//g_message("dupin_view_sync(%p): push to thread pools view->sync_map_thread=%p view->sync_reduce_thread=%p \n", g_thread_self (), view->sync_map_thread, view->sync_reduce_thread);

      if (!view->sync_map_thread)
        {
	  g_thread_pool_push(view->d->sync_map_workers_pool, view, NULL);
        }

      if (view->reduce != NULL
          && !view->sync_reduce_thread)
        {
	  g_thread_pool_push(view->d->sync_reduce_workers_pool, view, NULL);
        }
    }
}

gboolean
dupin_view_is_syncing (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (view->sync_map_thread
      || view->sync_reduce_thread)
    return TRUE;

  return FALSE;
}

gboolean
dupin_view_is_sync (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (dupin_view_is_syncing (view))
    return FALSE;

  /* TODO - distinguish between tosync because of insert / update or because of explicit view/_sync method call */
  return view->tosync ? FALSE : TRUE;
}

void
dupin_view_set_error (DupinView * view,
                      gchar * msg)
{
  g_return_if_fail (view != NULL);
  g_return_if_fail (msg != NULL);

  dupin_view_clear_error (view);

  view->error_msg = g_strdup ( msg );

  return;
}

void
dupin_view_clear_error (DupinView * view)
{
  g_return_if_fail (view != NULL);

  if (view->error_msg != NULL)
    g_free (view->error_msg);

  view->error_msg = NULL;

  return;
}

gchar * dupin_view_get_error (DupinView * view)
{
  g_return_val_if_fail (view != NULL, NULL);

  return view->error_msg;
}

void dupin_view_set_warning (DupinView * view,
                                 gchar * msg)
{
  g_return_if_fail (view != NULL);
  g_return_if_fail (msg != NULL);

  dupin_view_clear_warning (view);

  view->warning_msg = g_strdup ( msg );

  return;
}

void dupin_view_clear_warning (DupinView * view)
{
  g_return_if_fail (view != NULL);

  if (view->warning_msg != NULL)
    g_free (view->warning_msg);

  view->warning_msg = NULL;

  return;
}

gchar * dupin_view_get_warning (DupinView * view)
{
  g_return_val_if_fail (view != NULL, NULL);

  return view->warning_msg;
}

/* EOF */
