#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"
#include "dupin_view.h"

#include <stdlib.h>
#include <string.h>

#define DUPIN_VIEW_SQL_MAIN_CREATE \
  "CREATE TABLE IF NOT EXISTS Dupin (\n" \
  "  id      CHAR(255) NOT NULL,\n" \
  "  pid     CHAR(255) NOT NULL,\n" \
  "  obj     TEXT,\n" \
  "  PRIMARY KEY(id)\n" \
  ");"

#define DUPIN_VIEW_SQL_DESC_CREATE \
  "CREATE TABLE IF NOT EXISTS DupinView (\n" \
  "  parent      CHAR(255) NOT NULL,\n" \
  "  isdb        BOOL DEFAULT TRUE,\n" \
  "  map         TEXT,\n" \
  "  map_lang    CHAR(255),\n" \
  "  reduce      TEXT,\n" \
  "  reduce_lang CHAR(255),\n" \
  "  sync_id     CHAR(255)\n" \
  ");"

#define DUPIN_VIEW_SQL_INSERT \
	"INSERT INTO Dupin (id, pid, obj) " \
        "VALUES('%q', '%q', '%q')"

#define DUPIN_VIEW_SQL_DELETE \
	"DELETE FROM Dupin WHERE pid = '%q';"

#define DUPIN_VIEW_SQL_EXISTS \
	"SELECT count(id) FROM Dupin WHERE id = '%q' "

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
    g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		 "View '%s' doesn't exist.", view);

  else
    ret->ref++;

  g_mutex_unlock (d->mutex);

  return ret;
}

static gchar *
dupin_view_new_sync_id_db (Dupin * d, gchar * parent)
{
  DupinDB *db;
  GList *list;
  gchar *id = NULL;

  if (!(db = dupin_database_open (d, parent, NULL)))
    return NULL;

  if (dupin_record_get_list (db, 1, 0, FALSE, &list, NULL) == TRUE && list)
    {
      DupinRecord *record = list->data;
      id = g_strdup (dupin_record_get_id (record));
      dupin_record_get_list_close (list);
    }

  dupin_database_unref (db);
  return id;
}

static gchar *
dupin_view_new_sync_id_view (Dupin * d, gchar * parent)
{
  DupinView *view;
  GList *list;
  gchar *id = NULL;

  if (!(view = dupin_view_open (d, parent, NULL)))
    return NULL;

  if (dupin_view_record_get_list (view, 1, 0, FALSE, &list, NULL) == TRUE
      && list)
    {
      DupinViewRecord *record = list->data;
      id = g_strdup (dupin_view_record_get_id (record));
      dupin_view_record_get_list_close (list);
    }

  dupin_view_unref (view);
  return id;
}

static gchar *
dupin_view_new_sync_id (Dupin * d, gchar * parent, gboolean is_db)
{
  if (is_db)
    return dupin_view_new_sync_id_db (d, parent);

  return dupin_view_new_sync_id_view (d, parent);
}

DupinView *
dupin_view_new (Dupin * d, gchar * view, gchar * parent, gboolean is_db,
		gchar * map, DupinMRLang map_language, gchar * reduce,
		DupinMRLang reduce_language, GError ** error)
{
  DupinView *ret;
  gchar *path;
  gchar *name;
  gchar *sync_id;

  gchar *str;
  gchar *errmsg;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (view != NULL, NULL);
  g_return_val_if_fail (parent != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_view_name (view) == TRUE, NULL);

  if (is_db == TRUE)
    g_return_val_if_fail (dupin_database_exists (d, parent) == TRUE, NULL);
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

  if (!(ret = dupin_view_create (d, view, path, error)))
    {
      g_mutex_unlock (d->mutex);
      g_free (path);
      return NULL;
    }

  ret->map = g_strdup (map);
  ret->map_lang = map_language;

  ret->reduce = g_strdup (reduce);
  ret->reduce_lang = reduce_language;

  ret->parent = g_strdup (parent);
  ret->parent_is_db = is_db;

  g_free (path);
  ret->ref++;

  str =
    sqlite3_mprintf ("INSERT INTO DupinView "
		     "(parent, isdb, map, map_lang, reduce, reduce_lang) "
		     "VALUES('%q', '%s', '%q', '%q', '%q' ,'%q')", parent,
		     is_db ? "TRUE" : "FALSE", map,
		     dupin_util_mr_lang_to_string (map_language), reduce,
		     dupin_util_mr_lang_to_string (reduce_language));

  if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (d->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (str);
      dupin_view_free (ret);
      return NULL;
    }

  sqlite3_free (str);

  g_mutex_unlock (d->mutex);

  if ((sync_id = dupin_view_new_sync_id (d, parent, is_db)))
    {

      str = sqlite3_mprintf ("UPDATE DupinView SET sync_id = '%q'", sync_id);

      if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
	{
	  g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		       errmsg);

	  sqlite3_free (errmsg);
	  sqlite3_free (str);
	  dupin_view_free (ret);
	  g_free (sync_id);
	  return NULL;
	}

      sqlite3_free (str);
      g_free (sync_id);
    }

  if (dupin_view_p_update (ret, error) == FALSE)
    {
      dupin_view_free (ret);
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
  gboolean isdb;
};

static int
dupin_view_p_update_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_view_p_update_t *update = data;

  if (argv[0] && *argv[0])
    update->parent = g_strdup (argv[0]);

  if (argv[1] && *argv[1])
    update->isdb = !strcmp (argv[1], "TRUE") ? TRUE : FALSE;

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
  gchar *query = "SELECT parent, isdb FROM DupinView";

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

  g_free (update.parent);
  return TRUE;
}

void
dupin_view_p_record_insert (DupinViewP * p, gchar * id,
			    tb_json_object_t * obj)
{
  gsize i;

  for (i = 0; i < p->numb; i++)
    {
      DupinView *view = p->views[i];
      tb_json_object_t *nobj;

      if ((nobj = dupin_mr_record (view, obj)))
	{
	  dupin_view_record_save (view, id, nobj);

	  dupin_view_p_record_insert (&view->views, id, nobj);
	  tb_json_object_destroy (nobj);
	}
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
dupin_view_record_save (DupinView * view, gchar * pid, tb_json_object_t * obj)
{
  GList *nodes;

  gchar *id = NULL;
  gchar *tmp, *serialized;

  g_return_if_fail (dupin_util_is_valid_obj (obj) != FALSE);

  g_mutex_lock (view->mutex);

  for (nodes = tb_json_object_get_nodes (obj); nodes; nodes = nodes->next)
    {
      tb_json_node_t *node = nodes->data;
      gchar *str = tb_json_node_get_string (node);

      if (!strcmp (str, "_id"))
	{
	  tb_json_value_t *value = tb_json_node_get_value (node);
	  id = g_strdup (tb_json_value_get_string (value));

	  if (dupin_util_is_valid_record_id (id) == FALSE)
	    {
	      g_mutex_unlock (view->mutex);
	      g_return_if_fail (dupin_util_is_valid_record_id (id) != FALSE);
	    }

	  tb_json_object_remove_node (obj, node);
	  break;
	}
    }

  if (!id && !(id = dupin_view_generate_id (view)))
    {
      g_mutex_unlock (view->mutex);
      return;
    }

  tb_json_object_write_to_buffer (obj, &serialized, NULL, NULL);
  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_INSERT, id, pid, serialized);
  sqlite3_exec (view->db, tmp, NULL, NULL, NULL);

  g_mutex_unlock (view->mutex);

  sqlite3_free (tmp);
  g_free (serialized);
  g_free (id);
}

static void
dupin_view_generate_id_create (DupinView * view, gchar id[255])
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
  gchar id[255];

  dupin_view_generate_id_create (view, id);
  return g_strdup (id);
}

void
dupin_view_record_delete (DupinView * view, gchar * pid)
{
  gchar *tmp;

  g_mutex_lock (view->mutex);

  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_DELETE, pid);
  sqlite3_exec (view->db, tmp, NULL, NULL, NULL);
  sqlite3_free (tmp);

  g_mutex_unlock (view->mutex);
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
dupin_view_free (DupinView * view)
{
  if (view->db)
    sqlite3_close (view->db);

  if (view->todelete == TRUE)
    g_unlink (view->path);

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

  g_free (view);
}

static int
dupin_view_create_cb (void *data, int argc, char **argv, char **col)
{
  DupinView *view = data;

  if (argc == 6)
    {
      view->map = g_strdup (argv[0]);
      view->map_lang = dupin_util_mr_lang_to_enum (argv[1]);

      view->reduce = g_strdup (argv[2]);
      view->reduce_lang = dupin_util_mr_lang_to_enum (argv[3]);

      view->parent = g_strdup (argv[4]);
      view->parent_is_db = strcmp (argv[5], "TRUE") == 0 ? TRUE : FALSE;
    }

  return 0;
}

DupinView *
dupin_view_create (Dupin * d, gchar * name, gchar * path, GError ** error)
{
  gchar *query;
  gchar *errmsg;
  DupinView *view;

  view = g_malloc0 (sizeof (DupinView));

  view->d = d;

  view->name = g_strdup (name);
  view->path = g_strdup (path);

  if (sqlite3_open (view->path, &view->db) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "View error.");
      dupin_view_free (view);
      return NULL;
    }

  if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_MAIN_CREATE, NULL, NULL, &errmsg)
      != SQLITE_OK
      || sqlite3_exec (view->db, DUPIN_VIEW_SQL_DESC_CREATE, NULL, NULL,
		       &errmsg) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      dupin_view_free (view);
      return NULL;
    }

  query =
    "SELECT map, map_lang, reduce, reduce_lang, parent, isdb FROM DupinView";

  if (sqlite3_exec (view->db, query, dupin_view_create_cb, view, &errmsg) !=
      SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      dupin_view_free (view);
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
  gchar *query;

  g_return_val_if_fail (view != NULL, 0);

  query = "SELECT count(id) as c FROM Dupin";

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_count_cb, &size, NULL) !=
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
  tb_json_object_t *obj;
  gchar *pid;
};

static void
dupin_view_sync_thread_real_mr (DupinView * view, GList * list)
{
  for (; list; list = list->next)
    {
      struct dupin_view_sync_t *data = list->data;
      tb_json_object_t *nobj;

      if ((nobj = dupin_mr_record (view, data->obj)))
	{
	  dupin_view_record_save (view, data->pid, nobj);

	  dupin_view_p_record_insert (&view->views, data->pid, nobj);
	  tb_json_object_destroy (nobj);
	}
    }
}

static gboolean
dupin_view_sync_thread_real_db (DupinView * view, gsize count, gsize offset)
{
  DupinDB *db;
  GList *results, *list;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *sync_id = NULL;
  gchar *str;

  if (!(db = dupin_database_open (view->d, view->parent, NULL)))
    return FALSE;

  if (dupin_record_get_list (db, count, offset, FALSE, &results, NULL) ==
      FALSE || !results)
    {
      dupin_database_unref (db);
      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

  for (list = results; list; list = list->next)
    {
      struct dupin_view_sync_t *data =
	g_malloc0 (sizeof (struct dupin_view_sync_t));
      data->obj = dupin_record_get_revision (list->data, -1);
      data->pid = (gchar *) dupin_record_get_id (list->data);

      sync_id = data->pid;

      l = g_list_append (l, data);
    }

  dupin_view_sync_thread_real_mr (view, l);

  g_list_foreach (l, (GFunc) g_free, NULL);
  g_list_free (l);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_id = '%q'", sync_id);
  sqlite3_exec (view->db, str, NULL, NULL, NULL);
  sqlite3_free (str);

  dupin_record_get_list_close (results);
  dupin_database_unref (db);
  return ret;
}


static gboolean
dupin_view_sync_thread_real_view (DupinView * view, gsize count, gsize offset)
{
  DupinView *v;
  GList *results, *list;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *sync_id = NULL;
  gchar *str;

  if (!(v = dupin_view_open (view->d, view->parent, NULL)))
    return FALSE;

  if (dupin_view_record_get_list (v, count, offset, FALSE, &results, NULL) ==
      FALSE || !results)
    {
      dupin_view_unref (v);
      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

  for (list = results; list; list = list->next)
    {
      struct dupin_view_sync_t *data =
	g_malloc0 (sizeof (struct dupin_view_sync_t));
      data->obj = dupin_view_record_get (list->data);
      data->pid = (gchar *) dupin_view_record_get_id (list->data);

      sync_id = data->pid;

      l = g_list_append (l, data);
    }

  dupin_view_sync_thread_real_mr (view, l);

  g_list_foreach (l, (GFunc) g_free, NULL);
  g_list_free (l);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_id = '%q'", sync_id);
  sqlite3_exec (view->db, str, NULL, NULL, NULL);
  sqlite3_free (str);

  dupin_view_record_get_list_close (results);
  dupin_view_unref (v);
  return ret;
}

static gboolean
dupin_view_sync_thread_real (DupinView * view, gsize count, gsize offset)
{
  if (view->parent_is_db == TRUE)
    return dupin_view_sync_thread_real_db (view, count, offset);

  return dupin_view_sync_thread_real_view (view, count, offset);
}

static gpointer
dupin_view_sync_thread (DupinView * view)
{
  dupin_view_ref (view);

#define VIEW_SYNC_COUNT	100

  while (view->sync_toquit == FALSE || view->todelete == FALSE)
    {
      if (dupin_view_sync_thread_real
	  (view, VIEW_SYNC_COUNT, view->sync_offset) == FALSE)
	{
	  gchar *query = "UPDATE DupinView SET sync_id = NULL";

	  g_mutex_lock (view->mutex);
	  sqlite3_exec (view->db, query, NULL, NULL, NULL);
	  g_mutex_unlock (view->mutex);
	  break;
	}

      view->sync_offset += VIEW_SYNC_COUNT;
    }

  view->sync_thread = NULL;
  dupin_view_unref (view);
  g_thread_exit (NULL);

  return NULL;
}

void
dupin_view_sync (DupinView * view)
{
  gchar *sync_id = NULL;
  gchar *query;

  query = "SELECT sync_id as c FROM DupinView";

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_id, NULL) !=
      SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      return;
    }

  if (sync_id != NULL)
    {
      view->sync_thread =
	g_thread_create ((GThreadFunc) dupin_view_sync_thread, view, FALSE,
			 NULL);

      g_free (sync_id);
    }

  g_mutex_unlock (view->mutex);
}

gboolean
dupin_view_is_sync (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (view->sync_thread)
    return FALSE;

  return TRUE;
}

/* EOF */
