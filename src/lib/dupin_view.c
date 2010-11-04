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
  "  pid         TEXT,\n" \
  "  key         TEXT,\n" \
  "  obj         TEXT,\n" \
  "  PRIMARY KEY(id)\n" \
  ");"

#define DUPIN_VIEW_SQL_CREATE_INDEX \
  "CREATE INDEX IF NOT EXISTS DupinKey ON Dupin (key);\n" \
  "CREATE INDEX IF NOT EXISTS DupinPid ON Dupin (pid);\n" \
  "CREATE INDEX IF NOT EXISTS DupinId ON Dupin (id);"

#define DUPIN_VIEW_SQL_DESC_CREATE \
  "CREATE TABLE IF NOT EXISTS DupinView (\n" \
  "  parent              CHAR(255) NOT NULL,\n" \
  "  isdb                BOOL DEFAULT TRUE,\n" \
  "  map                 TEXT,\n" \
  "  map_lang            CHAR(255),\n" \
  "  reduce              TEXT,\n" \
  "  reduce_lang         CHAR(255),\n" \
  "  sync_map_id         CHAR(255),\n" \
  "  sync_reduce_id      CHAR(255),\n" \
  "  sync_rereduce_id    CHAR(255)\n" \
  ");"

#define DUPIN_VIEW_SQL_INSERT \
	"INSERT INTO Dupin (id, pid, key, obj) " \
        "VALUES('%q', '%q', '%q', '%q')"

#define DUPIN_VIEW_SQL_EXISTS \
	"SELECT count(id) FROM Dupin WHERE id = '%q' "

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

static gboolean
dupin_view_get_total_records_db (Dupin * d, gchar * parent, gsize * total)
{
  DupinDB *db;

  if (!(db = dupin_database_open (d, parent, NULL)))
    return FALSE;

  if (dupin_record_get_total_records (db, total) == FALSE)
    {
      dupin_database_unref (db);
      return FALSE;
    }

  dupin_database_unref (db);
  return TRUE;
}

static gboolean
dupin_view_get_total_records_view (Dupin * d, gchar * parent, gsize * total)
{
  DupinView *view;

  if (!(view = dupin_view_open (d, parent, NULL)))
    return FALSE;

  if (dupin_view_record_get_total_records (view, total) == FALSE)
    {
      dupin_view_unref (view);
      return FALSE;
    }

  dupin_view_unref (view);
  return TRUE;
}

static gboolean
dupin_view_get_total_records (Dupin * d, gchar * parent, gboolean is_db, gsize * total)
{
  if (is_db)
    return dupin_view_get_total_records_db (d, parent, total);

  return dupin_view_get_total_records_view (d, parent, total);
}

static gchar *
dupin_view_new_sync_map_id_db (Dupin * d, gchar * parent)
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
dupin_view_new_sync_map_id_view (Dupin * d, gchar * parent)
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
dupin_view_new_sync_map_id (Dupin * d, gchar * parent, gboolean is_db)
{
  if (is_db)
    return dupin_view_new_sync_map_id_db (d, parent);

  return dupin_view_new_sync_map_id_view (d, parent);
}

DupinView *
dupin_view_new (Dupin * d, gchar * view, gchar * parent, gboolean is_db,
		gchar * map, DupinMRLang map_language, gchar * reduce,
		DupinMRLang reduce_language, GError ** error)
{
  DupinView *ret;
  gchar *path;
  gchar *name;
  gchar *sync_map_id;

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

  if (reduce != NULL && strcmp(reduce,"(NULL)"))
    {
      ret->reduce = g_strdup (reduce);
      ret->reduce_lang = reduce_language;
    }

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

  if ((sync_map_id = dupin_view_new_sync_map_id (d, parent, is_db)))
    {

      str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

      g_mutex_lock (d->mutex);
      if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
	{
          g_mutex_unlock (d->mutex);
	  g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		       errmsg);

	  sqlite3_free (errmsg);
	  sqlite3_free (str);
	  dupin_view_free (ret);
	  g_free (sync_map_id);
	  return NULL;
	}
      sqlite3_free (str);

      g_mutex_unlock (d->mutex);

      g_free (sync_map_id);
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
			    JsonObject * obj)
{
  gsize i;

  for (i = 0; i < p->numb; i++)
    {
      DupinView *view = p->views[i];
      JsonArray *array;

      /* VERY IMPORTANT - we do only map on record insertion - the reduce step is only done on sync - but the synced flag must not be set if reduce is still needed */

      if ((array = dupin_mr_record_map (view, obj)))
	{
	  GList *nodes, *n;
	  nodes = json_array_get_elements (array);

          for (n = nodes; n != NULL; n = n->next)
            {
              JsonNode * element_node = (JsonNode*)n->data;
              JsonObject *nobj = json_node_get_object (element_node);

              GList *nodes, *n;
              JsonNode *key_node=NULL;
	      nodes = json_object_get_members (nobj);
              for (n = nodes; n != NULL; n = n->next)
                {
                  gchar *member_name = (gchar *) n->data;
                  if (!strcmp (member_name, "key"))
                    {
		      /* we extract this for SQLite table indexing */
                      key_node = json_node_copy (json_object_get_member (nobj, member_name) );
                    }
                }
              g_list_free (nodes);

              JsonNode *pid_node=json_node_new (JSON_NODE_ARRAY);
              JsonArray *pid_array=json_array_new ();
              json_array_add_string_element (pid_array, id);
              json_node_take_array (pid_node, pid_array);

	      dupin_view_record_save (view, pid_node, key_node, nobj);

              json_node_free (pid_node);
              if (key_node != NULL)
                json_node_free (key_node);

	      dupin_view_p_record_insert (&view->views, id, nobj); /* TODO - check if this is nobj or obj ?! */
            }
          g_list_free (nodes);

	  json_array_unref (array);
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
dupin_view_record_save (DupinView * view, JsonNode * pid, JsonNode * key, JsonObject * obj)
{
  GList *nodes, *n;
  JsonNode *node;
  JsonGenerator *gen;

  const gchar *id = NULL;
  gchar *tmp, *obj_serialised, *key_serialised, *pid_serialised;
  JsonNode *key_node=NULL;
  JsonNode *pid_node=NULL;

  g_return_if_fail (dupin_util_is_valid_obj (obj) != FALSE);

  if (key != NULL)
    key_node = json_node_copy (key);

  if (pid != NULL)
    pid_node = json_node_copy (pid);

  g_mutex_lock (view->mutex);

  nodes = json_object_get_members (obj);

  for (n = nodes; n != NULL; n = n->next)
    {
      gchar *member_name = (gchar *) n->data;

      if (!strcmp (member_name, "_id"))
        {
          /* NOTE - we always force a new _id - due records must be sorted by a controlled ID in a view for mp/r/rr purposes */
          json_object_remove_member (obj, member_name);
	}
    }
  g_list_free (nodes);

  if (!id && !(id = dupin_view_generate_id (view)))
    {
      g_mutex_unlock (view->mutex);
      return;
    }

  /* serialise the obj */
  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    {
      g_mutex_unlock (view->mutex);
      g_free ((gchar *)id);
      return;
    }

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    {
      g_mutex_unlock (view->mutex);
      g_free ((gchar *)id);
      if (node != NULL)
        json_node_free (node);
      return;
    }

  json_generator_set_root (gen, node );
  obj_serialised = json_generator_to_data (gen,NULL);

  if (obj_serialised == NULL)
    {
      g_mutex_unlock (view->mutex);
      g_free ((gchar *)id);
      if (gen != NULL)
        g_object_unref (gen);
      if (node != NULL)
        json_node_free (node);
      return;
    }

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);

  /* serialise the key */

  if (key_node != NULL)
    {
      if (json_node_get_node_type (key_node) == JSON_NODE_VALUE)
        {
          if (json_node_get_value_type (key_node) == G_TYPE_STRING)
          {
            key_serialised = g_strdup (json_node_get_string (key_node));
          }

          if (json_node_get_value_type (key_node) == G_TYPE_DOUBLE
                || json_node_get_value_type (key_node) == G_TYPE_FLOAT)
          {
            gdouble numb = json_node_get_double (key_node);
            key_serialised = g_strdup_printf ("%f", numb);
          }

          if (json_node_get_value_type (key_node) == G_TYPE_INT
                || json_node_get_value_type (key_node) == G_TYPE_INT64
                || json_node_get_value_type (key_node) == G_TYPE_UINT)
          {
            gint numb = (gint) json_node_get_int (key_node);
            key_serialised = g_strdup_printf ("%d", numb);
          }
          if (json_node_get_value_type (key_node) == G_TYPE_BOOLEAN)
          {
            key_serialised = g_strdup_printf (json_node_get_boolean (key_node) == TRUE ? "true" : "false");
          }
        }
      else
        {
          gen = json_generator_new();

          if (gen == NULL)
            {
              g_mutex_unlock (view->mutex);
              g_free ((gchar *)id);
              g_free (obj_serialised);
              if (key_node != NULL)
                json_node_free (key_node);
              return;
            }

          json_generator_set_root (gen, key_node );
          key_serialised = json_generator_to_data (gen,NULL);

          if (key_node != NULL)
            json_node_free (key_node);

          if (key_serialised == NULL)
            {
              g_mutex_unlock (view->mutex);
              g_free ((gchar *)id);
              g_free (obj_serialised);
              if (gen != NULL)
                g_object_unref (gen);
              return;
            }

          if (gen != NULL)
            g_object_unref (gen);
        }
    }

  if (pid_node != NULL)
    {
      gen = json_generator_new();

      if (gen == NULL)
        {
          g_mutex_unlock (view->mutex);
          g_free ((gchar *)id);
          g_free (obj_serialised);
          if (key_serialised)
            g_free (key_serialised);
          return;
        }

      json_generator_set_root (gen, pid_node );
      pid_serialised = json_generator_to_data (gen,NULL);

      if (pid_node != NULL)
        json_node_free (pid_node);

      if (pid_serialised == NULL)
        {
          g_mutex_unlock (view->mutex);
          g_free ((gchar *)id);
          g_free (obj_serialised);
          if (key_serialised)
            g_free (key_serialised);
          if (gen != NULL)
            g_object_unref (gen);
          return;
        }

      if (gen != NULL)
        g_object_unref (gen);
    }

  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_INSERT, id, pid_serialised, key_serialised, obj_serialised);

//g_message("query: %s\n",tmp);

  sqlite3_exec (view->db, tmp, NULL, NULL, NULL);

  g_mutex_unlock (view->mutex);

  sqlite3_free (tmp);
  g_free (obj_serialised);
  if (key_serialised)
    g_free (key_serialised);
  if (pid_serialised)
    g_free (pid_serialised);
  g_free ((gchar *)id);
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
  GString *str;
  gchar *query;

  /* NOTE - hack to avoid to keep another table and be able to delete entries
            from a view generated from multiple input documents */
     
  str = g_string_new ("DELETE FROM Dupin WHERE pid ");
  str = g_string_append (str, " LIKE '%\"");
  str = g_string_append (str, pid);
  str = g_string_append (str, "\"%';");
  query = g_string_free (str, FALSE);

//g_message("dupin_view_record_delete() query=%s\n",query);

  g_mutex_lock (view->mutex);
  sqlite3_exec (view->db, query, NULL, NULL, NULL);
  g_mutex_unlock (view->mutex);

  g_free (query);
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

      if (argv[2] != NULL && strcmp(argv[2],"(NULL)"))
        {
          view->reduce = g_strdup (argv[2]);
          view->reduce_lang = dupin_util_mr_lang_to_enum (argv[3]);
        }

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
		       &errmsg) != SQLITE_OK
      || sqlite3_exec (view->db, DUPIN_VIEW_SQL_CREATE_INDEX, NULL, NULL,
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
  JsonObject *obj;
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

      gchar * id = g_strdup ( (gchar *)json_node_get_string ( json_array_get_element ( json_node_get_array (data->pid), 0) ) );

      if ((array = dupin_mr_record_map (view, data->obj)))
	{
	  GList *nodes, *n;
	  nodes = json_array_get_elements (array);

          for (n = nodes; n != NULL; n = n->next)
            {
              JsonObject *nobj;
              JsonNode *element_node = (JsonNode*)n->data;

              nobj = json_node_get_object (element_node);

              GList *nodes, *n;
              JsonNode *key_node=NULL;
              nodes = json_object_get_members (nobj);
              for (n = nodes; n != NULL; n = n->next)
                {
                  gchar *member_name = (gchar *) n->data;
                  if (!strcmp (member_name, "key"))
                    {
		      /* we extract this for SQLite table indexing */
                      key_node = json_node_copy ( json_object_get_member (nobj, member_name));
                    }
                }
              g_list_free (nodes);

	      dupin_view_record_save (view, data->pid, key_node, nobj);

              if (key_node != NULL)
                json_node_free (key_node);
 
	      dupin_view_p_record_insert (&view->views, id, nobj); /* TODO - check if this is nobj or obj ?! */
            }
          g_list_free (nodes);

	  json_array_unref (array);
	}

        g_free((gchar *) id);
    }
}

static void
dupin_view_sync_thread_real_reduce (DupinView * view, GList * list)
{
  /* NOTE - then group the list element by same key and call reduce for each of those
            groups - the size of group is max to VIEW_SYNC_COUNT anyway */

 /* do reduce then based on the above */

 /* view must not be set to sync till reduce step terminated I.e. the results returned are not supposed to be ready/consistent */

#if 0
              /* reduce([ [key1,id1], [key2,id2], [key3,id3] ], [value1,value2,value3], false) */
	      if (view->reduce != NULL)
                {
                  element_node = (JsonNode*)n->data;
                  nobj = json_node_get_object (element_node);

	          /* TODO - reduce the whole set of key everytime - need to be completely optimised/refactored later */

                  GList *n;
                  const gchar *key = NULL;
  	          GList *nodes = json_object_get_members (nobj);
	          JsonNode  * key_node=NULL;
	          JsonArray * keys = json_array_new();
	          JsonArray * values = json_array_new();
                  for (n = nodes; n != NULL; n = n->next)
                    {
                      gchar *member_name = (gchar *) n->data;

                      if (!strcmp (member_name, "key"))
                        {
	                  key_node = json_object_get_member (nobj, member_name);
	                  if (json_node_get_node_type (key_node) == JSON_NODE_VALUE)
                            {
			      key_node = json_node_copy (key_node);
	                      key = g_strdup ( json_node_get_string (key_node) );
                            }
	                  else if (json_node_get_node_type (key_node) == JSON_NODE_ARRAY)
                            {
                              /* TODO - always pick the first element as key due we do not deal with multiple keys yet... */
	                      key_node = json_node_copy ( json_array_get_element ( json_node_get_array (key_node), 0 ) );
	                      key = g_strdup ( json_node_get_string (key_node) );
                            }

			  /* keys = [ [k1, pid1] ... [kN, pidN] ] - we pass one key from current map */
                          JsonNode  * subarray_node = json_node_new (JSON_NODE_ARRAY);
                          JsonNode  * docid_node = json_node_new (JSON_NODE_VALUE);
                          json_node_set_string (docid_node, id);
                          JsonArray * subarray = json_array_new ();
		          json_array_add_element (subarray, key_node);
		          json_array_add_element (subarray, docid_node);
		          json_node_take_array (subarray_node, subarray);

                          json_array_add_element (keys, subarray_node);
                        }
                      else if (!strcmp (member_name, "value"))
                        {
                          json_array_add_element (values, json_node_copy (json_object_get_member (nobj, member_name)) );
                        }
                    }
                  g_list_free (nodes);

                  /* get a list of values of view table and add to values */

	          /* push value_node into values */

                  /* call function(keys, values, rereduce)  values = [ v1, v2... vN ] */
                  element_node = dupin_mr_record_reduce (view, keys, values, FALSE); /* no rereduce */

                  g_free ((gchar *)key);
                  json_array_unref (keys);
                  json_array_unref (values);

		  if (element_node == NULL)
                    {
                      /* TODO - set error? fail how ? */
                      return;
                    }

		  /* remember that JSON null is a valid reduce result */

                  nobj = json_object_new ();
		  /* add key */
		  /* add value */

		  /* assamble nobj like { 'key': key, 'value': element_node } */

		  /* delete view rows of values - later below a new row of reduce will be added see dupin_view_record_save() */
                }
	      else
                {
                  element_node = (JsonNode*)n->data;
                  nobj = json_node_get_object (element_node);
                }
#endif

}

static gboolean
dupin_view_sync_thread_map_db (DupinView * view, gsize count, gsize offset, gsize total_records)
{
  DupinDB *db;
  GList *results, *list;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *sync_map_id = NULL;
  gchar *str;

  if (!(db = dupin_database_open (view->d, view->parent, NULL)))
    return FALSE;

  if (dupin_record_get_list (db, count, offset, FALSE, &results, NULL) ==
      FALSE || !results)
    {
      dupin_database_unref (db);
      return FALSE;
    }

//g_message("dupin_view_sync_thread_map_db()    g_list_length (results) = %d\n", (gint) g_list_length (results) );

  if (g_list_length (results) != count)
    ret = FALSE;

  gsize total_processed = offset;

  for (list = results; list; list = list->next)
    {
      total_processed++;

      if (dupin_record_is_deleted (list->data, -1) == TRUE)
        continue;

      struct dupin_view_sync_t *data =
	g_malloc0 (sizeof (struct dupin_view_sync_t));
      data->obj = json_node_get_object (json_node_copy (dupin_record_get_revision (list->data, -1)));

      data->pid = json_node_new (JSON_NODE_ARRAY);
      JsonArray *pid_array=json_array_new ();
      sync_map_id = g_strdup ( (gchar *) dupin_record_get_id (list->data) );

      json_array_add_string_element (pid_array, sync_map_id);
      json_node_take_array (data->pid, pid_array);

      /* NOTE - key not set for dupin_view_sync_thread_map_db() - see dupin_view_sync_thread_map_view() instead */

      l = g_list_append (l, data);

      if (total_processed == total_records)
        {
          ret=FALSE;
          break;
        }
    }

  dupin_view_sync_thread_real_map (view, l);

  /* g_list_foreach (l, (GFunc) g_free, NULL); */
  /* NOTE - free each list JSON node properly - the following is not freeing the json_node_copy() above */
  for (; l; l = l->next)
    {
      struct dupin_view_sync_t *data = l->data;
      json_object_unref (data->obj);
      json_node_free (data->pid);
      g_free (data);
    }
  g_list_free (l);
  dupin_record_get_list_close (results);

//g_message("dupin_view_sync_thread_map_db() sync_map_id=%s as to be stored",sync_map_id);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

//g_message("dupin_view_sync_thread_map_db() query=%s\n",str);

  if (sync_map_id != NULL)
    g_free((gchar *)sync_map_id);

  g_mutex_lock (view->mutex);
  sqlite3_exec (view->db, str, NULL, NULL, NULL);
  g_mutex_unlock (view->mutex);

  sqlite3_free (str);

  dupin_database_unref (db);
  return ret;
}


static gboolean
dupin_view_sync_thread_map_view (DupinView * view, gsize count, gsize offset, gsize total_records)
{
  DupinView *v;
  GList *results, *list;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *sync_map_id = NULL;
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

  gsize total_processed = offset;

  for (list = results; list; list = list->next)
    {
      total_processed++;

      /* NOTE - views are read-only we do not have to skip deleted records - see dupin_view_sync_thread_map_db() instead */

      struct dupin_view_sync_t *data =
	g_malloc0 (sizeof (struct dupin_view_sync_t));
      data->obj = json_node_get_object (json_node_copy (dupin_view_record_get (list->data)));

      /* TODO - check shouldn't this be more simply json_node_copy (dupin_view_record_get_pid (list->data))  or not ?! */
      data->pid = json_node_new (JSON_NODE_ARRAY);
      JsonArray *pid_array=json_array_new ();

      /* NOTE - key not set for dupin_view_sync_thread_map_db() - see dupin_view_sync_thread_map_view() instead */
      sync_map_id = g_strdup ( (gchar *) dupin_view_record_get_id (list->data) );
      json_array_add_string_element (pid_array, sync_map_id);
      json_node_take_array (data->pid, pid_array);

//g_message("dupin_view_sync_thread_map_view() sync_map_id=%s as fetched",sync_map_id);

      data->key = json_node_copy (dupin_view_record_get_key (list->data));

      l = g_list_append (l, data);

      if (total_processed == total_records)
        {
          ret=FALSE;
          break;
        }
    }

  dupin_view_sync_thread_real_map (view, l);

  /* g_list_foreach (l, (GFunc) g_free, NULL); */
  /* NOTE - free each list JSON node properly - the following is not freeing the json_node_copy() above */
  for (; l; l = l->next)
    {
      struct dupin_view_sync_t *data = l->data;
      json_object_unref (data->obj);
      json_node_free (data->key);
      json_node_free (data->pid);
      g_free (data);
    }
  g_list_free (l);
  dupin_view_record_get_list_close (results);

//g_message("dupin_view_sync_thread_map_view() sync_map_id=%s as to be stored",sync_map_id);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

//g_message("dupin_view_sync_thread_map_view() query=%s\n",str);

  if (sync_map_id != NULL)
    g_free((gchar *)sync_map_id);

  g_mutex_lock (view->mutex);
  sqlite3_exec (view->db, str, NULL, NULL, NULL);
  g_mutex_unlock (view->mutex);

  sqlite3_free (str);

  dupin_view_unref (v);
  return ret;
}

static gboolean
dupin_view_sync_thread_map (DupinView * view, gsize count, gsize offset, gsize total_records)
{
  if (view->parent_is_db == TRUE)
    return dupin_view_sync_thread_map_db (view, count, offset, total_records);

  return dupin_view_sync_thread_map_view (view, count, offset, total_records);
}

static gboolean
dupin_view_sync_thread_reduce_view (DupinView * view, gsize count, gsize offset, gsize total_records)
{
  DupinView *v;
  GList *results, *list;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *sync_reduce_id = NULL;
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
      data->obj = json_node_get_object (json_node_copy (dupin_view_record_get (list->data)));

      /* TODO - check shouldn't this be more simply json_node_copy (dupin_view_record_get_pid (list->data))  or not ?! */
      data->pid = json_node_new (JSON_NODE_ARRAY);
      JsonArray *pid_array=json_array_new ();

      sync_reduce_id = g_strdup ( (gchar *) dupin_view_record_get_id (list->data) );
      json_array_add_string_element (pid_array, sync_reduce_id);
      json_node_take_array (data->pid, pid_array);

//g_message("dupin_view_sync_thread_reduce_view() sync_reduce_id=%s as fetched",sync_reduce_id);

      data->key = json_node_copy (dupin_view_record_get_key (list->data));

      l = g_list_append (l, data);
    }

  dupin_view_sync_thread_real_reduce (view, l);

  /* g_list_foreach (l, (GFunc) g_free, NULL); */
  /* NOTE - free each list JSON node properly - the following is not freeing the json_node_copy() above */
  for (; l; l = l->next)
    {
      struct dupin_view_sync_t *data = l->data;
      json_object_unref (data->obj);
      json_node_free (data->key);
      json_node_free (data->pid);
      g_free (data);
    }
  g_list_free (l);
  dupin_view_record_get_list_close (results);

//g_message("dupin_view_sync_thread_reduce_view() sync_reduce_id=%s as to be stored",sync_reduce_id);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_reduce_id = '%q'", sync_reduce_id); /* is the (key,pid) we stopped */

//g_message("dupin_view_sync_thread_reduce_view() query=%s\n",str);

  if (sync_reduce_id != NULL)
    g_free((gchar *)sync_reduce_id);

  g_mutex_lock (view->mutex);
  sqlite3_exec (view->db, str, NULL, NULL, NULL);
  g_mutex_unlock (view->mutex);

  sqlite3_free (str);

  dupin_view_unref (v);
  return ret;
}

static gboolean
dupin_view_sync_thread_reduce (DupinView * view, gsize count, gsize offset, gsize total_records)
{
  /* we ignore view->parent_is_db due reduce works on view only results - likewise re-reduce */

  return dupin_view_sync_thread_reduce_view (view, count, offset, total_records);
}

static gpointer
dupin_view_sync_map_thread (DupinView * view)
{
  dupin_view_ref (view);

  view->sync_map_offset = 0;

  gsize total_records=0;

  if (dupin_view_get_total_records (view->d, view->parent, view->parent_is_db, &total_records) == FALSE)
    return NULL;

//g_message("dupin_view_sync_map_thread() started with %d total records to MAP\n",(gint)total_records);

  while (view->sync_toquit == FALSE || view->todelete == FALSE)
    {
      if (dupin_view_sync_thread_map (view, VIEW_SYNC_COUNT, view->sync_map_offset, total_records) == FALSE)
        {
          gchar *query = "UPDATE DupinView SET sync_map_id = NULL";

          g_mutex_lock (view->mutex);
          sqlite3_exec (view->db, query, NULL, NULL, NULL);
          g_mutex_unlock (view->mutex);

          break;
        }

      view->sync_map_offset += VIEW_SYNC_COUNT;
    }

  view->sync_map_thread = NULL;
  dupin_view_unref (view);
  g_thread_exit (NULL);

  return NULL;
}

static gpointer
dupin_view_sync_reduce_thread (DupinView * view)
{
  dupin_view_ref (view);

  view->sync_reduce_offset = 0;

  gsize total_records=0;

  if (dupin_view_get_total_records (view->d, view->parent, view->parent_is_db, &total_records) == FALSE)
    return NULL;

//g_message("dupin_view_sync_reduce_thread() started with %d total records to MAP\n",(gint)total_records);

  while (view->sync_toquit == FALSE || view->todelete == FALSE
         || view->sync_map_thread) /* NOTE - if map hangs, reduce also hangs */
    {
      if (dupin_view_sync_thread_reduce (view, VIEW_SYNC_COUNT, view->sync_reduce_offset, total_records) == FALSE)
	{
          if (view->sync_map_thread)
            {
	      /* restart waiting for any mapped result */
              view->sync_reduce_offset = 0;

	      continue;
            }

          gchar *query = "UPDATE DupinView SET sync_reduce_id = NULL";

          g_mutex_lock (view->mutex);
          sqlite3_exec (view->db, query, NULL, NULL, NULL);
          g_mutex_unlock (view->mutex);

	  break;
	}

      view->sync_reduce_offset += VIEW_SYNC_COUNT;
    }

  view->sync_reduce_thread = NULL;
  dupin_view_unref (view);
  g_thread_exit (NULL);

  return NULL;
}

/* NOTE- we try to spawn two threads map, reduce 
         when reduce is done we re-reduce till no map and reduce is still running,
         finished scan and only one key is left (count=1) */

void
dupin_view_sync (DupinView * view)
{
  gchar *sync_map_id = NULL;
  gchar *query;

  query = "SELECT sync_map_id as c FROM DupinView";

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_map_id, NULL) !=
      SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      return;
    }

  if (sync_map_id != NULL)
    {
      view->sync_map_thread =
	g_thread_create ((GThreadFunc) dupin_view_sync_map_thread, view, FALSE,
			 NULL);

      g_free (sync_map_id);
    }
  g_mutex_unlock (view->mutex);

  if (view->reduce != NULL)
    {
      /* spawn other two threads: reduce and rereduce */

      gchar *sync_reduce_id = NULL;

      query = "SELECT sync_reduce_id as c FROM DupinView";

      g_mutex_lock (view->mutex);

      if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_reduce_id, NULL) !=
          SQLITE_OK)
        {
          g_mutex_unlock (view->mutex);
          return;
        }

      if (sync_reduce_id != NULL)
        {
          view->sync_reduce_thread =
	    g_thread_create ((GThreadFunc) dupin_view_sync_reduce_thread, view, FALSE,
			 NULL);

          g_free (sync_reduce_id);
        }
      g_mutex_unlock (view->mutex);
    }
}

gboolean
dupin_view_is_sync (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (view->sync_map_thread
      || view->sync_reduce_thread
      || view->sync_rereduce_thread)
    return FALSE;

  return TRUE;
}

/* EOF */
