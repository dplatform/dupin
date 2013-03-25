#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"
#include "dupin_date.h"
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
  ");\n" \
  "CREATE TABLE IF NOT EXISTS DupinPid2Id (\n" \
  "  pid         CHAR(255) NOT NULL,\n" \
  "  id          TEXT NOT NULL\n" \
  ");"

  /*"CREATE INDEX IF NOT EXISTS DupinId ON Dupin (id);\n" \ - created by default see http://web.utk.edu/~jplyon/sqlite/SQLite_optimization_FAQ.html#indexes */

#define DUPIN_VIEW_SQL_CREATE_INDEX \
  "CREATE INDEX IF NOT EXISTS DupinPid ON Dupin (pid);\n" \
  "CREATE INDEX IF NOT EXISTS DupinKey ON Dupin (key);\n" \
  "CREATE INDEX IF NOT EXISTS DupinObj ON Dupin (obj);\n" \
  "CREATE INDEX IF NOT EXISTS DupinKeyObj ON Dupin (key, obj);\n" \
  "CREATE INDEX IF NOT EXISTS DupinPid2IdPid ON DupinPid2Id (pid);\n" \
  "CREATE INDEX IF NOT EXISTS DupinPid2IdId ON DupinPid2Id (id);"

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
  "  sync_rereduce             BOOL DEFAULT FALSE,\n" \
  "  output                    CHAR(255),\n" \
  "  output_isdb               BOOL DEFAULT TRUE,\n" \
  "  output_islinkb            BOOL DEFAULT FALSE,\n" \
  "  last_to_delete_id         CHAR(255) DEFAULT NULL,\n" \
  "  creation_time   	       CHAR(255) NOT NULL DEFAULT '0'\n" \
  ");\n" \
  "PRAGMA user_version = 2"

#define DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_1 \
  "ALTER TABLE DupinView ADD COLUMN creation_time CHAR(255) NOT NULL DEFAULT '0';\n" \
  "PRAGMA user_version = 2"

#define DUPIN_VIEW_SQL_INSERT \
	"INSERT OR REPLACE INTO Dupin (id, pid, key, obj) " \
        "VALUES('%q', '%q', '%q', '%q')"

#define DUPIN_VIEW_SQL_INSERT_PID2ID \
	"INSERT OR REPLACE INTO DupinPid2Id (pid, id) " \
        "VALUES('%q', '%q')"

#define DUPIN_VIEW_SQL_TOTAL_REREDUCE \
	"SELECT key AS inner_key, count(*) AS inner_count FROM Dupin GROUP BY inner_key HAVING inner_count > 1 "

#define DUPIN_VIEW_SQL_COUNT \
	"SELECT count(id) as c FROM Dupin"

#define VIEW_SYNC_COUNT	100

static gchar *dupin_view_generate_id (DupinView * view);

static int dupin_view_record_delete_cb (void *data, int argc, char **argv, char **col);

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
    {
      ret->ref++;

#if DEBUG
      fprintf(stderr,"dupin_view_open: (%p) name=%s \t ref++=%d\n", g_thread_self (), view, (gint) ret->ref);
#endif
    }

  g_mutex_unlock (d->mutex);

  return ret;
}

DupinView *
dupin_view_new (Dupin * d, gchar * view, gchar * parent, gboolean is_db, gboolean is_linkb,
		gchar * map, DupinMRLang map_language, gchar * reduce,
		DupinMRLang reduce_language,
	        gchar * output, gboolean output_is_db, gboolean output_is_linkb, GError ** error)
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
    {
      if (dupin_database_exists (d, parent) == FALSE)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		       "View '%s' parent database '%s' does not exist.", view, parent);
	  return NULL;
        }
    }
  else if (is_linkb == TRUE)
    {
      if (dupin_linkbase_exists (d, parent) == FALSE)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		       "View '%s' parent linkbase '%s' does not exist.", view, parent);
	  return NULL;
        }
    }
  else
    {
      if (dupin_view_exists (d, parent) == FALSE)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		       "View '%s' parent view '%s' does not exist.", view, parent);
	  return NULL;
        }
    }

  if (output != NULL && g_strcmp0(output,"(NULL)") && g_strcmp0(output,"null") )
    {
      if (output_is_db == TRUE)
        {
          if (dupin_database_exists (d, output) == FALSE)
            {
              g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		           "View '%s' output database '%s' does not exist.", view, output);
	      return NULL;
	    }
	}
      else if (output_is_linkb == TRUE)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		       "Output to linkbase is not implemented yet");
	  return NULL;
        }
    }

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

  if (output != NULL && g_strcmp0(output,"(NULL)") && g_strcmp0(output,"null") )
    {
      ret->output = g_strdup (output);
    }
  ret->output_is_db = output_is_db;
  ret->output_is_linkb = output_is_linkb;

  g_free (path);

  ret->ref++;

#if DEBUG
  fprintf(stderr,"dupin_view_new: (%p) name=%s \t ref++=%d\n", g_thread_self (), view, (gint) ret->ref);
#endif

  if (dupin_view_begin_transaction (ret, error) < 0)
    {
      g_mutex_unlock (d->mutex);
      dupin_view_disconnect (ret);
      return NULL;
    }

  gchar * creation_time = g_strdup_printf ("%" G_GSIZE_FORMAT, dupin_date_timestamp_now (0));

  str =
    sqlite3_mprintf ("INSERT OR REPLACE INTO DupinView "
		     "(parent, isdb, islinkb, map, map_lang, reduce, reduce_lang, output, output_isdb, output_islinkb, creation_time) "
		     "VALUES('%q', '%s', '%s', '%q', '%q', '%q' ,'%q', '%q', '%s', '%s', '%q')", parent,
		     is_db ? "TRUE" : "FALSE",
		     is_linkb ? "TRUE" : "FALSE",
		     map,
		     dupin_util_mr_lang_to_string (map_language), reduce,
		     dupin_util_mr_lang_to_string (reduce_language),
		     output,
		     output_is_db ? "TRUE" : "FALSE",
		     output_is_linkb ? "TRUE" : "FALSE",
		     creation_time);

  g_free (creation_time);

  if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (d->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      sqlite3_free (str);
      dupin_view_rollback_transaction (ret, error);
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
      dupin_view_rollback_transaction (ret, error);
      dupin_view_disconnect (ret);
      return NULL;
    }

  sqlite3_free (str);

  if (dupin_view_commit_transaction (ret, error) < 0)
    {
      g_mutex_unlock (d->mutex);
      dupin_view_disconnect (ret);
      return NULL;
    }

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
  gchar *output;
  gboolean output_isdb;
  gboolean output_islinkb;
};

static int
dupin_view_p_update_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_view_p_update_t *update = data;

  if (argc == 10)
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

      if (argv[7] != NULL && g_strcmp0(argv[7],"(NULL)") && g_strcmp0(argv[7],"null") )
        update->output = g_strdup (argv[7]);

      if (argv[8] && *argv[8])
        update->output_isdb = !g_strcmp0 (argv[8], "TRUE") ? TRUE : FALSE;

      if (argv[9] && *argv[9])
        update->output_islinkb = !g_strcmp0 (argv[9], "TRUE") ? TRUE : FALSE;

    }

  return 0;
}

#define DUPIN_VIEW_P_SIZE	64

static void
dupin_view_p_update_real (DupinViewP * p, DupinView * view)
{
  /* NOTE - need to remove pointer from parent if view is "hot deleted" */

  if (view->todelete == TRUE)
    {
      if (p->views != NULL)
        {
	  DupinView ** views = p->views;
          p->views = g_malloc (sizeof (DupinView *) * p->size);

	  gint i;
	  gint current_numb = p->numb;
	  p->numb = 0;
	  for (i=0; i < current_numb ; i++)
	    {
	      if (views[i] != view)
	        {
                  p->views[p->numb] = views[i];
                  p->numb++;
		}
	    }

	  g_free (views);
        }

      return;
    }

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
  gchar *query = "SELECT parent, isdb, islinkb, map, map_lang, reduce, reduce_lang, output, output_isdb, output_islinkb FROM DupinView LIMIT 1";

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

      if (update.output != NULL)
        g_free (update.output);

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

          if (update.map != NULL)
            g_free (update.map);

          if (update.reduce != NULL)
            g_free (update.reduce);

          if (update.output != NULL)
            g_free (update.output);

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

          if (update.map != NULL)
            g_free (update.map);

          if (update.reduce != NULL)
            g_free (update.reduce);

          if (update.output != NULL)
            g_free (update.output);

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

          if (update.map != NULL)
            g_free (update.map);

          if (update.reduce != NULL)
            g_free (update.reduce);

          if (update.output != NULL)
            g_free (update.output);

	  return FALSE;
	}

      g_mutex_lock (v->mutex);
      dupin_view_p_update_real (&v->views, view);
      g_mutex_unlock (v->mutex);

      dupin_view_unref (v);
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

  if (update.output != NULL && g_strcmp0(update.output,"(NULL)") && g_strcmp0(update.output,"null") )
    {
      if (view->output == NULL)
        view->output = update.output;
      else
        g_free (update.output);
    }

  view->output_is_db = update.output_isdb;
  view->output_is_linkb = update.output_islinkb;

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

static int
dupin_view_p_record_delete_cb (void *data, int argc, char **argv, char **col)
{
  gchar **last_to_delete_id = data;

  if (argv[0] && *argv[0])
    *last_to_delete_id = g_strdup (argv[0]);

  return 0;
}

void
dupin_view_p_record_delete (DupinViewP * p, gchar * pid)
{
  gsize i;

  for (i = 0; i < p->numb; i++)
    {
      DupinView *view = p->views[i];
      dupin_view_p_record_delete (&view->views, pid);

      /* NOTE - check if deletes queue is full and need to be flushed first */

      if (view->deletes_queue_size >= DUPIN_VIEW_DELETES_QUEUE_MAXSIZE)
        dupin_view_record_delete (view);

      /* NOTE - add 'pid' to the deletes queue */

      view->deletes_queue = g_list_prepend (view->deletes_queue, g_strdup (pid));
      view->deletes_queue_size++;

      /* TODO - delete any PID where 'pid' is context_id or href of links; and viceversa */
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

//g_message("dupin_view_record_save_map: %s query: %s\n",view->name, tmp);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      g_free ((gchar *)id);
      g_free (node_serialized);
      if (key_serialized)
        g_free (key_serialized);
      if (pid_serialized)
        g_free (pid_serialized);

      sqlite3_free (tmp);

      return;
    }

  if (sqlite3_exec (view->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_error("dupin_view_record_save_map: %s", errmsg);
      sqlite3_free (errmsg);

      g_free ((gchar *)id);
      g_free (node_serialized);
      if (key_serialized)
        g_free (key_serialized);
      if (pid_serialized)
        g_free (pid_serialized);

      sqlite3_free (tmp);
      dupin_view_rollback_transaction (view, NULL);

      return;
    }

  sqlite3_free (tmp);

  /* NOTE - store PID -> IDs mappings
            foreach PIDs add one row (pid,id) so we can use select ... in (select ...) subquery on deletion or other */

  if (pid_node != NULL)
    {
      GList *nodes, *n;
      nodes = json_array_get_elements (json_node_get_array (pid_node));
      for (n = nodes; n != NULL; n = n->next)
        {
          JsonNode * p = (JsonNode *)n->data;

          if (json_node_get_node_type (p) != JSON_NODE_VALUE
              || json_node_get_value_type (p) != G_TYPE_STRING)
            {
              g_error("dupin_view_record_save_map: %s non string array element found in pid", pid_serialized);

              continue;
            }

          gchar * pid_string = (gchar *)json_node_get_string (p);

	  /* NOTE - fetch an previous entry, if matched parse array of IDs */

	  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_INSERT_PID2ID, pid_string, id);

          if (sqlite3_exec (view->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
            {
              g_mutex_unlock (view->mutex);

              g_error("dupin_view_record_save_map: %s", errmsg);
              sqlite3_free (errmsg);

              g_free ((gchar *)id);
              g_free (node_serialized);
              if (key_serialized)
                g_free (key_serialized);
              if (pid_serialized)
                g_free (pid_serialized);

              sqlite3_free (tmp);
              dupin_view_rollback_transaction (view, NULL);

      	      g_list_free (nodes);

              return;
            }

          sqlite3_free (tmp);
	}
      g_list_free (nodes);
    }

  /* NOTE - get last_to_delete if there and delete it */

  gchar * query = "SELECT last_to_delete_id as c FROM DupinView LIMIT 1";

  gchar * last_to_delete_id = NULL;
  if (sqlite3_exec (view->db, query, dupin_view_p_record_delete_cb, &last_to_delete_id, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_error("dupin_view_record_save_map: %s", errmsg);
      sqlite3_free (errmsg);

      g_free ((gchar *)id);
      g_free (node_serialized);
      if (key_serialized)
        g_free (key_serialized);
      if (pid_serialized)
        g_free (pid_serialized);

      dupin_view_rollback_transaction (view, NULL);

      return;
    }

  if (last_to_delete_id != NULL
      && g_strcmp0(last_to_delete_id,"(NULL)") && g_strcmp0(last_to_delete_id,"null") )
    {
      /* DELETE + UPDATE */

      tmp = sqlite3_mprintf ("DELETE FROM Dupin WHERE ROWID = %q", last_to_delete_id);

      if (sqlite3_exec (view->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_mutex_unlock (view->mutex);

          g_error("dupin_view_record_save_map: %s", errmsg);
          sqlite3_free (errmsg);

          g_free ((gchar *)id);
          g_free (node_serialized);
          if (key_serialized)
            g_free (key_serialized);
          if (pid_serialized)
            g_free (pid_serialized);

          sqlite3_free (tmp);
          dupin_view_rollback_transaction (view, NULL);

          g_free (last_to_delete_id);

          return;
        }

      sqlite3_free (tmp);

      query = "UPDATE DupinView SET last_to_delete_id = NULL";

      if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_mutex_unlock (view->mutex);

          g_error("dupin_view_record_save_map: %s", errmsg);
          sqlite3_free (errmsg);

          g_free ((gchar *)id);
          g_free (node_serialized);
          if (key_serialized)
            g_free (key_serialized);
          if (pid_serialized)
            g_free (pid_serialized);

          dupin_view_rollback_transaction (view, NULL);

          g_free (last_to_delete_id);

          return;
        }
    }
 
  if (last_to_delete_id != NULL)
    g_free (last_to_delete_id);

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      g_free ((gchar *)id);
      g_free (node_serialized);
      if (key_serialized)
        g_free (key_serialized);
      if (pid_serialized)
        g_free (pid_serialized);

      return;
    }

  g_mutex_unlock (view->mutex);

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

static int
dupin_view_record_delete_cb (void *data, int argc, char **argv, char **col)
{
  gsize *numb = data;

  if (argv[0] && *argv[0])
    *numb = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

void
dupin_view_record_delete (DupinView * view)
{
  gchar *query;
  gchar *errmsg;
  gsize max_rowid;
  GList *list;
  
  /* NOTE - we get max_rowid in a separate transaction, in worse case it will be incremented
	    since last fetch, pretty safe */

  g_mutex_lock (view->mutex);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);

      return;
    }

  if (dupin_view_record_get_max_rowid (view, &max_rowid, FALSE) == FALSE)
    {
      g_mutex_unlock (view->mutex);

      dupin_view_rollback_transaction (view, NULL);

      return;
    }

  gchar * max_rowid_str = g_strdup_printf ("%d", (gint)max_rowid);

  /* NOTE - we never delete the last record of the SQLite database to avoid ROWID recycling */

  for (list = view->deletes_queue; list != NULL; list = list->next)
    {
      gchar * pid = (gchar *)list->data;

      //query = sqlite3_mprintf ("DELETE FROM Dupin WHERE ROWID < %q AND pid LIKE '%%\"%q\"%%' ;", max_rowid_str, pid);
      query = sqlite3_mprintf ("DELETE FROM Dupin WHERE ROWID < %q AND id IN (SELECT id FROM DupinPid2Id WHERE pid = '%q') ;", max_rowid_str, pid);

//g_message("dupin_view_record_delete: view %s query=%s\n",view->name, query);

      if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_mutex_unlock (view->mutex);

          g_error("dupin_view_record_delete: %s for pid %s", errmsg, pid);
          sqlite3_free (errmsg);

          dupin_view_rollback_transaction (view, NULL);
          g_free (max_rowid_str);
          sqlite3_free (query);

   	  g_free (pid);
          view->deletes_queue = g_list_delete_link (view->deletes_queue, list);
	  view->deletes_queue_size--;

          return;
        }

      sqlite3_free (query);

      /* NOTE - check if last one needs to be deleted after next / future insert */
      gsize count=0;

      //query = sqlite3_mprintf ("SELECT count(*) FROM Dupin WHERE ROWID = %q AND pid LIKE '%%\"%q\"%%' ;", max_rowid_str, pid);
      query = sqlite3_mprintf ("SELECT count(*) FROM Dupin WHERE ROWID = %q AND id IN (SELECT id FROM DupinPid2Id WHERE pid = '%q') ;", max_rowid_str, pid);

//g_message("dupin_view_record_delete: view %s query=%s\n",view->name, query);

      if (sqlite3_exec (view->db, query, dupin_view_record_delete_cb, &count, &errmsg) != SQLITE_OK)
        {
          g_mutex_unlock (view->mutex);

          g_error("dupin_view_record_delete: %s for pid %s", errmsg, pid);
          sqlite3_free (errmsg);

          dupin_view_rollback_transaction (view, NULL);
          g_free (max_rowid_str);
          sqlite3_free (query);

   	  g_free (pid);
          view->deletes_queue = g_list_delete_link (view->deletes_queue, list);
	  view->deletes_queue_size--;

          return;
        }

      sqlite3_free (query);

      if (count == 1)
        {
          query = sqlite3_mprintf ("UPDATE DupinView SET last_to_delete_id = '%q'", max_rowid_str);

//g_message("dupin_view_record_delete: view %s query=%s\n",view->name, query);

          if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
            {
              g_mutex_unlock (view->mutex);

              g_error("dupin_view_record_delete: %s for pid %s", errmsg, pid);
              sqlite3_free (errmsg);

              dupin_view_rollback_transaction (view, NULL);
              g_free (max_rowid_str);
              sqlite3_free (query);

   	      g_free (pid);
              view->deletes_queue = g_list_delete_link (view->deletes_queue, list);
	      view->deletes_queue_size--;

              return;
            }

          sqlite3_free (query);
        }

      /* NOTE - clean up pid <-> id table too */

      query = sqlite3_mprintf ("DELETE FROM DupinPid2Id WHERE pid = '%q' ;", pid);

//g_message("dupin_view_record_delete: view %s query=%s\n",view->name, query);

      if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_mutex_unlock (view->mutex);

          g_error("dupin_view_record_delete: %s for pid %s", errmsg, pid);
          sqlite3_free (errmsg);

          dupin_view_rollback_transaction (view, NULL);
          g_free (max_rowid_str);
          sqlite3_free (query);

   	  g_free (pid);
          view->deletes_queue = g_list_delete_link (view->deletes_queue, list);
	  view->deletes_queue_size--;

          return;
        }

      sqlite3_free (query);
    }

  g_free (max_rowid_str);

  /* NOTE - clean up and reset the deletes queue */

  g_list_foreach (view->deletes_queue, (GFunc) g_free, NULL);
  g_list_free (view->deletes_queue);
  view->deletes_queue = NULL;
  view->deletes_queue_size = 0;

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);

      return;
    }

  g_mutex_unlock (view->mutex);

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

#if DEBUG
  fprintf(stderr,"dupin_view_ref: (%p) name=%s \t ref++=%d\n", g_thread_self (), view->name, (gint) view->ref);
#endif

  g_mutex_unlock (d->mutex);
}

void
dupin_view_unref (DupinView * view)
{
  Dupin *d;

  g_return_if_fail (view != NULL);

  d = view->d;
  g_mutex_lock (d->mutex);

  if (view->ref > 0)
    {
      view->ref--;

#if DEBUG
      fprintf(stderr,"dupin_view_ref: (%p) name=%s \t ref--=%d\n", g_thread_self (), view->name, (gint) view->ref);
#endif
    }

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

  if (dupin_view_p_update (view, error) == FALSE)
    {
      dupin_view_disconnect (view);
      return FALSE;
    }

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
dupin_view_get_output (DupinView * view)
{
  g_return_val_if_fail (view != NULL, NULL);

  return view->output;
}

gboolean
dupin_view_get_output_is_db (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  return view->output_is_db;
}

gboolean
dupin_view_get_output_is_linkb (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  return view->output_is_linkb;
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

static int
dupin_view_get_creation_time_cb (void *data, int argc, char **argv, char **col)
{
  gsize *creation_time = data;

  if (argv[0])
    *creation_time = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gboolean
dupin_view_get_creation_time (DupinView * view, gsize * creation_time)
{
  gchar * query;
  gchar * errmsg=NULL;

  *creation_time = 0;

  g_return_val_if_fail (view != NULL, 0);

  /* get creation time out of view */
  query = "SELECT creation_time as creation_time FROM DupinView";
  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_get_creation_time_cb, creation_time, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_error("dupin_view_get_creation_time: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  return TRUE;
}

/* Internal: */
void
dupin_view_disconnect (DupinView * view)
{
#if DEBUG
  g_message("dupin_view_disconnect: total number of changes for '%s' view database: %d\n", view->name, (gint)sqlite3_total_changes (view->db));
#endif

  /* NOTE - empty deletes queue before quitting */

  dupin_view_record_delete (view);

  /* NOTE - make double sure the deletes queue is freed in worse case */

  if (view->deletes_queue != NULL
      || view->deletes_queue_size > 0)
    {
      g_list_foreach (view->deletes_queue, (GFunc) g_free, NULL);
      g_list_free (view->deletes_queue);
      view->deletes_queue = NULL;
      view->deletes_queue_size = 0;
    }

  if (view->db)
    sqlite3_close (view->db);

  if (view->todelete == TRUE)
    g_unlink (view->path);

#if GLIB_CHECK_VERSION (2,31,3)
  g_cond_clear (view->sync_map_has_new_work);
  g_free (view->sync_map_has_new_work);
#else
  g_cond_free(view->sync_map_has_new_work);
#endif

  if (view->name)
    g_free (view->name);

  if (view->path)
    g_free (view->path);

  if (view->parent)
    g_free (view->parent);

  if (view->output)
    g_free (view->output);

  if (view->mutex)
    {
#if GLIB_CHECK_VERSION (2,31,3)
      g_mutex_clear (view->mutex);
      g_free (view->mutex);
#else
      g_mutex_free (view->mutex);
#endif
    }

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

  if (argc == 10)
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

      if (argv[7] != NULL && g_strcmp0(argv[7],"(NULL)") && g_strcmp0(argv[7],"null") )
        {
          view->output = g_strdup (argv[7]);
        }
      view->output_is_db = g_strcmp0 (argv[8], "TRUE") == 0 ? TRUE : FALSE;
      view->output_is_linkb = g_strcmp0 (argv[9], "TRUE") == 0 ? TRUE : FALSE;
    }

  return 0;
}

static int
dupin_view_get_user_version_cb (void *data, int argc, char **argv,
                                    char **col)
{
  gint *user_version = data;

  if (argv[0])
    *user_version = atoi (argv[0]);

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

#if GLIB_CHECK_VERSION (2,31,3)
  view->sync_map_has_new_work = g_new0 (GCond, 1);
  g_cond_init (view->sync_map_has_new_work);
#else
  view->sync_map_has_new_work = g_cond_new();
#endif

  view->d = d;

  view->name = g_strdup (name);
  view->path = g_strdup (path);

  view->collation_parser = json_parser_new ();

  view->deletes_queue = NULL;
  view->deletes_queue_size = 0;

  if (sqlite3_open_v2 (view->path, &view->db, dupin_util_dupin_mode_to_sqlite_mode (mode), NULL) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "View error.");
      dupin_view_disconnect (view);
      return NULL;
    }

  sqlite3_busy_timeout (view->db, DUPIN_SQLITE_TIMEOUT);

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
      if (sqlite3_exec (view->db, "PRAGMA journal_mode = WAL", NULL, NULL, &errmsg) != SQLITE_OK
          || sqlite3_exec (view->db, "PRAGMA encoding = \"UTF-8\"", NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma journal_mode or encoding: %s",
		   errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          return NULL;
        }

      if (dupin_view_begin_transaction (view, error) < 0)
        {
          dupin_view_disconnect (view);
          return NULL;
        }

      if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_MAIN_CREATE, NULL, NULL, &errmsg) != SQLITE_OK
          || sqlite3_exec (view->db, DUPIN_VIEW_SQL_DESC_CREATE, NULL, NULL, &errmsg) != SQLITE_OK
          || sqlite3_exec (view->db, DUPIN_VIEW_SQL_CREATE_INDEX, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          dupin_view_rollback_transaction (view, error);
          return NULL;
        }

      if (dupin_view_commit_transaction (view, error) < 0)
        {
          dupin_view_disconnect (view);
          return NULL;
        }
    }

  /* check view version */
  gint user_version = 0;

  if (sqlite3_exec (view->db, "PRAGMA user_version", dupin_view_get_user_version_cb, &user_version, &errmsg) != SQLITE_OK)
    {
      /* default to 1 if not found or error - TODO check not SQLITE_OK error only */
      user_version = 1;
    }

  if (user_version > DUPIN_SQLITE_MAX_USER_VERSION)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "SQLite view user version (%d) is newer than I know how to work with (%d).",
                        user_version, DUPIN_SQLITE_MAX_USER_VERSION);
      sqlite3_free (errmsg);
      dupin_view_disconnect (view);
      return NULL;
    }

  if (user_version <= 1)
    {
      if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_1, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                   errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          return NULL;
        }
    }

  gchar * cache_size = g_strdup_printf ("PRAGMA cache_size = %d", DUPIN_SQLITE_CACHE_SIZE);
  if (sqlite3_exec (view->db, "PRAGMA temp_store = memory", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (view->db, cache_size, NULL, NULL, &errmsg) != SQLITE_OK) 
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma temp_store: %s",
                   errmsg);
      sqlite3_free (errmsg);
      if (cache_size)
        g_free (cache_size);
      dupin_view_disconnect (view);
      return NULL;
    }

  if (cache_size)
    g_free (cache_size);

  /*
   TODO - check if the below can be optimized using NORMAL or OFF and use separated syncing thread
          see also http://www.sqlite.org/pragma.html#pragma_synchronous
   */

  if (sqlite3_exec (view->db, "PRAGMA synchronous = NORMAL", NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma synchronous: %s",
                   errmsg);
      sqlite3_free (errmsg);
      dupin_view_disconnect (view);
      return NULL;
    }

  /* NOTE - we know this is inefficient, but we need it till proper Elastic search or lucene used as frontend */

  sqlite3_create_function(view->db, "filterBy", 5, SQLITE_ANY, d, dupin_sqlite_json_filterby, NULL, NULL);

  query =
    "SELECT map, map_lang, reduce, reduce_lang, parent, isdb, islinkb, output, output_isdb, output_islinkb FROM DupinView LIMIT 1";

  if (sqlite3_exec (view->db, query, dupin_view_connect_cb, view, &errmsg) !=
      SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      dupin_view_disconnect (view);
    }

#if GLIB_CHECK_VERSION (2,31,3)
  view->mutex = g_new0 (GMutex, 1);
  g_mutex_init (view->mutex);
#else
  view->mutex = g_mutex_new ();
#endif

  return view;
}

/* NOTE - 0 = ok, 1 = already in transaction, -1 = error */

/* NOTE - we do *NOT* use bulk_transaction for views */

gint
dupin_view_begin_transaction (DupinView * view, GError ** error)
{
  g_return_val_if_fail (view != NULL, -1);

  gchar *errmsg;
  gint rc = -1;

  rc = sqlite3_exec (view->db, "BEGIN TRANSACTION", NULL, NULL, &errmsg);

  if (rc == SQLITE_BUSY)
    {
        rc = dupin_sqlite_subs_mgr_busy_handler(view->db, "BEGIN TRANSACTION", NULL, NULL, &errmsg, rc);
    }

  if (rc != SQLITE_OK)
    {
      if (error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot begin view %s transaction: %s", view->name, errmsg);
      
      sqlite3_free (errmsg);

      return -1;
    }

//g_message ("dupin_view_begin_transaction: view %s transaction begin", view->name);

  return 0;
}

gint
dupin_view_rollback_transaction (DupinView * view, GError ** error)
{
  g_return_val_if_fail (view != NULL, -1);

  gchar *errmsg;
  gint rc = -1;

  rc = sqlite3_exec (view->db, "ROLLBACK", NULL, NULL, &errmsg);

  if (rc == SQLITE_BUSY)
    {
        rc = dupin_sqlite_subs_mgr_busy_handler(view->db, "ROLLBACK", NULL, NULL, &errmsg, rc);
    }

  if (rc != SQLITE_OK)
    {
      if (error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot rollback view %s transaction: %s", view->name, errmsg);
      
      sqlite3_free (errmsg);

      return -1;
    }

//g_message ("dupin_view_rollback_transaction: view %s transaction rollback", view->name);

  return 0;
}

gint
dupin_view_commit_transaction (DupinView * view, GError ** error)
{
  g_return_val_if_fail (view != NULL, -1);

  gchar *errmsg;
  gint rc = -1;

  rc = sqlite3_exec (view->db, "COMMIT", NULL, NULL, &errmsg);

  if (rc == SQLITE_BUSY)
    {
        rc = dupin_sqlite_subs_mgr_busy_handler(view->db, "COMMIT", NULL, NULL, &errmsg, rc);
    }

  if (rc != SQLITE_OK)
    {
      if (error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot commit view %s transaction: %s", view->name, errmsg);
      
      sqlite3_free (errmsg);

      return -1;
    }

//g_message ("dupin_view_commit_transaction: view %s transaction commit", view->name);

  return 0;
}

static int
dupin_view_count_cb (void *data, int argc, char **argv, char **col)
{
  gsize *size = data;

  if (argv[0] && *argv[0])
    *size = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

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

/* NOTE - we always bulk insert using the latest revision */

JsonNode *
dupin_view_output_insert_bulk (DupinView * view, JsonNode * bulk_node)
{
  g_return_val_if_fail (view != NULL, NULL);
  g_return_val_if_fail (bulk_node != NULL, NULL);
  g_return_val_if_fail (json_node_get_node_type (bulk_node) == JSON_NODE_OBJECT, NULL);

  GList * response_list=NULL;
  JsonNode * response_node = NULL;

  if (dupin_view_get_output_is_db (view) == TRUE)
    {
      if (json_object_has_member (json_node_get_object (bulk_node), REQUEST_POST_BULK_DOCS_DOCS) == FALSE)
        return NULL;

      DupinDB * db = NULL;
      if (! (db = dupin_database_open (view->d, (gchar *)dupin_view_get_output (view), NULL)))
        return NULL;
                    
      GError * error = NULL;

      if (dupin_record_insert_bulk (db, bulk_node, &response_list, TRUE, &error) == TRUE)
        {
          response_node = json_node_new (JSON_NODE_ARRAY);
          JsonArray * response_array = json_array_new ();
          json_node_take_array (response_node, response_array);
              
          GList * l = NULL;
          for (l=response_list; l; l = l->next)
            {
              JsonNode * r_node = (JsonNode *)l->data;
              json_array_add_element (response_array, json_node_copy (r_node));
            }
        }

      while (response_list)
        {
          json_node_free (response_list->data);
          response_list = g_list_remove (response_list, response_list->data);
        } 

      dupin_database_unref (db);
    }
  else if (dupin_view_get_output_is_linkb (view) == TRUE)
    {
      g_warning("dupin_view_output_insert_bulk: output to linkbase not implemented yet\n");

      return NULL;
    }

  return response_node;
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

		  continue; // TODO - shall we fail instead?
                }

	      if (element_node_serialized != NULL)
	        g_free (element_node_serialized);

	      element_node = json_parser_get_root (parser);

              nobj = json_node_get_object (element_node);
		
	      JsonNode * key_node = json_object_get_member (nobj, DUPIN_VIEW_KEY);
	      JsonNode * node = json_object_get_member (nobj, DUPIN_VIEW_VALUE);

	      /* TODO - do bulk insert/update of 'array' element if view has output */

              JsonNode * response_node = NULL;
	      if (view->reduce == NULL
		  && dupin_view_get_output (view) != NULL)
	        {
	          if (! (response_node = dupin_view_output_insert_bulk (view, node)))
	            {
		      continue; // TODO - shall we fail instead?
		    }
	        }

	      dupin_view_record_save_map (view, data->pid, key_node, (response_node != NULL) ? response_node : node);

	      if (response_node != NULL)
	        json_node_free (response_node);

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
  gchar * query = "SELECT sync_map_id as c FROM DupinView LIMIT 1";
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

  gsize start_rowid = (sync_map_id != NULL) ? (gsize) g_ascii_strtoll (sync_map_id, NULL, 10)+1 : 1;

  if (dupin_record_get_list (db, count, 0, start_rowid, 0, NULL, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE, NULL, DP_FILTERBY_EQUALS,
				NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == FALSE || !results)
    {
      if (sync_map_id != NULL)
        g_free(sync_map_id);
      dupin_database_unref (db);
      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

//g_message("dupin_view_sync_thread_map_db(%p/%s)    g_list_length (results) = %d start_rowid=%d - mapped %d\n", g_thread_self (), view->name, (gint) g_list_length (results), (gint)start_rowid, (gint)view->sync_map_processed_count);

  for (list = results; list; list = list->next)
    {
      /* NOTE - we do *not* count deleted records are processed */

      struct dupin_view_sync_t *data =
	g_malloc0 (sizeof (struct dupin_view_sync_t));

      JsonNode * obj_node = dupin_record_get_revision_node (list->data, NULL);

      if (obj_node)
        {
          data->obj = json_node_copy (obj_node);

          /* Setting _id, _rev and _created fields - we do not store them into serialized object */
          JsonObject * obj = json_node_get_object (data->obj);
          json_object_set_string_member (obj, "_id", (gchar *) dupin_record_get_id (list->data));
          json_object_set_string_member (obj, "_rev", dupin_record_get_last_revision (list->data));

	  if (json_object_has_member (obj, "_created") == TRUE)
            json_object_remove_member (obj, "_created"); // ignore any record one if set by user, ever
          gchar * created = dupin_date_timestamp_to_iso8601 (dupin_record_get_created (list->data));
          json_object_set_string_member (obj, "_created", created);
          g_free (created);

	  if (json_object_has_member (obj, "_type") == TRUE)
            json_object_remove_member (obj, "_type"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_type", (gchar *)dupin_record_get_type (list->data));

	  /* NOTE - needed for m/r dupin.docpath() and dupin.links() methods - see dupin_js.c */

          if (json_object_has_member (obj, "_linkbase") == TRUE)
            json_object_remove_member (obj, "_linkbase"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_linkbase", dupin_database_get_default_linkbase_name (db));
        }

      data->pid = json_node_new (JSON_NODE_ARRAY);
      JsonArray *pid_array=json_array_new ();

      rowid = dupin_record_get_rowid (list->data);

      if (sync_map_id != NULL)
        g_free(sync_map_id);
        
      sync_map_id = g_strdup_printf ("%i", (gint)rowid);

//g_message("dupin_view_sync_thread_map_db(%p/%s) sync_map_id=%s as fetched",g_thread_self (), view->name, sync_map_id);

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

//g_message("dupin_view_sync_thread_map_db() view %s sync_map_id=%s as to be stored",view->name, sync_map_id);

//g_message("dupin_view_sync_thread_map_db(%p/%s)  finished last_map_rowid=%s - mapped %d\n", g_thread_self (), view->name, sync_map_id, (gint)view->sync_map_processed_count);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

  if (sync_map_id != NULL)
    g_free (sync_map_id);

  g_mutex_lock (view->mutex);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_database_unref (db);

      return FALSE;
    }

  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_database_unref (db);

      g_error("dupin_view_sync_thread_map_db: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);

      return FALSE;
    }

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_database_unref (db);

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
  gchar * query = "SELECT sync_map_id as c FROM DupinView LIMIT 1";
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

  gsize start_rowid = (sync_map_id != NULL) ? (gsize) g_ascii_strtoll (sync_map_id, NULL, 10)+1 : 1;

  if (dupin_link_record_get_list (linkb, count, 0, start_rowid, 0, DP_LINK_TYPE_ANY, NULL, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE,
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

//g_message("dupin_view_sync_thread_map_linkb(%p/%s)    g_list_length (results) = %d start_rowid=%d - mapped %d\n", g_thread_self (), view->name, (gint) g_list_length (results), (gint)start_rowid, (gint)view->sync_map_processed_count);

  for (list = results; list; list = list->next)
    {
      /* NOTE - we do *not* count deleted records are processed */

      struct dupin_view_sync_t *data =
	g_malloc0 (sizeof (struct dupin_view_sync_t));

      JsonNode * obj_node = dupin_link_record_get_revision_node (list->data, NULL);

      if (obj_node)
        {
          data->obj = json_node_copy (obj_node);

          /* Setting _id, _rev and _created fields - we do not store them into serialized object */
          JsonObject * obj = json_node_get_object (data->obj);
          json_object_set_string_member (obj, "_id", (gchar *) dupin_link_record_get_id (list->data));
          json_object_set_string_member (obj, "_rev", dupin_link_record_get_last_revision (list->data));

          if (json_object_has_member (obj, "_created") == TRUE)
            json_object_remove_member (obj, "_created"); // ignore any record one if set by user, ever
	  gchar * created = dupin_date_timestamp_to_iso8601 (dupin_link_record_get_created (list->data));
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
        }

      data->pid = json_node_new (JSON_NODE_ARRAY);
      JsonArray *pid_array=json_array_new ();

      rowid = dupin_link_record_get_rowid (list->data);

      if (sync_map_id != NULL)
        g_free(sync_map_id);
        
      sync_map_id = g_strdup_printf ("%i", (gint)rowid);

//g_message("dupin_view_sync_thread_map_linkb(%p/%s) sync_map_id=%s as fetched", g_thread_self (), view->name, sync_map_id);

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

//g_message("dupin_view_sync_thread_map_linkb() view %s sync_map_id=%s as to be stored",view->name, sync_map_id);

//g_message("dupin_view_sync_thread_map_linkb(%p/%s)  finished last_map_rowid=%s - mapped %d\n", g_thread_self (), view->name, sync_map_id, (gint)view->sync_map_processed_count);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

  if (sync_map_id != NULL)
    g_free (sync_map_id);

  g_mutex_lock (view->mutex);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_linkbase_unref (linkb);

      return FALSE;
    }

  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_linkbase_unref (linkb);

      g_error("dupin_view_sync_thread_map_linkb: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);

      return FALSE;
    }

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_linkbase_unref (linkb);

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
  gchar * query = "SELECT sync_map_id as c FROM DupinView LIMIT 1";
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

  gsize start_rowid = (sync_map_id != NULL) ? (gsize) g_ascii_strtoll (sync_map_id, NULL, 10)+1 : 1;

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

//g_message("dupin_view_sync_thread_map_view(%p/%s)    g_list_length (results) = %d\n", g_thread_self (), view->name, (gint) g_list_length (results) );

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

//g_message("dupin_view_sync_thread_map_view() view %s sync_map_id=%s as fetched",view->name, sync_map_id);

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

//g_message("dupin_view_sync_thread_map_view() view %s sync_map_id=%s as to be stored",view->name, sync_map_id);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

  if (sync_map_id != NULL)
    g_free (sync_map_id);

//g_message("dupin_view_sync_thread_map_view() view %s query=%s\n",view->name, str);

  g_mutex_lock (view->mutex);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_view_unref (v);

      return FALSE;
    }

  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_view_unref (v);

      g_error("dupin_view_sync_thread_map_view: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);

      return FALSE;
    }

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      dupin_view_unref (v);

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

static int
dupin_view_sync_record_update_cb (void *data, int argc, char **argv, char **col)
{
  gchar **id = data;

  if (argv[0] && *argv[0])
    *id = g_strdup (argv[0]);

  return 0;
}

void
dupin_view_sync_record_update (DupinView * view, gchar * previous_rowid, gint replace_rowid,
			       gchar * key, gchar * value, JsonNode * pid_node)
{
  g_return_if_fail (pid_node != NULL);
  g_return_if_fail (json_node_get_node_type (pid_node) == JSON_NODE_ARRAY);

  gchar *query, *errmsg;
  gchar *replace_rowid_str=NULL;
  gchar *pid_serialized=NULL;
  gchar *id=NULL;

  if (pid_node != NULL)
    {
      pid_serialized = dupin_util_json_serialize (pid_node);

      if (pid_serialized == NULL)
        {
          g_error("dupin_view_sync_record_update: could not serialize pid node");

          return;
        }
    }

  replace_rowid_str = g_strdup_printf ("%d", (gint)replace_rowid);

  g_mutex_lock (view->mutex);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      g_free (replace_rowid_str);
      if (pid_serialized)
        g_free (pid_serialized);

      return;
    }

  /* NOTE - cleanup PID <-> ID mappings first */

  query = sqlite3_mprintf ("DELETE FROM DupinPid2Id WHERE id IN (SELECT id FROM Dupin WHERE key='%q' AND ROWID > %q AND ROWID < %q) ;",
				key,
				(previous_rowid != NULL) ? previous_rowid : "0",
				replace_rowid_str);

//g_message("dupin_view_sync_record_update() view %s delete query=%s\n",view->name, query);

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (query);
      g_free (replace_rowid_str);
      if (pid_serialized)
        g_free (pid_serialized);

      g_error("dupin_view_sync_record_update: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);

      return;
    }

  sqlite3_free (query);

  /* TODO - escape keys due we do not catch erros below !!!!! */

  /* NOTE - we never delete the last record of the SQLite database to avoid ROWID recycling */

  query = sqlite3_mprintf ("DELETE FROM Dupin WHERE key='%q' AND ROWID > %q AND ROWID < %q ;",
				key,
				(previous_rowid != NULL) ? previous_rowid : "0",
				replace_rowid_str);

//g_message("dupin_view_sync_record_update() view %s delete query=%s\n",view->name, query);

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (query);
      g_free (replace_rowid_str);
      if (pid_serialized)
        g_free (pid_serialized);

      g_error("dupin_view_sync_record_update: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);

      return;
    }

  sqlite3_free (query);

  query = sqlite3_mprintf ("UPDATE Dupin SET key='%q', pid='%q', obj='%q' WHERE rowid=%q ;",
				key,
				pid_serialized,
				value,
				replace_rowid_str);

//g_message("dupin_view_sync_record_update() view %s update query=%s\n",view->name, query);

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (query);
      g_free (replace_rowid_str);
      if (pid_serialized)
        g_free (pid_serialized);

      g_error("dupin_view_sync_record_update: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);

      return;
    }

  sqlite3_free (query);

  query = sqlite3_mprintf ("SELECT id FROM Dupin WHERE rowid=%q ;", replace_rowid_str);

//g_message("dupin_view_sync_record_update() view %s select query=%s\n",view->name, query);

  if (sqlite3_exec (view->db, query, dupin_view_sync_record_update_cb, &id, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (query);
      g_free (replace_rowid_str);
      if (pid_serialized)
        g_free (pid_serialized);

      g_error("dupin_view_sync_record_update: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);

      return;
    }

  sqlite3_free (query);

  g_free (replace_rowid_str);

  /* NOTE - store new / updated PIDs -> ID mappings
            foreach PIDs delete any old mappings and add one row (pid,id) so we can use select ... in (select ...) subquery on deletion or other */

  if (pid_node != NULL)
    {
      GList *nodes, *n;
      nodes = json_array_get_elements (json_node_get_array (pid_node));
      for (n = nodes; n != NULL; n = n->next)
        {
          JsonNode * p = (JsonNode *)n->data;

          if (json_node_get_node_type (p) != JSON_NODE_VALUE
              || json_node_get_value_type (p) != G_TYPE_STRING)
            {
              g_error("dupin_view_sync_record_update: %s non string array element found in pid", pid_serialized);

              continue;
            }

          gchar * pid_string = (gchar *)json_node_get_string (p);

          /* NOTE - fetch an previous entry, if matched parse array of IDs */

          query = sqlite3_mprintf (DUPIN_VIEW_SQL_INSERT_PID2ID, pid_string, id);

          if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
            {
              g_mutex_unlock (view->mutex);

              g_error("dupin_view_sync_record_update: %s", errmsg);
              sqlite3_free (errmsg);

	      g_free (id);
              if (pid_serialized)
                g_free (pid_serialized);

              sqlite3_free (query);
              dupin_view_rollback_transaction (view, NULL);

              g_list_free (nodes);

              return;
            }

          sqlite3_free (query);
        }
      g_list_free (nodes);
    }

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);

      g_free (id);
      if (pid_serialized)
        g_free (pid_serialized);

      return;
    }

  g_mutex_unlock (view->mutex);

  g_free (id);
  if (pid_serialized)
    g_free (pid_serialized);
}

static gboolean
dupin_view_sync_thread_reduce (DupinView * view, gsize count, gboolean rereduce, gchar * matching_key)
{
  if (view->reduce == NULL)
    return FALSE;

//g_message("dupin_view_sync_thread_reduce(%p/%s) count=%d\n",g_thread_self (), view->name, (gint)count);

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
  query = "SELECT sync_reduce_id as c FROM DupinView LIMIT 1";
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

  gsize start_rowid = (sync_reduce_id != NULL) ? (gsize) g_ascii_strtoll (sync_reduce_id, NULL, 10)+1 : 1;

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

//g_message("dupin_view_sync_thread_reduce(%p/%s)    g_list_length (results) = %d start_rowid=%d - reduced %d of total to reduce=%d\n", g_thread_self (), view->name, (gint) g_list_length (results), (gint)start_rowid, (gint)view->sync_reduce_processed_count, (gint)view->sync_reduce_total_records );

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

//g_message("dupin_view_sync_thread_reduce(%p/%s) sync_reduce_id=%s\n",g_thread_self (), view->name, sync_reduce_id);

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

//g_message("view %s key_string =%s\n",view->name, key_string);

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
          JsonNode * pid_node = json_node_copy (json_object_get_member ( json_node_get_object(json_object_get_member (reduce_parameters_obj, member_name)), "pids"));
          if (pid_node == NULL)
            {
	      json_node_free (result);
	      ret = FALSE;
              break;
            }

	  /* NOTE - do bulk insert/update of 'result' if view has output */

	  JsonNode * response_node = NULL;
          if (dupin_view_get_output (view) != NULL)
            {
              if (! (response_node = dupin_view_output_insert_bulk (view, result)))
                {
	          json_node_free (result);
	          json_node_free (pid_node);
	          ret = FALSE;
                  break;
                }
            }

          /* NOTE - if bulk inserted, store the output IDs tree of docs, links and relationships created */

          gchar * value_string = dupin_util_json_serialize ((response_node != NULL) ? response_node : result);
          if (value_string == NULL)
            {
	      json_node_free (result);
              if (response_node != NULL)
                json_node_free (response_node);
	      json_node_free (pid_node);
	      ret = FALSE;
              break;
            }

//g_message ("dupin_view_sync_thread_reduce: view %s KEY: %s", view->name, member_name);
//DUPIN_UTIL_DUMP_JSON ("dupin_view_sync_thread_reduce: PID", pid_node);
//DUPIN_UTIL_DUMP_JSON ("dupin_view_sync_thread_reduce: OBJ", result);

          if (response_node != NULL)
            json_node_free (response_node);

	  json_node_free (result);

          /* TODO - check if we need double to gurantee larger number of rowids and use json_node_get_number () in the below */

	  /* NOTE - delete all rows but last one and replace last one with result where last one is rowid */
          dupin_view_sync_record_update (view,
				         previous_sync_reduce_id,
				         (gint)json_node_get_int (json_object_get_member ( json_node_get_object(json_object_get_member (reduce_parameters_obj, member_name)), "rowid")),
                                         member_name,
                                         value_string,
				         pid_node);

          g_free (value_string);
          json_node_free (pid_node);

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

//g_message("dupin_view_sync_thread_reduce(%p/%s) finished last_reduce_rowid=%s - reduced %d of total to reduce=%d\n", g_thread_self (), view->name, sync_reduce_id, (gint)view->sync_reduce_processed_count, (gint)view->sync_reduce_total_records);

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_reduce_id = '%q'", sync_reduce_id); /* is the ROWID we stopped */

  if (sync_reduce_id != NULL)
    g_free(sync_reduce_id);

//g_message("dupin_view_sync_thread_reduce() view %s query=%s\n",view->name, str);

  g_mutex_lock (view->mutex);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      return FALSE;
    }

  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

      g_error("dupin_view_sync_thread_reduce: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);

      return FALSE;
    }

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_mutex_unlock (view->mutex);
      sqlite3_free (str);

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
    rere->total += (gsize) g_ascii_strtoll (argv[1], NULL, 10);

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

//g_message("dupin_view_sync_map_func(%p/%s) started\n",g_thread_self (), view->name);

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
//g_message("dupin_view_sync_map_func(%p/%s) Mapped %d records - sending signal to reduce thread (%p)\n", g_thread_self (), view->name, (gint)view->sync_map_processed_count, view->sync_reduce_thread);

          g_mutex_lock (view->mutex);
	  g_cond_signal(view->sync_map_has_new_work);
          g_mutex_unlock (view->mutex);
        }

      if (map_operation == FALSE)
        {
//g_message("dupin_view_sync_map_func(%p/%s) Mapped TOTAL %d records\n", g_thread_self (), view->name, (gint)view->sync_map_processed_count);

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

//g_message("dupin_view_sync_map_func(%p/%s) finished and view map part is in sync\n",g_thread_self (), view->name);

  /* NOTE - make sure reduce thread can terminate too eventually */
  if (view->reduce != NULL)
    {
//g_message("dupin_view_sync_map_func(%p/%s) Sending signal to reduce thread (%p)\n", g_thread_self (), view->name, view->sync_reduce_thread);

      g_mutex_lock (view->mutex);
      g_cond_signal(view->sync_map_has_new_work);
      g_mutex_unlock (view->mutex);
    }
  else if (view->sync_map_processed_count > 0)
    {
//g_message("dupin_view_sync_map_func(%p/%s): ANALYZE\n", g_thread_self (), view->name);

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

  GTimeVal endtime = {0,0};
#if GLIB_CHECK_VERSION (2,31,3)
  gint64 endtime_usec;
#endif
  gboolean reduce_wait_timed_out;

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

//g_message("dupin_view_sync_reduce_func(%p/%s) started", g_thread_self (), view->name);

  /* NOTE - if map hangs, reduce also hangs - for the moment we should make sure a _rest method is allowed on views to avoid disasters */

  /* TODO - added processing step when restarted and sync_reduce_id is set to the ID of view table latest record processed, and continue
            and when done, wait for another bunch ... */

  query = "SELECT sync_rereduce as c FROM DupinView LIMIT 1";
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
//g_message("view %s rereduce=%d\n", view->name, rereduce);

      if (rereduce == FALSE
	  && view->sync_map_thread)
        {
//g_message("dupin_view_sync_reduce_func(%p/%s) Going to wait for signal from map thread (%p)\n", g_thread_self (), view->name, view->sync_map_thread);

	  /* NOTE - wait for message for a maximum of limit_reduce_timeoutforthread seconds */

	  /* TODO - check if FreeBSD, Apple and Win32 platforms need special care
		    see for example http://skype4pidgin.googlecode.com/svn/trunk/skype_messaging.c */

          g_mutex_lock (view->mutex);

	  g_get_current_time(&endtime);
	  g_time_val_add(&endtime, view->d->conf->limit_reduce_timeoutforthread * G_USEC_PER_SEC);

#if GLIB_CHECK_VERSION (2,31,3)
          /* see https://gitorious.org/ghelp/glib/blobs/99f0eaa4c5a86f6fa721044bb6841f6bda4c689b/glib/deprecated/gthread-deprecated.c */

          endtime_usec = endtime.tv_sec;
          endtime_usec *= G_USEC_PER_SEC;
          endtime_usec += endtime.tv_usec;

#ifdef CLOCK_MONOTONIC
          /* would be nice if we had clock_rtoffset, but that didn't seem to make it into the kernel yet... */
          endtime_usec += g_get_monotonic_time () - g_get_real_time ();
#else
          /* if CLOCK_MONOTONIC is not defined then g_get_montonic_time() and g_get_real_time() are returning the same clock, so don't bother... */
#endif
          reduce_wait_timed_out = g_cond_wait_until (view->sync_map_has_new_work, view->mutex, endtime_usec);
#else
          reduce_wait_timed_out = g_cond_timed_wait (view->sync_map_has_new_work, view->mutex, &endtime);
#endif

	  if (reduce_wait_timed_out == FALSE)
	    {
//g_message("dupin_view_sync_reduce_func(%p/%s) waiting for signal from %p timeout after %d seconds\n", g_thread_self (), view->name, view->sync_map_thread, view->d->conf->limit_reduce_timeoutforthread);
	    }

//g_message("dupin_view_sync_reduce_func(%p/%s) CONTINUE after wait\n", g_thread_self (), view->name);

          g_mutex_unlock (view->mutex);
        }

      if (view->sync_map_processed_count > view->sync_reduce_total_records /* got a new bunch to work on */
	  || rereduce)
        {
          g_mutex_lock (view->mutex);
          view->sync_reduce_total_records = (rereduce) ? rere_matching.total : view->sync_map_processed_count;
          g_mutex_unlock (view->mutex);

//g_message("dupin_view_sync_reduce_func(%p/%s) got %d records to REDUCE (rereduce=%d)\n",g_thread_self (), view->name, (gint)view->sync_reduce_total_records,(gint)rereduce);

          while (dupin_view_sync_thread_reduce (view, VIEW_SYNC_COUNT, rereduce, rere_matching.first_matching_key) == TRUE);

//g_message("dupin_view_sync_reduce_func(%p/%s) Reduced %d records of %d\n", g_thread_self (), view->name, (gint)view->sync_reduce_processed_count, (gint)view->sync_reduce_total_records);
        }

      if (!view->sync_map_thread) /* map finished */
        {
//g_message("View %s map was finished in meantime\n", view->name);

	  /* check if there is anything to re-reduce */
          rere_matching.total = 0;
          if (rere_matching.first_matching_key != NULL)
            g_free (rere_matching.first_matching_key);
          rere_matching.first_matching_key = NULL;
          dupin_view_sync_total_rereduce (view, &rere_matching);

//g_message("View %s done first round of reduce but there are still %d record to re-reduce and first key to process is '%s'\n", view->name, (gint)rere_matching.total, rere_matching.first_matching_key);

          if (rere_matching.total > 0)
            {
              /* still work to do */
              rereduce = TRUE;

//g_message("View %s going to re-reduce\n", view->name);

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

		  gsize max_rowid;

                  g_mutex_lock (view->mutex);

		  if (dupin_view_begin_transaction (view, NULL) < 0)
		    {
		      g_mutex_unlock (view->mutex);

      		      break;
		    }

  		  if (dupin_view_record_get_max_rowid (view, &max_rowid, FALSE) == FALSE)
                    {
                      g_mutex_unlock (view->mutex);

                      dupin_view_rollback_transaction (view, NULL);

      		      break;
    		    }

  		  gchar * max_rowid_str = g_strdup_printf ("%d", (gint)max_rowid);

		  /* NOTE - delete records giving problems */

		  /* NOTE - cleanup PID <-> ID mappings first */

                  gchar * str = sqlite3_mprintf ("DELETE FROM DupinPid2Id WHERE id IN (SELECT id FROM Dupin WHERE ROWID < %q AND key='%q') ;", max_rowid_str, rere_matching.first_matching_key);

                  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
                    {
                      g_mutex_unlock (view->mutex);

                      g_error("dupin_view_sync_reduce_func: %s", errmsg);
                      sqlite3_free (errmsg);

                      dupin_view_rollback_transaction (view, NULL);
                      g_free (max_rowid_str);
                      sqlite3_free (str);

                      break;
                    }

                  sqlite3_free (str);

  		  /* NOTE - we never delete the last record of the SQLite database to avoid ROWID recycling */

                  str = sqlite3_mprintf ("DELETE FROM Dupin WHERE ROWID < %q AND key='%q' ;", max_rowid_str, rere_matching.first_matching_key);

                  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
                    {
                      g_mutex_unlock (view->mutex);

                      g_error("dupin_view_sync_reduce_func: %s", errmsg);
                      sqlite3_free (errmsg);

                      dupin_view_rollback_transaction (view, NULL);
                      g_free (max_rowid_str);
                      sqlite3_free (str);

                      break;
                    }

                  sqlite3_free (str);

  		  /* NOTE - check if last one needs to be deleted after next / future insert */
  		  gsize count=0;

  		  str = sqlite3_mprintf ("SELECT count(*) FROM Dupin WHERE ROWID = %q AND key='%q' ;", max_rowid_str, rere_matching.first_matching_key);

  		  if (sqlite3_exec (view->db, str, dupin_view_record_delete_cb, &count, &errmsg) != SQLITE_OK)
    		    {
                      g_mutex_unlock (view->mutex);

                      g_error("dupin_view_sync_reduce_func: %s", errmsg);
                      sqlite3_free (errmsg);

                      dupin_view_rollback_transaction (view, NULL);
                      g_free (max_rowid_str);
                      sqlite3_free (str);

                      break;
    		    }

  		  sqlite3_free (str);

  		  if (count == 1)
    		    {
		      str = sqlite3_mprintf ("UPDATE DupinView SET last_to_delete_id = '%q'", max_rowid_str);

      		      if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
		        {
                          g_mutex_unlock (view->mutex);

                          g_error("dupin_view_sync_reduce_func: %s", errmsg);
                          sqlite3_free (errmsg);

                          dupin_view_rollback_transaction (view, NULL);
                          g_free (max_rowid_str);
                          sqlite3_free (str);

                          break;
        		}

      		      sqlite3_free (str);
		    }

  		  g_free (max_rowid_str);

		  if (dupin_view_commit_transaction (view, NULL) < 0)
		    {
		      g_mutex_unlock (view->mutex);

      		      break;
		    }

                  g_mutex_unlock (view->mutex);

		  /* flag to carry on (for any future request to sync) */

                  query = "UPDATE DupinView SET sync_rereduce = 'TRUE'";

		  continue;
                }

              g_mutex_lock (view->mutex);

	      if (dupin_view_begin_transaction (view, NULL) < 0)
                {
                  g_mutex_unlock (view->mutex);

                  break;
                }

              if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
                {
                  g_mutex_unlock (view->mutex);

                  g_error("dupin_view_sync_reduce_func: %s", errmsg);
                  sqlite3_free (errmsg);

                  dupin_view_rollback_transaction (view, NULL);

                  break;
                }

	      if (dupin_view_commit_transaction (view, NULL) < 0)
                {
                  g_mutex_unlock (view->mutex);

                  break;
                }

              g_mutex_unlock (view->mutex);

              if (rereduce_previous_matching_key != NULL)
                g_free (rereduce_previous_matching_key);

              rereduce_previous_matching_key = g_strdup (rere_matching.first_matching_key);
            }
          else
            {
//g_message("View %s done rereduce=%d\n", view->name, (gint)rereduce);
              rereduce = FALSE;

              query = "UPDATE DupinView SET sync_rereduce = 'FALSE'";

              g_mutex_lock (view->mutex);

	      if (dupin_view_begin_transaction (view, NULL) < 0)
                {
                  g_mutex_unlock (view->mutex);

                  break;
                }

              if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
                {
                  g_mutex_unlock (view->mutex);

                  g_error("dupin_view_sync_reduce_func: %s", errmsg);
                  sqlite3_free (errmsg);

                  dupin_view_rollback_transaction (view, NULL);

                  break;
                }

	      if (dupin_view_commit_transaction (view, NULL) < 0)
                {
                  g_mutex_unlock (view->mutex);

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

//g_message("dupin_view_sync_reduce_func(%p/%s) finished to reduce %d total records and view reduce part is in sync\n",g_thread_self (), view->name, (gint)view->sync_reduce_total_records);


  /* claim disk space back due reduce did actually remove rows from view table */

//g_message("dupin_view_sync_reduce_func: view %s VACUUM and ANALYZE\n", view->name);

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
  view->tosync = FALSE;
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

//g_message("dupin_view_sync(%p/%s): view is still syncing view->sync_map_thread=%p view->sync_reduce_thread=%p \n", g_thread_self (), view->name, view->sync_map_thread, view->sync_reduce_thread);
    }
  else
    {
      /* TODO - have a master sync thread which manage the all three rather than have chain of
            dependency between map, reduce and re-reduce threads */

//g_message("dupin_view_sync(%p/%s): push to thread pools view->sync_map_thread=%p view->sync_reduce_thread=%p \n", g_thread_self (), view->name, view->sync_map_thread, view->sync_reduce_thread);

      GError *error=NULL;

      if (!view->sync_map_thread)
        {
	  g_thread_pool_push(view->d->sync_map_workers_pool, view, &error);

	  if (error)
            {
              g_error("dupin_view_sync: view %s map thread creation error: %s", view->name, error->message);
	      dupin_view_set_error (view, error->message);
              g_error_free (error);
	    }
        }

      if (view->reduce != NULL
          && !view->sync_reduce_thread)
        {
	  g_thread_pool_push(view->d->sync_reduce_workers_pool, view, &error);

	  if (error)
            {
              g_error("dupin_view_sync: view %s reduce thread creation error: %s", view->name, error->message);
	      dupin_view_set_error (view, error->message);
              g_error_free (error);
	    }
        }
    }
}

gboolean
dupin_view_is_syncing (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  gboolean ret = FALSE;

  g_mutex_lock (view->mutex);

  if (view->sync_map_thread
      || view->sync_reduce_thread)
    ret = TRUE;

  g_mutex_unlock (view->mutex);

  return ret;
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

/* View compaction - basically just SQLite VACUUM and ANALYSE for the moment */

void
dupin_view_compact_func (gpointer data, gpointer user_data)
{
  DupinView * view = (DupinView*) data;
  gchar * errmsg;

  if (view->todelete == TRUE)
    {
g_message("dupin_view_compact_func(%p) view scheduled for deletion, skipping compaction\n",g_thread_self ());

      return;
    }

  dupin_view_ref (view);

  /* claim disk space back */

//g_message("dupin_view_compact_func(%p) started\n",g_thread_self ());

  g_mutex_lock (view->mutex);

  view->compact_thread = g_thread_self ();
  view->compact_processed_count = 0;

  /* NOTE - make sure last transaction is commited */

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      dupin_view_rollback_transaction (view, NULL);
    }

  /*
    IMPORTANT: rowids may change after a VACUUM, so the cursor of views is reset as well
               see http://www.sqlite.org/lang_vacuum.html
   */

//g_message("dupin_view_compact_func: VACUUM and ANALYZE\n");

  if (sqlite3_exec (view->db, "VACUUM", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (view->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (view->db, "ANALYZE DupinPid2Id", NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_error ("dupin_view_compact_func: %s while vacuum and analyze view", errmsg);
      sqlite3_free (errmsg);
    }
  else
    {
//g_message("dupin_view_compact_func(%p) finished and view is compacted\n",g_thread_self ());
    }

  view->tocompact = FALSE;
  view->compact_thread = NULL;

  g_mutex_unlock (view->mutex);

  dupin_view_unref (view);
}

void
dupin_view_compact (DupinView * view)
{
  g_return_if_fail (view != NULL);

  if (dupin_view_is_compacting (view))
    {
      g_mutex_lock (view->mutex);
      view->tocompact = TRUE;
      g_mutex_unlock (view->mutex);

//g_message("dupin_view_compact(%p): view is still compacting view->compact_thread=%p\n", g_thread_self (), view->compact_thread);
    }
  else
    {
//g_message("dupin_view_compact(%p): push to thread pools view->compact_thread=%p\n", g_thread_self (), view->compact_thread);

      GError * error=NULL;

      if (!view->compact_thread)
        {
          g_thread_pool_push(view->d->view_compact_workers_pool, view, &error);

          if (error)
            {
              g_error("dupin_view_compact: view %s compact thread creation error: %s", view->name, error->message);
              dupin_view_set_error (view, error->message);
              g_error_free (error);
            }
        }
    }
}

gboolean
dupin_view_is_compacting (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (view->compact_thread)
    return TRUE;

  return FALSE;
}

gboolean
dupin_view_is_compacted (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (dupin_view_is_compacting (view))
    return FALSE;

  return view->tocompact ? FALSE : TRUE;
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
