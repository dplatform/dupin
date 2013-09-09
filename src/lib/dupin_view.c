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
  "  seq         INTEGER PRIMARY KEY AUTOINCREMENT,\n" \
  "  id          CHAR(255) NOT NULL,\n" \
  "  pid         TEXT NOT NULL,\n" \
  "  key         TEXT NOT NULL COLLATE dupincmp,\n" \
  "  obj         TEXT COLLATE dupincmp,\n" \
  "  tm          INTEGER NOT NULL,\n" \
  "  UNIQUE      (id)\n" \
  ");\n" \
  "CREATE TABLE IF NOT EXISTS DupinPid2Id (\n" \
  "  pid         CHAR(255) NOT NULL PRIMARY KEY,\n" \
  "  id          TEXT NOT NULL\n" \
  ");"

  /*"CREATE INDEX IF NOT EXISTS DupinId ON Dupin (id);\n" \ - created by default see http://web.utk.edu/~jplyon/sqlite/SQLite_optimization_FAQ.html#indexes */

#define DUPIN_VIEW_SQL_CREATE_INDEX \
  "CREATE INDEX IF NOT EXISTS DupinPid ON Dupin (pid);\n" \
  "CREATE INDEX IF NOT EXISTS DupinKey ON Dupin (key);\n" \
  "CREATE INDEX IF NOT EXISTS DupinObj ON Dupin (obj);\n" \
  "CREATE INDEX IF NOT EXISTS DupinKeyObj ON Dupin (key, obj);\n" \
  "CREATE INDEX IF NOT EXISTS DupinPid2IdId ON DupinPid2Id (id);"

#define DUPIN_VIEW_SQL_DESC_CREATE \
  "CREATE TABLE IF NOT EXISTS DupinView (\n" \
  "  parent                    CHAR(255) NOT NULL,\n" \
  "  isdb                      BOOL DEFAULT TRUE,\n" \
  "  islinkb                   BOOL DEFAULT FALSE,\n" \
  "  language                  CHAR(255) NOT NULL,\n" \
  "  map                       TEXT,\n" \
  "  reduce                    TEXT,\n" \
  "  sync_map_id               CHAR(255),\n" \
  "  sync_reduce_id            CHAR(255),\n" \
  "  sync_rereduce             BOOL DEFAULT FALSE,\n" \
  "  output                    CHAR(255),\n" \
  "  output_isdb               BOOL DEFAULT TRUE,\n" \
  "  output_islinkb            BOOL DEFAULT FALSE,\n" \
  "  creation_time   	       CHAR(255) NOT NULL DEFAULT '0'\n" \
  ");\n" \
  "PRAGMA user_version = 7"

#define DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_1 \
  "ALTER TABLE Dupin     ADD COLUMN tm INTEGER NOT NULL DEFAULT 0;\n" \
  "ALTER TABLE DupinView ADD COLUMN creation_time CHAR(255) NOT NULL DEFAULT '0';\n" \
  "ALTER TABLE Dupin     ADD COLUMN language CHAR(255) NOT NULL DEFAULT 'javascript';\n" \
  "PRAGMA user_version = 7"

#define DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_2 \
  "ALTER TABLE Dupin ADD COLUMN tm INTEGER NOT NULL DEFAULT 0;\n" \
  "ALTER TABLE Dupin ADD COLUMN language CHAR(255) NOT NULL DEFAULT 'javascript';\n" \
  "PRAGMA user_version = 7"

#define DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_3 \
  "ALTER TABLE Dupin ADD COLUMN language CHAR(255) NOT NULL DEFAULT 'javascript';\n" \
  "PRAGMA user_version = 7"

/* NOTE - added seq INTEGER PRIMARY KEY AUTOINCREMENT and UNIQUE (id) */

#define DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_4 \
  "PRAGMA user_version = 7"

/* NOTE - dropped last_to_delete_id on DupinView */

#define DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_5 \
  "PRAGMA user_version = 7"

/* NOTE - set pid as PRIMARY KEY in DupinPid2Id and dropped index DupinPid2IdPid */
#define DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_6 \
  "PRAGMA user_version = 7"

#define DUPIN_VIEW_SQL_USES_OLD_ROWID \
        "SELECT seq FROM Dupin"

#define DUPIN_VIEW_SQL_INSERT \
	"INSERT OR REPLACE INTO Dupin (id, pid, key, obj, tm) " \
        "VALUES('%q', '%q', '%q', '%q', '%" G_GSIZE_FORMAT "')"

#define DUPIN_VIEW_SQL_INSERT_PID2ID \
	"INSERT OR REPLACE INTO DupinPid2Id (pid, id) " \
        "VALUES('%q', '%q')"

#define DUPIN_VIEW_SQL_TOTAL_REREDUCE \
	"SELECT key AS inner_key, count(*) AS inner_count FROM Dupin GROUP BY inner_key HAVING inner_count > 1 LIMIT 1"

#define DUPIN_VIEW_SQL_COUNT \
	"SELECT count(id) as c FROM Dupin"

#define DUPIN_VIEW_SQL_GET_RECORD \
        "SELECT parent, isdb, islinkb, language, map, reduce, output, output_isdb, output_islinkb FROM DupinView LIMIT 1"

#define VIEW_SYNC_COUNT	100

static gchar *dupin_view_generate_id (DupinView * view, GError ** error, gboolean lock);

gchar **
dupin_get_views (Dupin * d)
{
  guint i;
  gsize size;
  gchar **ret;
  gpointer key;
  GHashTableIter iter;

  g_return_val_if_fail (d != NULL, NULL);

  g_rw_lock_reader_lock (d->rwlock);

  if (!(size = g_hash_table_size (d->views)))
    {
      g_rw_lock_reader_unlock (d->rwlock);
      return NULL;
    }

  ret = g_malloc (sizeof (gchar *) * (g_hash_table_size (d->views) + 1));

  i = 0;
  g_hash_table_iter_init (&iter, d->views);
  while (g_hash_table_iter_next (&iter, &key, NULL) == TRUE)
    ret[i++] = g_strdup (key);

  ret[i] = NULL;

  g_rw_lock_reader_unlock (d->rwlock);

  return ret;
}

gboolean
dupin_view_exists (Dupin * d,
		   gchar * view_name)
{
  gboolean ret;

  g_rw_lock_reader_lock (d->rwlock);
  DupinView * view = g_hash_table_lookup (d->views, view_name);
  ret = ((view != NULL) && view->todelete == FALSE) ? TRUE : FALSE;
  g_rw_lock_reader_unlock (d->rwlock);

  return ret;
}

DupinView *
dupin_view_open (Dupin * d, gchar * view, GError ** error)
{
  DupinView *ret;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (view != NULL, NULL);

  g_rw_lock_reader_lock (d->rwlock);

  if (!(ret = g_hash_table_lookup (d->views, view)) || ret->todelete == TRUE)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		 "View '%s' doesn't exist.", view);

      g_rw_lock_reader_unlock (d->rwlock);
      return NULL;
    }
  else
    {
      ret->ref++;

#if DEBUG
      fprintf(stderr,"dupin_view_open: (%p) name=%s \t ref++=%d\n", g_thread_self (), view, (gint) ret->ref);
#endif
    }

  g_rw_lock_reader_unlock (d->rwlock);

  return ret;
}

DupinView *
dupin_view_new (Dupin * d,
		gchar * view,
		gchar * parent,
		gboolean parent_is_db,
		gboolean parent_is_linkb,
		DupinViewEngineLang language,
		gchar * map,
		gchar * reduce,
	        gchar * output,
		gboolean output_is_db,
		gboolean output_is_linkb,
		GError ** error)
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

  if (parent_is_db == TRUE)
    {
      if (dupin_database_exists (d, parent) == FALSE)
        {
	  if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		       "View '%s' parent database '%s' does not exist.", view, parent);
	  return NULL;
        }
    }
  else if (parent_is_linkb == TRUE)
    {
      if (dupin_linkbase_exists (d, parent) == FALSE)
        {
	  if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		       "View '%s' parent linkbase '%s' does not exist.", view, parent);
	  return NULL;
        }
    }
  else
    {
      if (dupin_view_exists (d, parent) == FALSE)
        {
	  if (error != NULL && *error != NULL)
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
	      if (error != NULL && *error != NULL)
                g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		           "View '%s' output database '%s' does not exist.", view, output);
	      return NULL;
	    }
	}
      else if (output_is_linkb == TRUE)
        {
	  if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		       "Output to linkbase is not implemented yet");
	  return NULL;
        }
    }

  g_rw_lock_writer_lock (d->rwlock);

  if ((ret = g_hash_table_lookup (d->views, view)))
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "View '%s' already exist.", view);
      g_rw_lock_writer_unlock (d->rwlock);
      return NULL;
    }

  name = g_strdup_printf ("%s%s", view, DUPIN_VIEW_SUFFIX);
  path = g_build_path (G_DIR_SEPARATOR_S, d->path, name, NULL);
  g_free (name);

  if (!(ret = dupin_view_connect (d, view, path, DP_SQLITE_OPEN_CREATE, error)))
    {
      g_rw_lock_writer_unlock (d->rwlock);
      g_free (path);
      return NULL;
    }

  g_free (path);

  ret->ref++;

#if DEBUG
  fprintf(stderr,"dupin_view_new: (%p) name=%s \t ref++=%d\n", g_thread_self (), view, (gint) ret->ref);
#endif

  ret->engine = dupin_view_engine_new (d, language, map, reduce, error);

  if (ret->engine == NULL)
    {
      g_rw_lock_writer_unlock (d->rwlock);
      dupin_view_disconnect (ret);
      return NULL;
    }

  ret->parent = g_strdup (parent);
  ret->parent_is_db = parent_is_db;
  ret->parent_is_linkb = parent_is_linkb;
  ret->output = g_strdup (output);
  ret->output_is_db = output_is_db;
  ret->output_is_linkb = output_is_linkb;

  if (dupin_view_begin_transaction (ret, error) < 0)
    {
      g_rw_lock_writer_unlock (d->rwlock);
      dupin_view_disconnect (ret);
      return NULL;
    }

  gchar * creation_time = g_strdup_printf ("%" G_GSIZE_FORMAT, dupin_date_timestamp_now (0));

  str =
    sqlite3_mprintf ("INSERT OR REPLACE INTO DupinView "
		           "(parent, isdb, islinkb, language, map, reduce, output, output_isdb, output_islinkb, creation_time) "
		     "VALUES('%q',   '%s', '%s',    '%q',     '%q','%q',   '%q',   '%s',        '%s',           '%q')", parent,
		     parent_is_db ? "TRUE" : "FALSE",
		     parent_is_linkb ? "TRUE" : "FALSE",
		     dupin_util_view_engine_lang_to_string (language),
		     map,
 		     reduce,
		     output,
		     output_is_db ? "TRUE" : "FALSE",
		     output_is_linkb ? "TRUE" : "FALSE",
		     creation_time);

  g_free (creation_time);

  if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (d->rwlock);

      if (error != NULL && *error != NULL)
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
      g_rw_lock_writer_unlock (d->rwlock);
      if (error != NULL && *error != NULL)
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
      g_rw_lock_writer_unlock (d->rwlock);
      dupin_view_disconnect (ret);
      return NULL;
    }

  g_rw_lock_writer_unlock (d->rwlock);

  if (dupin_view_p_update (ret, error) == FALSE)
    {
      dupin_view_disconnect (ret);
      return NULL;
    }

  g_rw_lock_writer_lock (d->rwlock);
  g_hash_table_insert (d->views, g_strdup (view), ret);
  g_rw_lock_writer_unlock (d->rwlock);

  dupin_view_sync (ret);
  return ret;
}

#define DUPIN_VIEW_P_SIZE	64

static void
dupin_view_p_update_real (DupinViewP * p, DupinView * view)
{
  g_rw_lock_reader_lock (view->rwlock);
  gboolean todelete = view->todelete;
  g_rw_lock_reader_unlock (view->rwlock);

  if (todelete == TRUE)
    {
      if (p->views != NULL)
        {
          /* NOTE - need to remove pointer from parent if view is "hot deleted" */

          DupinView ** views = g_malloc (sizeof (DupinView *) * p->size);

          gint i;
          gint current_numb = p->numb;
          p->numb = 0;
          for (i=0; i < current_numb ; i++)
            {
              if (p->views[i] != view)
                {
                  views[p->numb] = p->views[i];
                  p->numb++;
                }
            }
          g_free (p->views);
          p->views = views;
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

static int
dupin_view_p_update_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_view_db_record_t * db_record = data;

  if (argc == 9)
    {
      if (argv[0] && *argv[0])
        db_record->parent = g_strdup (argv[0]);

      if (argv[1] && *argv[1])
        db_record->isdb = !g_strcmp0 (argv[1], "TRUE") ? TRUE : FALSE;

      if (argv[2] && *argv[2])
        db_record->islinkb = !g_strcmp0 (argv[2], "TRUE") ? TRUE : FALSE;

      if (argv[3] && *argv[3])
        db_record->language = dupin_util_view_engine_lang_to_enum (argv[3]);

      if (argv[4] && *argv[4])
        db_record->map = g_strdup (argv[4]);

      if (argv[5] != NULL && g_strcmp0(argv[5],"(NULL)") && g_strcmp0(argv[5],"null") )
        {
          db_record->reduce = g_strdup (argv[5]);
        }

      if (argv[6] != NULL && g_strcmp0(argv[6],"(NULL)") && g_strcmp0(argv[6],"null") )
        db_record->output = g_strdup (argv[6]);

      if (argv[7] && *argv[7])
        db_record->output_isdb = !g_strcmp0 (argv[7], "TRUE") ? TRUE : FALSE;

      if (argv[8] && *argv[8])
        db_record->output_islinkb = !g_strcmp0 (argv[8], "TRUE") ? TRUE : FALSE;
    }

  return 0;
}

gboolean
dupin_view_p_update (DupinView * view,
		    GError ** error)
{
  gchar *errmsg;
  struct dupin_view_db_record_t db_record;
  memset (&db_record, 0, sizeof (struct dupin_view_db_record_t));

  g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_GET_RECORD, dupin_view_p_update_cb, &db_record, &errmsg)
      != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (view->rwlock);

      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      return FALSE;
    }

  g_rw_lock_reader_unlock (view->rwlock);

  if (!db_record.parent)
    {
      if (db_record.map != NULL)
        g_free (db_record.map);

      if (db_record.reduce != NULL)
        g_free (db_record.reduce);

      if (db_record.output != NULL)
        g_free (db_record.output);

      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
                   "Internal error.");
      return FALSE;
    }

  if (db_record.isdb == TRUE)
    {
      DupinDB *db;

      if (!(db = dupin_database_open (view->d, db_record.parent, error)))
        {
          g_free (db_record.parent);

          if (db_record.map != NULL)
            g_free (db_record.map);

          if (db_record.reduce != NULL)
            g_free (db_record.reduce);

          if (db_record.output != NULL)
            g_free (db_record.output);

          return FALSE;
        }

      g_rw_lock_writer_lock (db->rwlock);
      dupin_view_p_update_real (&db->views, view);
      g_rw_lock_writer_unlock (db->rwlock);

      dupin_database_unref (db);
      }
  else if (db_record.islinkb == TRUE)
    {
      DupinLinkB *linkb;

      if (!(linkb = dupin_linkbase_open (view->d, db_record.parent, error)))
        {
          g_free (db_record.parent);

          if (db_record.map != NULL)
            g_free (db_record.map);

          if (db_record.reduce != NULL)
            g_free (db_record.reduce);

          if (db_record.output != NULL)
            g_free (db_record.output);

          return FALSE;
        }

      g_rw_lock_writer_lock (linkb->rwlock);
      dupin_view_p_update_real (&linkb->views, view);
      g_rw_lock_writer_unlock (linkb->rwlock);

      dupin_linkbase_unref (linkb);
    }
  else
    {
      DupinView *v;

      if (!(v = dupin_view_open (view->d, db_record.parent, error)))
        {
          g_free (db_record.parent);

          if (db_record.map != NULL)
            g_free (db_record.map);

          if (db_record.reduce != NULL)
            g_free (db_record.reduce);

          if (db_record.output != NULL)
            g_free (db_record.output);

          return FALSE;
        }

      g_rw_lock_writer_lock (v->rwlock);
      dupin_view_p_update_real (&v->views, view);
      g_rw_lock_writer_unlock (v->rwlock);

      dupin_view_unref (v);
    }

  /* make sure parameters are set after dupin server restart on existing view */

  if (view->engine == NULL)
    {
      view->engine = dupin_view_engine_new (view->d,
                                            db_record.language,
                                            db_record.map,
                                            db_record.reduce,
                                            error);
      if (view->engine == NULL)
        {
          g_free (db_record.parent);

          if (db_record.output != NULL)
            g_free (db_record.output);

          if (db_record.map != NULL)
            g_free (db_record.map);

          if (db_record.reduce != NULL)
            g_free (db_record.reduce);

          return FALSE;
        }
    }

  if (db_record.map != NULL)
    g_free (db_record.map);

  if (db_record.reduce != NULL)
    g_free (db_record.reduce);

  if (view->parent == NULL)
    view->parent = db_record.parent;
  else
    g_free (db_record.parent);

  view->parent_is_db = db_record.isdb;
  view->parent_is_linkb = db_record.islinkb;

  if (view->output == NULL)
    view->output = db_record.output;
  else
    g_free (db_record.output);

  view->output_is_db = db_record.output_isdb;
  view->output_is_linkb = db_record.output_islinkb;

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

  GError * error = NULL;

  g_rw_lock_writer_lock (view->rwlock);

  if (!(id = dupin_view_generate_id (view, &error, FALSE)))
    {
      g_rw_lock_writer_unlock (view->rwlock);

      return;
    }

#if DUPIN_VIEW_DEBUG
  DUPIN_UTIL_DUMP_JSON ("dupin_view_record_save_map: KEY", key_node);
  DUPIN_UTIL_DUMP_JSON ("dupin_view_record_save_map: PID", pid_node);
  DUPIN_UTIL_DUMP_JSON ("dupin_view_record_save_map: OBJ", node);
#endif

  /* serialize the node */

  node_serialized = dupin_util_json_serialize (node);

  if (node_serialized == NULL)
    {
      g_rw_lock_writer_unlock (view->rwlock);
      g_free ((gchar *)id);
      return;
    }

  /* serialize the key */

  if (key_node != NULL)
    {
      key_serialized = dupin_util_json_serialize (key_node);

      if (key_serialized == NULL)
        {
          g_rw_lock_writer_unlock (view->rwlock);
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
          g_rw_lock_writer_unlock (view->rwlock);
          g_free ((gchar *)id);
          g_free (node_serialized);
          if (key_serialized)
            g_free (key_serialized);
          return;
        }
    }

  gsize modified = dupin_date_timestamp_now (0);

  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_INSERT, id, pid_serialized, key_serialized, node_serialized,
						modified);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_record_save_map: %s query: %s\n",view->name, tmp);
#endif

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_rw_lock_writer_unlock (view->rwlock);
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
      g_rw_lock_writer_unlock (view->rwlock);

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
              g_rw_lock_writer_unlock (view->rwlock);

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

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_rw_lock_writer_unlock (view->rwlock);
      g_free ((gchar *)id);
      g_free (node_serialized);
      if (key_serialized)
        g_free (key_serialized);
      if (pid_serialized)
        g_free (pid_serialized);

      return;
    }

  g_rw_lock_writer_unlock (view->rwlock);

  g_free (node_serialized);
  if (key_serialized)
    g_free (key_serialized);
  if (pid_serialized)
    g_free (pid_serialized);
  g_free ((gchar *)id);
}

gchar *
dupin_view_generate_id (DupinView * view,
		        GError ** error,
			gboolean lock)
{
  g_return_val_if_fail (view != NULL, NULL);

  if (lock == TRUE)
    g_rw_lock_writer_lock (view->rwlock);

  while (TRUE)
    { 
      gchar * id = NULL;

      id = dupin_util_generate_id (error);

      if (id != NULL)
        {   
          if (dupin_view_record_exists_real (view, id, FALSE) == TRUE)
            { 
              g_free (id);
            }
          else
            { 
	      if (lock == TRUE)
                g_rw_lock_writer_unlock (view->rwlock);

              return id;
            }
        }
      else
        break;
    }

  if (lock == TRUE)
   g_rw_lock_writer_unlock (view->rwlock);

  return NULL;
}

void
dupin_view_record_delete (DupinView * view,
			  gchar * pid)
{
  g_return_if_fail (view != NULL);
  g_return_if_fail (pid != NULL);

  gchar *query;
  gchar *errmsg;
  gsize max_rowid;
  
  /* NOTE - we get max_rowid in a separate transaction, in worse case it will be incremented
	    since last fetch, pretty safe */

  g_rw_lock_writer_lock (view->rwlock);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_rw_lock_writer_unlock (view->rwlock);

      return;
    }

  if (dupin_view_record_get_max_rowid (view, &max_rowid, FALSE) == FALSE)
    {
      g_rw_lock_writer_unlock (view->rwlock);

      dupin_view_rollback_transaction (view, NULL);

      return;
    }

  gchar * max_rowid_str = g_strdup_printf ("%d", (gint)max_rowid);

  //query = sqlite3_mprintf ("DELETE FROM Dupin WHERE ROWID <= %q AND pid LIKE '%%\"%q\"%%' ;", max_rowid_str, pid);
  query = sqlite3_mprintf ("DELETE FROM Dupin WHERE ROWID <= %q AND id IN (SELECT id FROM DupinPid2Id WHERE pid = '%q') ;", max_rowid_str, pid);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_record_delete: view %s query=%s\n",view->name, query);
#endif

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (view->rwlock);

      g_error("dupin_view_record_delete: %s for pid %s", errmsg, pid);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);
      g_free (max_rowid_str);
      sqlite3_free (query);

      return;
    }

  sqlite3_free (query);

  /* NOTE - clean up pid <-> id table too */

  query = sqlite3_mprintf ("DELETE FROM DupinPid2Id WHERE pid = '%q' ;", pid);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_record_delete: view %s query=%s\n",view->name, query);
#endif

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (view->rwlock);

      g_error("dupin_view_record_delete: %s for pid %s", errmsg, pid);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);
      g_free (max_rowid_str);
      sqlite3_free (query);

      return;
    }

  sqlite3_free (query);

  g_free (max_rowid_str);

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_rw_lock_writer_unlock (view->rwlock);

      return;
    }

  g_rw_lock_writer_unlock (view->rwlock);

  /* NOTE - delete operations do not need re-index and call map/reduce due we use PIDs of DB
            or views to delete record in view which where accepted by record delete */
}

void
dupin_view_ref (DupinView * view)
{
  g_return_if_fail (view != NULL);

  g_rw_lock_writer_lock (view->rwlock);

  view->ref++;

#if DEBUG
  fprintf(stderr,"dupin_view_ref: (%p) name=%s \t ref++=%d\n", g_thread_self (), view->name, (gint) view->ref);
#endif

  g_rw_lock_writer_unlock (view->rwlock);
}

void
dupin_view_unref (DupinView * view)
{
  Dupin *d;

  g_return_if_fail (view != NULL);

  d = view->d;

  g_rw_lock_writer_lock (view->rwlock);

  if (view->ref > 0)
    {
      view->ref--;

#if DEBUG
      fprintf(stderr,"dupin_view_unref: (%p) name=%s \t ref--=%d\n", g_thread_self (), view->name, (gint) view->ref);
#endif
    }

  g_rw_lock_writer_unlock (view->rwlock);

  if (view->todelete == TRUE &&
      dupin_view_is_syncing (view) == FALSE)
    {
      g_rw_lock_reader_lock (view->rwlock);

      if (view->ref > 0)
        {
          g_rw_lock_reader_unlock (view->rwlock);

          g_warning ("dupin_view_unref: (thread=%p) view %s flagged for deletion but can't free it due ref is %d\n", g_thread_self (), view->name, (gint) view->ref);
	}
      else
        {
          g_rw_lock_reader_unlock (view->rwlock);

	  if (dupin_view_p_update (view, NULL) == FALSE)
            {
              g_warning("dupin_view_unref: could not remove reference from parent for view '%s'\n", view->name);
            }

          g_rw_lock_writer_lock (d->rwlock);
          g_hash_table_remove (d->views, view->name);
          g_rw_lock_writer_unlock (d->rwlock);
	}
    }
}

gboolean
dupin_view_delete (DupinView * view, GError ** error)
{
  g_return_val_if_fail (view != NULL, FALSE);

  g_rw_lock_writer_lock (view->rwlock);
  view->todelete = TRUE;
  g_rw_lock_writer_unlock (view->rwlock);

  return TRUE;
}

gboolean
dupin_view_force_quit (DupinView * view, GError ** error)
{
  g_return_val_if_fail (view != NULL, FALSE);

  g_rw_lock_writer_lock (view->rwlock);
  view->sync_toquit = TRUE;
  g_rw_lock_writer_unlock (view->rwlock);

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

DupinViewEngine *
dupin_view_get_engine (DupinView * view)
{
  g_return_val_if_fail (view != NULL, NULL);

  return view->engine;
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
  g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, query, dupin_view_get_creation_time_cb, creation_time, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (view->rwlock);

      g_error("dupin_view_get_creation_time: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_rw_lock_reader_unlock (view->rwlock);

  return TRUE;
}

/* Internal: */
void
dupin_view_disconnect (DupinView * view)
{
  g_return_if_fail (view != NULL);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_disconnect: total number of changes for '%s' view database: %d\n", view->name, (gint)sqlite3_total_changes (view->db));
#endif

  if (view->db)
    sqlite3_close (view->db);

  if (view->todelete == TRUE)
    g_unlink (view->path);

  g_cond_clear (view->sync_map_has_new_work);
  g_free (view->sync_map_has_new_work);

  if (view->engine != NULL)
    dupin_view_engine_free (view->engine);

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
      g_mutex_clear (view->mutex);
      g_free (view->mutex);
    }

  if (view->rwlock)
    {
      g_rw_lock_clear (view->rwlock);
      g_free (view->rwlock);
    }

  if (view->views.views)
    g_free (view->views.views);

  if (view->error_msg)
    g_free (view->error_msg);

  if (view->warning_msg)
    g_free (view->warning_msg);

  if (view->collation_parser)
    g_object_unref (view->collation_parser);

  g_free (view);
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
  gchar *errmsg;
  DupinView *view;

  view = g_malloc0 (sizeof (DupinView));

  view->tosync = FALSE;

  view->sync_map_processed_count = 0;
  view->sync_reduce_total_records = 0;
  view->sync_reduce_processed_count = 0;

  view->sync_map_has_new_work = g_new0 (GCond, 1);
  g_cond_init (view->sync_map_has_new_work);

  view->d = d;

  view->name = g_strdup (name);
  view->path = g_strdup (path);

  view->collation_parser = json_parser_new ();

  view->mutex = g_new0 (GMutex, 1);
  g_mutex_init (view->mutex);

  view->rwlock = g_new0 (GRWLock, 1);
  g_rw_lock_init (view->rwlock);

  if (sqlite3_open_v2 (view->path, &view->db, dupin_util_dupin_mode_to_sqlite_mode (mode), NULL) != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "View error.");
      dupin_view_disconnect (view);
      return NULL;
    }

  sqlite3_busy_timeout (view->db, DUPIN_SQLITE_TIMEOUT);

  /* NOTE - set simple collation functions for views - see http://wiki.apache.org/couchdb/View_collation */

  if (sqlite3_create_collation (view->db, "dupincmp", SQLITE_UTF8,  view->collation_parser, dupin_util_collation) != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
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
	  if (error != NULL && *error != NULL)
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
          if (error != NULL && *error != NULL)
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
      if (error != NULL && *error != NULL)
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
          if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                   errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          return NULL;
        }
    }
  else if (user_version == 2)
    {
      if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_2, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                   errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          return NULL;
        }
    }
  else if (user_version == 3)
    {
      if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_3, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                   errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          return NULL;
        }
    }
  else if (user_version == 4)
    {
      if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_4, NULL, NULL, &errmsg) != SQLITE_OK)
        {
	  if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s", errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          return NULL;
        }
    }
  else if (user_version == 5)
    {
      if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_5, NULL, NULL, &errmsg) != SQLITE_OK)
        {
	  if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s", errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          return NULL;
        }
    }
  else if (user_version == 6)
    {
      if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_DESC_UPGRADE_FROM_VERSION_6, NULL, NULL, &errmsg) != SQLITE_OK)
        {
	  if (error != NULL && *error != NULL)
            g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s", errmsg);
          sqlite3_free (errmsg);
          dupin_view_disconnect (view);
          return NULL;
        }
    }

  if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_USES_OLD_ROWID, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s", errmsg);
      sqlite3_free (errmsg);

      g_warning ("dupin_view_connect: Consider to recreate your %s SQLite database and reingest your data. Since version 3 the Dupin table uses a seq column INTEGER PRIMARY KEY AUTOINCREMENT as ROWID and UNIQUE (id, rev) constraint rather then PRIMARY KEY(id, rev). See http://www.sqlite.org/autoinc.html for more information.\n", path);
    }

  gchar * cache_size = g_strdup_printf ("PRAGMA cache_size = %d", DUPIN_SQLITE_CACHE_SIZE);
  if (sqlite3_exec (view->db, "PRAGMA temp_store = memory", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (view->db, cache_size, NULL, NULL, &errmsg) != SQLITE_OK) 
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma temp_store: %s", errmsg);
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
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot set pragma synchronous: %s",
                   errmsg);
      sqlite3_free (errmsg);
      dupin_view_disconnect (view);
      return NULL;
    }

  /* NOTE - we know this is inefficient, but we need it till proper Elastic search or lucene used as frontend */

  sqlite3_create_function(view->db, "filterBy", 5, SQLITE_ANY, d, dupin_sqlite_json_filterby, NULL, NULL);

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
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot begin view %s transaction: %s", view->name, errmsg);
      
      sqlite3_free (errmsg);

      return -1;
    }

#if DUPIN_VIEW_DEBUG
  //g_message ("dupin_view_begin_transaction: view %s transaction begin", view->name);
#endif

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
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot rollback view %s transaction: %s", view->name, errmsg);
      
      sqlite3_free (errmsg);

      return -1;
    }

#if DUPIN_VIEW_DEBUG
  //g_message ("dupin_view_rollback_transaction: view %s transaction rollback", view->name);
#endif

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
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "Cannot commit view %s transaction: %s", view->name, errmsg);
      
      sqlite3_free (errmsg);

      return -1;
    }

#if DUPIN_VIEW_DEBUG
  //g_message ("dupin_view_commit_transaction: view %s transaction commit", view->name);
#endif

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

  g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, DUPIN_VIEW_SQL_COUNT, dupin_view_count_cb, &size, NULL) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (view->rwlock);
      return 0;
    }

  g_rw_lock_reader_unlock (view->rwlock);
  return size;
}

/* NOTE - we always bulk insert using the latest revision and update the records only if modified (so we reduce revisions too) */

JsonNode *
dupin_view_output_insert (DupinView * view, JsonNode * node)
{
  g_return_val_if_fail (view != NULL, NULL);
  g_return_val_if_fail (node != NULL, NULL);
  g_return_val_if_fail (json_node_get_node_type (node) == JSON_NODE_OBJECT, NULL);

#if DUPIN_VIEW_DEBUG
  DUPIN_UTIL_DUMP_JSON ("dupin_view_output_insert: NODE", node);
#endif

  GList * response_list=NULL;
  JsonNode * response_node = NULL;

  if (dupin_view_get_output_is_db (view) == TRUE)
    {
      gboolean is_bulk = (json_object_has_member (json_node_get_object (node), REQUEST_POST_BULK_DOCS_DOCS) == FALSE) ? FALSE : TRUE;

      DupinDB * db = NULL;
      if (! (db = dupin_database_open (view->d, (gchar *)dupin_view_get_output (view), NULL)))
        return NULL;
                    
      GError * error = NULL;

      if (is_bulk == TRUE)
        {
          if (dupin_record_insert_bulk (db, node, &response_list, TRUE, TRUE, &error) == TRUE)
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
        }
      else
        {
	  if (dupin_record_insert (db, node, NULL, NULL, &response_list, TRUE, TRUE, &error) == TRUE)
            {
	      if (g_list_length (response_list) == 1)
                {
      		  JsonNode * r = (JsonNode *) response_list->data;

      		  response_node = json_node_copy (r);

      		  if (json_object_has_member (json_node_get_object (response_node), RESPONSE_STATUS_ERROR) == FALSE)
        	    json_object_set_boolean_member (json_node_get_object (response_node), "ok", TRUE);
                }
            }
        }

      while (response_list)
        {
	  if (response_list->data != NULL)
            json_node_free (response_list->data);
          response_list = g_list_remove (response_list, response_list->data);
        } 

      dupin_database_unref (db);
    }
  else if (dupin_view_get_output_is_linkb (view) == TRUE)
    {
      g_warning("dupin_view_output_insert: output to linkbase not implemented yet\n");

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

static void
dupin_view_sync_thread_real_map (DupinView * view, GList * list)
{
  for (; list; list = list->next)
    {
      struct dupin_view_sync_t *data = list->data;
      JsonNode * array_node;

      gchar * id = g_strdup ( (gchar *)json_node_get_string ( json_array_get_element ( json_node_get_array (data->pid), 0) ) );

      array_node = dupin_view_engine_record_map (view->engine, data->obj);

      if (array_node != NULL && 
          json_node_get_node_type (array_node) == JSON_NODE_ARRAY)
	{
	  GList *nodes, *n;
	  JsonArray * array = json_node_get_array (array_node);
	  nodes = json_array_get_elements (array);

          for (n = nodes; n != NULL; n = n->next)
            {
	      /* TODO - Check current json-glib if still/has bug or feature ?! where get member cast
		        integers to double automatically - we need to reparse each returned array element??!!! why?!
	
		        The potential problem is 1 != 1.0 in JSON but WebKit sets 1 to 1.0 etc.
	       */

              JsonNode * mapped_result = (JsonNode*)n->data;
  
	      if (json_node_get_node_type (mapped_result) != JSON_NODE_OBJECT)
	        {
		  /* TODO - report error? */

		  continue;
		}

              JsonObject * mapped_result_obj = json_node_get_object (mapped_result);

	      JsonNode * key_node = json_object_get_member (mapped_result_obj, DUPIN_VIEW_KEY);
	      JsonNode * node = json_object_get_member (mapped_result_obj, DUPIN_VIEW_VALUE);

	      /* TODO - do bulk insert/update of 'array' element if view has output */

              JsonNode * response_node = NULL;
	      if (dupin_view_engine_get_reduce_code (view->engine) == NULL
		  && dupin_view_get_output (view) != NULL)
	        {
	          if (! (response_node = dupin_view_output_insert (view, node)))
	            {
		      continue; // TODO - shall we fail instead?
		    }
	        }

	      if (response_node != NULL)
	        json_node_free (response_node);

	      dupin_view_record_save_map (view,
					  data->pid,
					  key_node,
					  node);

              g_rw_lock_writer_lock (view->rwlock);
              view->sync_map_processed_count++;
              g_rw_lock_writer_unlock (view->rwlock);

	      dupin_view_p_record_insert (&view->views, id, mapped_result_obj);
            }

          g_list_free (nodes);
	}

        if (array_node != NULL)
	  json_node_free (array_node);

        g_free(id);
    }
}

static int
dupin_view_remap_cb (void *data, int argc, char **argv, char **col)
{
  JsonNode  * pids_to_remap = data;
  JsonArray * pids_to_remap_array = json_node_get_array (pids_to_remap);

  if (argv[0] && *argv[0])
    {
      JsonNode * pid = json_node_new (JSON_NODE_VALUE);
      json_node_set_string (pid, argv[0]);

      json_array_add_element (pids_to_remap_array, pid);
    }

  return 0;
}

/* NOTE - Check if we have any record to remap as part of a reduced view where one of the
	  pids has been deleted i.e. need to regenerate the end result with remaing parts */

JsonNode *
dupin_view_remap (DupinView * view,
		  gsize count)
{
  g_return_val_if_fail (view != NULL, NULL);

  gchar * errmsg;

  if (dupin_view_engine_get_reduce_code (view->engine) != NULL)
    {
      JsonNode * pids_to_remap = json_node_new (JSON_NODE_ARRAY);
      JsonArray * pids_to_remap_array = json_array_new ();
      json_node_take_array (pids_to_remap, pids_to_remap_array);

      gchar * query = sqlite3_mprintf ("SELECT pid, id AS vid FROM DupinPid2Id WHERE id IS NOT (SELECT id FROM Dupin WHERE id=vid) LIMIT %" G_GSIZE_FORMAT, count);

      g_rw_lock_reader_lock (view->rwlock);

      if (sqlite3_exec (view->db, query, dupin_view_remap_cb, pids_to_remap, &errmsg) != SQLITE_OK)
        {
          g_rw_lock_reader_unlock (view->rwlock);

          g_error ("dupin_view_remap: %s", errmsg);
          sqlite3_free (errmsg);

          sqlite3_free (query);

	  if (pids_to_remap != NULL)
	    json_node_free (pids_to_remap);

          return NULL;
        }

      g_rw_lock_reader_unlock (view->rwlock);

      sqlite3_free (query);

      if (json_array_get_length (pids_to_remap_array) != 0)
        {

#if DUPIN_VIEW_DEBUG
          DUPIN_UTIL_DUMP_JSON ("Found the following PIDs to remap", pids_to_remap);
#endif

	  return pids_to_remap;
        }
      else
        {
          if (pids_to_remap != NULL)
            json_node_free (pids_to_remap);

          return NULL;
        }
    }
  else
    {
      return NULL;
    }
}

static gboolean
dupin_view_sync_thread_map_db (DupinView * view,
			       gsize count)
{
  gchar * sync_map_id = NULL;
  gchar * errmsg;
  DupinDB *db;
  GList *results, *list;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *str;
  gchar * query = NULL;

  JsonNode * pids_to_remap = dupin_view_remap (view, count);

  gsize start_rowid = 1;

  if (!(db = dupin_database_open (view->d, view->parent, NULL)))
    {
      if (pids_to_remap != NULL)
        json_node_free (pids_to_remap);

      return FALSE;
    }

  if (pids_to_remap != NULL)
    { 
      GList * keys = NULL;

      if ((!(keys = json_array_get_elements (json_node_get_array (pids_to_remap)))) ||
	  (dupin_record_get_list (db, count, 0, start_rowid, 0, keys, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE, NULL, DP_FILTERBY_EQUALS,
				NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == FALSE) ||
	  (!results))
        {
          json_node_free (pids_to_remap);

          if (keys != NULL)
            g_list_free (keys);

          dupin_database_unref (db);

          return FALSE;
        }

      if (keys != NULL)
        g_list_free (keys);
    }
  else
    {
      /* get last position we reduced and get anything up to count after that */

      query = "SELECT sync_map_id as c FROM DupinView LIMIT 1";

      g_rw_lock_reader_lock (view->rwlock);

      if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_map_id, &errmsg) != SQLITE_OK)
        {
          g_rw_lock_reader_unlock (view->rwlock);

          g_error("dupin_view_sync_thread_map_db: %s", errmsg);
          sqlite3_free (errmsg);

          dupin_database_unref (db);

          return FALSE;
        }

      g_rw_lock_reader_unlock (view->rwlock);

      if (sync_map_id != NULL)
        start_rowid = (gsize) g_ascii_strtoll (sync_map_id, NULL, 10)+1;

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
    }

#if DUPIN_VIEW_DEBUG
  g_rw_lock_reader_lock (view->rwlock);
  gsize sync_map_processed_count = view->sync_map_processed_count;
  g_rw_lock_reader_unlock (view->rwlock);

  g_message("dupin_view_sync_thread_map_db(%p/%s)    g_list_length (results) = %d start_rowid=%d - mapped %d\n", g_thread_self (), view->name, (gint) g_list_length (results), (gint)start_rowid, (gint)sync_map_processed_count);
#endif

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

	  if (json_object_has_member (obj, "_expire") == TRUE)
            json_object_remove_member (obj, "_expire"); // ignore any record one if set by user, ever
 	  if (dupin_record_get_expire (list->data) != 0)
	    {
              gchar * expire = dupin_date_timestamp_to_iso8601 (dupin_record_get_expire (list->data));
              json_object_set_string_member (obj, "_expire", expire);
              g_free (expire);
	    }

	  if (json_object_has_member (obj, "_type") == TRUE)
            json_object_remove_member (obj, "_type"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_type", (gchar *)dupin_record_get_type (list->data));

	  /* NOTE - needed for m/r dupin.docpath() and dupin.links() methods - see dupin_webkit.c */

          if (json_object_has_member (obj, "_linkbase") == TRUE)
            json_object_remove_member (obj, "_linkbase"); // ignore any record one if set by user, ever
          json_object_set_string_member (obj, "_linkbase", dupin_linkbase_get_name (dupin_database_get_default_linkbase (db)));
        }

      data->pid = json_node_new (JSON_NODE_ARRAY);
      JsonArray *pid_array=json_array_new ();

      if (pids_to_remap == NULL)
        {
          gsize rowid = dupin_record_get_rowid (list->data);

          if (sync_map_id != NULL)
            g_free(sync_map_id);
        
          sync_map_id = g_strdup_printf ("%i", (gint)rowid);

#if DUPIN_VIEW_DEBUG
          g_message("dupin_view_sync_thread_map_db(%p/%s) sync_map_id=%s as fetched",g_thread_self (), view->name, sync_map_id);
#endif
        }

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

  if (pids_to_remap == NULL)
    {

#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_thread_map_db() view %s sync_map_id=%s as to be stored",view->name, sync_map_id);
      g_message("dupin_view_sync_thread_map_db(%p/%s)  finished last_map_rowid=%s - mapped %d\n", g_thread_self (), view->name, sync_map_id, (gint)sync_map_processed_count);
#endif

      str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

      if (sync_map_id != NULL)
        g_free (sync_map_id);

      g_rw_lock_writer_lock (view->rwlock);

      if (dupin_view_begin_transaction (view, NULL) < 0)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          sqlite3_free (str);

          dupin_database_unref (db);

          return FALSE;
        }

      if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          sqlite3_free (str);

          dupin_database_unref (db);

          g_error("dupin_view_sync_thread_map_db: %s", errmsg);
          sqlite3_free (errmsg);

          dupin_view_rollback_transaction (view, NULL);

          return FALSE;
        }

      if (dupin_view_commit_transaction (view, NULL) < 0)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          sqlite3_free (str);

          dupin_database_unref (db);

          return FALSE;
        }

      g_rw_lock_writer_unlock (view->rwlock);

      sqlite3_free (str);
    }
  else
    {

#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_thread_map_db(%p/%s)  remapped %d\n", g_thread_self (), view->name, (gint)sync_map_processed_count);
#endif

      json_node_free (pids_to_remap);
    }

  dupin_database_unref (db);

  return ret;
}

static gboolean
dupin_view_sync_thread_map_linkb (DupinView * view, gsize count)
{
  gchar * sync_map_id = NULL;
  gchar * errmsg;
  DupinLinkB *linkb;
  GList *results, *list;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *str;
  gchar * query = NULL;

  JsonNode * pids_to_remap = dupin_view_remap (view, count);

  gsize start_rowid = 1;

  if (!(linkb = dupin_linkbase_open (view->d, view->parent, NULL)))
    {
      if (pids_to_remap != NULL)
        json_node_free (pids_to_remap);

      return FALSE;
    }

  if (pids_to_remap != NULL)
    { 
      GList * keys = NULL;

      if ((!(keys = json_array_get_elements (json_node_get_array (pids_to_remap)))) ||
	  (dupin_link_record_get_list (linkb, count, 0, start_rowid, 0, DP_LINK_TYPE_ANY, keys, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE,
                                  NULL, NULL, DP_FILTERBY_EQUALS, NULL, DP_FILTERBY_EQUALS, NULL, DP_FILTERBY_EQUALS,
                                  NULL, DP_FILTERBY_EQUALS, NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == FALSE) ||
	  (!results))
        {
          json_node_free (pids_to_remap);

          if (keys != NULL)
            g_list_free (keys);

          dupin_linkbase_unref (linkb);

          return FALSE;
        }

      if (keys != NULL)
        g_list_free (keys);
    }
  else
    {
      /* get last position we reduced and get anything up to count after that */

      query = "SELECT sync_map_id as c FROM DupinView LIMIT 1";

      g_rw_lock_reader_lock (view->rwlock);

      if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_map_id, &errmsg) != SQLITE_OK)
        {
          g_rw_lock_reader_unlock (view->rwlock);

          g_error("dupin_view_sync_thread_map_linkb: %s", errmsg);
          sqlite3_free (errmsg);

          dupin_linkbase_unref (linkb);

          return FALSE;
        }

      g_rw_lock_reader_unlock (view->rwlock);

      if (sync_map_id != NULL)
        start_rowid = (gsize) g_ascii_strtoll (sync_map_id, NULL, 10)+1;

      if (dupin_link_record_get_list (linkb, count, 0, start_rowid, 0, DP_LINK_TYPE_ANY, NULL, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE,
				  NULL, NULL, DP_FILTERBY_EQUALS, NULL, DP_FILTERBY_EQUALS, NULL, DP_FILTERBY_EQUALS,
				  NULL, DP_FILTERBY_EQUALS, NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == FALSE || !results)
        {
          if (sync_map_id != NULL)
            g_free(sync_map_id);

          dupin_linkbase_unref (linkb);

          return FALSE;
        }

      if (g_list_length (results) != count)
        ret = FALSE;
    }

#if DUPIN_VIEW_DEBUG
  g_rw_lock_reader_lock (view->rwlock);
  gsize sync_map_processed_count = view->sync_map_processed_count;
  g_rw_lock_reader_unlock (view->rwlock);

  g_message("dupin_view_sync_thread_map_linkb(%p/%s)    g_list_length (results) = %d start_rowid=%d - mapped %d\n", g_thread_self (), view->name, (gint) g_list_length (results), (gint)start_rowid, (gint)sync_map_processed_count);
#endif

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

          if (json_object_has_member (obj, "_expire") == TRUE)
            json_object_remove_member (obj, "_expire"); // ignore any record one if set by user, ever
	  if (dupin_link_record_get_expire (list->data) != 0)
            {
	      gchar * expire = dupin_date_timestamp_to_iso8601 (dupin_link_record_get_expire (list->data));
              json_object_set_string_member (obj, "_expire", expire);
              g_free (expire);
	    }

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

      if (pids_to_remap == NULL)
        {
          gsize rowid = dupin_link_record_get_rowid (list->data);

          if (sync_map_id != NULL)
            g_free(sync_map_id);
        
          sync_map_id = g_strdup_printf ("%i", (gint)rowid);

#if DUPIN_VIEW_DEBUG
          g_message("dupin_view_sync_thread_map_linkb(%p/%s) sync_map_id=%s as fetched", g_thread_self (), view->name, sync_map_id);
#endif
        }

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

  if (pids_to_remap == NULL)
    {

#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_thread_map_linkb() view %s sync_map_id=%s as to be stored",view->name, sync_map_id);
      g_message("dupin_view_sync_thread_map_linkb(%p/%s)  finished last_map_rowid=%s - mapped %d\n", g_thread_self (), view->name, sync_map_id, (gint)sync_map_processed_count);
#endif

      str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

      if (sync_map_id != NULL)
        g_free (sync_map_id);

      g_rw_lock_writer_lock (view->rwlock);

      if (dupin_view_begin_transaction (view, NULL) < 0)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          sqlite3_free (str);

          dupin_linkbase_unref (linkb);

          return FALSE;
        }

      if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          sqlite3_free (str);

          dupin_linkbase_unref (linkb);

          g_error("dupin_view_sync_thread_map_linkb: %s", errmsg);
          sqlite3_free (errmsg);

          dupin_view_rollback_transaction (view, NULL);

          return FALSE;
        }

      if (dupin_view_commit_transaction (view, NULL) < 0)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          sqlite3_free (str);

          dupin_linkbase_unref (linkb);

          return FALSE;
        }

      g_rw_lock_writer_unlock (view->rwlock);

      sqlite3_free (str);
    }
  else
    {

#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_thread_map_linkb(%p/%s)  remapped %d\n", g_thread_self (), view->name, (gint)sync_map_processed_count);
#endif

      json_node_free (pids_to_remap);
    }

  dupin_linkbase_unref (linkb);

  return ret;
}

static gboolean
dupin_view_sync_thread_map_view (DupinView * view, gsize count)
{
  DupinView *v;
  gchar *errmsg;
  GList *results, *list;
  gchar * sync_map_id = NULL;

  GList *l = NULL;
  gboolean ret = TRUE;

  gchar *str;
  gchar * query = NULL;

  JsonNode * pids_to_remap = dupin_view_remap (view, count);

  gsize start_rowid = 1;

  if (!(v = dupin_view_open (view->d, view->parent, NULL)))
    {
      if (pids_to_remap != NULL)
        json_node_free (pids_to_remap);

      return FALSE;
    }

  if (pids_to_remap != NULL)
    { 
      GList * keys = NULL;

      if ((!(keys = json_array_get_elements (json_node_get_array (pids_to_remap)))) ||
          (dupin_view_record_get_list (v, count, 0, start_rowid, 0, DP_ORDERBY_ROWID, FALSE, keys, NULL, NULL, TRUE, NULL, NULL, TRUE,
					NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == FALSE) ||
	  (!results))
        {
          json_node_free (pids_to_remap);

          if (keys != NULL)
            g_list_free (keys);

          dupin_view_unref (v);

          return FALSE;
        }

      if (keys != NULL)
        g_list_free (keys);
    }
  else
    {
      /* get last position we reduced and get anything up to count after that */

      query = "SELECT sync_map_id as c FROM DupinView LIMIT 1";

      g_rw_lock_reader_lock (view->rwlock);

      if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_map_id, &errmsg) != SQLITE_OK)
        {
          g_rw_lock_reader_unlock (view->rwlock);

          g_error("dupin_view_sync_thread_map_view: %s", errmsg);
          sqlite3_free (errmsg);

          dupin_view_unref (v);

          return FALSE;
        }

      g_rw_lock_reader_unlock (view->rwlock);

      if (sync_map_id != NULL)
        start_rowid = (gsize) g_ascii_strtoll (sync_map_id, NULL, 10)+1;

      if (dupin_view_record_get_list (v, count, 0, start_rowid, 0, DP_ORDERBY_ROWID, FALSE, NULL, NULL, NULL, TRUE, NULL, NULL, TRUE,
					NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == FALSE || !results)
        {
          if (sync_map_id != NULL)
            g_free(sync_map_id);

          dupin_view_unref (v);

          return FALSE;
        }

      if (g_list_length (results) != count)
        ret = FALSE;
    }

#if DUPIN_VIEW_DEBUG
  g_rw_lock_reader_lock (view->rwlock);
  gsize sync_map_processed_count = view->sync_map_processed_count;
  g_rw_lock_reader_unlock (view->rwlock);

  g_message("dupin_view_sync_thread_map_view(%p/%s)    g_list_length (results) = %d\n", g_thread_self (), view->name, (gint) g_list_length (results) );
#endif

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

      if (pids_to_remap == NULL)
        {
          gsize rowid = dupin_view_record_get_rowid (list->data);

          if (sync_map_id != NULL)
            g_free(sync_map_id);
        
          sync_map_id = g_strdup_printf ("%i", (gint)rowid);

#if DUPIN_VIEW_DEBUG
          g_message("dupin_view_sync_thread_map_view(%p/%s) sync_map_id=%s as fetched",g_thread_self (), view->name, sync_map_id);
#endif
        }

      json_array_add_string_element (pid_array, (gchar *) dupin_view_record_get_id (list->data));
      json_node_take_array (data->pid, pid_array);

      JsonNode * key = dupin_view_record_get_key (list->data);

      if (key)
        data->key = json_node_copy (key);

      /* NOTE - key not set for dupin_view_sync_thread_map_db() - see dupin_view_sync_thread_map_view() instead */

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

  if (pids_to_remap == NULL)
    {

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_thread_map_view() view %s sync_map_id=%s as to be stored",view->name, sync_map_id);
#endif

      str = sqlite3_mprintf ("UPDATE DupinView SET sync_map_id = '%q'", sync_map_id);

      if (sync_map_id != NULL)
        g_free (sync_map_id);

#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_thread_map_view() view %s query=%s\n",view->name, str);
#endif

      g_rw_lock_writer_lock (view->rwlock);

      if (dupin_view_begin_transaction (view, NULL) < 0)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          sqlite3_free (str);

          dupin_view_unref (v);

          return FALSE;
        }

      if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          sqlite3_free (str);

          dupin_view_unref (v);

          g_error("dupin_view_sync_thread_map_view: %s", errmsg);
          sqlite3_free (errmsg);

          dupin_view_rollback_transaction (view, NULL);

          return FALSE;
        }

      if (dupin_view_commit_transaction (view, NULL) < 0)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          sqlite3_free (str);

          dupin_view_unref (v);

          return FALSE;
        }

      g_rw_lock_writer_unlock (view->rwlock);

      sqlite3_free (str);
    }
  else
    {

#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_thread_map_view(%p/%s)  remapped %d\n", g_thread_self (), view->name, (gint)sync_map_processed_count);
#endif

      json_node_free (pids_to_remap);
    }

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

  g_rw_lock_writer_lock (view->rwlock);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_rw_lock_writer_unlock (view->rwlock);
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

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_record_update() view %s delete query=%s\n",view->name, query);
#endif

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (view->rwlock);
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

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_record_update() view %s delete query=%s\n",view->name, query);
#endif

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (view->rwlock);
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

  gsize modified = dupin_date_timestamp_now (0);

  query = sqlite3_mprintf ("UPDATE Dupin SET key='%q', pid='%q', obj='%q', tm='%" G_GSIZE_FORMAT "' WHERE rowid=%q ;",
				key,
				pid_serialized,
				value,
				modified,
				replace_rowid_str);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_record_update() view %s update query=%s\n",view->name, query);
#endif

  if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (view->rwlock);
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

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_record_update() view %s select query=%s\n",view->name, query);
#endif

  if (sqlite3_exec (view->db, query, dupin_view_sync_record_update_cb, &id, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (view->rwlock);
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
              g_rw_lock_writer_unlock (view->rwlock);

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
      g_rw_lock_writer_unlock (view->rwlock);

      g_free (id);
      if (pid_serialized)
        g_free (pid_serialized);

      return;
    }

  g_rw_lock_writer_unlock (view->rwlock);

  g_free (id);
  if (pid_serialized)
    g_free (pid_serialized);
}

static gboolean
dupin_view_sync_thread_reduce (DupinView * view,
			       gsize count,
			       gboolean rereduce,
			       gchar * matching_key,
			       gboolean * reduce_error)
{
  if (dupin_view_engine_get_reduce_code (view->engine) == NULL)
    return FALSE;

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_thread_reduce(%p/%s) count=%d\n",g_thread_self (), view->name, (gint)count);
#endif

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
  g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, query, dupin_view_sync_cb, &sync_reduce_id, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (view->rwlock);

      g_error("dupin_view_sync_thread_reduce: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  previous_sync_reduce_id = g_strdup (sync_reduce_id);

  g_rw_lock_reader_unlock (view->rwlock);

  gsize start_rowid = (sync_reduce_id != NULL) ? (gsize) g_ascii_strtoll (sync_reduce_id, NULL, 10)+1 : 1;

  if (dupin_view_record_get_list (view, count, 0, start_rowid, 0, (rereduce) ? DP_ORDERBY_KEY : DP_ORDERBY_ROWID, FALSE,
					NULL, matching_key, matching_key, TRUE, NULL, NULL, TRUE,
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

  g_rw_lock_reader_lock (view->rwlock);
  gsize sync_reduce_processed_count = view->sync_reduce_processed_count;
  gsize sync_reduce_total_records = view->sync_reduce_total_records;
  g_rw_lock_reader_unlock (view->rwlock);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_thread_reduce(%p/%s)    g_list_length (results) = %d start_rowid=%d - reduced %d of total to reduce=%d\n", g_thread_self (), view->name, (gint) g_list_length (results), (gint)start_rowid, (gint)sync_reduce_processed_count, (gint)sync_reduce_total_records );
#endif

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

#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_thread_reduce(%p/%s) sync_reduce_id=%s\n",g_thread_self (), view->name, sync_reduce_id);
#endif

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

#if DUPIN_VIEW_DEBUG
      g_message("view %s key_string =%s\n",view->name, key_string);
#endif

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
#if DUPIN_VIEW_DEBUG
          //DUPIN_UTIL_DUMP_JSON("Key did exist \n",reduce_parameters_obj_key);
#endif

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
      JsonNode * reduce_parameters_obj_key_values_i = NULL;
      JsonNode * record_obj = dupin_view_record_get (list->data);
      if (record_obj != NULL)
        {
          reduce_parameters_obj_key_values_i = json_node_copy (record_obj);
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

#if DUPIN_VIEW_DEBUG
  DUPIN_UTIL_DUMP_JSON ("REDUCE parameters", reduce_parameters);
#endif

  nodes = json_object_get_members (reduce_parameters_obj);
  for (n = nodes; n != NULL; n = n->next)
    {
      gchar *member_name = (gchar *) n->data;

      /* call reduce for each group of keys */

      /* call function(keys, values, rereduce)  values = [ v1, v2... vN ] */

      JsonNode * result = dupin_view_engine_record_reduce (view->engine,
						           (rereduce) ? NULL : json_object_get_member (json_node_get_object 
										(json_object_get_member (reduce_parameters_obj, member_name)), DUPIN_VIEW_KEYS),
						           json_object_get_member (json_node_get_object
										(json_object_get_member (reduce_parameters_obj, member_name)), DUPIN_VIEW_VALUES),
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
              if (! (response_node = dupin_view_output_insert (view, result)))
                {
	          json_node_free (result);
	          json_node_free (pid_node);
	          ret = FALSE;
                  break;
                }
            }

          if (response_node != NULL)
            json_node_free (response_node);

          gchar * value_string = dupin_util_json_serialize (result);
          if (value_string == NULL)
            {
	      json_node_free (result);
	      json_node_free (pid_node);
	      ret = FALSE;
              break;
            }

#if DUPIN_VIEW_DEBUG
          g_message ("dupin_view_sync_thread_reduce: view %s KEY: %s", view->name, member_name);
	  DUPIN_UTIL_DUMP_JSON ("dupin_view_sync_thread_reduce: PID", pid_node);
	  DUPIN_UTIL_DUMP_JSON ("dupin_view_sync_thread_reduce: OBJ", result);
#endif

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

          g_rw_lock_writer_lock (view->rwlock);
          view->sync_reduce_processed_count++;
          sync_reduce_processed_count = view->sync_reduce_processed_count;
          g_rw_lock_writer_unlock (view->rwlock);
        }
      else
        {
	  *reduce_error = TRUE;
	  ret = FALSE;
          break;
	}
    }
  g_list_free (nodes);

  if (previous_sync_reduce_id != NULL)
    g_free(previous_sync_reduce_id);
  
  json_node_free (reduce_parameters); /* it shoulf freee the whole tree of objects, arrays and value ... */

  dupin_view_record_get_list_close (results);

  g_rw_lock_reader_lock (view->rwlock);
  sync_reduce_processed_count = view->sync_reduce_processed_count;
  sync_reduce_total_records = view->sync_reduce_total_records;
  g_rw_lock_reader_unlock (view->rwlock);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_thread_reduce(%p/%s) finished last_reduce_rowid=%s - reduced %d of total to reduce=%d\n", g_thread_self (), view->name, sync_reduce_id, (gint)sync_reduce_processed_count, (gint)sync_reduce_total_records);
#endif

  str = sqlite3_mprintf ("UPDATE DupinView SET sync_reduce_id = '%q'", sync_reduce_id); /* is the ROWID we stopped */

  if (sync_reduce_id != NULL)
    g_free(sync_reduce_id);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_thread_reduce() view %s query=%s\n",view->name, str);
#endif

  g_rw_lock_writer_lock (view->rwlock);

  if (dupin_view_begin_transaction (view, NULL) < 0)
    {
      g_rw_lock_writer_unlock (view->rwlock);
      sqlite3_free (str);

      return FALSE;
    }

  if (sqlite3_exec (view->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (view->rwlock);
      sqlite3_free (str);

      g_error("dupin_view_sync_thread_reduce: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_rollback_transaction (view, NULL);

      return FALSE;
    }

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      g_rw_lock_writer_unlock (view->rwlock);
      sqlite3_free (str);

      return FALSE;
    }

  g_rw_lock_writer_unlock (view->rwlock);

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

static int
dupin_view_sync_total_rereduce_cb (void *data, int argc, char **argv,
                                  char **col)
{
  struct dupin_view_sync_total_rereduce_t * rere = data;
 
  if (argv[0] && *argv[0])
    rere->first_matching_key = g_strdup (argv[0]);

  if (argv[1] && *argv[1])
    rere->total = (gsize) g_ascii_strtoll (argv[1], NULL, 10);

#if DUPIN_VIEW_DEBUG
  g_message ("dupin_view_sync_total_rereduce_cb(): first_matching_key='%s' total='%d'\n", rere->first_matching_key, (gint)rere->total);
#endif

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

  g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, tmp, dupin_view_sync_total_rereduce_cb, rere, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (view->rwlock);
      sqlite3_free (tmp);

      g_error("dupin_view_sync_total_rereduce: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_rw_lock_reader_unlock (view->rwlock);

  sqlite3_free (tmp);

  return TRUE;
}

void
dupin_view_sync_map_func (gpointer data, gpointer user_data)
{

#if DUPIN_VIEW_BENCHMARK
  gsize start_time = dupin_date_timestamp_now (0);
#endif

  gchar * errmsg;

  DupinView * view = (DupinView*) data;

  dupin_view_ref (view);

  g_rw_lock_writer_lock (view->rwlock);
  view->sync_map_thread = g_thread_self ();
  view->sync_map_processed_count = 0;
  gsize sync_map_processed_count = view->sync_map_processed_count;
  g_rw_lock_writer_unlock (view->rwlock);

  g_rw_lock_reader_lock (view->rwlock);
  gboolean sync_toquit = view->sync_toquit;
  gboolean todelete = view->todelete;
  GThread * sync_reduce_thread = view->sync_reduce_thread;
  g_rw_lock_reader_unlock (view->rwlock);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_map_func(%p/%s) started\n",g_thread_self (), view->name);
#endif

  while (sync_toquit == FALSE && todelete == FALSE)
    {
      gboolean map_operation = dupin_view_sync_thread_map (view, VIEW_SYNC_COUNT);

      g_rw_lock_reader_lock (view->rwlock);
      sync_map_processed_count = view->sync_map_processed_count;
      sync_reduce_thread = view->sync_reduce_thread;
      g_rw_lock_reader_unlock (view->rwlock);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_map_func(%p/%s) map_operation=%d\n", g_thread_self (), view->name, map_operation);
#endif

      /* NOTE - make sure waiting reduce thread is started as soon as the first set of mapped results is ready 
                 the sync_map_processed_count is set to the total of mapped results so far and used for very basic IPC map -> reduce threads */

      if (dupin_view_engine_get_reduce_code (view->engine) != NULL)
        {
#if DUPIN_VIEW_DEBUG
   	  g_message("dupin_view_sync_map_func(%p/%s) Mapped %d records - sending signal to reduce thread (%p)\n", g_thread_self (), view->name, (gint)sync_map_processed_count, sync_reduce_thread);
#endif

	  g_mutex_lock (view->mutex);
	  g_cond_signal(view->sync_map_has_new_work);
	  g_mutex_unlock (view->mutex);
        }

      if (map_operation == FALSE)
        {
#if DUPIN_VIEW_DEBUG
	  g_message("dupin_view_sync_map_func(%p/%s) Mapped TOTAL %d records\n", g_thread_self (), view->name, (gint)sync_map_processed_count);
#endif

          break;
        }

      g_rw_lock_reader_lock (view->rwlock);
      sync_toquit = view->sync_toquit;
      todelete = view->todelete;
      g_rw_lock_reader_unlock (view->rwlock);
    }

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_map_func(%p/%s) finished and view map part is in sync\n",g_thread_self (), view->name);
#endif

  /* NOTE - make sure reduce thread can terminate too eventually */
  if (dupin_view_engine_get_reduce_code (view->engine) != NULL)
    {
#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_map_func(%p/%s) Sending signal to reduce thread (%p)\n", g_thread_self (), view->name, sync_reduce_thread);
#endif

      g_mutex_lock (view->mutex);
      g_cond_signal(view->sync_map_has_new_work);
      g_mutex_unlock (view->mutex);
    }
  else if (sync_map_processed_count > 0)
    {
#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_map_func(%p/%s): ANALYZE\n", g_thread_self (), view->name);
#endif

      g_rw_lock_writer_lock (view->rwlock);
      if (sqlite3_exec (view->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_rw_lock_writer_unlock (view->rwlock);
          g_error ("dupin_view_sync_reduce_func: %s", errmsg);
          sqlite3_free (errmsg);
        }
      g_rw_lock_writer_unlock (view->rwlock);
    }

  g_rw_lock_writer_lock (view->rwlock);
  view->tosync = FALSE;
  view->sync_map_thread = NULL;
  g_rw_lock_writer_unlock (view->rwlock);

  dupin_view_unref (view);

#if DUPIN_VIEW_BENCHMARK
  g_message("dupin_view_sync_map_func(%p/%s) finished in %" G_GSIZE_FORMAT " seconds\n",g_thread_self (), view->name, ((dupin_date_timestamp_now (0)-start_time)/G_USEC_PER_SEC));
#endif

}

void
dupin_view_sync_reduce_func (gpointer data, gpointer user_data)
{

#if DUPIN_VIEW_BENCHMARK
  gsize start_time = dupin_date_timestamp_now (0);
#endif

  gchar * query;
  gchar * errmsg;
  struct dupin_view_sync_total_rereduce_t rere_matching;

  GTimeVal endtime = {0,0};
  gint64 endtime_usec;
  gboolean reduce_wait_timed_out;

  DupinView * view = (DupinView*) data;

  dupin_view_ref (view);

  g_rw_lock_writer_lock (view->rwlock);
  view->tosync = TRUE;
  view->sync_reduce_thread = g_thread_self ();
  view->sync_reduce_processed_count = 0;
  gsize sync_reduce_processed_count = view->sync_reduce_processed_count;
  view->sync_reduce_total_records = 0;
  gsize sync_reduce_total_records = view->sync_reduce_total_records;
  g_rw_lock_writer_unlock (view->rwlock);

  gboolean rereduce = FALSE;

  rere_matching.total = 0;
  rere_matching.first_matching_key = NULL;

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_reduce_func(%p/%s) started", g_thread_self (), view->name);
#endif

  /* NOTE - if map hangs, reduce also hangs - for the moment we should make sure a _rest method is allowed on views to avoid disasters */

  /* TODO - added processing step when restarted and sync_reduce_id is set to the ID of view table latest record processed, and continue
            and when done, wait for another bunch ... */

  query = "SELECT sync_rereduce as c FROM DupinView LIMIT 1";

  g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, query, dupin_view_rereduce_cb, &rereduce, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (view->rwlock);

      g_error("dupin_view_sync_reduce_func: %s", errmsg);
      sqlite3_free (errmsg);

      dupin_view_unref (view);

      return;
    }

  g_rw_lock_reader_unlock (view->rwlock);

  g_rw_lock_reader_lock (view->rwlock);
  gboolean sync_toquit = view->sync_toquit;
  gboolean todelete = view->todelete;
  GThread * sync_map_thread = view->sync_map_thread;
  gsize sync_map_processed_count = view->sync_map_processed_count;
  g_rw_lock_reader_unlock (view->rwlock);

  while (sync_toquit == FALSE && todelete == FALSE)
    {
#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_reduce_func(%p/%s) rereduce=%d\n", g_thread_self (), view->name, rereduce);
#endif

      if (rereduce == FALSE)
        {
#if DUPIN_VIEW_DEBUG
          g_message("dupin_view_sync_reduce_func(%p/%s) Going to wait for signal from map thread\n", g_thread_self (), view->name);
#endif

	  /* NOTE - wait for message for a maximum of limit_reduce_timeoutforthread seconds */

	  /* TODO - check if FreeBSD, Apple and Win32 platforms need special care
		    see for example http://skype4pidgin.googlecode.com/svn/trunk/skype_messaging.c */

	  g_mutex_lock (view->mutex);

	  g_get_current_time(&endtime);
	  g_time_val_add(&endtime, view->d->conf->limit_reduce_timeoutforthread * G_USEC_PER_SEC);

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

	  if (reduce_wait_timed_out == FALSE)
	    {
#if DUPIN_VIEW_DEBUG
              g_message("dupin_view_sync_reduce_func(%p/%s) waiting for signal from map thread timed out after %d seconds\n", g_thread_self (), view->name, view->d->conf->limit_reduce_timeoutforthread);
#endif
	    }

#if DUPIN_VIEW_DEBUG
          g_message("dupin_view_sync_reduce_func(%p/%s) CONTINUE after wait\n", g_thread_self (), view->name);
#endif

	  g_mutex_unlock (view->mutex);
        }

      g_rw_lock_reader_lock (view->rwlock);
      sync_map_thread = view->sync_map_thread;
      sync_map_processed_count = view->sync_map_processed_count;
      sync_reduce_total_records = view->sync_reduce_total_records;
      sync_reduce_processed_count = view->sync_reduce_processed_count;
      g_rw_lock_reader_unlock (view->rwlock);

#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync_reduce_func(%p/%s) sync_map_processed_count=%d > sync_reduce_total_records=%d - rereduce=%d\n",g_thread_self (), view->name, (gint)sync_map_processed_count,(gint)sync_reduce_total_records, (gint)rereduce);
#endif

      if (sync_map_processed_count > sync_reduce_total_records /* got a new bunch to work on */
	  || rereduce)
        {
          g_rw_lock_writer_lock (view->rwlock);
          view->sync_reduce_total_records = (rereduce) ? rere_matching.total : sync_map_processed_count;
          sync_reduce_total_records = view->sync_reduce_total_records;
          g_rw_lock_writer_unlock (view->rwlock);

#if DUPIN_VIEW_DEBUG
          g_message("dupin_view_sync_reduce_func(%p/%s) got new records to REDUCE (rereduce=%d, sync_reduce_total_records=%d)\n",g_thread_self (), view->name, (gint)rereduce, (gint)sync_reduce_total_records);
#endif

	  gboolean reduce_error = FALSE;
          while (dupin_view_sync_thread_reduce (view, VIEW_SYNC_COUNT, rereduce, rere_matching.first_matching_key, &reduce_error) == TRUE)
	    {
	      if (reduce_error == TRUE)
	        {
		  break;
		}
	    }

	  if (reduce_error == TRUE)
	    {
              // TODO - log something
              break;
	    }

          g_rw_lock_writer_lock (view->rwlock);
          sync_reduce_processed_count = view->sync_reduce_processed_count;
          sync_reduce_total_records = view->sync_reduce_total_records;
          g_rw_lock_writer_unlock (view->rwlock);

#if DUPIN_VIEW_DEBUG
          g_message("dupin_view_sync_reduce_func(%p/%s) Reduced %d records of %d\n", g_thread_self (), view->name, (gint)sync_reduce_processed_count, (gint)sync_reduce_total_records);
#endif
        }

      if (!sync_map_thread) /* map finished */
        {
#if DUPIN_VIEW_DEBUG
          g_message("dupin_view_sync_reduce_func(%p/%s) Map was finished in meantime\n", g_thread_self (), view->name);
#endif

	  /* check if there is anything to re-reduce */

          rere_matching.total = 0;

          if (rere_matching.first_matching_key != NULL)
            g_free (rere_matching.first_matching_key);

          rere_matching.first_matching_key = NULL;

          dupin_view_sync_total_rereduce (view, &rere_matching);

#if DUPIN_VIEW_DEBUG
          g_message("dupin_view_sync_reduce_func(%p/%s) Done first round of reduce but there are still %d record to re-reduce and first key to process is '%s'\n", g_thread_self (), view->name, (gint)rere_matching.total, rere_matching.first_matching_key);
#endif

          if (rere_matching.total > 0)
            {
              /* still work to do */
              rereduce = TRUE;

#if DUPIN_VIEW_DEBUG
	      g_message("dupin_view_sync_reduce_func(%p/%s) Going to re-reduce\n", g_thread_self (), view->name);
#endif

              g_rw_lock_writer_lock (view->rwlock);

	      if (dupin_view_begin_transaction (view, NULL) < 0)
                {
                  g_rw_lock_writer_unlock (view->rwlock);

                  break;
                }

	      /* TODO - check if sync_reduce_id can be set to an "higher" value after reduce and before re-reduce
			considering that the ROWID of the first key to re-reduce may be >>0 (i.e. thousands of ROWIDs higher) */

              query = "UPDATE DupinView SET sync_reduce_id = '0', sync_rereduce = 'TRUE'";

              if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
                {
                  g_rw_lock_writer_unlock (view->rwlock);

                  g_error("dupin_view_sync_reduce_func: %s", errmsg);
                  sqlite3_free (errmsg);

                  dupin_view_rollback_transaction (view, NULL);

                  break;
                }

	      if (dupin_view_commit_transaction (view, NULL) < 0)
                {
                  g_rw_lock_writer_unlock (view->rwlock);

                  break;
                }

              g_rw_lock_writer_unlock (view->rwlock);
            }
          else
            {
#if DUPIN_VIEW_DEBUG
   	      g_message("dupin_view_sync_reduce_func(%p/%s) Done rereduce=%d\n", g_thread_self (), view->name, (gint)rereduce);
#endif
              rereduce = FALSE;

              query = "UPDATE DupinView SET sync_rereduce = 'FALSE'";

              g_rw_lock_writer_lock (view->rwlock);

	      if (dupin_view_begin_transaction (view, NULL) < 0)
                {
                  g_rw_lock_writer_unlock (view->rwlock);

                  break;
                }

              if (sqlite3_exec (view->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
                {
                  g_rw_lock_writer_unlock (view->rwlock);

                  g_error("dupin_view_sync_reduce_func: %s", errmsg);
                  sqlite3_free (errmsg);

                  dupin_view_rollback_transaction (view, NULL);

                  break;
                }

	      if (dupin_view_commit_transaction (view, NULL) < 0)
                {
                  g_rw_lock_writer_unlock (view->rwlock);

                  break;
                }

              g_rw_lock_writer_unlock (view->rwlock);

	      break; /* both terminated, amen */
            }
        }

      g_rw_lock_reader_lock (view->rwlock);
      sync_toquit = view->sync_toquit;
      todelete = view->todelete;
      g_rw_lock_reader_unlock (view->rwlock);
    }

  if (rere_matching.first_matching_key != NULL)
    g_free (rere_matching.first_matching_key);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_reduce_func(%p/%s) finished to reduce %d total records and view reduce part is in sync\n",g_thread_self (), view->name, (gint)sync_reduce_total_records);
#endif

  /* claim disk space back due reduce did actually remove rows from view table */

#if DUPIN_VIEW_DEBUG
  g_message("dupin_view_sync_reduce_func: view %s VACUUM and ANALYZE\n", view->name);
#endif

  g_rw_lock_writer_lock (view->rwlock);
  if (sqlite3_exec (view->db, "VACUUM", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (view->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (view->rwlock);
      g_error ("dupin_view_sync_reduce_func: %s", errmsg);
      sqlite3_free (errmsg);
    }
  g_rw_lock_writer_unlock (view->rwlock);

  g_rw_lock_writer_lock (view->rwlock);
  view->tosync = FALSE;
  view->sync_reduce_thread = NULL;
  g_rw_lock_writer_unlock (view->rwlock);

  dupin_view_unref (view);

#if DUPIN_VIEW_BENCHMARK
  g_message("dupin_view_sync_reduce_func(%p/%s) finished in %" G_GSIZE_FORMAT " seconds\n",g_thread_self (), view->name, ((dupin_date_timestamp_now (0)-start_time)/G_USEC_PER_SEC));
#endif

}

/* NOTE- we try to spawn two threads map, reduce 
         when reduce is done we re-reduce till no map and reduce is still running,
         finished scan and only one key is left (count=1) */

void
dupin_view_sync (DupinView * view)
{
  g_rw_lock_reader_lock (view->rwlock);
  GThread * sync_map_thread = view->sync_map_thread;
  GThread * sync_reduce_thread = view->sync_reduce_thread;
  g_rw_lock_reader_unlock (view->rwlock);

  if (dupin_view_is_syncing (view))
    {
#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync(%p/%s): view is still syncing sync_map_thread=%p sync_reduce_thread=%p \n", g_thread_self (), view->name, sync_map_thread, sync_reduce_thread);
#endif
    }
  else
    {
      /* TODO - have a master sync thread which manage the all three rather than have chain of
            dependency between map, reduce and re-reduce threads */

#if DUPIN_VIEW_DEBUG
      g_message("dupin_view_sync(%p/%s): push map and reduce threads to respective thread pools\n", g_thread_self (), view->name);
#endif

      GError *error=NULL;

      if (!sync_map_thread)
        {
	  g_thread_pool_push(view->d->sync_map_workers_pool, view, &error);

	  if (error)
            {
              g_error("dupin_view_sync: view %s map thread creation error: %s", view->name, error->message);
	      dupin_view_set_error (view, error->message);
              g_error_free (error);
	    }
        }

      if (dupin_view_engine_get_reduce_code (view->engine) != NULL
          && !sync_reduce_thread)
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

  g_rw_lock_reader_lock (view->rwlock);

  if (view->sync_map_thread
      || view->sync_reduce_thread)
    ret = TRUE;

  g_rw_lock_reader_unlock (view->rwlock);

  return ret;
}

gboolean
dupin_view_is_sync (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (dupin_view_is_syncing (view))
    return FALSE;

  /* TODO - distinguish between tosync because of insert / update or because of explicit view/_sync method call */

  g_rw_lock_reader_lock (view->rwlock);
  gboolean tosync = view->tosync;
  g_rw_lock_reader_unlock (view->rwlock);

  return tosync ? FALSE : TRUE;
}

/* View compaction - basically just SQLite VACUUM and ANALYSE for the moment */

void
dupin_view_compact_func (gpointer data, gpointer user_data)
{
  DupinView * view = (DupinView*) data;
  gchar * errmsg;

  g_rw_lock_reader_lock (view->rwlock);
  gboolean todelete = view->todelete;
  g_rw_lock_reader_unlock (view->rwlock);

  if (todelete == TRUE)
    {
#if DUPIN_VIEW_DEBUG
      //g_message("dupin_view_compact_func(%p) view scheduled for deletion, skipping compaction\n",g_thread_self ());
#endif

      return;
    }

  dupin_view_ref (view);

  /* claim disk space back */

#if DUPIN_VIEW_DEBUG
  //g_message("dupin_view_compact_func(%p) started\n",g_thread_self ());
#endif

  g_rw_lock_writer_lock (view->rwlock);

  view->tocompact = TRUE;
  view->compact_thread = g_thread_self ();
  view->compact_processed_count = 0;

  /* NOTE - make sure last transaction is commited */

  if (dupin_view_commit_transaction (view, NULL) < 0)
    {
      dupin_view_rollback_transaction (view, NULL);
    }

#if DUPIN_VIEW_DEBUG
  //g_message("dupin_view_compact_func: VACUUM and ANALYZE\n");
#endif

  if (sqlite3_exec (view->db, "VACUUM", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (view->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (view->db, "ANALYZE DupinPid2Id", NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_error ("dupin_view_compact_func: %s while vacuum and analyze view", errmsg);
      sqlite3_free (errmsg);
    }
  else
    {
#if DUPIN_VIEW_DEBUG
      //g_message("dupin_view_compact_func(%p) finished and view is compacted\n",g_thread_self ());
#endif
    }

  view->tocompact = FALSE;
  view->compact_thread = NULL;

  g_rw_lock_writer_unlock (view->rwlock);

  dupin_view_unref (view);
}

void
dupin_view_compact (DupinView * view)
{
  g_return_if_fail (view != NULL);

  g_rw_lock_reader_lock (view->rwlock);
  GThread * compact_thread = view->compact_thread;
  g_rw_lock_reader_unlock (view->rwlock);

  if (dupin_view_is_compacting (view))
    {
#if DUPIN_VIEW_DEBUG
      //g_message("dupin_view_compact(%p): view is still compacting compact_thread=%p\n", g_thread_self (), compact_thread);
#endif
    }
  else
    {
#if DUPIN_VIEW_DEBUG
      //g_message("dupin_view_compact(%p): push to thread pools compact_thread=%p\n", g_thread_self (), compact_thread);
#endif

      GError * error=NULL;

      if (!compact_thread)
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

  g_rw_lock_reader_lock (view->rwlock);
  GThread * compact_thread = view->compact_thread;
  g_rw_lock_reader_unlock (view->rwlock);

  if (compact_thread)
    return TRUE;

  return FALSE;
}

gboolean
dupin_view_is_compacted (DupinView * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (dupin_view_is_compacting (view))
    return FALSE;

  g_rw_lock_reader_lock (view->rwlock);
  gboolean tocompact = view->tocompact;
  g_rw_lock_reader_unlock (view->rwlock);

  return tocompact ? FALSE : TRUE;
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
