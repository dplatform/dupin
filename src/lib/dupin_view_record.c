#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_view_record.h"

#include <stdlib.h>
#include <string.h>

#define DUPIN_VIEW_SQL_EXISTS \
	"SELECT count(id) FROM Dupin WHERE id = '%q' "

#define DUPIN_VIEW_SQL_TOTAL \
	"SELECT count(*) AS c FROM Dupin AS d"

#define DUPIN_VIEW_SQL_READ \
	"SELECT pid, key, obj, ROWID AS rowid FROM Dupin WHERE id='%q'"

#define DUPIN_VIEW_SQL_EXISTS \
        "SELECT count(id) FROM Dupin WHERE id = '%q' "

static DupinViewRecord *dupin_view_record_read_real (DupinView * view,
						     gchar * id,
						     GError ** error,
						     gboolean lock);
static DupinViewRecord *dupin_view_record_new (DupinView * view, gchar * id);

gboolean
dupin_view_record_exists (DupinView * view, gchar * id)
{
  g_return_val_if_fail (view != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);

  return dupin_view_record_exists_real (view, id, TRUE);
}

static int
dupin_view_record_exists_real_cb (void *data, int argc, char **argv,
				  char **col)
{
  guint *numb = data;

  if (argv[0])
    *numb = atoi (argv[0]);

  return 0;
}

gboolean
dupin_view_record_exists_real (DupinView * view, gchar * id, gboolean lock)
{
  gchar *errmsg;
  gchar *tmp;
  gint numb = 0;

  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_EXISTS, id);

  if (lock == TRUE)
    g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, tmp, dupin_view_record_exists_real_cb, &numb, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
        g_mutex_unlock (view->mutex);

      sqlite3_free (tmp);

      g_error ("dupin_view_record_exists_real: %s", errmsg);

      sqlite3_free (errmsg);

      return FALSE;
    }

  if (lock == TRUE)
    g_mutex_unlock (view->mutex);

  sqlite3_free (tmp);

  return numb > 0 ? TRUE : FALSE;
}

static int
dupin_view_record_get_total_records_cb (void *data, int argc, char **argv,
				  char **col)
{
  gsize *numb = data;

  if (argv[0])
    *numb = atoi (argv[0]);

  return 0;
}

/* NOTE - bear in mind SQLite might be able to store more than gsize total records
          see also ROWID and http://www.sqlite.org/autoinc.html */

gboolean
dupin_view_record_get_total_records (DupinView * view,
				    gsize * total,
				    gchar * start_key,
                                    gchar * end_key,
			    	    gboolean inclusive_end,
                                    GError ** error)
{
  g_return_val_if_fail (view != NULL, FALSE);

  gchar *errmsg;
  gchar *tmp;
  GString *str;

  *total = 0;

  gchar * key_range=NULL;

  str = g_string_new (DUPIN_VIEW_SQL_TOTAL);

  if (start_key!=NULL && end_key!=NULL)
    if (!g_utf8_collate (start_key, end_key) && inclusive_end == TRUE)
      key_range = sqlite3_mprintf (" d.key = '%q' ", start_key);
    else if (inclusive_end == TRUE)
      key_range = sqlite3_mprintf (" d.key >= '%q' AND d.key <= '%q' ", start_key, end_key);
    else
      key_range = sqlite3_mprintf (" d.key >= '%q' AND d.key < '%q' ", start_key, end_key);
  else if (start_key!=NULL)
    {
      key_range = sqlite3_mprintf (" d.key >= '%q' ", start_key);
    }
  else if (end_key!=NULL)
    {
      if (inclusive_end == TRUE)
        key_range = sqlite3_mprintf (" d.key <= '%q' ", end_key);
      else
        key_range = sqlite3_mprintf (" d.key < '%q' ", end_key);
    }

  if (key_range!=NULL)
    g_string_append_printf (str, " WHERE %s ", (key_range!=NULL) ? key_range : "");

  tmp = g_string_free (str, FALSE);
 
//g_message("dupin_view_record_get_total_records() query=%s\n",tmp);

  if (key_range!=NULL)
    sqlite3_free (key_range);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, tmp, dupin_view_record_get_total_records_cb, total, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_free (tmp);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  g_free (tmp);

  return TRUE;
}

static int
dupin_view_record_read_cb (void *data, int argc, char **argv, char **col)
{
  DupinViewRecord *record = data;
  gint i;

  for (i = 0; i < argc; i++)
    {
      if (!g_strcmp0 (col[i], "pid"))
	{
	  record->pid_serialized = g_strdup (argv[i]);
	  record->pid_serialized_len = strlen (argv[i]);
	}
      else if (!g_strcmp0 (col[i], "key"))
	{
	  record->key_serialized = g_strdup (argv[i]);
	  record->key_serialized_len = strlen (argv[i]);
	}
      else if (!g_strcmp0 (col[i], "obj"))
	{
	  record->obj_serialized = g_strdup (argv[i]);
	  record->obj_serialized_len = strlen (argv[i]);
	}
      else if (!g_strcmp0 (col[i], "rowid"))
	{
	  record->rowid = atoi(argv[i]);
        }
    }

  return 0;
}

DupinViewRecord *
dupin_view_record_read (DupinView * view, gchar * id, GError ** error)
{
  g_return_val_if_fail (view != NULL, NULL);
  g_return_val_if_fail (id != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_record_id (id) != FALSE, NULL);

  return dupin_view_record_read_real (view, id, error, TRUE);
}

static DupinViewRecord *
dupin_view_record_read_real (DupinView * view, gchar * id, GError ** error,
			     gboolean lock)
{
  DupinViewRecord *record;
  gchar *errmsg;
  gchar *tmp;

  dupin_view_ref (view);

  record = dupin_view_record_new (view, id);

  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_READ, id);

  if (lock == TRUE)
    g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, tmp, dupin_view_record_read_cb, record, &errmsg)
      != SQLITE_OK)
    {
      if (lock == TRUE)
	g_mutex_unlock (view->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);
      dupin_view_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return NULL;
    }

  if (lock == TRUE)
    g_mutex_unlock (view->mutex);

  sqlite3_free (tmp);

  if (!record->id || !record->rowid)
    {
      dupin_view_record_close (record);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD,
		   "The record '%s' doesn't exist.", id);
      return NULL;
    }

  return record;
}

struct dupin_view_record_get_list_t
{
  DupinView *view;
  GList *list;
};

static int
dupin_view_record_get_list_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_view_record_get_list_t *s = data;
  DupinViewRecord *record;

  if ((record = dupin_view_record_read_real (s->view, argv[0], NULL, FALSE)))
    s->list = g_list_append (s->list, record);

  return 0;
}

gboolean
dupin_view_record_get_list (DupinView * view, guint count, guint offset,
			    gsize rowid_start, gsize rowid_end,
			    DupinOrderByType orderby_type,
			    gboolean descending,
			    gchar * start_key,
			    gchar * end_key,
			    gboolean inclusive_end,
			    GList ** list, GError ** error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;

  gchar * key_range=NULL;

  struct dupin_view_record_get_list_t s;

  g_return_val_if_fail (view != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  memset (&s, 0, sizeof (s));
  s.view = view;

  /* TODO - check if group by key is OK - also when key can be null (I.e. map function returned (null,value) ) */

  /* TODO - double check with http://www.sqlite.org/datatype3.html and http://wiki.apache.org/couchdb/ViewCollation */

  str = g_string_new ("SELECT id FROM Dupin as d");

  if (start_key!=NULL && end_key!=NULL)
    if (!g_utf8_collate (start_key, end_key) && inclusive_end == TRUE)
      key_range = sqlite3_mprintf (" d.key = '%q' ", start_key);
    else if (inclusive_end == TRUE)
      key_range = sqlite3_mprintf (" d.key >= '%q' AND d.key <= '%q' ", start_key, end_key);
    else
      key_range = sqlite3_mprintf (" d.key >= '%q' AND d.key < '%q' ", start_key, end_key);
  else if (start_key!=NULL)
    {
      key_range = sqlite3_mprintf (" d.key >= '%q' ", start_key);
    }
  else if (end_key!=NULL)
    {
      if (inclusive_end == TRUE)
        key_range = sqlite3_mprintf (" d.key <= '%q' ", end_key);
      else
        key_range = sqlite3_mprintf (" d.key < '%q' ", end_key);
    }

  if (rowid_start > 0 && rowid_end > 0)
    g_string_append_printf (str, " WHERE %s %s d.ROWID >= %d AND d.ROWID <= %d ", (key_range!=NULL) ? key_range : "", (key_range!=NULL) ? "AND" : "", (gint)rowid_start, (gint)rowid_end);
  else if (rowid_start > 0)
    g_string_append_printf (str, " WHERE %s %s d.ROWID >= %d ", (key_range!=NULL) ? key_range : "", (key_range!=NULL) ? "AND" : "", (gint)rowid_start);
  else if (rowid_end > 0)
    g_string_append_printf (str, " WHERE %s %s d.ROWID <= %d ", (key_range!=NULL) ? key_range : "", (key_range!=NULL) ? "AND" : "", (gint)rowid_end);
  else if (key_range!=NULL)
    g_string_append_printf (str, " WHERE %s ", key_range);

  if (orderby_type == DP_ORDERBY_KEY)
    {
      str = g_string_append (str, " GROUP BY id ORDER BY d.key"); /* this should never be used for reduce internal operations */
    }
  else if (orderby_type == DP_ORDERBY_ROWID)
    str = g_string_append (str, " GROUP BY id ORDER BY d.ROWID");
  else
    str = g_string_append (str, " GROUP BY id ORDER BY d.ROWID");

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
 
  if (key_range!=NULL)
    sqlite3_free (key_range);

//g_message("dupin_view_record_get_list() query=%s\n",tmp);

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, tmp, dupin_view_record_get_list_cb, &s, &errmsg)
      != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  g_free (tmp);

  *list = s.list;
  return TRUE;
}

void
dupin_view_record_get_list_close (GList * list)
{
  while (list)
    {
      dupin_view_record_close (list->data);
      list = g_list_remove (list, list->data);
    }
}

static DupinViewRecord *
dupin_view_record_new (DupinView * view, gchar * id)
{
  DupinViewRecord *record;

  record = g_malloc0 (sizeof (DupinViewRecord));
  record->view = view;
  record->id = g_strdup (id);

  return record;
}

void
dupin_view_record_close (DupinViewRecord * record)
{
  g_return_if_fail (record != NULL);

  if (record->view)
    dupin_view_unref (record->view);

  if (record->id)
    g_free (record->id);

  if (record->pid_serialized)
    g_free (record->pid_serialized);

  if (record->pid)
    json_node_free (record->pid);

  if (record->key_serialized)
    g_free (record->key_serialized);

  if (record->key)
    json_node_free (record->key);

  if (record->obj_serialized)
    g_free (record->obj_serialized);

  if (record->obj)
    json_node_free (record->obj);

  g_free (record);
}

const gchar *
dupin_view_record_get_id (DupinViewRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->id;
}

gsize
dupin_view_record_get_rowid (DupinViewRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->rowid;
}

JsonNode *
dupin_view_record_get_key (DupinViewRecord * record)
{
  g_return_val_if_fail (record != NULL, NULL);

  /* record->key stays owernship of the view record - the caller eventually need to json_node_copy() it */
  if (record->key)
    return record->key;

  JsonParser * parser = json_parser_new ();

//g_message("dupin_view_record_get_key: key_serialized=%s key_serialized_len=%d\n", record->key_serialized, (gint)record->key_serialized_len);

  /* we do not check any parsing error due we stored earlier, we assume it is sane */
  if (json_parser_load_from_data (parser, record->key_serialized, record->key_serialized_len, NULL) == FALSE)
    goto dupin_view_record_get_key_error;

  record->key = json_node_copy (json_parser_get_root (parser));

  if (parser != NULL)
    g_object_unref (parser);

  /* record->key stays owernship of the view record - the caller eventually need to json_node_copy() it */
  return record->key;

dupin_view_record_get_key_error:

  if (parser != NULL)
    g_object_unref (parser);

  return NULL;
}

JsonNode *
dupin_view_record_get_pid (DupinViewRecord * record)
{
  g_return_val_if_fail (record != NULL, NULL);

  /* record->pid stays owernship of the view record - the caller eventually need to json_node_copy() it */
  if (record->pid)
    return record->pid;

  JsonParser * parser = json_parser_new ();

//g_message("dupin_view_record_get_pid: pid_serialized=%s pid_serialized_len=%d\n", record->pid_serialized, (gint)record->pid_serialized_len);

  /* we do not check any parsing error due we stored earlier, we assume it is sane */
  if (json_parser_load_from_data (parser, record->pid_serialized, record->pid_serialized_len, NULL) == FALSE)
    goto dupin_view_record_get_pid_error;

  record->pid = json_node_copy (json_parser_get_root (parser));

  if (parser != NULL)
    g_object_unref (parser);

  /* record->pid stays owernship of the view record - the caller eventually need to json_node_copy() it */
  return record->pid;

dupin_view_record_get_pid_error:

  if (parser != NULL)
    g_object_unref (parser);

  return NULL;
}

JsonNode *
dupin_view_record_get (DupinViewRecord * record)
{
  g_return_val_if_fail (record != NULL, NULL);

  /* record->obj stays owernship of the view record - the caller eventually need to json_node_copy() it */
  if (record->obj)
    return record->obj;

  JsonParser * parser = json_parser_new ();

  /* we do not check any parsing error due we stored earlier, we assume it is sane */
  if (json_parser_load_from_data (parser, record->obj_serialized, record->obj_serialized_len, NULL) == FALSE)
    goto dupin_view_record_get_error;

  record->obj = json_node_copy (json_parser_get_root (parser));

  if (parser != NULL)
    g_object_unref (parser);

  /* record->obj stays owernship of the view record - the caller eventually need to json_node_copy() it */
  return record->obj;

dupin_view_record_get_error:

  if (parser != NULL)
    g_object_unref (parser);

  return NULL;
}

static int
dupin_view_record_get_max_rowid_cb (void *data, int argc, char **argv,
                                  char **col)
{
  gsize *max_rowid = data;

  if (argv[0])
    *max_rowid = atoi (argv[0]);

  return 0;
}

gboolean
dupin_view_record_get_max_rowid (DupinView * view, gsize * max_rowid)
{
  gchar *query;
  gchar * errmsg=NULL;

  g_return_val_if_fail (view != NULL, FALSE);

  *max_rowid=0;

  query = "SELECT max(ROWID) as max_rowid FROM Dupin";

  g_mutex_lock (view->mutex);

  if (sqlite3_exec (view->db, query, dupin_view_record_get_max_rowid_cb, max_rowid, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (view->mutex);

      g_error("dupin_view_record_get_max_rowid: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (view->mutex);

  return TRUE;
}

/* EOF */
