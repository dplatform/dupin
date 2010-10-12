#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_view_record.h"

#include <stdlib.h>
#include <string.h>

#define DUPIN_VIEW_SQL_EXISTS \
	"SELECT count(id) FROM Dupin WHERE id = '%q' "

#define DUPIN_VIEW_SQL_READ \
	"SELECT pid, obj FROM Dupin WHERE id='%q'"

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
  gchar *tmp;
  gint numb = 0;

  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_EXISTS, id);

  if (lock == TRUE)
    g_mutex_lock (view->mutex);

  sqlite3_exec (view->db, tmp, dupin_view_record_exists_real_cb, &numb, NULL);

  if (lock == TRUE)
    g_mutex_unlock (view->mutex);

  sqlite3_free (tmp);

  return numb > 0 ? TRUE : FALSE;
}

static int
dupin_view_record_read_cb (void *data, int argc, char **argv, char **col)
{
  DupinViewRecord *record = data;
  gint i;

  for (i = 0; i < argc; i++)
    {
      if (!strcmp (col[i], "pid"))
	record->pid = g_strdup (argv[i]);

      else if (!strcmp (col[i], "obj"))
	{
	  record->obj_serialized = g_strdup (argv[i]);
	  record->obj_serialized_len = strlen (argv[i]);
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
      return 0;
    }

  if (lock == TRUE)
    g_mutex_unlock (view->mutex);

  sqlite3_free (tmp);

  if (!record->id)
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
			    gboolean descending, GList ** list,
			    GError ** error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;

  struct dupin_view_record_get_list_t s;

  g_return_val_if_fail (view != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  memset (&s, 0, sizeof (s));
  s.view = view;

  str = g_string_new ("SELECT id FROM Dupin GROUP BY id ORDER BY id");

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

  if (record->pid)
    g_free (record->pid);

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

const gchar *
dupin_view_record_get_pid (DupinViewRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->pid;
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

/* EOF */
