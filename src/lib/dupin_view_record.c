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
	"SELECT pid, key, obj, tm, ROWID AS rowid FROM Dupin WHERE id='%q'"

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
  gsize *numb = data;

  if (argv[0] && *argv[0])
    *numb = (gsize) g_ascii_strtoll (argv[0], NULL, 10);
  
  return 0;
}

gboolean
dupin_view_record_exists_real (DupinView * view, gchar * id, gboolean lock)
{
  gchar *errmsg;
  gchar *tmp;
  gsize numb = 0;

  tmp = sqlite3_mprintf (DUPIN_VIEW_SQL_EXISTS, id);

  if (lock == TRUE)
    g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, tmp, dupin_view_record_exists_real_cb, &numb, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
        g_rw_lock_reader_unlock (view->rwlock);

      sqlite3_free (tmp);

      g_error ("dupin_view_record_exists_real: %s", errmsg);

      sqlite3_free (errmsg);

      return FALSE;
    }

  if (lock == TRUE)
    g_rw_lock_reader_unlock (view->rwlock);

  sqlite3_free (tmp);

  return numb > 0 ? TRUE : FALSE;
}

static int
dupin_view_record_get_total_records_cb (void *data, int argc, char **argv,
				  char **col)
{
  gsize *numb = data;

  if (argv[0] && *argv[0])
    *numb+=1;

  return 0;
}

/* NOTE - bear in mind SQLite might be able to store more than gsize total records
          see also ROWID and http://www.sqlite.org/autoinc.html */

gboolean
dupin_view_record_get_list_total (DupinView * view,
				  gsize * total,
			          gsize rowid_start, gsize rowid_end,
				  GList * keys,
				  gchar * start_key,
                                  gchar * end_key,
			    	  gboolean inclusive_end,
				  gchar * start_value,
                                  gchar * end_value,
			    	  gboolean inclusive_end_value,
				  gchar * filter_by,
				  DupinFieldsFormatType filter_by_format,
                                  DupinFilterByType filter_op,
                                  gchar * filter_values,
                                  GError ** error)
{
  g_return_val_if_fail (view != NULL, FALSE);

  gchar *errmsg;
  gchar *tmp;
  GString *str;

  *total = 0;

  gchar * key_range=NULL;
  gchar * value_range=NULL;

  //str = g_string_new (DUPIN_VIEW_SQL_TOTAL);
  str = g_string_new ("SELECT id FROM Dupin as d");

  if (keys!=NULL)
    {
      GList * n;
      GString *str = g_string_new (NULL);

      for (n = keys; n != NULL; n = n->next)
        {
          if (n == keys)
            str = g_string_append (str, " ( ");

          gchar * json_key = dupin_util_json_serialize ((JsonNode *) n->data);

          gchar * tmp = sqlite3_mprintf (" d.key = '%q' ", json_key);
          str = g_string_append (str, tmp);
          sqlite3_free (tmp);

          g_free (json_key);

          if (n->next == NULL)
            str = g_string_append (str, " ) ");
          else
            str = g_string_append (str, " OR ");
        }

      gchar * kr = g_string_free (str, FALSE);

      key_range = sqlite3_mprintf ("%s", kr);

      g_free (kr);
    }
  else if (start_key!=NULL && end_key!=NULL)
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

  if (start_value!=NULL && end_value!=NULL)
    if (!g_utf8_collate (start_value, end_value) && inclusive_end_value == TRUE)
      value_range = sqlite3_mprintf (" d.obj = '%q' ", start_value);
    else if (inclusive_end_value == TRUE)
      value_range = sqlite3_mprintf (" d.obj >= '%q' AND d.obj <= '%q' ", start_value, end_value);
    else
      value_range = sqlite3_mprintf (" d.obj >= '%q' AND d.obj < '%q' ", start_value, end_value);
  else if (start_value!=NULL)
    {
      value_range = sqlite3_mprintf (" d.obj >= '%q' ", start_value);
    }
  else if (end_value!=NULL)
    {
      if (inclusive_end_value == TRUE)
        value_range = sqlite3_mprintf (" d.obj <= '%q' ", end_value);
      else
        value_range = sqlite3_mprintf (" d.obj < '%q' ", end_value);
    }

  gchar * op = "WHERE";

  if (rowid_start > 0 && rowid_end > 0)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s d.ROWID >= %d AND d.ROWID <= %d ", op, (gint)rowid_start, (gint)rowid_end);
      op = "AND";
    }
  else if (rowid_start > 0)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s d.ROWID >= %d ", op, (gint)rowid_start);
      op = "AND";
    }
  else if (rowid_end > 0)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s d.ROWID <= %d ", op, (gint)rowid_end);
      op = "AND";
    }

  if (key_range != NULL)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s %s ", op, key_range);
      op = "AND";
    }

  if (value_range != NULL)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s %s ", op, value_range);
      op = "AND";
    }

  if (filter_by != NULL
      && g_strcmp0 (filter_by, ""))
    {
      gchar * filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS;
      if (filter_op == DP_FILTERBY_EQUALS)
        filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS;
      else if (filter_op == DP_FILTERBY_CONTAINS)
        filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS;
      else if (filter_op == DP_FILTERBY_STARTS_WITH)
        filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH;
      else if (filter_op == DP_FILTERBY_PRESENT)
        filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT;
      else if (filter_op == DP_FILTERBY_UNDEF)
        {
          if (filter_values == NULL
              || !g_strcmp0 (filter_values, ""))
            filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT;
        }

      gchar * filter_by_format_string = REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED;
      if (filter_by_format == DP_FIELDS_FORMAT_DOTTED)
        filter_by_format_string = REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED;
      else if (filter_by_format == DP_FIELDS_FORMAT_JSONPATH)
        filter_by_format_string = REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_JSONPATH;

      gchar * tmp2 = sqlite3_mprintf (" %s filterBy('%s','%s','%s',obj,'%s') ", op,
                                                                filter_by,
                                                                filter_by_format_string,
                                                                filter_op_string,
                                                                filter_values);
      str = g_string_append (str, tmp2);
      sqlite3_free (tmp2);
    }

  tmp = g_string_free (str, FALSE);
 
//g_message("dupin_view_record_get_total_records() query=%s\n",tmp);

  if (key_range!=NULL)
    sqlite3_free (key_range);

  if (value_range!=NULL)
    sqlite3_free (value_range);

  g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, tmp, dupin_view_record_get_total_records_cb, total, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (view->rwlock);

      g_free (tmp);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);

      return FALSE;
    }

  g_rw_lock_reader_unlock (view->rwlock);

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
	  record->rowid = (gsize) g_ascii_strtoll (argv[i], NULL, 10);
        }
      else if (!g_strcmp0 (col[i], "tm"))
	{
	  record->modified = (gsize) g_ascii_strtoll (argv[i], NULL, 10);

	  gchar * etag = g_strdup_printf ("%" G_GSIZE_FORMAT, record->modified);
	  record->etag_len = strlen(etag);
	  record->etag = g_compute_checksum_for_string (DUPIN_ID_HASH_ALGO, etag, record->etag_len);
	  g_free (etag);
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
    g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, tmp, dupin_view_record_read_cb, record, &errmsg)
      != SQLITE_OK)
    {
      if (lock == TRUE)
	g_rw_lock_reader_unlock (view->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);
      dupin_view_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return NULL;
    }

  if (lock == TRUE)
    g_rw_lock_reader_unlock (view->rwlock);

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

  gchar * pid_serialized = NULL;
  gchar * key_serialized = NULL;
  gchar * obj_serialized = NULL;
  gsize rowid = 0;
  gchar * id = NULL;
  gsize tm = 0;

  gint i;

  for (i = 0; i < argc; i++)
    {
      if (!g_strcmp0 (col[i], "id"))
          id = argv[i];
      else if (!g_strcmp0 (col[i], "pid"))
        {
          pid_serialized = argv[i];
        }
      else if (!g_strcmp0 (col[i], "key"))
        {
          key_serialized = argv[i];
        }
      else if (!g_strcmp0 (col[i], "obj"))
        {
          obj_serialized = argv[i];
        }
      else if (!g_strcmp0 (col[i], "rowid"))
        {
          rowid = (gsize) g_ascii_strtoll (argv[i], NULL, 10);
        }
      else if (!g_strcmp0 (col[i], "tm"))
	{
	  tm = (gsize) g_ascii_strtoll (argv[i], NULL, 10);
        }
    }

  if (id != NULL && rowid)
    {
      g_rw_lock_reader_unlock (s->view->rwlock);

      dupin_view_ref (s->view);

      record = dupin_view_record_new (s->view, id);

      g_rw_lock_reader_lock (s->view->rwlock);

      record->pid_serialized = g_strdup (pid_serialized);
      record->pid_serialized_len = strlen (pid_serialized);
      record->key_serialized = g_strdup (key_serialized);
      record->key_serialized_len = strlen (key_serialized);
      record->obj_serialized = g_strdup (obj_serialized);
      record->obj_serialized_len = strlen (obj_serialized);
      record->rowid = rowid;
      record->modified = tm;

      gchar * etag = g_strdup_printf ("%" G_GSIZE_FORMAT, record->modified);
      record->etag_len = strlen(etag);
      record->etag = g_compute_checksum_for_string (DUPIN_ID_HASH_ALGO, etag, record->etag_len);
      g_free (etag);

      s->list = g_list_append (s->list, record);
    }

  return 0;
}

gboolean
dupin_view_record_get_list (DupinView * view, guint count, guint offset,
			    gsize rowid_start, gsize rowid_end,
			    DupinOrderByType orderby_type,
			    gboolean descending,
			    GList * keys,
			    gchar * start_key,
			    gchar * end_key,
			    gboolean inclusive_end,
		            gchar * start_value,
                            gchar * end_value,
			    gboolean inclusive_end_value,
			    gchar * filter_by,
                            DupinFieldsFormatType filter_by_format,
                            DupinFilterByType filter_op,
                            gchar * filter_values,
			    GList ** list, GError ** error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;

  gchar * key_range=NULL;
  gchar * value_range=NULL;

  struct dupin_view_record_get_list_t s;

  g_return_val_if_fail (view != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  memset (&s, 0, sizeof (s));
  s.view = view;

  /* TODO - check if group by key is OK - also when key can be null (I.e. map function returned (null,value) ) */

  /* TODO - double check with http://www.sqlite.org/datatype3.html and http://wiki.apache.org/couchdb/ViewCollation */

  str = g_string_new ("SELECT id, pid, key, obj, tm, ROWID as rowid FROM Dupin as d");

  if (keys!=NULL)
    {
      GList * n;
      GString *str = g_string_new (NULL);

      for (n = keys; n != NULL; n = n->next)
        {
          if (n == keys)
            str = g_string_append (str, " ( ");

          gchar * json_key = dupin_util_json_serialize ((JsonNode *) n->data);

          gchar * tmp = sqlite3_mprintf (" d.key = '%q' ", json_key);
          str = g_string_append (str, tmp);
          sqlite3_free (tmp);

          g_free (json_key);

          if (n->next == NULL)
            str = g_string_append (str, " ) ");
          else
            str = g_string_append (str, " OR ");
        }

      gchar * kr = g_string_free (str, FALSE);

      key_range = sqlite3_mprintf ("%s", kr);

      g_free (kr);
    }
  else if (start_key!=NULL && end_key!=NULL)
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

  if (start_value!=NULL && end_value!=NULL)
    if (!g_utf8_collate (start_value, end_value) && inclusive_end_value == TRUE)
      value_range = sqlite3_mprintf (" d.obj = '%q' ", start_value);
    else if (inclusive_end_value == TRUE)
      value_range = sqlite3_mprintf (" d.obj >= '%q' AND d.obj <= '%q' ", start_value, end_value);
    else
      value_range = sqlite3_mprintf (" d.obj >= '%q' AND d.obj < '%q' ", start_value, end_value);
  else if (start_value!=NULL)
    {
      value_range = sqlite3_mprintf (" d.obj >= '%q' ", start_value);
    }
  else if (end_value!=NULL)
    {
      if (inclusive_end_value == TRUE)
        value_range = sqlite3_mprintf (" d.obj <= '%q' ", end_value);
      else
        value_range = sqlite3_mprintf (" d.obj < '%q' ", end_value);
    }

  gchar * op = "WHERE";

  if (rowid_start > 0 && rowid_end > 0)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s d.ROWID >= %d AND d.ROWID <= %d ", op, (gint)rowid_start, (gint)rowid_end);
      op = "AND";
    }
  else if (rowid_start > 0)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s d.ROWID >= %d ", op, (gint)rowid_start);
      op = "AND";
    }
  else if (rowid_end > 0)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s d.ROWID <= %d ", op, (gint)rowid_end);
      op = "AND";
    }

  if (key_range != NULL)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s %s ", op, key_range);
      op = "AND";
    }

  if (value_range != NULL)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";
      g_string_append_printf (str, " %s %s ", op, value_range);
      op = "AND";
    }

  if (filter_by != NULL
      && g_strcmp0 (filter_by, ""))
    {
      gchar * filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS;
      if (filter_op == DP_FILTERBY_EQUALS)
        filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS;
      else if (filter_op == DP_FILTERBY_CONTAINS)
        filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS;
      else if (filter_op == DP_FILTERBY_STARTS_WITH)
        filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH;
      else if (filter_op == DP_FILTERBY_PRESENT)
        filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT;
      else if (filter_op == DP_FILTERBY_UNDEF)
        {
          if (filter_values == NULL
              || !g_strcmp0 (filter_values, ""))
            filter_op_string = REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT;
        }

      gchar * filter_by_format_string = REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED;
      if (filter_by_format == DP_FIELDS_FORMAT_DOTTED)
        filter_by_format_string = REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED;
      else if (filter_by_format == DP_FIELDS_FORMAT_JSONPATH)
        filter_by_format_string = REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_JSONPATH;

      gchar * tmp2 = sqlite3_mprintf (" %s filterBy('%s','%s','%s',obj,'%s') ", op,
                                                                filter_by,
                                                                filter_by_format_string,
                                                                filter_op_string,
                                                                filter_values);
      str = g_string_append (str, tmp2);
      sqlite3_free (tmp2);
    }

  if (orderby_type == DP_ORDERBY_KEY)
    {
      str = g_string_append (str, " ORDER BY d.key"); /* this should never be used for reduce internal operations */
    }
  else if (orderby_type == DP_ORDERBY_ROWID)
    str = g_string_append (str, " ORDER BY d.ROWID");
  else
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
 
  if (key_range!=NULL)
    sqlite3_free (key_range);

  if (value_range!=NULL)
    sqlite3_free (value_range);

//g_message("dupin_view_record_get_list() query=%s\n",tmp);

  g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, tmp, dupin_view_record_get_list_cb, &s, &errmsg)
      != SQLITE_OK)
    {
      g_rw_lock_reader_unlock (view->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_rw_lock_reader_unlock (view->rwlock);

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

  if (record->etag)
    g_free (record->etag);

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

gsize
dupin_view_record_get_modified (DupinViewRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->modified;
}

gchar *
dupin_view_record_get_etag (DupinViewRecord *      record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->etag;
}


JsonNode *
dupin_view_record_get_key (DupinViewRecord * record)
{
  g_return_val_if_fail (record != NULL, NULL);

  GError * error = NULL;

  /* record->key stays owernship of the view record - the caller eventually need to json_node_copy() it */
  if (record->key)
    return record->key;

  JsonParser * parser = json_parser_new ();

//g_message("dupin_view_record_get_key: key_serialized=%s key_serialized_len=%d\n", record->key_serialized, (gint)record->key_serialized_len);

  if (!json_parser_load_from_data (parser, record->key_serialized, record->key_serialized_len, &error))
    {
      if (error)
        {
          dupin_view_set_error (record->view, error->message);
          g_error_free (error);
        }
      goto dupin_view_record_get_key_error;
    }

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

  GError * error = NULL;

  /* record->pid stays owernship of the view record - the caller eventually need to json_node_copy() it */
  if (record->pid)
    return record->pid;

  JsonParser * parser = json_parser_new ();

//g_message("dupin_view_record_get_pid: pid_serialized=%s pid_serialized_len=%d\n", record->pid_serialized, (gint)record->pid_serialized_len);

  if (!json_parser_load_from_data (parser, record->pid_serialized, record->pid_serialized_len, &error))
    {
      if (error)
        {
          dupin_view_set_error (record->view, error->message);
          g_error_free (error);
        }
      goto dupin_view_record_get_pid_error;
    }

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

  GError * error = NULL;

  /* record->obj stays owernship of the view record - the caller eventually need to json_node_copy() it */
  if (record->obj)
    return record->obj;

  JsonParser * parser = json_parser_new ();

  if (!json_parser_load_from_data (parser, record->obj_serialized, record->obj_serialized_len, &error))
    {
      if (error)
        {
          dupin_view_set_error (record->view, error->message);
          g_error_free (error);
        }
      goto dupin_view_record_get_error;
    }

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
    *max_rowid = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gboolean
dupin_view_record_get_max_rowid (DupinView * view, gsize * max_rowid, gboolean lock)
{
  gchar *query;
  gchar * errmsg=NULL;

  g_return_val_if_fail (view != NULL, FALSE);

  *max_rowid=0;

  query = "SELECT max(ROWID) as max_rowid FROM Dupin";

  if (lock == TRUE)
    g_rw_lock_reader_lock (view->rwlock);

  if (sqlite3_exec (view->db, query, dupin_view_record_get_max_rowid_cb, max_rowid, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
        g_rw_lock_reader_unlock (view->rwlock);

      g_error("dupin_view_record_get_max_rowid: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  if (lock == TRUE)
    g_rw_lock_reader_unlock (view->rwlock);

  return TRUE;
}

/* EOF */
