#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_record.h"
#include "dupin_utils.h"
#include "dupin_date.h"

#include <string.h>
#include <stdlib.h>

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

static DupinRecord *dupin_record_create_with_id_real (DupinDB * db,
						      JsonNode * obj_node,
						      gchar * id,
						      GError ** error,
						      gboolean lock);
static DupinRecord *dupin_record_read_real (DupinDB * db, gchar * id,
					    GError ** error, gboolean lock);

static void dupin_record_rev_close (DupinRecordRev * rev);
static DupinRecord *dupin_record_new (DupinDB * db, gchar * id);
static gboolean dupin_record_add_revision_obj (DupinRecord * record, guint rev,
					       gchar ** hash,
					       JsonNode * obj_node,
					       gboolean delete,
					       gsize created,
			       		       gboolean ignore_updates_if_unmodified);
static void dupin_record_add_revision_str (DupinRecord * record, guint rev,
					   gchar * hash, gchar * type, gssize hash_size,
					   gchar * obj, gssize size,
					   gboolean delete, gsize created, gsize rowid);
static gboolean
	   dupin_record_generate_hash	(DupinRecord * record,
                            		 gchar * obj_serialized, gssize obj_serialized_len,
			    		 gboolean delete,
			    		 gchar * type,
			    		 gchar ** hash, gsize * hash_len);

int
dupin_record_select_total_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_record_select_total_t *t = data;

  if (argv[0] && *argv[0])
    t->total_doc_ins = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  if (argv[1] && *argv[1])
    t->total_doc_del = (gsize) g_ascii_strtoll (argv[1], NULL, 10);

  return 0;
}

gboolean
dupin_record_exists (DupinDB * db, gchar * id)
{
  g_return_val_if_fail (db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);

  return dupin_record_exists_real (db, id, TRUE);
}

static int
dupin_record_exists_real_cb (void *data, int argc, char **argv, char **col)
{
  gsize *numb = data;

  if (argv[0] && *argv[0])
    *numb = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gboolean
dupin_record_exists_real (DupinDB * db, gchar * id, gboolean lock)
{
  gchar *tmp;
  gchar * errmsg;
  gsize numb = 0;

  tmp = sqlite3_mprintf (DUPIN_DB_SQL_EXISTS, id);

  if (lock == TRUE)
    g_rw_lock_reader_lock (db->rwlock);

  if (sqlite3_exec (db->db, tmp, dupin_record_exists_real_cb, &numb, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
        g_rw_lock_reader_unlock (db->rwlock);

      sqlite3_free (tmp);

      g_error ("dupin_record_exists_real: %s", errmsg);

      sqlite3_free (errmsg);

      return FALSE;
    }

  if (lock == TRUE)
    g_rw_lock_reader_unlock (db->rwlock);

  sqlite3_free (tmp);

  return numb > 0 ? TRUE : FALSE;
}

DupinRecord *
dupin_record_create (DupinDB * db, JsonNode * obj_node, GError ** error)
{
  gchar *id;
  DupinRecord *record;

  g_return_val_if_fail (db != NULL, NULL);
  g_return_val_if_fail (obj_node != NULL, NULL);
  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  g_rw_lock_writer_lock (db->rwlock);

  if (!(id = dupin_database_generate_id_real (db, error, FALSE)))
    {
      g_rw_lock_writer_unlock (db->rwlock);
      return NULL;
    }

  record = dupin_record_create_with_id_real (db, obj_node, id, error, FALSE);

  g_rw_lock_writer_unlock (db->rwlock);
  g_free (id);

  return record;
}

DupinRecord *
dupin_record_create_with_id (DupinDB * db, JsonNode * obj_node, gchar * id,
			     GError ** error)
{
  g_return_val_if_fail (db != NULL, NULL);
  g_return_val_if_fail (obj_node != NULL, NULL);
  g_return_val_if_fail (id != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_record_id (id) != FALSE, NULL);
  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  return dupin_record_create_with_id_real (db, obj_node, id, error, TRUE);
}

static DupinRecord *
dupin_record_create_with_id_real (DupinDB * db, JsonNode * obj_node,
				  gchar * id, GError ** error, gboolean lock)
{
  DupinRecord *record;
  gchar *errmsg;
  gchar *tmp;
  gchar * md5=NULL;

  struct dupin_record_select_total_t t;
  memset (&t, 0, sizeof (t));

  if (lock == FALSE)
    g_rw_lock_writer_unlock (db->rwlock);

  dupin_database_ref (db);

  if (lock == FALSE)
    g_rw_lock_writer_lock (db->rwlock);

  if (lock == TRUE)
    g_rw_lock_writer_lock (db->rwlock);

  record = dupin_record_new (db, id);

  gsize created = dupin_date_timestamp_now (0);

  dupin_record_add_revision_obj (record, 1, &md5, obj_node, FALSE, created, FALSE);

  tmp =
    sqlite3_mprintf (DUPIN_DB_SQL_INSERT, id, 1, md5,
		     record->last->type,
		     record->last->obj_serialized, created);
//g_message("dupin_record_create_with_id_real: query=%s\n", tmp);

  if (dupin_database_begin_transaction (db, error) < 0)
    {
      if (lock == TRUE)
	g_rw_lock_writer_unlock (db->rwlock);

      dupin_record_close (record);
      sqlite3_free (tmp);
      return NULL;
    }

  if (sqlite3_exec (db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
	g_rw_lock_writer_unlock (db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      dupin_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      dupin_database_rollback_transaction (db, error);
      return NULL;
    }

  sqlite3_free (tmp);

  /* NOTE - update totals */

  if (sqlite3_exec (db->db, DUPIN_DB_SQL_GET_TOTALS, dupin_record_select_total_cb, &t, NULL) != SQLITE_OK)
    {
      if (lock == TRUE)
	g_rw_lock_writer_unlock (db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      dupin_record_close (record);
      sqlite3_free (errmsg);
      dupin_database_rollback_transaction (db, error);
      return NULL;
    }

  t.total_doc_ins++;

  tmp = sqlite3_mprintf (DUPIN_DB_SQL_SET_TOTALS, (gint)t.total_doc_ins, (gint)t.total_doc_del);

  if (sqlite3_exec (db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
	g_rw_lock_writer_unlock (db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      dupin_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      dupin_database_rollback_transaction (db, error);
      return NULL;
    }

  if (dupin_database_commit_transaction (db, error) < 0)
    {
      if (lock == TRUE)
        g_rw_lock_writer_unlock (db->rwlock);

      dupin_record_close (record);
      sqlite3_free (tmp);
      return NULL;
    }

  if (lock == TRUE)
    g_rw_lock_writer_unlock (db->rwlock);

  sqlite3_free (tmp);

  dupin_linkbase_p_record_insert (&db->linkbs,
			          (gchar *) dupin_record_get_id (record),
			          json_node_get_object (dupin_record_get_revision_node (record, NULL)));

  dupin_view_p_record_insert (&db->views,
			      (gchar *) dupin_record_get_id (record),
			      json_node_get_object (dupin_record_get_revision_node (record, NULL)));

  return record;
}

static int
dupin_record_read_cb (void *data, int argc, char **argv, char **col)
{
  guint rev = 0;
  gsize tm = 0;
  gchar *hash = NULL;
  gchar *type = NULL;
  gchar *obj = NULL;
  gboolean delete = FALSE;
  gsize rowid=0;
  gint i;

  for (i = 0; i < argc; i++)
    {
      /* shouldn't this be double and use g_ascii_strtoll() ?!? */
      if (!g_strcmp0 (col[i], "rev"))
	rev = atoi (argv[i]);

      else if (!g_strcmp0 (col[i], "tm"))
	tm = (gsize) g_ascii_strtoll (argv[i], NULL, 10);

      else if (!g_strcmp0 (col[i], "hash"))
	hash = argv[i];

      else if (!g_strcmp0 (col[i], "type"))
	type = argv[i];

      else if (!g_strcmp0 (col[i], "obj"))
	obj = argv[i];

      else if (!g_strcmp0 (col[i], "deleted"))
	delete = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;

      else if (!g_strcmp0 (col[i], "rowid"))
	rowid = (gsize) g_ascii_strtoll (argv[i], NULL, 10);
    }

  if (rev && hash !=NULL)
    dupin_record_add_revision_str (data, rev, hash, type, -1, obj, -1, delete, tm, rowid);

  return 0;
}

DupinRecord *
dupin_record_read (DupinDB * db, gchar * id, GError ** error)
{
  g_return_val_if_fail (db != NULL, NULL);
  g_return_val_if_fail (id != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_record_id (id) != FALSE, NULL);

  return dupin_record_read_real (db, id, error, TRUE);
}

static DupinRecord *
dupin_record_read_real (DupinDB * db, gchar * id, GError ** error,
			gboolean lock)
{
  DupinRecord *record;
  gchar *errmsg;
  gchar *tmp;

  if (lock == FALSE)
    g_rw_lock_writer_unlock (db->rwlock);

  dupin_database_ref (db);

  if (lock == FALSE)
    g_rw_lock_writer_lock (db->rwlock);

  record = dupin_record_new (db, id);

  tmp = sqlite3_mprintf (DUPIN_DB_SQL_READ, id);

  if (lock == TRUE)
    g_rw_lock_reader_lock (db->rwlock);

  if (sqlite3_exec (db->db, tmp, dupin_record_read_cb, record, &errmsg) !=
      SQLITE_OK)
    {
      if (lock == TRUE)
	g_rw_lock_reader_unlock (db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);
      dupin_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return NULL;
    }

  if (lock == TRUE)
    g_rw_lock_reader_unlock (db->rwlock);

  sqlite3_free (tmp);

  if (!record->last || !record->last->rowid)
    {
      dupin_record_close (record);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD,
		   "The record '%s' doesn't exist.", id);
      return NULL;
    }

  return record;
}

static int
dupin_record_get_list_total_cb (void *data, int argc, char **argv, char **col)
{
  gsize *numb = data;

  if (argv[0] && *argv[0])
    *numb = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gsize
dupin_record_get_list_total (DupinDB *         db,
			     gsize             rowid_start,
			     gsize             rowid_end,
			     GList *	       keys,
			     gchar *           start_key,
			     gchar *           end_key,
                             gboolean          inclusive_end,
                             DupinCountType    count_type,
			     gchar **          types,
			     DupinFilterByType types_op,
		       	     gchar * 	       filter_by,
		       	     DupinFieldsFormatType filter_by_format,
                             DupinFilterByType filter_op,
			     gchar *           filter_values,
                             GError **         error)
{
  gsize count = 0;
  GString * str;
  gchar *errmsg;
  gchar *query;
  gchar *check_deleted="";

  g_return_val_if_fail (db != NULL, 0);

  gint i;
  if (types != NULL
      && types_op == DP_FILTERBY_EQUALS )
    {
      for (i = 0; types[i]; i++)
        {
          g_return_val_if_fail (dupin_util_is_valid_record_type (types[i]) == TRUE, FALSE);
        }
    }

  gchar * key_range=NULL;

  if (keys!=NULL)
    {
      GList * n;
      GString *str = g_string_new (NULL);

      for (n = keys; n != NULL; n = n->next)
        {
          if (n == keys)
            str = g_string_append (str, " ( ");

	  gchar * json_key = dupin_util_json_serialize ((JsonNode *) n->data);
          gchar * key = dupin_util_json_string_normalize_docid (json_key);

          gchar * tmp = sqlite3_mprintf (" d.id = '%q' ", key);
          str = g_string_append (str, tmp);
          sqlite3_free (tmp);

	  g_free (key);
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
      key_range = sqlite3_mprintf (" d.id = '%q' ", start_key);
    else if (inclusive_end == TRUE)
      key_range = sqlite3_mprintf (" d.id >= '%q' AND d.id <= '%q' ", start_key, end_key);
    else
      key_range = sqlite3_mprintf (" d.id >= '%q' AND d.id < '%q' ", start_key, end_key);
  else if (start_key!=NULL)
    {
      key_range = sqlite3_mprintf (" d.id >= '%q' ", start_key);
    }
  else if (end_key!=NULL)
    {
      if (inclusive_end == TRUE)
        key_range = sqlite3_mprintf (" d.id <= '%q' ", end_key);
      else
        key_range = sqlite3_mprintf (" d.id < '%q' ", end_key);
    }

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  str = g_string_new ("SELECT count(*) FROM Dupin as d WHERE d.rev_head = 'TRUE' ");

  gchar * op = "AND";

  if (rowid_start > 0 && rowid_end > 0)
    {
      g_string_append_printf (str, " %s d.ROWID >= %d AND d.ROWID <= %d ", op, (gint)rowid_start, (gint)rowid_end);
      op = "AND";
    }
  else if (rowid_start > 0)
    {
      g_string_append_printf (str, " %s d.ROWID >= %d ", op, (gint)rowid_start);
      op = "AND";
    }
  else if (rowid_end > 0)
    {
      g_string_append_printf (str, " %s d.ROWID <= %d ", op, (gint)rowid_end);
      op = "AND";
    }

  if (key_range != NULL)
    {
      g_string_append_printf (str, " %s %s ", op, key_range);
      op = "AND";
    }

  if (g_strcmp0 (check_deleted, ""))
    {
      g_string_append_printf (str, " %s %s ", op, check_deleted);
      op = "AND";
    }

  if (types != NULL
      && types_op != DP_FILTERBY_PRESENT)
    {
      if (types[0])
        {
          gchar * tmp2 = sqlite3_mprintf (" %s ( ", op);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
        }

      for (i = 0; types[i]; i++)
        {
          gchar * tmp2;

          if (types_op == DP_FILTERBY_EQUALS)
            tmp2 = sqlite3_mprintf (" d.type = '%q' ", types[i]);
          else if (types_op == DP_FILTERBY_CONTAINS)
            tmp2 = sqlite3_mprintf (" d.type LIKE '%%%q%%' ", types[i]);
          else if (types_op == DP_FILTERBY_STARTS_WITH)
            tmp2 = sqlite3_mprintf (" d.type LIKE '%q%%' ", types[i]);

          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
          if (types[i+1])
            str = g_string_append (str, " OR ");
        }

      if (types[0])
        str = g_string_append (str, " ) ");

      op = "AND";
    }
  else
    {
      if (types_op == DP_FILTERBY_PRESENT)
        {
          gchar * tmp2 = tmp2 = sqlite3_mprintf (" %s ( d.type IS NOT NULL OR d.type != '' ) ", op);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);

          op = "AND";
        }
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

  //str = g_string_append (str, " GROUP BY id");

  query = g_string_free (str, FALSE);

  if (key_range!=NULL)
    sqlite3_free (key_range);

//g_message("dupin_record_get_list_total: query=%s\n", query);

  g_rw_lock_reader_lock (db->rwlock);

  if (sqlite3_exec (db->db, query, dupin_record_get_list_total_cb, &count, &errmsg) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);
      g_free (query);

      return 0;
    }

  g_rw_lock_reader_unlock (db->rwlock);

  g_free (query);

  return count;
}

struct dupin_record_get_list_t
{
  DupinDB *db;
  GList *list;
};

static int
dupin_record_get_list_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_record_get_list_t *s = data;
  DupinRecord *record;

  gchar *id = NULL;
  guint rev = 0;
  gsize tm = 0;
  gchar *hash = NULL;
  gchar *type = NULL;
  gchar *obj = NULL;
  gboolean delete = FALSE;
  gsize rowid=0;
  gint i;

  for (i = 0; i < argc; i++)
    {
      /* shouldn't this be double and use g_ascii_strtoll() ?!? */
      if (!g_strcmp0 (col[i], "rev"))
        rev = atoi (argv[i]);

      else if (!g_strcmp0 (col[i], "tm"))
        tm = (gsize) g_ascii_strtoll (argv[i], NULL, 10);

      else if (!g_strcmp0 (col[i], "hash"))
        hash = argv[i];

      else if (!g_strcmp0 (col[i], "type"))
        type = argv[i];

      else if (!g_strcmp0 (col[i], "obj"))
        obj = argv[i];

      else if (!g_strcmp0 (col[i], "deleted"))
        delete = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;

      else if (!g_strcmp0 (col[i], "rowid"))
        rowid = (gsize) g_ascii_strtoll (argv[i], NULL, 10);

      else if (!g_strcmp0 (col[i], "id"))
        id = argv[i];
    }

  if (rev && hash !=NULL)
    {
      g_rw_lock_reader_unlock (s->db->rwlock);

      dupin_database_ref (s->db);

      g_rw_lock_reader_lock (s->db->rwlock);

      record = dupin_record_new (s->db, id);

      dupin_record_add_revision_str (record, rev, hash, type, -1, obj, -1, delete, tm, rowid);

      s->list = g_list_append (s->list, record);
    }

  return 0;
}

gboolean
dupin_record_get_list (DupinDB * db,
		       guint count,
		       guint offset,
                       gsize rowid_start,
		       gsize rowid_end,
		       GList * keys,
		       gchar * start_key,
		       gchar * end_key,
                       gboolean inclusive_end,
		       DupinCountType count_type,
		       DupinOrderByType orderby_type,
		       gboolean descending,
		       gchar ** types,
		       DupinFilterByType types_op,
		       gchar * filter_by,
		       DupinFieldsFormatType filter_by_format,
                       DupinFilterByType filter_op,
                       gchar * filter_values,
		       GList ** list,
		       GError ** error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;
  gchar *check_deleted="";

  struct dupin_record_get_list_t s;

  g_return_val_if_fail (db != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  gint i;
  if (types != NULL
      && types_op == DP_FILTERBY_EQUALS )
    {
      for (i = 0; types[i]; i++)
        {
          g_return_val_if_fail (dupin_util_is_valid_record_type (types[i]) == TRUE, FALSE);
        }
    }

  memset (&s, 0, sizeof (s));
  s.db = db;

  gchar * key_range=NULL;

  if (keys!=NULL)
    {
      GList * n;
      GString *str = g_string_new (NULL);

      for (n = keys; n != NULL; n = n->next)
        {
          if (n == keys)
            str = g_string_append (str, " ( ");

          gchar * json_key = dupin_util_json_serialize ((JsonNode *) n->data);
          gchar * key = dupin_util_json_string_normalize_docid (json_key);

          gchar * tmp = sqlite3_mprintf (" d.id = '%q' ", key);
          str = g_string_append (str, tmp);
          sqlite3_free (tmp);

          g_free (key);
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
      key_range = sqlite3_mprintf (" d.id = '%q' ", start_key);
    else if (inclusive_end == TRUE)
      key_range = sqlite3_mprintf (" d.id >= '%q' AND d.id <= '%q' ", start_key, end_key);
    else
      key_range = sqlite3_mprintf (" d.id >= '%q' AND d.id < '%q' ", start_key, end_key);
  else if (start_key!=NULL)
    {
      key_range = sqlite3_mprintf (" d.id >= '%q' ", start_key);
    }
  else if (end_key!=NULL)
    {
      if (inclusive_end == TRUE)
        key_range = sqlite3_mprintf (" d.id <= '%q' ", end_key);
      else
        key_range = sqlite3_mprintf (" d.id < '%q' ", end_key);
    }

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  str = g_string_new ("SELECT *, ROWID as rowid FROM Dupin as d WHERE d.rev_head = 'TRUE' ");

  gchar * op = "AND";

  if (rowid_start > 0 && rowid_end > 0)
    {
      g_string_append_printf (str, " %s d.ROWID >= %d AND d.ROWID <= %d ", op, (gint)rowid_start, (gint)rowid_end);
      op = "AND";
    }
  else if (rowid_start > 0)
    {
      g_string_append_printf (str, " %s d.ROWID >= %d ", op, (gint)rowid_start);
      op = "AND";
    }
  else if (rowid_end > 0)
    {
      g_string_append_printf (str, " %s d.ROWID <= %d ", op, (gint)rowid_end);
      op = "AND";
    }

  if (key_range != NULL)
    {
      g_string_append_printf (str, " %s %s ", op, key_range);
      op = "AND";
    }

  if (g_strcmp0 (check_deleted, ""))
    {
      g_string_append_printf (str, " %s %s ", op, check_deleted);
      op = "AND";
    }

  if (types != NULL
      && types_op != DP_FILTERBY_PRESENT)
    {
      if (types[0])
        {
          gchar * tmp2 = sqlite3_mprintf (" %s ( ", op);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
        }

      for (i = 0; types[i]; i++)
        {
          gchar * tmp2;

          if (types_op == DP_FILTERBY_EQUALS)
            tmp2 = sqlite3_mprintf (" d.type = '%q' ", types[i]);
          else if (types_op == DP_FILTERBY_CONTAINS)
            tmp2 = sqlite3_mprintf (" d.type LIKE '%%%q%%' ", types[i]);
          else if (types_op == DP_FILTERBY_STARTS_WITH)
            tmp2 = sqlite3_mprintf (" d.type LIKE '%q%%' ", types[i]);

          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
          if (types[i+1])
            str = g_string_append (str, " OR ");
        }

      if (types[0])
        str = g_string_append (str, " ) ");

      op = "AND";
    }
  else
    {
      if (types_op == DP_FILTERBY_PRESENT)
        {
          gchar * tmp2 = tmp2 = sqlite3_mprintf (" %s ( d.type IS NOT NULL OR d.type != '' ) ", op);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);

          op = "AND";
        }
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

  if (orderby_type == DP_ORDERBY_ROWID)
    //str = g_string_append (str, " GROUP BY id ORDER BY d.ROWID");
    str = g_string_append (str, " ORDER BY d.ROWID");
  else
    //str = g_string_append (str, " GROUP BY id ORDER BY d.ROWID");
    //str = g_string_append (str, " ORDER BY d.ROWID");
    str = g_string_append (str, " ORDER BY d.id");

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

//g_message("dupin_record_get_list() query=%s\n",tmp);

  g_rw_lock_reader_lock (db->rwlock);

  if (sqlite3_exec (db->db, tmp, dupin_record_get_list_cb, &s, &errmsg) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_rw_lock_reader_unlock (db->rwlock);

  g_free (tmp);

  *list = s.list;
  return TRUE;
}

void
dupin_record_get_list_close (GList * list)
{
  while (list)
    {
      dupin_record_close (list->data);
      list = g_list_remove (list, list->data);
    }
}

struct dupin_record_get_revisions_list_t
{
  GList *list;
};

static int
dupin_record_get_revisions_list_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_record_get_revisions_list_t *s = data;

  if (argv[0] && argv[1])
    {
      gchar mvcc[DUPIN_ID_MAX_LEN];
      dupin_util_mvcc_new (atoi(argv[0]), argv[1], mvcc);

      s->list = g_list_append (s->list, g_strdup (mvcc));
    }

  return 0;
}

gboolean
dupin_record_get_revisions_list (DupinRecord * record,
				 guint count, guint offset,
				 gsize rowid_start, gsize rowid_end,
				 DupinCountType         count_type,
				 DupinOrderByType       orderby_type,
				 gboolean descending,
				 GList ** list, GError ** error)
{
  g_return_val_if_fail (record != NULL, FALSE);

  GString *str;
  gchar *tmp;
  gchar *errmsg;
  gchar *check_deleted="";
  gchar * where_id=NULL;

  g_return_val_if_fail (record != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  struct dupin_record_get_revisions_list_t s;
  memset (&s, 0, sizeof (s));

  str = g_string_new ("SELECT rev, hash FROM Dupin as d");

  where_id = sqlite3_mprintf (" WHERE d.id = '%q' ", (gchar *) dupin_record_get_id (record));
  str = g_string_append (str, where_id);
  sqlite3_free (where_id);

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " AND d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " AND d.deleted = 'TRUE' ";

  if (rowid_start > 0 && rowid_end > 0)
    g_string_append_printf (str, " %s AND d.ROWID >= %d AND d.ROWID <= %d ", check_deleted, (gint)rowid_start, (gint)rowid_end);
  else if (rowid_start > 0)
    g_string_append_printf (str, " %s AND d.ROWID >= %d ", check_deleted, (gint)rowid_start);
  else if (rowid_end > 0)
    g_string_append_printf (str, " %s AND d.ROWID <= %d ", check_deleted, (gint)rowid_end);
  else if (g_strcmp0 (check_deleted, ""))
    g_string_append_printf (str, " %s ", check_deleted);

  if (orderby_type == DP_ORDERBY_REV)
    str = g_string_append (str, " ORDER BY d.rev");
  else if (orderby_type == DP_ORDERBY_HASH)
    str = g_string_append (str, " ORDER BY d.hash");
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

//g_message("dupin_record_get_revisions_list() query=%s\n",tmp);

  g_rw_lock_reader_lock (record->db->rwlock);

  if (sqlite3_exec (record->db->db, tmp, dupin_record_get_revisions_list_cb, &s, &errmsg) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (record->db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_rw_lock_reader_unlock (record->db->rwlock);

  g_free (tmp);

  *list = s.list;

  return TRUE;
}

static int
dupin_record_get_total_revisions_cb (void *data, int argc, char **argv,
                                     char **col)
{
  gsize *numb = data;

  if (argv[0] && *argv[0])
    *numb = (gsize) g_ascii_strtoll (argv[0], NULL, 10);

  return 0;
}

gboolean
dupin_record_get_total_revisions (DupinRecord * record,
				  gsize * total,
                                  GError ** error)
{
  g_return_val_if_fail (record != NULL, FALSE);

  GString *str;
  gchar *tmp;
  gchar *errmsg;
  gchar * where_id=NULL;

  guint total_revisions=0;

  str = g_string_new ("SELECT count(*) FROM Dupin as d");

  where_id = sqlite3_mprintf (" WHERE d.id = '%q' ", (gchar *) dupin_record_get_id (record));
  str = g_string_append (str, where_id);
  sqlite3_free (where_id);

  tmp = g_string_free (str, FALSE);

//g_message("dupin_record_get_total_revisions() query=%s\n",tmp);

  g_rw_lock_reader_lock (record->db->rwlock);

  if (sqlite3_exec (record->db->db, tmp, dupin_record_get_total_revisions_cb, &total_revisions, &errmsg) !=
      SQLITE_OK)
    {
      g_rw_lock_reader_unlock (record->db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_rw_lock_reader_unlock (record->db->rwlock);

  g_free (tmp);

  *total = total_revisions;

  return TRUE;
}

void
dupin_record_get_revisions_list_close (GList * list)
{
  while (list)
    {
      g_free (list->data);
      list = g_list_remove (list, list->data);
    }
}

gboolean
dupin_record_update (DupinRecord * record, JsonNode * obj_node,
		     gboolean ignore_updates_if_unmodified,
		     GError ** error)
{
  guint rev;
  gchar *tmp;
  gchar *errmsg;
  gchar * md5=NULL;
  gboolean record_was_deleted = FALSE;

  g_return_val_if_fail (record != NULL, FALSE);
  g_return_val_if_fail (obj_node != NULL, FALSE);
  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);

  g_rw_lock_writer_lock (record->db->rwlock);

  rev = record->last->revision + 1;

  gsize created = dupin_date_timestamp_now (0);

  if (dupin_record_add_revision_obj (record, rev, &md5, obj_node, FALSE, created, ignore_updates_if_unmodified) == FALSE)
    {
      g_rw_lock_writer_unlock (record->db->rwlock);

      return TRUE; // or return FALSE with error like "unchanged" ?
    }

  /* NOTE - flag any previous revision as non head - we need this to optimise searches
	    and avoid slowness of max(rev) as rev or even nested select like
	    rev = (select max(rev) as rev FROM Dupin WHERE id=d.id) ... */

  tmp = sqlite3_mprintf (DUPIN_DB_SQL_UPDATE_REV_HEAD, record->id);

  if (dupin_database_begin_transaction (record->db, error) < 0)
    {
      g_rw_lock_writer_unlock (record->db->rwlock);

      sqlite3_free (tmp);
      return FALSE;
    }

  if (sqlite3_exec (record->db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (record->db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      dupin_database_rollback_transaction (record->db, error);
      return FALSE;
    }

  sqlite3_free (tmp);

  record_was_deleted = record->last->deleted;

  tmp =
    sqlite3_mprintf (DUPIN_DB_SQL_INSERT, record->id, rev, md5,
		     record->last->type,
		     record->last->obj_serialized, created);

//g_message("dupin_record_update: record->last->revision = %d - new rev=%d - query=%s\n", (gint) record->last->revision, (gint) rev, tmp);

  if (sqlite3_exec (record->db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (record->db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      dupin_database_rollback_transaction (record->db, error);
      return FALSE;
    }
  else
    {
      if (record_was_deleted == TRUE)
        {
          /* NOTE - update totals */

          struct dupin_record_select_total_t t;
          memset (&t, 0, sizeof (t));

          if (sqlite3_exec (record->db->db, DUPIN_DB_SQL_GET_TOTALS, dupin_record_select_total_cb, &t, NULL) != SQLITE_OK)
            {
              g_rw_lock_writer_unlock (record->db->rwlock);

              g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

              sqlite3_free (errmsg);
              sqlite3_free (tmp);
              dupin_database_rollback_transaction (record->db, error);
              return FALSE;
            }
          else
            {
              t.total_doc_del--;
              t.total_doc_ins++;

              sqlite3_free (tmp);

              tmp = sqlite3_mprintf (DUPIN_DB_SQL_SET_TOTALS, (gint)t.total_doc_ins, (gint)t.total_doc_del);

              if (sqlite3_exec (record->db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
                {
                  g_rw_lock_writer_unlock (record->db->rwlock);

                  g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

                  sqlite3_free (errmsg);
                  sqlite3_free (tmp);
                  dupin_database_rollback_transaction (record->db, error);
                  return FALSE;
                }
            }
        }
    }

  if (dupin_database_commit_transaction (record->db, error) < 0)
    {
      g_rw_lock_writer_unlock (record->db->rwlock);

      sqlite3_free (tmp);
      return FALSE;
    }

  g_rw_lock_writer_unlock (record->db->rwlock);

  sqlite3_free (tmp);

  dupin_linkbase_p_record_delete (&record->db->linkbs,
			          (gchar *) dupin_record_get_id (record));
  dupin_linkbase_p_record_insert (&record->db->linkbs,
			          (gchar *) dupin_record_get_id (record),
			          json_node_get_object (dupin_record_get_revision_node (record, NULL)));

  dupin_view_p_record_delete (&record->db->views,
			      (gchar *) dupin_record_get_id (record));
  dupin_view_p_record_insert (&record->db->views,
			      (gchar *) dupin_record_get_id (record),
			      json_node_get_object (dupin_record_get_revision_node (record, NULL)));

  return TRUE;
}

gboolean
dupin_record_patch (DupinRecord * record, JsonNode * obj_node,
		    gboolean ignore_updates_if_unmodified,
		    GError ** error)
{
  g_return_val_if_fail (record != NULL, FALSE);
  g_return_val_if_fail (obj_node != NULL, FALSE);
  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);
  /*g_return_val_if_fail (dupin_record_is_deleted (record, dupin_record_get_last_revision (record)) == FALSE, FALSE);*/

  /* MERGE the current revision with the one passed */

  JsonNode * patched_revision = dupin_util_json_node_object_patch (
					dupin_record_get_revision_node (record, dupin_record_get_last_revision (record)),
				     	obj_node);

  if (patched_revision == NULL)
    return FALSE;

  if (dupin_record_update (record, patched_revision, ignore_updates_if_unmodified, error) == FALSE)
    {
      if (patched_revision != NULL)
        json_node_free (patched_revision);

      return FALSE;
    }

  if (patched_revision != NULL)
    json_node_free (patched_revision);

  return TRUE;
}

gboolean
dupin_record_delete (DupinRecord * record, GError ** error)
{
  guint rev;
  gchar *tmp;
  gchar *errmsg;
  gchar * md5=NULL;
  gboolean ret = TRUE;

  g_return_val_if_fail (record != NULL, FALSE);

  if (dupin_record_is_deleted (record, NULL) == TRUE)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD,
		   "Record already deleted");
      return FALSE;
    }

  g_rw_lock_writer_lock (record->db->rwlock);

  rev = record->last->revision + 1;

  gsize created = dupin_date_timestamp_now (0);

  dupin_record_add_revision_obj (record, rev, &md5, NULL, TRUE, created, FALSE);

  /* NOTE - flag any previous revision as non head - we need this to optimise searches
            and avoid slowness of max(rev) as rev or even nested select like
            rev = (select max(rev) as rev FROM Dupin WHERE id=d.id) ... */

  tmp = sqlite3_mprintf (DUPIN_DB_SQL_UPDATE_REV_HEAD, record->id);

  if (dupin_database_begin_transaction (record->db, error) < 0)
    {
      g_rw_lock_writer_unlock (record->db->rwlock);

      sqlite3_free (tmp);
      return FALSE;
    }

  if (sqlite3_exec (record->db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_rw_lock_writer_unlock (record->db->rwlock);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      dupin_database_rollback_transaction (record->db, error);
      return FALSE;
    }

  sqlite3_free (tmp);

  tmp = sqlite3_mprintf (DUPIN_DB_SQL_DELETE, record->id, rev, md5, record->last->type, created);

  if (sqlite3_exec (record->db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      ret = FALSE;

      dupin_database_rollback_transaction (record->db, error);
    }
  else
    {
      /* NOTE - update totals */

      struct dupin_record_select_total_t t;
      memset (&t, 0, sizeof (t));

      if (sqlite3_exec (record->db->db, DUPIN_DB_SQL_GET_TOTALS, dupin_record_select_total_cb, &t, NULL) != SQLITE_OK)
        {
          g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

          sqlite3_free (errmsg);
          ret = FALSE;

          dupin_database_rollback_transaction (record->db, error);
        }
      else
        {
          t.total_doc_del++;
          t.total_doc_ins--;

          sqlite3_free (tmp);

          tmp = sqlite3_mprintf (DUPIN_DB_SQL_SET_TOTALS, (gint)t.total_doc_ins, (gint)t.total_doc_del);

          if (sqlite3_exec (record->db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
            {
              g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

             sqlite3_free (errmsg);
             ret = FALSE;

             dupin_database_rollback_transaction (record->db, error);
           }
       }
    }

  if (ret != FALSE
      && dupin_database_commit_transaction (record->db, error) < 0)
    {
      g_rw_lock_writer_unlock (record->db->rwlock);

      sqlite3_free (tmp);
      return FALSE;
    }

  g_rw_lock_writer_unlock (record->db->rwlock);

  sqlite3_free (tmp);

  dupin_view_p_record_delete (&record->db->views,
			      (gchar *) dupin_record_get_id (record));

  return ret;
}

void
dupin_record_close (DupinRecord * record)
{
  g_return_if_fail (record != NULL);

  if (record->db)
    dupin_database_unref (record->db);

  if (record->id)
    g_free (record->id);

  if (record->revisions)
    g_hash_table_destroy (record->revisions);

  g_free (record);
}

const gchar *
dupin_record_get_id (DupinRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->id;
}

gchar *
dupin_record_get_type (DupinRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->type;
}

gsize
dupin_record_get_rowid (DupinRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->rowid;
}

gsize
dupin_record_get_created (DupinRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->created;
}

gchar *
dupin_record_get_last_revision (DupinRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->mvcc;
}

JsonNode *
dupin_record_get_revision_node (DupinRecord * record, gchar * mvcc)
{
  DupinRecordRev *r;
  GError * error = NULL;

  g_return_val_if_fail (record != NULL, NULL);

  if (mvcc != NULL)
    g_return_val_if_fail (dupin_util_is_valid_mvcc (mvcc) == TRUE, NULL);

  if (mvcc == NULL || (!dupin_util_mvcc_revision_cmp (mvcc, record->last->mvcc)))
    r = record->last;
  else
    {
      /* TODO - check if the following check does make any sense ? */
      if (dupin_util_mvcc_revision_cmp (mvcc,record->last->mvcc) > 0)
	g_return_val_if_fail (dupin_util_mvcc_revision_cmp (dupin_record_get_last_revision (record), mvcc) >= 0 , NULL);

      if (!(r = g_hash_table_lookup (record->revisions, mvcc)))
	return NULL;
    }

  if (r->deleted == TRUE)
    g_return_val_if_fail (dupin_record_is_deleted (record, mvcc) != FALSE,
			  NULL);

  /* r->obj stays owernship of the record revision - the caller eventually need to json_node_copy() it */
  if (r->obj)
    return r->obj;

  JsonParser * parser = json_parser_new();

  /* we do not check any parsing error due we stored earlier, we assume it is sane */
  if (!json_parser_load_from_data (parser, r->obj_serialized, r->obj_serialized_len, &error))
    {
      if (error)
        {
          dupin_database_set_error (record->db, error->message);
          g_error_free (error);
        }
      goto dupin_record_get_revision_error;
    }

  r->obj = json_node_copy (json_parser_get_root (parser));

  if (parser != NULL)
    g_object_unref (parser);

  /* r->obj stays owernship of the record revision - the caller eventually need to json_node_copy() it */
  return r->obj;

dupin_record_get_revision_error:

  if (parser != NULL)
    g_object_unref (parser);

  return NULL;
}

gboolean
dupin_record_is_deleted (DupinRecord * record, gchar * mvcc)
{
  DupinRecordRev *r;

  g_return_val_if_fail (record != NULL, FALSE);

  if (mvcc != NULL)
    g_return_val_if_fail (dupin_util_is_valid_mvcc (mvcc) == TRUE, FALSE);

  if (mvcc == NULL || (!dupin_util_mvcc_revision_cmp (mvcc, record->last->mvcc)))
    r = record->last;
  else
    {
      /* TODO - check if the following check does make any sense ? */
      if (dupin_util_mvcc_revision_cmp (mvcc,record->last->mvcc) > 0)
	g_return_val_if_fail (dupin_util_mvcc_revision_cmp (dupin_record_get_last_revision (record), mvcc) >= 0 , FALSE);

      if (!(r = g_hash_table_lookup (record->revisions, mvcc)))
	return FALSE;
    }

  return r->deleted;
}

/* Internal: */
static DupinRecord *
dupin_record_new (DupinDB * db, gchar * id)
{
  DupinRecord *record;

  record = g_malloc0 (sizeof (DupinRecord));
  record->db = db;
  record->id = g_strdup (id);

  record->revisions =
    g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
			   (GDestroyNotify) dupin_record_rev_close);

  return record;
}

static void
dupin_record_rev_close (DupinRecordRev * rev)
{
  if (rev->obj_serialized)
    g_free (rev->obj_serialized);

  if (rev->hash)
    g_free (rev->hash);

  if (rev->type)
    g_free (rev->type);

  if (rev->mvcc)
    g_free (rev->mvcc);

  if (rev->obj)
    json_node_free (rev->obj);

  g_free (rev);
}

static gboolean
dupin_record_add_revision_obj (DupinRecord * record, guint rev,
			       gchar ** hash,
			       JsonNode * obj_node, gboolean delete,
			       gsize created,
			       gboolean ignore_updates_if_unmodified)
{
  DupinRecordRev *r;
  gchar mvcc[DUPIN_ID_MAX_LEN];
  JsonNode * obj_node_copy = NULL;
  JsonObject * obj = NULL;
  JsonNode * type_node = NULL;
  gchar * type = NULL;
  gchar * obj_serialized = NULL;
  gsize obj_serialized_len = 0;
  gchar * obj_hash;
  gsize obj_hash_len = 0;

  if (obj_node)
    {
      obj_node_copy = json_node_copy (obj_node);

      /* NOTE - check if JSON object has _type and store it separately */
      obj = json_node_get_object (obj_node_copy);

      if (json_object_has_member (obj, REQUEST_OBJ_TYPE) == TRUE)
        {
          type_node = json_object_get_member (obj, REQUEST_OBJ_TYPE);
	  if (json_node_get_node_type (type_node) == JSON_NODE_VALUE
	      && json_node_get_value_type (type_node) == G_TYPE_STRING)
            {
	      type = g_strdup (json_node_get_string (type_node));
            }
	  json_object_remove_member (obj, REQUEST_OBJ_TYPE);
	}

      JsonGenerator * gen = json_generator_new();

      json_generator_set_root (gen, obj_node_copy);

      obj_serialized = json_generator_to_data (gen, &obj_serialized_len);

      g_object_unref (gen);
    }

  dupin_record_generate_hash (record,
			      obj_serialized, obj_serialized_len,
			      delete,
			      type,
			      &obj_hash, &obj_hash_len);

  /* NOTE - ignore updates if they are containing exactly the same object data */

  if (ignore_updates_if_unmodified == TRUE &&
      record->last &&
      !g_strcmp0 (obj_hash, record->last->hash))
    {
      json_node_free (obj_node_copy);
      g_free (obj_serialized);
      g_free (obj_hash);
      g_free (type);

      return FALSE;
    }

  r = g_malloc0 (sizeof (DupinRecordRev));
  r->revision = rev;

  r->type = type;

  if (r->type == NULL
      && record->last
      && record->last->type)
    r->type = g_strdup (record->last->type);

  r->obj = obj_node_copy;
  r->obj_serialized = obj_serialized;
  r->obj_serialized_len = obj_serialized_len;
  r->hash = obj_hash;
  r->hash_len = obj_hash_len;

  *hash = r->hash; // no need to copy - see caller logic

//g_message("dupin_record_add_revision_obj: md5 hash = %s (len=%d)\n", r->hash, (gint)r->hash_len);

  dupin_util_mvcc_new (rev, r->hash, mvcc);

  r->mvcc = g_strdup (mvcc);
  r->mvcc_len = strlen (mvcc);
  r->deleted = delete;
  r->created = created;

  /* TODO - double check that the revision record 'r' is freeded properly when hash table disposed */

  g_hash_table_insert (record->revisions, g_strdup (mvcc), r);

  if (!record->last || (dupin_util_mvcc_revision_cmp (dupin_record_get_last_revision (record), mvcc) < 0 ))
    record->last = r;

  return TRUE;
}

static void
dupin_record_add_revision_str (DupinRecord * record, guint rev, gchar * hash, gchar * type, gssize hash_size,
 			       gchar * str, gssize size, gboolean delete, gsize created, gsize rowid)
{
  DupinRecordRev *r;
  gchar mvcc[DUPIN_ID_MAX_LEN];

  r = g_malloc0 (sizeof (DupinRecordRev));
  r->revision = rev;

  if (hash && *hash)
    {
      if (hash_size < 0)
	hash_size = strlen (hash);

      r->hash = g_strndup (hash, hash_size);
      r->hash_len = hash_size;
    }

  r->type = g_strdup (type);

  if (str && *str)
    {
      if (size < 0)
	size = strlen (str);

      r->obj_serialized = g_strndup (str, size);
      r->obj_serialized_len = size;
    }

  r->deleted = delete;
  r->created = created;
  r->rowid = rowid;

  dupin_util_mvcc_new (rev, r->hash, mvcc);

  r->mvcc = g_strdup (mvcc);
  r->mvcc_len = strlen (mvcc);

  /* TODO - double check that the revision record 'r' is freeded properly when hash table disposed */

  g_hash_table_insert (record->revisions, g_strdup (mvcc), r);

  if (!record->last || (dupin_util_mvcc_revision_cmp (dupin_record_get_last_revision (record), mvcc) < 0 ))
    record->last = r;
}

/* NOTE - compute DUPIN_ID_HASH_ALGO hash of JSON + deleted flag + attachments */

static gboolean
dupin_record_generate_hash (DupinRecord * record,
                            gchar * obj_serialized, gssize obj_serialized_len,
			    gboolean delete,
			    gchar * type,
			    gchar ** hash, gsize * hash_len)
{
  g_return_val_if_fail (record != NULL, FALSE);

  GString * str= g_string_new ("");
  gchar * tmp=NULL;

  /* JSON string */
  str = g_string_append_len (str, obj_serialized, (obj_serialized_len < 0) ? strlen(obj_serialized) : obj_serialized_len);

  /* delete flag */
  g_string_append_printf (str, "%d", (gint)delete);

  /* type */
  g_string_append_printf (str, "%s", type);

  /* attachment hashes for any connected attachment DB */
  DupinAttachmentDBP * p = &record->db->attachment_dbs;

  if (p != NULL)
    {
      gsize i;

      for (i = 0; i < p->numb; i++)
        {
          DupinAttachmentDB *attachment_db = p->attachment_dbs[i];
	  gchar * attachment_hash = NULL;

//g_message("dupin_record_generate_hash: got attachment_db=%p\n", attachment_db);

          if (!(attachment_hash = dupin_attachment_record_get_aggregated_hash (attachment_db, (gchar *) dupin_record_get_id (record))))
            continue;

  	  str = g_string_append (str, attachment_hash);

//g_message("dupin_record_generate_hash: concatenated_hash = %s for id=%s\n",attachment_hash, (gchar *) dupin_record_get_id (record));

	  g_free (attachment_hash);
        }
   }

  tmp = g_string_free (str, FALSE);

  *hash = g_compute_checksum_for_string (DUPIN_ID_HASH_ALGO, tmp, strlen(tmp));
  *hash_len = DUPIN_ID_HASH_ALGO_LEN;

  g_free (tmp);

  return TRUE;
}

/* Insert */

static gchar *
dupin_record_insert_extract_rev (DupinDB * db, JsonNode * obj_node)
{
  g_return_val_if_fail (db != NULL, FALSE);

  gchar * mvcc=NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_OBJ_REV) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_OBJ_REV);

  if (node == NULL
      || json_node_get_node_type  (node) != JSON_NODE_VALUE
      || json_node_get_value_type (node) != G_TYPE_STRING)
    return NULL;

  mvcc = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_OBJ_REV);

  return mvcc;
}

static gchar *
dupin_record_insert_extract_id (DupinDB * db, JsonNode * obj_node)
{
  g_return_val_if_fail (db != NULL, FALSE);

  gchar *id = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_OBJ_ID) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_OBJ_ID);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    id = g_strdup (json_node_get_string (node));
  else
    {
      GString * str = g_string_new (NULL);
      g_string_append_printf (str, "Identifier is of type %s and not string. The system has generated a new ID automatically.", json_node_type_name (node));
      gchar * tmp = g_string_free (str, FALSE);
      dupin_database_set_warning (db, tmp);
      g_free (tmp);
    }

  json_object_remove_member (obj, REQUEST_OBJ_ID);

  return id;
}

static gboolean
dupin_record_insert_extract_deleted (DupinDB * db, JsonNode * obj_node)
{
  g_return_val_if_fail (db != NULL, FALSE);

  gboolean deleted=FALSE;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_OBJ_DELETED) == FALSE)
    return FALSE;

  node = json_object_get_member (obj, REQUEST_OBJ_DELETED);

  if (node == NULL
      || json_node_get_node_type  (node) != JSON_NODE_VALUE
      || json_node_get_value_type (node) != G_TYPE_BOOLEAN)
    return FALSE;

  deleted = json_node_get_boolean (node);

  json_object_remove_member (obj, REQUEST_OBJ_DELETED);

  return deleted;
}

static gboolean
dupin_record_insert_extract_patched (DupinDB * db, JsonNode * obj_node)
{
  g_return_val_if_fail (db != NULL, FALSE);

  gboolean patched=FALSE;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_OBJ_PATCHED) == FALSE)
    return FALSE;

  node = json_object_get_member (obj, REQUEST_OBJ_PATCHED);

  if (node == NULL
      || json_node_get_node_type  (node) != JSON_NODE_VALUE
      || json_node_get_value_type (node) != G_TYPE_BOOLEAN)
    return FALSE;

  patched = json_node_get_boolean (node);

  json_object_remove_member (obj, REQUEST_OBJ_PATCHED);

  return patched;
}

/* insert = create or update */

gboolean
dupin_record_insert (DupinDB * db,
		     JsonNode * obj_node,
                     gchar * id,
		     gchar * caller_mvcc,
                     GList ** response_list,
	             gboolean use_latest_revision,
	             gboolean ignore_updates_if_unmodified,
		     GError ** error)
{
  g_return_val_if_fail (db != NULL, FALSE);

  DupinRecord *record=NULL;

  gchar * mvcc=NULL;
  gchar * json_record_id=NULL;

  JsonNode * links_node=NULL;
  JsonNode * relationships_node=NULL;
  JsonNode * attachments_node=NULL;

  if (caller_mvcc != NULL)
    g_return_val_if_fail (dupin_util_is_valid_mvcc (caller_mvcc) == TRUE, FALSE);

  if (json_node_get_node_type (obj_node) != JSON_NODE_OBJECT)
    {
      dupin_database_set_error (db, "Input must be a JSON object");
      return FALSE;
    }

  gboolean to_delete = dupin_record_insert_extract_deleted (db, obj_node);

  gboolean to_patch = dupin_record_insert_extract_patched (db, obj_node);

  /* NOTE - to_delete takes always precedence */
  if (to_patch == TRUE
      && to_delete == TRUE)
    to_patch = FALSE; 

  JsonNode * record_response_node = json_node_new (JSON_NODE_OBJECT);
  JsonObject * record_response_obj = json_object_new ();
  json_node_take_object (record_response_node, record_response_obj);

  /* fetch the _rev field in the record first, if there */
  mvcc = dupin_record_insert_extract_rev (db, obj_node);

  /* otherwise check passed parameters */
  if (mvcc == NULL
      && caller_mvcc != NULL)
    mvcc = g_strdup (caller_mvcc);

  if ((json_record_id = dupin_record_insert_extract_id (db, obj_node)))
    {
      if (id && g_strcmp0 (id, json_record_id))
        {
          if (mvcc != NULL)
            g_free (mvcc);
          g_free (json_record_id);
          
          dupin_database_set_error (db, "Specified record id does not match");
          if (record_response_node != NULL)
            json_node_free (record_response_node);
          return FALSE;
        }

      id = json_record_id;
    }

  if (mvcc != NULL && !id)
    {
      if (json_record_id != NULL)
        g_free (json_record_id);
      if (mvcc != NULL)
        g_free (mvcc);
      
      dupin_database_set_error (db, "No valid record id or MVCC specified");
      if (record_response_node != NULL)
        json_node_free (record_response_node);
      return FALSE;
    }

  /* get and remove inline _attachments element */
  attachments_node = json_object_get_member (json_node_get_object (obj_node), REQUEST_OBJ_ATTACHMENTS);
  if (attachments_node != NULL)
    {
      attachments_node = json_node_copy (attachments_node);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_ATTACHMENTS);
    }

  /* get and remove inline _links element */
  links_node = json_object_get_member (json_node_get_object (obj_node), REQUEST_OBJ_LINKS);
  if (links_node != NULL)
    {
      links_node = json_node_copy (links_node);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_LINKS);
    }

  /* get and remove inline _relationships element */
  relationships_node = json_object_get_member (json_node_get_object (obj_node), REQUEST_OBJ_RELATIONSHIPS);
  if (relationships_node != NULL)
    {
      relationships_node = json_node_copy (relationships_node);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_RELATIONSHIPS);
    }

  if (mvcc != NULL
      || (id && use_latest_revision == TRUE))
    {
      if (dupin_record_exists (db, id) == TRUE)
	{
          record = dupin_record_read (db, id, error);
	}

      /* NOTE - With this we allow selective update implicitly on the latest version if requested. For example
		to allow incremental updates of a record - this is used in support/dupin_loader
                and made available via the bulk REST API */
      if (mvcc == NULL
          && (id && use_latest_revision == TRUE)
          && record != NULL)
        mvcc = g_strdup (dupin_record_get_last_revision (record)); 

      if (to_delete == TRUE)
        {
          if (!record || dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record))
              || dupin_record_delete (record, error) == FALSE)
            {
              if (record)
                dupin_record_close (record);
              record = NULL;
            }
        }
      else if (record == NULL)
        {
          if (dupin_record_exists (db, id) == FALSE)
            record = dupin_record_create_with_id (db, obj_node, id, error);
          else
            record = NULL;
        }
      else if (to_patch == TRUE)
        {
          if (!record || dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record))
              || dupin_record_patch (record, obj_node, ignore_updates_if_unmodified, error) == FALSE)
            {
              if (record)
                dupin_record_close (record);
              record = NULL;
            }
        }
      else
        {
          if (!record || dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record))
              || dupin_record_update (record, obj_node, ignore_updates_if_unmodified, error) == FALSE)
            {
              if (record)
                dupin_record_close (record);
              record = NULL;
            }
        }
    }
  else if (!id)
    {
      if ( to_delete == TRUE )
        {
          if (record)
            dupin_record_close (record);
          record = NULL;
        }
      else
        {
          record = dupin_record_create (db, obj_node, error);
        }
    }

  else
    {
      if ( to_delete == TRUE )
        {
          if (record)
            dupin_record_close (record);
          record = NULL;
        }
      else
        {
          if (dupin_record_exists (db, id) == FALSE)
            record = dupin_record_create_with_id (db, obj_node, id, error);
          else
            record = NULL;
        }
    }

  if (json_record_id)
    g_free (json_record_id);

  if (!record)
    {
      if (attachments_node != NULL)
        json_node_free (attachments_node);
      if (links_node != NULL)
        json_node_free (links_node);
      if (relationships_node != NULL)
        json_node_free (relationships_node);
      
      if (error != NULL && *error != NULL)
        dupin_database_set_error (db, (*error)->message);
      else
        {
          if (to_delete == TRUE)
            {
              if (mvcc == NULL)
                dupin_database_set_error (db, "Deleted flag not allowed on record creation");
              else
                dupin_database_set_error (db, "Cannot delete record");
            }
          else if (mvcc != NULL)
            dupin_database_set_error (db, "Cannot update record");
          else
	    {
	      if (id)
	        {
                  dupin_database_set_error (db, "The record could not be created, it already exists.");
		  g_set_error (error, dupin_error_quark (), DUPIN_ERROR_RECORD_CONFLICT, "%s", dupin_database_get_error (db));
		}
	      else
                dupin_database_set_error (db, "Cannot insert record");
	    }
        }

      if (mvcc != NULL)
        g_free (mvcc);

      if (record_response_node != NULL)
        json_node_free (record_response_node);

      return FALSE;
    }

  /* process _attachments object for inline attachments */

  if (attachments_node != NULL
      && json_node_get_node_type (attachments_node) == JSON_NODE_OBJECT)
    {
//g_message("process _attachments object for inline attachments\n");

      JsonObject * attachments_obj = json_node_get_object (attachments_node);

      GList *n;
      GList *nodes = json_object_get_members (attachments_obj);

      gboolean found_valid_attachments = FALSE;

      for (n = nodes; n != NULL; n = n->next)
        {
          gchar *member_name = (gchar *) n->data;
          JsonNode *inline_attachment_node = json_object_get_member (attachments_obj, member_name);

          if (json_node_get_node_type (inline_attachment_node) != JSON_NODE_OBJECT)
            {
              /* TODO - should log something or fail ? */
              continue;
            }

          JsonObject *inline_attachment_obj = json_node_get_object (inline_attachment_node);

          gboolean attachment_to_delete = FALSE;
          if (json_object_has_member (inline_attachment_obj, REQUEST_OBJ_DELETED) == TRUE)
            attachment_to_delete = json_object_get_boolean_member (inline_attachment_obj, REQUEST_OBJ_DELETED);

          gboolean stub = FALSE;
          if (json_object_has_member (inline_attachment_obj, REQUEST_OBJ_INLINE_ATTACHMENTS_STUB) == TRUE)
            stub = json_object_get_boolean_member (inline_attachment_obj, REQUEST_OBJ_INLINE_ATTACHMENTS_STUB);

          if (stub == TRUE)
            {
              if (attachment_to_delete == TRUE)
                {
                  // TODO - warning we used _deleted: true while we meant to ingnore attachment ? */
                  continue;
                }
              else
                {
                  // do we need to update attachment metadata in any way ? */
                  continue;
                }
            }

          gchar * content_type = NULL;
          guchar * buff = NULL;
          const void * buff_ref = NULL;

          gsize buff_size;
          if (attachment_to_delete == FALSE)
            {
              content_type = (gchar *) json_object_get_string_member (inline_attachment_obj, REQUEST_OBJ_INLINE_ATTACHMENTS_TYPE);
              gchar * data = (gchar *) json_object_get_string_member (inline_attachment_obj, REQUEST_OBJ_INLINE_ATTACHMENTS_DATA);

              /* decode base64 assuming data is a single (even if long) line/string */
              if (data != NULL)
                buff = g_base64_decode ((const gchar *)data, &buff_size);

              if (content_type == NULL
                  || data == NULL
                  || buff == NULL)
                {
                  if (buff != NULL)
                    g_free (buff);

                  /* TODO - should log something or fail ? */
                  continue;
                }

              buff_ref = (const void *) buff;
            }

          /* NOTE - ignore unmodified attachment */

	  DupinAttachmentRecord * attachment = NULL;
 
	  if (ignore_updates_if_unmodified == TRUE &&
              attachment_to_delete == FALSE &&
              (dupin_attachment_record_exists (dupin_database_get_default_attachment_db (db), (gchar *) dupin_record_get_id (record), member_name) == TRUE) &&
              (attachment = dupin_attachment_record_read (dupin_database_get_default_attachment_db (db), (gchar *) dupin_record_get_id (record), member_name, NULL)) &&
              (dupin_attachment_record_is_unmodified (attachment, member_name, buff_size, content_type, &buff_ref) == TRUE))
            {
              if (buff != NULL)
                g_free (buff);

              dupin_attachment_record_close (attachment);

              continue;
            }

          /* NOTE - store inline attachment as normal one */

          if ( (attachment_to_delete == TRUE
                && dupin_attachment_record_exists (dupin_database_get_default_attachment_db (db), (gchar *) dupin_record_get_id (record), member_name) == FALSE)
              || dupin_attachment_record_delete (dupin_database_get_default_attachment_db (db), (gchar *) dupin_record_get_id (record), member_name) == FALSE
              || (attachment_to_delete == FALSE
                  && dupin_attachment_record_create (dupin_database_get_default_attachment_db (db), (gchar *) dupin_record_get_id (record),
						     member_name, buff_size, content_type,
                                          	     &buff_ref) == FALSE))
            {
              if (buff != NULL)
                g_free (buff);

              dupin_record_close (record);

              if (attachments_node != NULL)
                json_node_free (attachments_node);
              if (links_node != NULL)
                json_node_free (links_node);
              if (relationships_node != NULL)
                json_node_free (relationships_node);
              if (mvcc != NULL)
                g_free (mvcc);
              
              dupin_database_set_error (db, "Cannot insert, update or delete attachment");

              if (record_response_node != NULL)
                json_node_free (record_response_node);

              return FALSE;
            }

          g_free (buff);

          found_valid_attachments = TRUE;
        }

      g_list_free (nodes);

      /* NOTE - need to "touch" (update) the metadata record anyway due we do not use dupin_attachment_record_insert () */

      if (found_valid_attachments == TRUE)
        {
          JsonNode * record_node = NULL;
          JsonNode * record_node_copy = NULL; 
	
	  if ((!(record_node = dupin_record_get_revision_node (record, dupin_record_get_last_revision (record)))) ||
              (!(record_node_copy = json_node_copy (record_node))) ||
              dupin_record_update (record, record_node_copy, FALSE, error) == FALSE)
	    {
	      if (record_node_copy != NULL)
	        json_node_free (record_node_copy);

              dupin_record_close (record);

              if (attachments_node != NULL)
                json_node_free (attachments_node);
              if (links_node != NULL)
                json_node_free (links_node);
              if (relationships_node != NULL)
                json_node_free (relationships_node);
              if (mvcc != NULL)
                g_free (mvcc);
              
              dupin_database_set_error (db, "Cannot update attachment document data");

              if (record_response_node != NULL)
                json_node_free (record_response_node);

              return FALSE;
	    }

	  if (record_node_copy != NULL)
	    json_node_free (record_node_copy);
	}
    }

  /* process _links and _relationships objects for inline links and relationships */

  if (links_node != NULL || relationships_node != NULL)
    {
      GList *n, *nodes;
      gchar * context_id = (gchar *)dupin_record_get_id (record);

      if (links_node != NULL && json_node_get_node_type (links_node) == JSON_NODE_OBJECT)
        {
          JsonNode * record_response_links_node = json_node_new (JSON_NODE_OBJECT);
          JsonObject * record_response_links_object = json_object_new ();
          json_node_take_object (record_response_links_node, record_response_links_object);

          JsonObject * links_obj = json_node_get_object (links_node);
          nodes = json_object_get_members (links_obj);
          for (n = nodes; n != NULL; n = n->next)
            {
              gchar *label = (gchar *) n->data;
              JsonNode *inline_link_node = json_object_get_member (links_obj, label);

              JsonNode * record_response_links_label_node = json_node_new (JSON_NODE_ARRAY);
              JsonArray * record_response_links_label_array = json_array_new ();
              json_node_take_array (record_response_links_label_node, record_response_links_label_array);

              if (json_node_get_node_type (inline_link_node) == JSON_NODE_ARRAY)
                {
                  GList *sn, *snodes;
                  snodes = json_array_get_elements (json_node_get_array (inline_link_node));
                  for (sn = snodes; sn != NULL; sn = sn->next)
                    {
                      GList * links_response_list = NULL;
                      JsonNode * lnode = (JsonNode *) sn->data;
                      gchar * lnode_label = NULL;
                      gchar * lnode_id = NULL;
                      gchar * lnode_rev = NULL;

                      if (json_node_get_node_type (lnode) != JSON_NODE_OBJECT)
                        {
                          /* TODO - should log something or fail ? */
                          continue;
                        }

                      /* add each link with context_id and label */

                      JsonObject * lobj = json_node_get_object (lnode);

                      if (json_object_has_member (lobj, REQUEST_LINK_OBJ_LABEL) == TRUE)
                        lnode_label = (gchar *)json_object_get_string_member (lobj, REQUEST_LINK_OBJ_LABEL);
                      else
                        json_object_set_string_member (lobj, REQUEST_LINK_OBJ_LABEL, label);

                      if (json_object_has_member (lobj, REQUEST_LINK_OBJ_ID) == TRUE)
                        lnode_id = (gchar *)json_object_get_string_member (lobj, REQUEST_LINK_OBJ_ID);

                      if (json_object_has_member (lobj, REQUEST_LINK_OBJ_REV) == TRUE)
                        lnode_rev = (gchar *)json_object_get_string_member (lobj, REQUEST_LINK_OBJ_REV);

//g_message("dupin_record_insert: context_id=%s label=%s lnode_label=%s\n", context_id, label, lnode_label);

                      /* TODO - rework this to report errors to poort user ! perhaps using contextual logging if useful */

                      if ((lnode_label != NULL ) && (g_strcmp0 (lnode_label, label)))
                        {
                          JsonObject * error_obj = json_object_new ();
                          GString * str = g_string_new("");
                          g_string_append_printf (str, "Link record (%s) has an invalid valid label or cannot be added to document", lnode_label);
                          gchar * error_msg = g_string_free (str, FALSE);
                          json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, error_msg);
                          json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, "bad_request");
		 	  if (lnode_id != NULL)
			    json_object_set_string_member (error_obj, RESPONSE_OBJ_ID,lnode_id);
          		  if (lnode_rev != NULL)
            		    json_object_set_string_member (error_obj, RESPONSE_OBJ_REV,lnode_rev);
                          g_free (error_msg);
                          json_array_add_object_element (record_response_links_label_array, error_obj);

                          continue;
                        }

                      if (dupin_link_record_insert (dupin_database_get_default_linkbase (db), lnode, NULL, NULL, 
						    context_id, DP_LINK_TYPE_WEB_LINK, &links_response_list, FALSE, use_latest_revision,
						    ignore_updates_if_unmodified, error) == FALSE)
                        {
                          JsonObject * error_obj = json_object_new ();
                          GString * str = g_string_new("");
                          if (dupin_linkbase_get_error (dupin_database_get_default_linkbase (db)) != NULL)
                            {
                              g_string_append_printf (str, "%s", dupin_linkbase_get_error (dupin_database_get_default_linkbase (db)));
                              dupin_linkbase_clear_error (dupin_database_get_default_linkbase (db));
                            }
                          else
                            {
                              g_string_append_printf (str, "Link record (%s) cannnot be inserted", lnode_label);
                            }
                          gchar * error_msg = g_string_free (str, FALSE);
                          json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, error_msg);
                          json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, "bad_request");
		 	  if (lnode_id != NULL)
			    json_object_set_string_member (error_obj, RESPONSE_OBJ_ID,lnode_id);
          		  if (lnode_rev != NULL)
            		    json_object_set_string_member (error_obj, RESPONSE_OBJ_REV,lnode_rev);
                          g_free (error_msg);
                          json_array_add_object_element (record_response_links_label_array, error_obj);

                          while (links_response_list)
                            {
                              json_node_free (links_response_list->data);
                              links_response_list = g_list_remove (links_response_list, links_response_list->data);
                            }

                          continue;
                        }

//g_message("dupin_record_insert: DONE link context_id=%s label=%s lnode_label=%s\n", context_id, label, lnode_label);

                      while (links_response_list)
                        {
                          json_array_add_element (record_response_links_label_array, (JsonNode *)links_response_list->data);
                          links_response_list = g_list_remove (links_response_list, links_response_list->data);
                        }
                    }
                  g_list_free (snodes);
                }

              if (json_array_get_length (record_response_links_label_array) == 0)
                {
                  json_node_free (record_response_links_label_node);
                }
              else
                {
                  json_object_set_member (record_response_links_object, label, record_response_links_label_node);
                }
            }
          g_list_free (nodes);

          if (json_object_get_size (record_response_links_object) == 0)
            {
              json_node_free (record_response_links_node);
            }
          else
            {
              json_object_set_member (record_response_obj, RESPONSE_OBJ_LINKS, record_response_links_node);
            }
        }

      nodes = NULL;

      if (relationships_node != NULL && json_node_get_node_type (relationships_node) == JSON_NODE_OBJECT)
        {
          JsonNode * record_response_relationships_node = json_node_new (JSON_NODE_OBJECT);
          JsonObject * record_response_relationships_object = json_object_new ();
          json_node_take_object (record_response_relationships_node, record_response_relationships_object);

          JsonObject * relationships_obj = json_node_get_object (relationships_node);
          nodes = json_object_get_members (relationships_obj);
          for (n = nodes; n != NULL; n = n->next)
            {
              gchar *label = (gchar *) n->data;
              JsonNode *inline_relationship_node = json_object_get_member (relationships_obj, label);

              JsonNode * record_response_relationships_label_node = json_node_new (JSON_NODE_ARRAY);
              JsonArray * record_response_relationships_label_array = json_array_new ();
              json_node_take_array (record_response_relationships_label_node, record_response_relationships_label_array);

              if (json_node_get_node_type (inline_relationship_node) == JSON_NODE_ARRAY)
                {
                  GList *sn, *snodes;
                  snodes = json_array_get_elements (json_node_get_array (inline_relationship_node));
                  for (sn = snodes; sn != NULL; sn = sn->next)
                    {
                      GList * relationships_response_list = NULL;
                      JsonNode * rnode = (JsonNode *) sn->data;
                      gchar * rnode_label = NULL;
		      gchar * rnode_id = NULL;
                      gchar * rnode_rev = NULL;

                      if (json_node_get_node_type (rnode) != JSON_NODE_OBJECT)
                        {
                          /* TODO - should log something or fail ? */
                          continue;
                        }

                      /* add each relationship with context_id and label */

                      JsonObject * robj = json_node_get_object (rnode);

                      if (json_object_has_member (robj, REQUEST_LINK_OBJ_LABEL) == TRUE)
                        rnode_label = (gchar *)json_object_get_string_member (robj, REQUEST_LINK_OBJ_LABEL);
                      else
                        json_object_set_string_member (robj, REQUEST_LINK_OBJ_LABEL, label);

                      if (json_object_has_member (robj, REQUEST_LINK_OBJ_ID) == TRUE)
                        rnode_id = (gchar *)json_object_get_string_member (robj, REQUEST_LINK_OBJ_ID);

                      if (json_object_has_member (robj, REQUEST_LINK_OBJ_REV) == TRUE)
                        rnode_rev = (gchar *)json_object_get_string_member (robj, REQUEST_LINK_OBJ_REV);

//g_message("dupin_record_insert: context_id=%s label=%s rnode_label=%s\n", context_id, label, rnode_label);

                      /* TODO - rework this to report errors to poort user ! perhaps using contextual logging if useful */

                      if ((rnode_label != NULL ) && (g_strcmp0 (rnode_label, label)))
                        {
                          JsonObject * error_obj = json_object_new ();
                          GString * str = g_string_new("");
                          g_string_append_printf (str, "Relationship record (%s) has an invalid valid label or cannot be added to document", rnode_label);
                          gchar * error_msg = g_string_free (str, FALSE);
                          json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, error_msg);
                          json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, "bad_request");
		 	  if (rnode_id != NULL)
			    json_object_set_string_member (error_obj, RESPONSE_OBJ_ID,rnode_id);
          		  if (rnode_rev != NULL)
            		    json_object_set_string_member (error_obj, RESPONSE_OBJ_REV,rnode_rev);
                          g_free (error_msg);
                          json_array_add_object_element (record_response_relationships_label_array, error_obj);

                          continue;
                        }

                      if (dupin_link_record_insert (dupin_database_get_default_linkbase (db), rnode, NULL, NULL, 
						    context_id, DP_LINK_TYPE_RELATIONSHIP, &relationships_response_list, FALSE, use_latest_revision, 
						    ignore_updates_if_unmodified, error) == FALSE)
                        {
                          JsonObject * error_obj = json_object_new ();
                          GString * str = g_string_new("");
                          if (dupin_linkbase_get_error (dupin_database_get_default_linkbase (db)) != NULL)
                            {
                              g_string_append_printf (str, "%s", dupin_linkbase_get_error (dupin_database_get_default_linkbase (db)));
                              dupin_linkbase_clear_error (dupin_database_get_default_linkbase (db));
                            }
                          else
                            {
                              g_string_append_printf (str, "Relationship record (%s) cannnot be inserted", rnode_label);
                            }
                          gchar * error_msg = g_string_free (str, FALSE);
                          json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, error_msg);
                          json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, "bad_request");
		 	  if (rnode_id != NULL)
			    json_object_set_string_member (error_obj, RESPONSE_OBJ_ID,rnode_id);
          		  if (rnode_rev != NULL)
            		    json_object_set_string_member (error_obj, RESPONSE_OBJ_REV,rnode_rev);
                          g_free (error_msg);
                          json_array_add_object_element (record_response_relationships_label_array, error_obj);

                          while (relationships_response_list)
                            {
                              json_node_free (relationships_response_list->data);
                              relationships_response_list = g_list_remove (relationships_response_list, relationships_response_list->data);
                            }

                          continue;
                        }

//g_message("dupin_record_insert: DONE relationship context_id=%s label=%s rnode_label=%s\n", context_id, label, rnode_label);

                      while (relationships_response_list)
                        {
                          json_array_add_element (record_response_relationships_label_array, (JsonNode *)relationships_response_list->data);
                          relationships_response_list = g_list_remove (relationships_response_list, relationships_response_list->data);
                        }
                    }
                  g_list_free (snodes);
                }

              if (json_array_get_length (record_response_relationships_label_array) == 0)
                {
                  json_node_free (record_response_relationships_label_node);
                }
              else
                {
                  json_object_set_member (record_response_relationships_object, label, record_response_relationships_label_node);
                }
            }
          g_list_free (nodes);

          if (json_object_get_size (record_response_relationships_object) == 0)
            {
              json_node_free (record_response_relationships_node);
            }
          else
            {
              json_object_set_member (record_response_obj, RESPONSE_OBJ_RELATIONSHIPS, record_response_relationships_node);
            }
        }
    }

  if (attachments_node != NULL)
    json_node_free (attachments_node);

  if (links_node != NULL)
    json_node_free (links_node);

  if (relationships_node != NULL)
    json_node_free (relationships_node);

  if (mvcc != NULL)
    g_free (mvcc);

  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_ID, (gchar *) dupin_record_get_id (record));
  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_REV, dupin_record_get_last_revision (record));

  gchar * created = dupin_date_timestamp_to_http_date (dupin_record_get_created (record));
  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_CREATED, created);
  g_free (created);

  dupin_record_close (record);
  
  *response_list = g_list_prepend (*response_list, record_response_node);

  return TRUE;
}

/* NOTE - receive an object containing an array of objects, and return an array of objects as result */

gboolean
dupin_record_insert_bulk (DupinDB * db,
			  JsonNode * bulk_node,
			  GList ** response_list,
	                  gboolean use_latest_revision,
	                  gboolean ignore_updates_if_unmodified,
			  GError ** error)
{
  g_return_val_if_fail (db != NULL, FALSE);

  JsonObject *obj;
  JsonNode *node;
  JsonArray *array;
  GList *nodes, *n;

  if (json_node_get_node_type (bulk_node) != JSON_NODE_OBJECT)
    {
      dupin_database_set_error (db, "Bulk body must be a JSON object");
      return FALSE;
    }

  obj = json_node_get_object (bulk_node);

  if (json_object_has_member (obj, REQUEST_POST_BULK_DOCS_DOCS) == FALSE)
    {
      dupin_database_set_error (db, "Bulk body does not contain a mandatory " REQUEST_POST_BULK_DOCS_DOCS " object member");
      return FALSE;
    }

  node = json_object_get_member (obj, REQUEST_POST_BULK_DOCS_DOCS);

  if (node == NULL)
    {
      dupin_database_set_error (db, "Bulk body does not contain a valid " REQUEST_POST_BULK_DOCS_DOCS " object member");
      return FALSE;
    }

  if (json_node_get_node_type (node) != JSON_NODE_ARRAY)
    {
      dupin_database_set_error (db, "Bulk body " REQUEST_POST_BULK_DOCS_DOCS " object member is not an array");
      return FALSE;
    }

  array = json_node_get_array (node);

  if (array == NULL)
    {
      dupin_database_set_error (db, "Bulk body " REQUEST_POST_BULK_DOCS_DOCS " object member is not a valid array");
      return FALSE;
    }

  /* scan JSON array */
  nodes = json_array_get_elements (array);

  /* TODO - for further efficency we may avoid the following linkbase and attachment database begin/commit if no
	    links or attachments are added or deleted */

  g_rw_lock_writer_lock (db->rwlock);
  if (dupin_database_begin_transaction (db, NULL) < 0)
    {
      g_rw_lock_writer_unlock (db->rwlock);

      dupin_database_set_error (db, "dupin_record_insert_bulk: Cannot begin database transaction");

      return FALSE;
    }
  g_rw_lock_writer_unlock (db->rwlock);

  g_rw_lock_writer_lock (dupin_database_get_default_attachment_db (db)->rwlock);
  if (dupin_attachment_db_begin_transaction (dupin_database_get_default_attachment_db (db), NULL) < 0)
    {
      g_rw_lock_writer_lock (db->rwlock);
      dupin_database_rollback_transaction (db, NULL);
      g_rw_lock_writer_unlock (db->rwlock);

      g_rw_lock_writer_unlock (dupin_database_get_default_attachment_db (db)->rwlock);

      dupin_database_set_error (db, "dupin_record_insert_bulk: Cannot begin attachment database transaction");

      return FALSE;
    }
  g_rw_lock_writer_unlock (dupin_database_get_default_attachment_db (db)->rwlock);

  g_rw_lock_writer_lock (dupin_database_get_default_linkbase (db)->rwlock);
  if (dupin_linkbase_begin_transaction (dupin_database_get_default_linkbase (db), NULL) < 0)
    {
      g_rw_lock_writer_lock (db->rwlock);
      dupin_database_rollback_transaction (db, NULL);
      g_rw_lock_writer_unlock (db->rwlock);

      g_rw_lock_writer_lock (dupin_database_get_default_attachment_db (db)->rwlock);
      dupin_attachment_db_rollback_transaction (dupin_database_get_default_attachment_db (db), NULL);
      g_rw_lock_writer_unlock (dupin_database_get_default_attachment_db (db)->rwlock);

      g_rw_lock_writer_unlock (dupin_database_get_default_linkbase (db)->rwlock);

      dupin_database_set_error (db, "dupin_record_insert_bulk: Cannot begin linkbase transaction");

      return FALSE;
    }
  g_rw_lock_writer_unlock (dupin_database_get_default_linkbase (db)->rwlock);

  g_rw_lock_writer_lock (db->d->rwlock);
  db->d->bulk_transaction = TRUE;
  g_rw_lock_writer_unlock (db->d->rwlock);
 
  for (n = nodes; n != NULL; n = n->next)
    {
      JsonNode *element_node = (JsonNode*)n->data;
      gchar * id = NULL;
      gchar * rev = NULL;

      if (json_node_get_node_type (element_node) != JSON_NODE_OBJECT)
        {
          dupin_database_set_error (db, "Bulk body " REQUEST_POST_BULK_DOCS_DOCS " array memebr is not a valid JSON object");
          g_list_free (nodes);

          g_rw_lock_writer_lock (dupin_database_get_default_linkbase (db)->rwlock);
          dupin_linkbase_rollback_transaction (dupin_database_get_default_linkbase (db), NULL);
          g_rw_lock_writer_unlock (dupin_database_get_default_linkbase (db)->rwlock);

          g_rw_lock_writer_lock (dupin_database_get_default_attachment_db (db)->rwlock);
          dupin_attachment_db_rollback_transaction (dupin_database_get_default_attachment_db (db), NULL);
          g_rw_lock_writer_unlock (dupin_database_get_default_attachment_db (db)->rwlock);

          g_rw_lock_writer_lock (db->rwlock);
          dupin_database_rollback_transaction (db, NULL);
          g_rw_lock_writer_unlock (db->rwlock);

          g_rw_lock_writer_lock (db->d->rwlock);
          db->d->bulk_transaction = FALSE;
          g_rw_lock_writer_unlock (db->d->rwlock);

          return FALSE;
        }

      if (json_object_has_member (json_node_get_object (element_node), REQUEST_OBJ_ID) == TRUE)
        id = g_strdup ((gchar *)json_object_get_string_member (json_node_get_object (element_node), REQUEST_OBJ_ID));

      if (json_object_has_member (json_node_get_object (element_node), REQUEST_OBJ_REV) == TRUE)
        rev = g_strdup ((gchar *)json_object_get_string_member (json_node_get_object (element_node), REQUEST_OBJ_REV));

      if (dupin_record_insert (db, element_node, NULL, NULL, response_list, use_latest_revision, ignore_updates_if_unmodified, error) == FALSE)
        {
          /* NOTE - we report errors inline in the JSON response */

          JsonNode * error_node = json_node_new (JSON_NODE_OBJECT);
          JsonObject * error_obj = json_object_new ();
          json_node_take_object (error_node, error_obj);

          json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, "bad_request");
          json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, dupin_database_get_error (db));

          if (id != NULL)
            json_object_set_string_member (error_obj, RESPONSE_OBJ_ID,id);

          if (rev != NULL)
            json_object_set_string_member (error_obj, RESPONSE_OBJ_REV,rev);

          *response_list = g_list_prepend (*response_list, error_node);
        }
      else
        {
          JsonNode * generated_node = (*response_list)->data;
          JsonObject * generated_obj = json_node_get_object (generated_node);

          if (dupin_database_get_warning (db) != NULL)
            json_object_set_string_member (generated_obj, RESPONSE_STATUS_WARNING, dupin_database_get_warning (db));
        }
      if (id != NULL)
        g_free (id);

      if (rev!= NULL)
        g_free (rev);
    }

  if (db->d->super_bulk_transaction == FALSE)
    {
      g_rw_lock_writer_lock (db->d->rwlock);
      db->d->bulk_transaction = FALSE;
      g_rw_lock_writer_unlock (db->d->rwlock);
    }

  g_rw_lock_writer_lock (dupin_database_get_default_linkbase (db)->rwlock);
  if (dupin_linkbase_commit_transaction (dupin_database_get_default_linkbase (db), NULL) < 0)
    {
      dupin_linkbase_rollback_transaction (dupin_database_get_default_linkbase (db), NULL);
      g_rw_lock_writer_unlock (dupin_database_get_default_linkbase (db)->rwlock);

      g_rw_lock_writer_lock (dupin_database_get_default_attachment_db (db)->rwlock);
      dupin_attachment_db_rollback_transaction (dupin_database_get_default_attachment_db (db), NULL);
      g_rw_lock_writer_unlock (dupin_database_get_default_attachment_db (db)->rwlock);

      g_rw_lock_writer_lock (db->rwlock);
      dupin_database_rollback_transaction (db, NULL);
      g_rw_lock_writer_unlock (db->rwlock);

      dupin_database_set_error (db, "dupin_record_insert_bulk: Cannot commit linkbase transaction");
      g_list_free (nodes);

      return FALSE;
    }
  g_rw_lock_writer_unlock (dupin_database_get_default_linkbase (db)->rwlock);

  g_rw_lock_writer_lock (dupin_database_get_default_attachment_db (db)->rwlock);
  if (dupin_attachment_db_commit_transaction (dupin_database_get_default_attachment_db (db), NULL) < 0)
    {
      dupin_attachment_db_rollback_transaction (dupin_database_get_default_attachment_db (db), NULL);
      g_rw_lock_writer_unlock (dupin_database_get_default_attachment_db (db)->rwlock);

      g_rw_lock_writer_lock (db->rwlock);
      dupin_database_rollback_transaction (db, NULL);
      g_rw_lock_writer_unlock (db->rwlock);

      dupin_database_set_error (db, "dupin_record_insert_bulk: Cannot commit attachment database transaction");
      g_list_free (nodes);

      return FALSE;
    }
  g_rw_lock_writer_unlock (dupin_database_get_default_attachment_db (db)->rwlock);

  g_rw_lock_writer_lock (db->rwlock);
  if (dupin_database_commit_transaction (db, NULL) < 0)
    {
      dupin_database_rollback_transaction (db, NULL);
      g_rw_lock_writer_unlock (db->rwlock);

      dupin_database_set_error (db, "dupin_record_insert_bulk: Cannot commit database transaction");
      g_list_free (nodes);

      return FALSE;
    }
  g_rw_lock_writer_unlock (db->rwlock);

//g_message("dupin_record_insert_bulk: inserted %d records into database %s\n", (gint)g_list_length (nodes), db->name);

  g_list_free (nodes); 

  return TRUE;
}

/* EOF */
