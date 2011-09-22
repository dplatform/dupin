#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_attachment_record.h"

#include <stdlib.h>
#include <string.h>

#define DUPIN_ATTACHMENT_DB_SQL_EXISTS \
	"SELECT count(*) FROM Dupin WHERE id = '%q' AND title = '%q' "

#define DUPIN_ATTACHMENT_DB_SQL_TOTAL \
	"SELECT count(*) AS c FROM Dupin AS d"

#define DUPIN_ATTACHMENT_DB_SQL_READ \
	"SELECT type, hash, length, ROWID AS rowid FROM Dupin WHERE id = '%q' AND title = '%q' "

#define DUPIN_ATTACHMENT_DB_SQL_INSERT \
        "INSERT INTO Dupin (id, title, type, length, hash, content) " \
        "VALUES(?, ?, ?, ?, ?, ?)"

#define DUPIN_ATTACHMENT_DB_SQL_DELETE \
        "DELETE FROM Dupin WHERE id = '%q' AND title = '%q' "

#define DUPIN_ATTACHMENT_DB_SQL_DELETE_ALL \
        "DELETE FROM Dupin WHERE id = '%q' "

#define DUPIN_ATTACHMENT_DB_SQL_HASHES \
	"SELECT group_concat(hash,'') AS h from Dupin where id = '%q' "

static DupinAttachmentRecord *dupin_attachment_record_read_real
							(DupinAttachmentDB * attachment_db,
                                		     	 gchar *        id,
                                		     	 gchar *        title,
						     	 GError ** error,
						     	 gboolean lock);

static DupinAttachmentRecord *dupin_attachment_record_new
							(DupinAttachmentDB * attachment_db,
                                		     	 gchar *        id,
                                		     	 gchar *        title);

gboolean dupin_attachment_record_exists_real (DupinAttachmentDB *    attachment_db,
                                     	      gchar *        id,
                                     	      gchar *        title,
                                     	      gboolean       lock);

gboolean dupin_attachment_record_get_aggregated_hash_real (DupinAttachmentDB * attachment_db,
                                                  	   gchar *        id,
                                                  	   gchar **       hash,
                                                  	   gboolean       lock);

/* NOTE - this is completely inefficient due we pass the whole BLOB in RAM and on the stack */

gboolean
dupin_attachment_record_create (DupinAttachmentDB * attachment_db,
                                gchar *       id,
                                gchar *       title,
                                gsize         length,
                                gchar *       type,
                                const void ** content) // try to avoid to pass megabytes on stack
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);
  g_return_val_if_fail (length >= 0, FALSE);
  g_return_val_if_fail (type != NULL, FALSE);
  g_return_val_if_fail (*content != NULL, FALSE);

//g_message("dupin_attachment_record_create:\n\tid=%s\n\ttitle=%s\n\tlength=%d\n\ttype=%s\n",id, title, (gint)length, type);

  gchar *query;
  sqlite3_stmt *insertstmt;
  gchar * md5=NULL;

  g_mutex_lock (attachment_db->mutex);

  query = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_INSERT);

  if (sqlite3_prepare(attachment_db->db, query, strlen(query), &insertstmt, NULL) != SQLITE_OK)
    {
      g_mutex_unlock (attachment_db->mutex);
      g_error("dupin_attachment_record_create: %s", sqlite3_errmsg (attachment_db->db));
      sqlite3_free (query);
      return FALSE;
    }

  sqlite3_bind_text (insertstmt, 1, id, strlen(id), SQLITE_STATIC);
  sqlite3_bind_text (insertstmt, 2, title, strlen(title), SQLITE_STATIC);
  sqlite3_bind_text (insertstmt, 3, type, strlen(type), SQLITE_STATIC);
  sqlite3_bind_int  (insertstmt, 4, length);
  md5 = g_compute_checksum_for_string (DUPIN_ID_HASH_ALGO, *content, length); // inefficient of course
  sqlite3_bind_text (insertstmt, 5, md5, strlen(md5), SQLITE_STATIC);
  sqlite3_bind_blob (insertstmt, 6, (const void*)(*content), length, SQLITE_STATIC);

  if (sqlite3_step (insertstmt) != SQLITE_DONE)
    {
      g_mutex_unlock (attachment_db->mutex);
      g_error("dupin_attachment_db_p_record_insert: %s", sqlite3_errmsg (attachment_db->db));
      sqlite3_free (query);
      g_free (md5);
      return FALSE;
    }

  sqlite3_finalize (insertstmt);

  g_mutex_unlock (attachment_db->mutex);

  sqlite3_free (query);
  g_free (md5);

  return TRUE;
}

gboolean
dupin_attachment_record_delete (DupinAttachmentDB * attachment_db,
                                gchar *        id,
                                gchar *        title)
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);

  gchar *query, *errmsg;

  query = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_DELETE, id, title);

//g_message("dupin_attachment_record_delete: query=%s\n",query);

  g_mutex_lock (attachment_db->mutex);

  if (dupin_attachment_db_begin_transaction (attachment_db, NULL) < 0)
    {
      g_mutex_unlock (attachment_db->mutex);

      sqlite3_free (query);
      return FALSE;
    }

  if (sqlite3_exec (attachment_db->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (attachment_db->mutex);
      g_error("dupin_attachment_record_delete: %s", errmsg);
      sqlite3_free (errmsg);
      sqlite3_free (query);
      dupin_attachment_db_rollback_transaction (attachment_db, NULL);
      return FALSE;
    }

  if (dupin_attachment_db_commit_transaction (attachment_db, NULL) < 0)
    {
      g_mutex_unlock (attachment_db->mutex);

      sqlite3_free (query);
      return FALSE;
    }

  g_mutex_unlock (attachment_db->mutex);

  sqlite3_free (query);

  return TRUE;
}

gboolean
dupin_attachment_record_delete_all (DupinAttachmentDB * attachment_db,
                                    gchar *        id)
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);

  gchar *query, *errmsg;

  query = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_DELETE_ALL, id);

//g_message("dupin_attachment_record_delete_all: query=%s\n",query);

  g_mutex_lock (attachment_db->mutex);

  if (dupin_attachment_db_begin_transaction (attachment_db, NULL) < 0)
    {
      g_mutex_unlock (attachment_db->mutex);

      sqlite3_free (query);
      return FALSE;
    }

  if (sqlite3_exec (attachment_db->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (attachment_db->mutex);
      g_error("dupin_attachment_record_delete_all: %s", errmsg);
      sqlite3_free (errmsg);
      sqlite3_free (query);
      dupin_attachment_db_rollback_transaction (attachment_db, NULL);
      return FALSE;
    }

  if (dupin_attachment_db_commit_transaction (attachment_db, NULL) < 0)
    {
      g_mutex_unlock (attachment_db->mutex);

      sqlite3_free (query);
      return FALSE;
    }

  g_mutex_unlock (attachment_db->mutex);

  sqlite3_free (query);

  return TRUE;
}

gboolean
dupin_attachment_record_exists (DupinAttachmentDB * attachment_db,
                                gchar *        id,
                                gchar *        title)
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);

  return dupin_attachment_record_exists_real (attachment_db, id, title, TRUE);
}

static int
dupin_attachment_record_exists_real_cb (void *data, int argc, char **argv,
				  char **col)
{
  guint *numb = data;

  if (argv[0])
    *numb = atoi (argv[0]);

  return 0;
}

gboolean
dupin_attachment_record_exists_real (DupinAttachmentDB *    attachment_db,
                                     gchar *        id,
                                     gchar *        title,
                                     gboolean       lock)
{
  gchar *errmsg;
  gchar *tmp;
  gint numb = 0;

  tmp = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_EXISTS, id, title);

  if (lock == TRUE)
    g_mutex_lock (attachment_db->mutex);

  if (sqlite3_exec (attachment_db->db, tmp, dupin_attachment_record_exists_real_cb, &numb, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
        g_mutex_unlock (attachment_db->mutex);

      sqlite3_free (tmp);

      g_error ("dupin_attachment_record_exists_real: %s", errmsg);

      sqlite3_free (errmsg);

      return FALSE;
    }

  if (lock == TRUE)
    g_mutex_unlock (attachment_db->mutex);

  sqlite3_free (tmp);

  return numb > 0 ? TRUE : FALSE;
}

gchar *
dupin_attachment_record_get_aggregated_hash (DupinAttachmentDB * attachment_db,
                                             gchar *        id)
{
  g_return_val_if_fail (attachment_db != NULL, NULL);
  g_return_val_if_fail (id != NULL, NULL);

  gchar * hash=NULL;

  if (dupin_attachment_record_get_aggregated_hash_real (attachment_db, id, &hash, TRUE) == FALSE)
    return NULL;

//g_message("dupin_attachment_record_get_aggregated_hash: whole attachments aggregated hash=%s\n", hash); 

  return hash;
}

static int
dupin_attachment_record_get_aggregated_hash_real_cb (void *data, int argc, char **argv,
				  char **col)
{
  gchar ** concatenated_hash = data;

  if (argv[0] && *argv[0])
    *concatenated_hash = g_strdup (argv[0]);

  return 0;
}

gboolean
dupin_attachment_record_get_aggregated_hash_real (DupinAttachmentDB * attachment_db,
                                                  gchar *        id,
                                                  gchar **       hash,
                                                  gboolean       lock)
{
  gchar *errmsg;
  gchar *tmp;
  gchar * concatenated_hash = NULL;

  tmp = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_HASHES, id);

//g_message("dupin_attachment_record_get_aggregated_hash_real() query=%s\n",tmp);

  if (lock == TRUE)
    g_mutex_lock (attachment_db->mutex);

  if (sqlite3_exec (attachment_db->db, tmp, dupin_attachment_record_get_aggregated_hash_real_cb, &concatenated_hash, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
        g_mutex_unlock (attachment_db->mutex);

      sqlite3_free (tmp);

      g_error ("dupin_attachment_record_get_hashes_real: %s", errmsg);

      if (concatenated_hash != NULL)
        g_free (concatenated_hash);

      sqlite3_free (errmsg);

      return FALSE;
    }

  if (lock == TRUE)
    g_mutex_unlock (attachment_db->mutex);

  sqlite3_free (tmp);

//g_message("dupin_attachment_record_get_aggregated_hash_real() concatenated_hash=%s for id=%s\n",concatenated_hash, id);

  if (concatenated_hash != NULL)
    {
      *hash = g_compute_checksum_for_string (DUPIN_ID_HASH_ALGO, concatenated_hash, strlen(concatenated_hash));

      g_free (concatenated_hash);

      return TRUE;
    }
  else
    {
      return FALSE;
    }
}

static int
dupin_attachment_record_read_cb (void *data, int argc, char **argv, char **col)
{
  DupinAttachmentRecord *record = data;
  gint i;

  for (i = 0; i < argc; i++)
    {
      if (!g_strcmp0 (col[i], "type") && argv[i])
	{
	  record->type = g_strdup (argv[i]);
	  record->type_len = strlen (argv[i]);
	}
      else if (!g_strcmp0 (col[i], "hash") && argv[i])
	{
	  record->hash = g_strdup (argv[i]);
	  record->hash_len = strlen (argv[i]);
	}
      else if (!g_strcmp0 (col[i], "length") && argv[i])
	{
	  record->length = atoi(argv[i]);
        }
      else if (!g_strcmp0 (col[i], "rowid") && argv[i])
	{
	  record->rowid = atoi(argv[i]);
        }
    }

  return 0;
}

DupinAttachmentRecord *
dupin_attachment_record_read (DupinAttachmentDB *            attachment_db,
                              gchar *        id,
                              gchar *        title,
                              GError **              error) 
{
  g_return_val_if_fail (attachment_db != NULL, NULL);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);
  g_return_val_if_fail (dupin_util_is_valid_record_id (id) != FALSE, NULL);

  return dupin_attachment_record_read_real (attachment_db, id, title, error, TRUE);
}

static DupinAttachmentRecord *
dupin_attachment_record_read_real (DupinAttachmentDB * attachment_db,
                                   gchar *        id,
                                   gchar *        title,
				   GError ** error,
				   gboolean lock)
{
  DupinAttachmentRecord *record;
  gchar *errmsg;
  gchar *tmp;

  dupin_attachment_db_ref (attachment_db);

  record = dupin_attachment_record_new (attachment_db, id, title);

  tmp = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_READ, id, title);

  if (lock == TRUE)
    g_mutex_lock (attachment_db->mutex);

  if (sqlite3_exec (attachment_db->db, tmp, dupin_attachment_record_read_cb, record, &errmsg)
      != SQLITE_OK)
    {
      if (lock == TRUE)
	g_mutex_unlock (attachment_db->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);
      dupin_attachment_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return NULL;
    }

  if (lock == TRUE)
    g_mutex_unlock (attachment_db->mutex);

  sqlite3_free (tmp);

  if (!record->id || !record->rowid)
    {
      dupin_attachment_record_close (record);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD,
		   "The record '%s' doesn't exist.", id);
      return NULL;
    }

  return record;
}

static int
dupin_attachment_record_get_list_total_cb (void *data, int argc, char **argv, char **col)
{
  gsize *numb = data;

  if (argv[0] && *argv[0])
    *numb = atoi(argv[0]);

  return 0;
}

gsize
dupin_attachment_record_get_list_total (DupinAttachmentDB * attachment_db,
			    		gsize rowid_start, gsize rowid_end,
                            	        gchar * id,
			    		gchar * start_title,
			    		gchar * end_title,
			    		gboolean inclusive_end,
			    		GError ** error)
{
  gsize count = 0;
  GString *str;
  gchar *tmp;
  gchar *errmsg;

  gchar * id_range=NULL;
  gchar * title_range=NULL;

  g_return_val_if_fail (attachment_db != NULL, 0);
  g_return_val_if_fail (id != NULL, 0);

  str = g_string_new ("SELECT count(*) FROM Dupin as d");

  id_range = sqlite3_mprintf (" d.id = '%q' ", id);

  if (start_title!=NULL && end_title!=NULL)
    if (!g_utf8_collate (start_title, end_title) && inclusive_end == TRUE)
      title_range = sqlite3_mprintf (" d.title = '%q' ", start_title);
    else if (inclusive_end == TRUE)
      title_range = sqlite3_mprintf (" d.title >= '%q' AND d.title <= '%q' ", start_title, end_title);
    else
      title_range = sqlite3_mprintf (" d.title >= '%q' AND d.title < '%q' ", start_title, end_title);
  else if (start_title!=NULL)
    {
      title_range = sqlite3_mprintf (" d.title >= '%q' ", start_title);
    }
  else if (end_title!=NULL)
    {
      if (inclusive_end == TRUE)
        title_range = sqlite3_mprintf (" d.title <= '%q' ", end_title);
      else
        title_range = sqlite3_mprintf (" d.title < '%q' ", end_title);
    }

  g_string_append_printf (str, " WHERE %s ", id_range);

  if (rowid_start > 0 && rowid_end > 0)
    g_string_append_printf (str, " %s %s AND d.ROWID >= %d AND d.ROWID <= %d ",
					(title_range!=NULL) ? "AND" : "", (title_range!=NULL) ? title_range : "",
					(gint)rowid_start, (gint)rowid_end);
  else if (rowid_start > 0)
    g_string_append_printf (str, " %s %s AND d.ROWID >= %d ",
					(title_range!=NULL) ? "AND" : "", (title_range!=NULL) ? title_range : "",
					(gint)rowid_start);
  else if (rowid_end > 0)
    g_string_append_printf (str, " %s %s AND d.ROWID <= %d ",
					(title_range!=NULL) ? "AND" : "", (title_range!=NULL) ? title_range : "",
					(gint)rowid_end);
  else if (title_range!=NULL)
    g_string_append_printf (str, " AND %s ", title_range);

  tmp = g_string_free (str, FALSE);
 
  sqlite3_free (id_range);
  if (title_range!=NULL)
    sqlite3_free (title_range);

//g_message("dupin_attachment_record_get_list_total() query=%s\n",tmp);

  g_mutex_lock (attachment_db->mutex);

  if (sqlite3_exec (attachment_db->db, tmp, dupin_attachment_record_get_list_total_cb, &count, &errmsg)
      != SQLITE_OK)
    {
      g_mutex_unlock (attachment_db->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return 0;
    }

  g_mutex_unlock (attachment_db->mutex);

  g_free (tmp);

  return count;
}

struct dupin_attachment_record_get_list_t
{
  DupinAttachmentDB *attachment_db;
  GList *list;
};

static int
dupin_attachment_record_get_list_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_attachment_record_get_list_t *s = data;
  DupinAttachmentRecord *record;

  if ((record = dupin_attachment_record_read_real (s->attachment_db, argv[0], argv[1], NULL, FALSE)))
    s->list = g_list_append (s->list, record);

  return 0;
}

gboolean
dupin_attachment_record_get_list (DupinAttachmentDB * attachment_db, guint count, guint offset,
			    gsize rowid_start, gsize rowid_end,
			    DupinOrderByType orderby_type,
			    gboolean descending,
                            gchar * id,
			    gchar * start_title,
			    gchar * end_title,
			    gboolean inclusive_end,
			    GList ** list, GError ** error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;

  gchar * id_range=NULL;
  gchar * title_range=NULL;

  struct dupin_attachment_record_get_list_t s;

  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  memset (&s, 0, sizeof (s));
  s.attachment_db = attachment_db;

  str = g_string_new ("SELECT id, title FROM Dupin as d");

  id_range = sqlite3_mprintf (" d.id = '%q' ", id);

  if (start_title!=NULL && end_title!=NULL)
    if (!g_utf8_collate (start_title, end_title) && inclusive_end == TRUE)
      title_range = sqlite3_mprintf (" d.title = '%q' ", start_title);
    else if (inclusive_end == TRUE)
      title_range = sqlite3_mprintf (" d.title >= '%q' AND d.title <= '%q' ", start_title, end_title);
    else
      title_range = sqlite3_mprintf (" d.title >= '%q' AND d.title < '%q' ", start_title, end_title);
  else if (start_title!=NULL)
    {
      title_range = sqlite3_mprintf (" d.title >= '%q' ", start_title);
    }
  else if (end_title!=NULL)
    {
      if (inclusive_end == TRUE)
        title_range = sqlite3_mprintf (" d.title <= '%q' ", end_title);
      else
        title_range = sqlite3_mprintf (" d.title < '%q' ", end_title);
    }

  g_string_append_printf (str, " WHERE %s ", id_range);

  if (rowid_start > 0 && rowid_end > 0)
    g_string_append_printf (str, " %s %s AND d.ROWID >= %d AND d.ROWID <= %d ",
					(title_range!=NULL) ? "AND" : "", (title_range!=NULL) ? title_range : "",
					(gint)rowid_start, (gint)rowid_end);
  else if (rowid_start > 0)
    g_string_append_printf (str, " %s %s AND d.ROWID >= %d ",
					(title_range!=NULL) ? "AND" : "", (title_range!=NULL) ? title_range : "",
					(gint)rowid_start);
  else if (rowid_end > 0)
    g_string_append_printf (str, " %s %s AND d.ROWID <= %d ",
					(title_range!=NULL) ? "AND" : "", (title_range!=NULL) ? title_range : "",
					(gint)rowid_end);
  else if (title_range!=NULL)
    g_string_append_printf (str, " AND %s ", title_range);

  if (orderby_type == DP_ORDERBY_TITLE)
    str = g_string_append (str, " ORDER BY d.title"); /* this should never be used for reduce internal operations */
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
 
  sqlite3_free (id_range);
  if (title_range!=NULL)
    sqlite3_free (title_range);

//g_message("dupin_attachment_record_get_list() query=%s\n",tmp);

  g_mutex_lock (attachment_db->mutex);

  if (sqlite3_exec (attachment_db->db, tmp, dupin_attachment_record_get_list_cb, &s, &errmsg)
      != SQLITE_OK)
    {
      g_mutex_unlock (attachment_db->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (attachment_db->mutex);

  g_free (tmp);

  *list = s.list;
  return TRUE;
}

void
dupin_attachment_record_get_list_close (GList * list)
{
  while (list)
    {
      dupin_attachment_record_close (list->data);
      list = g_list_remove (list, list->data);
    }
}

static DupinAttachmentRecord *
dupin_attachment_record_new (DupinAttachmentDB * attachment_db,
                             gchar *        id,
                             gchar *        title)
{
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);

  DupinAttachmentRecord *record;

  record = g_malloc0 (sizeof (DupinAttachmentRecord));
  record->attachment_db = attachment_db;
  record->id = g_strdup (id);
  record->id_len = strlen (id);
  record->title = g_strdup (title);
  record->title_len = strlen (title);

  record->blob = NULL;

  return record;
}

void
dupin_attachment_record_close (DupinAttachmentRecord * record)
{
  g_return_if_fail (record != NULL);

  if (record->blob)
    dupin_attachment_record_blob_close (record);

  if (record->attachment_db)
    dupin_attachment_db_unref (record->attachment_db);

  if (record->id)
    g_free (record->id);

  if (record->title)
    g_free (record->title);

  if (record->type)
    g_free (record->type);

  if (record->hash)
    g_free (record->hash);

  g_free (record);
}

const gchar *
dupin_attachment_record_get_id (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->id;
}

const gchar *
dupin_attachment_record_get_title (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->title;
}

const gchar *
dupin_attachment_record_get_type (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->type;
}

const gchar *
dupin_attachment_record_get_hash (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->hash;
}

gsize
dupin_attachment_record_get_length (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->length;
}

gsize
dupin_attachment_record_get_rowid (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->rowid;
}

JsonNode *
dupin_attachment_record_get (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, NULL);

  JsonNode * obj_node = json_node_new (JSON_NODE_OBJECT);
  gchar * hash=NULL;

  if (obj_node == NULL)
    return NULL;

  JsonObject * obj = json_object_new ();

  if (obj_node == NULL)
    {
      json_node_free (obj_node);
      return NULL;
    }

  json_node_take_object (obj_node, obj);

  json_object_set_string_member (obj, RESPONSE_OBJ_INLINE_ATTACHMENTS_TYPE, dupin_attachment_record_get_type (record));
  json_object_set_int_member (obj, RESPONSE_OBJ_INLINE_ATTACHMENTS_LENGTH, dupin_attachment_record_get_length (record));
  hash = (gchar *)dupin_attachment_record_get_hash (record);
  if (hash != NULL)
    json_object_set_string_member (obj, RESPONSE_OBJ_INLINE_ATTACHMENTS_HASH, hash);

  return obj_node;
}

static int
dupin_attachment_record_get_max_rowid_cb (void *data, int argc, char **argv,
                                  char **col)
{
  gsize *max_rowid = data;

  if (argv[0])
    *max_rowid = atoi (argv[0]);

  return 0;
}

gboolean
dupin_attachment_record_get_max_rowid (DupinAttachmentDB * attachment_db, gsize * max_rowid)
{
  gchar *query;
  gchar * errmsg=NULL;

  g_return_val_if_fail (attachment_db != NULL, FALSE);

  *max_rowid=0;

  query = "SELECT max(ROWID) as max_rowid FROM Dupin";

  g_mutex_lock (attachment_db->mutex);

  if (sqlite3_exec (attachment_db->db, query, dupin_attachment_record_get_max_rowid_cb, max_rowid, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (attachment_db->mutex);

      g_error("dupin_attachment_record_get_max_rowid: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (attachment_db->mutex);

  return TRUE;
}

gboolean
dupin_attachment_record_blob_open (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, FALSE);
  g_return_val_if_fail (record->blob == NULL, FALSE);

  g_mutex_lock (record->attachment_db->mutex);

  if (sqlite3_blob_open(record->attachment_db->db, "main", "Dupin", "content",
			dupin_attachment_record_get_rowid (record), 0, &record->blob) != SQLITE_OK)
    {
      g_mutex_unlock (record->attachment_db->mutex);
      g_error("dupin_attachment_record_blob_open: %s", sqlite3_errmsg (record->attachment_db->db));
      return FALSE;
    }

  g_mutex_unlock (record->attachment_db->mutex);

  return TRUE;
}

gboolean
dupin_attachment_record_blob_close (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, FALSE);
  g_return_val_if_fail (record->blob != NULL, FALSE);

  g_mutex_lock (record->attachment_db->mutex);

  if (sqlite3_blob_close(record->blob) != SQLITE_OK)
    {
      g_mutex_unlock (record->attachment_db->mutex);
      g_error("dupin_attachment_record_blob_close: %s", sqlite3_errmsg (record->attachment_db->db));
      return FALSE;
    }

  g_mutex_unlock (record->attachment_db->mutex);

  record->blob = NULL;

  return TRUE;
}

gboolean
dupin_attachment_record_blob_read (DupinAttachmentRecord * record,
                                   gchar *buf,
                                   gsize count,    
                                   gsize offset,
                                   gsize *bytes_read,
                                   GError **error)
{
  g_return_val_if_fail (record != NULL, FALSE);

  gint left = sqlite3_blob_bytes (record->blob) - offset;

  if (left > 0 && left < count)
    count = left;

  g_mutex_lock (record->attachment_db->mutex);

  if (sqlite3_blob_read (record->blob, buf, count, offset) != SQLITE_OK)
    {
      g_mutex_unlock (record->attachment_db->mutex);
      *bytes_read = 0;
      return FALSE;
    }

  g_mutex_unlock (record->attachment_db->mutex);

  *bytes_read = count;

  return TRUE;
}

gboolean
dupin_attachment_record_blob_write (DupinAttachmentRecord * record,
                                    const gchar *buf,
                                    gsize count,
                                    gsize offset,
                                    gsize *bytes_written, 
                                    GError **error)
{
  g_return_val_if_fail (record != NULL, FALSE);
  g_return_val_if_fail (buf != NULL, FALSE);

  g_mutex_lock (record->attachment_db->mutex);

  if (sqlite3_blob_write(record->blob, buf, count, offset) != SQLITE_OK)
    {
      g_mutex_unlock (record->attachment_db->mutex);
      return FALSE;
    }

  g_mutex_unlock (record->attachment_db->mutex);

  return TRUE;
}

/* Insert */

gboolean
dupin_attachment_record_insert (DupinAttachmentDB * attachment_db,
				gchar * id,
				gchar * caller_mvcc,
				GList * title_parts,
			        gsize  attachment_body_size,	
			        gchar * attachment_input_mime,
                                const void ** attachment_body, // try to avoid to pass megabytes on stack
                                GList ** response_list)
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);

  DupinDB *db;
  DupinRecord *record=NULL;
  JsonNode * obj_node=NULL;

  GError *error = NULL;
  GString *str;
  gchar * title = NULL;
  GList * l=NULL;

  if (caller_mvcc != NULL)
    g_return_val_if_fail (dupin_util_is_valid_mvcc (caller_mvcc) == TRUE, FALSE);

  /* process input attachment name parameter */

  str = g_string_new (title_parts->data);

  for (l=title_parts->next ; l != NULL ; l=l->next)
    {
      g_string_append_printf (str, "/%s", (gchar *)l->data);
    }

  title = g_string_free (str, FALSE);

  if (title == NULL)
    {
      dupin_attachment_db_set_error (attachment_db, "Missing attachment name");
      return FALSE;
    }

//g_message("dupin_attachment_record_insert: title=%s\n", title);

  if (!(db = dupin_database_open (attachment_db->d, attachment_db->parent, &error)))
    {
      g_free (title);
      if (error != NULL)
        dupin_attachment_db_set_error (attachment_db, error->message);
      else
        dupin_attachment_db_set_error (attachment_db, "Cannot connect to record database");
      return FALSE;
    }

  record = dupin_record_read (db, id, &error);

  if (caller_mvcc == NULL && record != NULL)
    {
      g_free (title);
      dupin_record_close (record);
      
      dupin_database_unref (db);

      if (error != NULL)
        dupin_attachment_db_set_error (attachment_db, error->message);
      else
        dupin_attachment_db_set_error (attachment_db, "Record found but MVCC revision number is missing");

      return FALSE;
    }

  if (record == NULL)
    {
      /* TODO - create new record instead */
     obj_node = json_node_new (JSON_NODE_OBJECT);
     JsonObject * obj = json_object_new ();
     json_node_take_object (obj_node, obj);

     if ( dupin_attachment_record_create (attachment_db, id, title,
                                          attachment_body_size,
                                          attachment_input_mime,
                                          attachment_body) == FALSE
         || (!( record = dupin_record_create_with_id (db, obj_node, id, &error))))
        {
          g_free (title);
          json_node_free (obj_node);
          
          dupin_database_unref (db);

          if (error != NULL)
            dupin_attachment_db_set_error (attachment_db, error->message);
          else
            dupin_attachment_db_set_error (attachment_db, "Cannot insert attachment or create record to contain attachment");

          return FALSE;
        }
    }
  else
    {
      if (caller_mvcc == NULL
          || dupin_util_mvcc_revision_cmp (caller_mvcc, dupin_record_get_last_revision (record)))
        {
          g_free (title);
          dupin_record_close (record);
          
          dupin_database_unref (db);
          dupin_attachment_db_set_error (attachment_db, "No record MVCC revision found or record revision not matching latest");
          return FALSE;
        }

      /* NOTE - need to "touch" (update) the metadata record anyway */

      if (!(obj_node = dupin_record_get_revision_node (record, caller_mvcc)))
        {
          g_free (title);
          dupin_record_close (record);
          
          dupin_database_unref (db);
          dupin_attachment_db_set_error (attachment_db, "Cannot fetch record for update");
          return FALSE;
        }

      if ( dupin_attachment_record_delete (attachment_db, id, title) == FALSE
          || dupin_attachment_record_create (attachment_db, id, title,
                                             attachment_body_size,
                                             attachment_input_mime,
                                             attachment_body) == FALSE
          || dupin_record_update (record, obj_node, &error) == FALSE)
        {
          g_free (title);
          dupin_record_close (record);
          
          dupin_database_unref (db);
          json_node_free (obj_node);

          if (error != NULL)
            dupin_attachment_db_set_error (attachment_db, error->message);
          else
            dupin_attachment_db_set_error (attachment_db, "Cannot replace attachment");

          return FALSE;
        }
    }

  JsonNode * record_response_node = json_node_new (JSON_NODE_OBJECT);
  JsonObject * record_response_obj = json_object_new ();
  json_node_take_object (record_response_node, record_response_obj);

  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_ID, (gchar *) dupin_record_get_id (record));
  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_REV, dupin_record_get_last_revision (record));

  dupin_record_close (record);
  
  dupin_database_unref (db);
  g_free (title);
  json_node_free (obj_node);

  *response_list = g_list_prepend (*response_list, record_response_node);

  return TRUE;
}

/* EOF */
