#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_attachment_record.h"

#include <stdlib.h>
#include <string.h>

#define DUPIN_ATTACHMENT_DB_SQL_EXISTS \
	"SELECT count(*) FROM Dupin WHERE id = '%q' AND rev = '%" G_GSIZE_FORMAT "' AND title = '%q' "

#define DUPIN_ATTACHMENT_DB_SQL_TOTAL \
	"SELECT count(*) AS c FROM Dupin AS d"

#define DUPIN_ATTACHMENT_DB_SQL_READ \
	"SELECT id, rev, title, type, hash, length, ROWID AS rowid FROM Dupin WHERE id = '%q' AND rev = '%" G_GSIZE_FORMAT "' AND title = '%q' "

#define DUPIN_ATTACHMENT_DB_SQL_INSERT \
        "INSERT INTO Dupin (id, rev, title, type, length, content) " \
        "VALUES(?, ?, ?, ?, ?, ?)"

#define DUPIN_ATTACHMENT_DB_SQL_DELETE \
        "DELETE FROM Dupin WHERE id = '%q' AND rev = '%" G_GSIZE_FORMAT "' AND title = '%q' "

static DupinAttachmentRecord *dupin_attachment_record_read_real
							(DupinAttachmentDB * attachment_db,
                                		     	 gchar *        id,
                                		     	 guint          revision,
                                		     	 gchar *        title,
						     	 GError ** error,
						     	 gboolean lock);

static DupinAttachmentRecord *dupin_attachment_record_new
							(DupinAttachmentDB * attachment_db,
                                		     	 gchar *        id,
                                		     	 guint          revision,
                                		     	 gchar *        title);

gboolean dupin_attachment_record_exists_real (DupinAttachmentDB *    attachment_db,
                                     	      gchar *        id,
                                     	      guint          revision,
                                     	      gchar *        title,
                                     	      gboolean       lock);

gboolean
dupin_attachment_record_insert (DupinAttachmentDB * attachment_db,
                                gchar *       id,
                                guint         revision,
                                gchar *       title,
                                gsize         length,
                                gchar *       type,
                                gchar *       hash,
                                const void *  content)
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (revision > 0, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);
  g_return_val_if_fail (length >= 0, FALSE);
  g_return_val_if_fail (type != NULL, FALSE);
  g_return_val_if_fail (content != NULL, FALSE);

g_message("dupin_attachment_record_insert:\n\tid=%s\n\trevision=%d\n\ttitle=%s\n\tlength=%d\n\ttype=%s\n\thash=%s",id, (gint)revision, title, (gint)length, type, hash);

  gchar *query;
  sqlite3_stmt *insertstmt;

  g_mutex_lock (attachment_db->mutex);

  query = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_INSERT);

  if (sqlite3_prepare(attachment_db->db, query, strlen(query), &insertstmt, NULL) != SQLITE_OK)
    {
      g_mutex_unlock (attachment_db->mutex);
      g_error("dupin_attachment_record_insert: %s", sqlite3_errmsg (attachment_db->db));
      sqlite3_free (query);
      return FALSE;
    }

  sqlite3_bind_text (insertstmt, 1, id, strlen(id), SQLITE_STATIC);
  sqlite3_bind_int  (insertstmt, 2, revision);
  sqlite3_bind_text (insertstmt, 3, title, strlen(title), SQLITE_STATIC);
  sqlite3_bind_text (insertstmt, 4, type, strlen(type), SQLITE_STATIC);
  sqlite3_bind_int  (insertstmt, 5, length);
  sqlite3_bind_blob (insertstmt, 6, (const void*)content, length, SQLITE_STATIC);

  if (sqlite3_step (insertstmt) != SQLITE_DONE)
    {
      g_mutex_unlock (attachment_db->mutex);
      g_error("dupin_attachment_db_p_record_insert: %s", sqlite3_errmsg (attachment_db->db));
      sqlite3_free (query);
      return FALSE;
    }

  sqlite3_finalize (insertstmt);

  g_mutex_unlock (attachment_db->mutex);

  sqlite3_free (query);

  return TRUE;
}

gboolean
dupin_attachment_record_delete (DupinAttachmentDB * attachment_db,
                                gchar *        id,
                                guint          revision,
                                gchar *        title)
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (revision > 0, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);

  gchar *query, *errmsg;

  query = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_DELETE, id, revision, title);

g_message("dupin_attachment_record_delete: query=%s\n",query);

  g_mutex_lock (attachment_db->mutex);

  if (sqlite3_exec (attachment_db->db, query, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (attachment_db->mutex);
      g_error("dupin_attachment_db_p_record_delete: %s", errmsg);
      sqlite3_free (errmsg);
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
                                guint          revision,
                                gchar *        title)
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (revision > 0, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);

  return dupin_attachment_record_exists_real (attachment_db, id, revision, title, TRUE);
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
                                     guint          revision,
                                     gchar *        title,
                                     gboolean       lock)
{
  gchar *errmsg;
  gchar *tmp;
  gint numb = 0;

  tmp = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_EXISTS, id, revision, title);

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

static int
dupin_attachment_record_get_total_records_cb (void *data, int argc, char **argv,
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
dupin_attachment_record_get_total_records (DupinAttachmentDB * attachment_db,
				    gsize * total,
                                    gchar * id,
                                    guint   revision,
				    gchar * start_title,
                                    gchar * end_title,
			    	    gboolean inclusive_end,
                                    GError ** error)
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (revision > 0, FALSE);

  gchar *errmsg;
  gchar *tmp;
  GString *str;

  *total = 0;

  gchar * title_range=NULL;

  str = g_string_new (DUPIN_ATTACHMENT_DB_SQL_TOTAL);

  if (start_title!=NULL && end_title!=NULL)
    if (!strcmp (start_title, end_title) && inclusive_end == TRUE)
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

  if (title_range!=NULL)
    g_string_append_printf (str, " WHERE %s ", (title_range!=NULL) ? title_range : "");

  tmp = g_string_free (str, FALSE);
 
//g_message("dupin_attachment_record_get_total_records() query=%s\n",tmp);

  if (title_range!=NULL)
    sqlite3_free (title_range);

  g_mutex_lock (attachment_db->mutex);

  if (sqlite3_exec (attachment_db->db, tmp, dupin_attachment_record_get_total_records_cb, total, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (attachment_db->mutex);

      g_free (tmp);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (attachment_db->mutex);

  g_free (tmp);

  return TRUE;
}

static int
dupin_attachment_record_read_cb (void *data, int argc, char **argv, char **col)
{
  DupinAttachmentRecord *record = data;
  gint i;

  for (i = 0; i < argc; i++)
    {
      if (!strcmp (col[i], "id") && argv[i])
	{
	  record->id = g_strdup (argv[i]);
	  record->id_len = strlen (argv[i]);
	}
      else if (!strcmp (col[i], "rev") && argv[i])
	{
	  record->revision = atoi(argv[i]);
        }
      else if (!strcmp (col[i], "title") && argv[i])
	{
	  record->title = g_strdup (argv[i]);
	  record->title_len = strlen (argv[i]);
	}
      else if (!strcmp (col[i], "type") && argv[i])
	{
	  record->type = g_strdup (argv[i]);
	  record->type_len = strlen (argv[i]);
	}
      else if (!strcmp (col[i], "hash") && argv[i])
	{
	  record->hash = g_strdup (argv[i]);
	  record->hash_len = strlen (argv[i]);
	}
      else if (!strcmp (col[i], "length") && argv[i])
	{
	  record->length = atoi(argv[i]);
        }
      else if (!strcmp (col[i], "rowid") && argv[i])
	{
	  record->rowid = atoi(argv[i]);
        }
    }

  return 0;
}

DupinAttachmentRecord *
dupin_attachment_record_read (DupinAttachmentDB *            attachment_db,
                              gchar *        id,
                              guint          revision,
                              gchar *        title,
                              GError **              error) 
{
  g_return_val_if_fail (attachment_db != NULL, NULL);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (revision > 0, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);
  g_return_val_if_fail (dupin_util_is_valid_record_id (id) != FALSE, NULL);

  return dupin_attachment_record_read_real (attachment_db, id, revision, title, error, TRUE);
}

static DupinAttachmentRecord *
dupin_attachment_record_read_real (DupinAttachmentDB * attachment_db,
                                   gchar *        id,
                                   guint          revision,
                                   gchar *        title,
				   GError ** error,
				   gboolean lock)
{
  DupinAttachmentRecord *record;
  gchar *errmsg;
  gchar *tmp;

  dupin_attachment_db_ref (attachment_db);

  record = dupin_attachment_record_new (attachment_db, id, revision, title);

  tmp = sqlite3_mprintf (DUPIN_ATTACHMENT_DB_SQL_READ, id, revision, title);

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

  if ((record = dupin_attachment_record_read_real (s->attachment_db, argv[0], atoi(argv[1]), argv[2], NULL, FALSE)))
    s->list = g_list_append (s->list, record);

  return 0;
}

gboolean
dupin_attachment_record_get_list (DupinAttachmentDB * attachment_db, guint count, guint offset,
			    gsize rowid_start, gsize rowid_end,
			    DupinOrderByType orderby_type,
			    gboolean descending,
                            gchar * id,
                            guint   revision,
			    gchar * start_title,
			    gchar * end_title,
			    gboolean inclusive_end,
			    GList ** list, GError ** error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;

  gchar * id_rev_range=NULL;
  gchar * title_range=NULL;

  struct dupin_attachment_record_get_list_t s;

  g_return_val_if_fail (attachment_db != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (revision > 0, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  memset (&s, 0, sizeof (s));
  s.attachment_db = attachment_db;

  str = g_string_new ("SELECT id, rev, title FROM Dupin as d");

  id_rev_range = sqlite3_mprintf (" d.id = '%q' AND d.rev = '%d' ", id, (gint)revision);

  if (start_title!=NULL && end_title!=NULL)
    if (!strcmp (start_title, end_title) && inclusive_end == TRUE)
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

  g_string_append_printf (str, " WHERE %s ", id_rev_range);

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
 
  sqlite3_free (id_rev_range);
  if (title_range!=NULL)
    sqlite3_free (title_range);

g_message("dupin_attachment_record_get_list() query=%s\n",tmp);

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
                             guint          revision,
                             gchar *        title)
{
  g_return_val_if_fail (id != NULL, FALSE);
  g_return_val_if_fail (revision > 0, FALSE);
  g_return_val_if_fail (title != NULL, FALSE);

  DupinAttachmentRecord *record;

  record = g_malloc0 (sizeof (DupinAttachmentRecord));
  record->attachment_db = attachment_db;
  record->id = g_strdup (id);
  record->id_len = strlen (id);
  record->title = g_strdup (title);
  record->title_len = strlen (title);
  record->revision = revision;

  return record;
}

void
dupin_attachment_record_close (DupinAttachmentRecord * record)
{
  g_return_if_fail (record != NULL);

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

guint
dupin_attachment_record_get_revision (DupinAttachmentRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->revision;
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

  json_object_set_string_member (obj, "type", dupin_attachment_record_get_type (record));
  json_object_set_int_member (obj, "length", dupin_attachment_record_get_length (record));
  hash = (gchar *)dupin_attachment_record_get_hash (record);
  if (hash != NULL)
    json_object_set_string_member (obj, "hash", hash);

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

/* EOF */
