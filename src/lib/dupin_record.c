#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_record.h"
#include "dupin_utils.h"

#include <string.h>
#include <stdlib.h>

#define DUPIN_DB_SQL_EXISTS \
	"SELECT count(id) FROM Dupin WHERE id = '%q' "

#define DUPIN_DB_SQL_INSERT \
	"INSERT INTO Dupin (id, rev, obj) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', '%q')"

#define DUPIN_DB_SQL_UPDATE \
	"INSERT OR REPLACE INTO Dupin (id, rev, obj) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', '%q')"

#define DUPIN_DB_SQL_READ \
	"SELECT rev, obj, deleted FROM Dupin WHERE id='%q'"

#define DUPIN_DB_SQL_DELETE \
	"INSERT OR REPLACE INTO Dupin (id, rev, deleted) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', 'TRUE')"

static DupinRecord *dupin_record_create_with_id_real (DupinDB * db,
						      tb_json_object_t * obj,
						      gchar * id,
						      GError ** error,
						      gboolean lock);
static DupinRecord *dupin_record_read_real (DupinDB * db, gchar * id,
					    GError ** error, gboolean lock);

static void dupin_record_rev_close (DupinRecordRev * rev);
static DupinRecord *dupin_record_new (DupinDB * db, gchar * id);
static void dupin_record_add_revision_obj (DupinRecord * record, guint rev,
					   tb_json_object_t * obj,
					   gboolean delete);
static void dupin_record_add_revision_str (DupinRecord * record, guint rev,
					   gchar * obj, gssize size,
					   gboolean delete);

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
  guint *numb = data;

  if (argv[0])
    *numb = atoi (argv[0]);

  return 0;
}

gboolean
dupin_record_exists_real (DupinDB * db, gchar * id, gboolean lock)
{
  gchar *tmp;
  gint numb = 0;

  tmp = sqlite3_mprintf (DUPIN_DB_SQL_EXISTS, id);

  if (lock == TRUE)
    g_mutex_lock (db->mutex);

  sqlite3_exec (db->db, tmp, dupin_record_exists_real_cb, &numb, NULL);

  if (lock == TRUE)
    g_mutex_unlock (db->mutex);

  sqlite3_free (tmp);

  return numb > 0 ? TRUE : FALSE;
}

DupinRecord *
dupin_record_create (DupinDB * db, tb_json_object_t * obj, GError ** error)
{
  gchar *id;
  DupinRecord *record;

  g_return_val_if_fail (db != NULL, NULL);
  g_return_val_if_fail (obj != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_obj (obj) != FALSE, NULL);

  g_mutex_lock (db->mutex);

  if (!(id = dupin_database_generate_id_real (db, error, FALSE)))
    {
      g_mutex_unlock (db->mutex);
      return NULL;
    }

  record = dupin_record_create_with_id_real (db, obj, id, error, FALSE);

  g_mutex_unlock (db->mutex);
  g_free (id);

  return record;
}

DupinRecord *
dupin_record_create_with_id (DupinDB * db, tb_json_object_t * obj, gchar * id,
			     GError ** error)
{
  g_return_val_if_fail (db != NULL, NULL);
  g_return_val_if_fail (obj != NULL, NULL);
  g_return_val_if_fail (id != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_record_id (id) != FALSE, NULL);
  g_return_val_if_fail (dupin_util_is_valid_obj (obj) != FALSE, NULL);

  return dupin_record_create_with_id_real (db, obj, id, error, TRUE);
}

static DupinRecord *
dupin_record_create_with_id_real (DupinDB * db, tb_json_object_t * obj,
				  gchar * id, GError ** error, gboolean lock)
{
  DupinRecord *record;
  gchar *errmsg;
  gchar *tmp;

  dupin_database_ref (db);

  if (lock == TRUE)
    g_mutex_lock (db->mutex);

  if (dupin_record_exists_real (db, id, FALSE) == TRUE)
    {
      g_mutex_unlock (db->mutex);
      g_return_val_if_fail (dupin_record_exists (db, id) == FALSE, NULL);
      return NULL;
    }

  record = dupin_record_new (db, id);
  dupin_record_add_revision_obj (record, 1, obj, FALSE);

  tmp =
    sqlite3_mprintf (DUPIN_DB_SQL_INSERT, id, 1,
		     record->last->obj_serialized);

  if (sqlite3_exec (db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
	g_mutex_unlock (db->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      dupin_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return NULL;
    }

  if (lock == TRUE)
    g_mutex_unlock (db->mutex);

  dupin_view_p_record_insert (&db->views,
			      (gchar *) dupin_record_get_id (record),
			      dupin_record_get_revision (record, -1));

  sqlite3_free (tmp);
  return record;
}

static int
dupin_record_read_cb (void *data, int argc, char **argv, char **col)
{
  guint rev = 0;
  gchar *obj = NULL;
  gboolean delete = FALSE;
  gint i;

  for (i = 0; i < argc; i++)
    {
      if (!strcmp (col[i], "rev"))
	rev = atoi (argv[i]);

      else if (!strcmp (col[i], "obj"))
	obj = argv[i];

      else if (!strcmp (col[i], "deleted"))
	delete = !strcmp (argv[i], "TRUE") ? TRUE : FALSE;
    }

  if (rev)
    dupin_record_add_revision_str (data, rev, obj, -1, delete);

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

  dupin_database_ref (db);

  record = dupin_record_new (db, id);

  tmp = sqlite3_mprintf (DUPIN_DB_SQL_READ, id);

  if (lock == TRUE)
    g_mutex_lock (db->mutex);

  if (sqlite3_exec (db->db, tmp, dupin_record_read_cb, record, &errmsg) !=
      SQLITE_OK)
    {
      if (lock == TRUE)
	g_mutex_unlock (db->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);
      dupin_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return 0;
    }

  if (lock == TRUE)
    g_mutex_unlock (db->mutex);

  sqlite3_free (tmp);

  if (!record->last)
    {
      dupin_record_close (record);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD,
		   "The record '%s' doesn't exist.", id);
      return NULL;
    }

  return record;
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

  if ((record = dupin_record_read_real (s->db, argv[0], NULL, FALSE)))
    s->list = g_list_append (s->list, record);

  return 0;
}

gboolean
dupin_record_get_list (DupinDB * db, guint count, guint offset,
		       gboolean descending, GList ** list, GError ** error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;

  struct dupin_record_get_list_t s;

  g_return_val_if_fail (db != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  memset (&s, 0, sizeof (s));
  s.db = db;

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

  g_mutex_lock (db->mutex);

  if (sqlite3_exec (db->db, tmp, dupin_record_get_list_cb, &s, &errmsg) !=
      SQLITE_OK)
    {
      g_mutex_unlock (db->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (db->mutex);

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

gboolean
dupin_record_update (DupinRecord * record, tb_json_object_t * obj,
		     GError ** error)
{
  guint rev;
  gchar *tmp;
  gchar *errmsg;

  g_return_val_if_fail (record != NULL, FALSE);
  g_return_val_if_fail (obj != NULL, FALSE);
  g_return_val_if_fail (dupin_util_is_valid_obj (obj) != FALSE, FALSE);

  g_mutex_lock (record->db->mutex);

  rev = record->last->revision + 1;
  dupin_record_add_revision_obj (record, rev, obj, FALSE);

  tmp =
    sqlite3_mprintf (DUPIN_DB_SQL_INSERT, record->id, rev,
		     record->last->obj_serialized);

  if (sqlite3_exec (record->db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (record->db->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (record->db->mutex);

  dupin_view_p_record_delete (&record->db->views,
			      (gchar *) dupin_record_get_id (record));
  dupin_view_p_record_insert (&record->db->views,
			      (gchar *) dupin_record_get_id (record),
			      dupin_record_get_revision (record, -1));

  sqlite3_free (tmp);
  return TRUE;
}

gboolean
dupin_record_delete (DupinRecord * record, GError ** error)
{
  guint rev;
  gchar *tmp;
  gchar *errmsg;
  gboolean ret = TRUE;

  g_return_val_if_fail (record != NULL, FALSE);

  if (dupin_record_is_deleted (record, -1) == TRUE)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD,
		   "Record already deleted");
      return FALSE;
    }

  g_mutex_lock (record->db->mutex);

  rev = record->last->revision + 1;
  dupin_record_add_revision_obj (record, rev, NULL, TRUE);

  tmp = sqlite3_mprintf (DUPIN_DB_SQL_DELETE, record->id, rev);

  if (sqlite3_exec (record->db->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);
      ret = FALSE;
    }

  g_mutex_unlock (record->db->mutex);

  dupin_view_p_record_delete (&record->db->views,
			      (gchar *) dupin_record_get_id (record));

  sqlite3_free (tmp);
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

guint
dupin_record_get_last_revision (DupinRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->revision;
}

tb_json_object_t *
dupin_record_get_revision (DupinRecord * record, gint revision)
{
  tb_json_t *json;
  DupinRecordRev *r;

  g_return_val_if_fail (record != NULL, NULL);

  if (revision < 0 || revision == record->last->revision)
    r = record->last;

  else
    {
      if (revision > record->last->revision)
	g_return_val_if_fail (dupin_record_get_last_revision (record) >=
			      revision, NULL);

      if (!(r = g_hash_table_lookup (record->revisions, &revision)))
	return NULL;
    }

  if (r->deleted == TRUE)
    g_return_val_if_fail (dupin_record_is_deleted (record, revision) != FALSE,
			  NULL);

  if (r->obj)
    return r->obj;

  json = tb_json_new ();

  if (tb_json_load_from_buffer
      (json, r->obj_serialized, r->obj_serialized_len, NULL) == FALSE
      || tb_json_is_object (json) == FALSE)
    {
      tb_json_destroy (json);
      return NULL;
    }

  r->obj = tb_json_object_and_detach (json);
  tb_json_destroy (json);
  return r->obj;
}

gboolean
dupin_record_is_deleted (DupinRecord * record, gint revision)
{
  DupinRecordRev *r;

  g_return_val_if_fail (record != NULL, FALSE);

  if (revision < 0 || revision == record->last->revision)
    r = record->last;

  else
    {
      if (revision > record->last->revision)
	g_return_val_if_fail (dupin_record_get_last_revision (record) >=
			      revision, FALSE);

      if (!(r = g_hash_table_lookup (record->revisions, &revision)))
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
    g_hash_table_new_full (g_int_hash, g_int_equal, g_free,
			   (GDestroyNotify) dupin_record_rev_close);

  return record;
}

static void
dupin_record_rev_close (DupinRecordRev * rev)
{
  if (rev->obj_serialized)
    g_free (rev->obj_serialized);

  if (rev->obj)
    tb_json_object_destroy (rev->obj);

  g_free (rev);
}

static void
dupin_record_add_revision_obj (DupinRecord * record, guint rev,
			       tb_json_object_t * obj, gboolean delete)
{
  DupinRecordRev *r;
  gint *revp;

  r = g_malloc0 (sizeof (DupinRecordRev));
  r->revision = rev;

  if (obj)
    {
      tb_json_object_duplicate (obj, &r->obj);
      tb_json_object_write_to_buffer (obj, &r->obj_serialized,
				      &r->obj_serialized_len, NULL);
    }

  r->deleted = delete;

  revp = g_malloc (sizeof (guint));
  *revp = rev;

  g_hash_table_insert (record->revisions, revp, r);

  if (!record->last || record->last->revision < rev)
    record->last = r;
}

static void
dupin_record_add_revision_str (DupinRecord * record, guint rev, gchar * str,
			       gssize size, gboolean delete)
{
  DupinRecordRev *r;
  gint *revp;

  r = g_malloc0 (sizeof (DupinRecordRev));
  r->revision = rev;

  if (str && *str)
    {
      if (size < 0)
	size = strlen (str);

      r->obj_serialized = g_strndup (str, size);
      r->obj_serialized_len = size;
    }

  r->deleted = delete;

  revp = g_malloc (sizeof (guint));
  *revp = rev;

  g_hash_table_insert (record->revisions, revp, r);

  if (!record->last || record->last->revision < rev)
    record->last = r;
}

/* EOF */
