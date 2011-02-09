#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"
#include "dupin_linkb.h"
#include "dupin_link_record.h"
#include "dupin_view.h"

#include <stdlib.h>
#include <string.h>

#define DUPIN_LINKB_SQL_MAIN_CREATE \
  "CREATE TABLE IF NOT EXISTS Dupin (\n" \
  "  id          CHAR(255) NOT NULL,\n" \
  "  rev         INTEGER NOT NULL DEFAULT 1,\n" \
  "  hash        CHAR(255) NOT NULL,\n" \
  "  obj         TEXT,\n" \
  "  deleted     BOOL DEFAULT FALSE,\n" \
  "  tm          INTEGER NOT NULL,\n" \
  "  context_id  CHAR(255) NOT NULL,\n" \
  "  label       CHAR(255) NOT NULL,\n" \
  "  href        TEXT NOT NULL,\n" \
  "  rel         TEXT DEFAULT NULL,\n" \
  "  is_weblink  BOOL DEFAULT FALSE,\n" \
  "  tag         TEXT DEFAULT NULL,\n" \
  "  idspath     TEXT NOT NULL COLLATE dupincmp,\n" \
  "  labelspath  TEXT NOT NULL COLLATE dupincmp,\n" \
  "  PRIMARY     KEY(id, rev, hash)\n" \
  ");"

#define DUPIN_LINKB_SQL_CREATE_INDEX \
  "CREATE INDEX IF NOT EXISTS DupinId ON Dupin (id);" \
  "CREATE INDEX IF NOT EXISTS DupinHrefDeletedContextIdTag ON Dupin (href,deleted,tag);"

#define DUPIN_LINKB_SQL_DESC_CREATE \
  "CREATE TABLE IF NOT EXISTS DupinLinkB (\n" \
  "  total_webl_ins  INTEGER NOT NULL DEFAULT 0,\n" \
  "  total_webl_del  INTEGER NOT NULL DEFAULT 0,\n" \
  "  total_rel_ins   INTEGER NOT NULL DEFAULT 0,\n" \
  "  total_rel_del   INTEGER NOT NULL DEFAULT 0,\n" \
  "  parent          CHAR(255) NOT NULL,\n" \
  "  isdb            BOOL DEFAULT TRUE,\n" \
  "  compact_id      CHAR(255),\n" \
  "  check_id        CHAR(255)\n" \
  ");"

#define DUPIN_LINKB_SQL_TOTAL \
        "SELECT count(*) AS c, max(rev) as rev FROM Dupin AS d"

#define DUPIN_LINKB_COMPACT_COUNT 100
#define DUPIN_LINKB_CHECK_COUNT   100

gchar **
dupin_get_linkbases (Dupin * d)
{
  guint i;
  gsize size;
  gchar **ret;
  gpointer key;
  GHashTableIter iter;

  g_return_val_if_fail (d != NULL, NULL);

  g_mutex_lock (d->mutex);

  if (!(size = g_hash_table_size (d->linkbs)))
    {
      g_mutex_unlock (d->mutex);
      return NULL;
    }

  ret = g_malloc (sizeof (gchar *) * (g_hash_table_size (d->linkbs) + 1));

  i = 0;
  g_hash_table_iter_init (&iter, d->linkbs);
  while (g_hash_table_iter_next (&iter, &key, NULL) == TRUE)
    ret[i++] = g_strdup (key);

  ret[i] = NULL;

  g_mutex_unlock (d->mutex);

  return ret;
}

gboolean
dupin_linkbase_exists (Dupin * d, gchar * linkb)
{
  gboolean ret;

  g_mutex_lock (d->mutex);
  ret = g_hash_table_lookup (d->linkbs, linkb) != NULL ? TRUE : FALSE;
  g_mutex_unlock (d->mutex);

  return ret;
}

DupinLinkB *
dupin_linkbase_open (Dupin * d, gchar * linkb, GError ** error)
{
  DupinLinkB *ret;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (linkb != NULL, NULL);

  g_mutex_lock (d->mutex);

  if (!(ret = g_hash_table_lookup (d->linkbs, linkb)) || ret->todelete == TRUE)
    g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		 "Linkbase '%s' doesn't exist.", linkb);

  else {
    /* fprintf(stderr,"ref++\n"); */
    ret->ref++;
	};

  g_mutex_unlock (d->mutex);

  return ret;
}

DupinLinkB *
dupin_linkbase_new (Dupin * d, gchar * linkb,
		    gchar * parent, gboolean is_db,
		    GError ** error)
{
  DupinLinkB *ret;
  gchar *path;
  gchar *name;
  gchar * str;
  gchar * errmsg;

  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (linkb != NULL, NULL);
  g_return_val_if_fail (parent != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_linkb_name (linkb) == TRUE, NULL);

  if (is_db == TRUE)
    g_return_val_if_fail (dupin_database_exists (d, parent) == TRUE, NULL);
  else
    g_return_val_if_fail (dupin_linkbase_exists (d, parent) == TRUE, NULL);

  g_mutex_lock (d->mutex);

  if ((ret = g_hash_table_lookup (d->linkbs, linkb)))
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Linkbase '%s' already exist.", linkb);
      g_mutex_unlock (d->mutex);
      return NULL;
    }

  name = g_strdup_printf ("%s%s", linkb, DUPIN_LINKB_SUFFIX);
  path = g_build_path (G_DIR_SEPARATOR_S, d->path, name, NULL);
  g_free (name);

  if (!(ret = dupin_linkb_create (d, linkb, path, error)))
    {
      g_mutex_unlock (d->mutex);
      g_free (path);
      return NULL;
    }

  g_free (path);

  ret->parent = g_strdup (parent);
  ret->parent_is_db = is_db;

  /* fprintf(stderr,"ref++\n"); */
  ret->ref++;

  str = sqlite3_mprintf ("INSERT INTO DupinLinkB "
                         "(parent, isdb, compact_id, check_id) "
                         "VALUES('%q', '%s', 0, 0)", parent, is_db ? "TRUE" : "FALSE");

  if (sqlite3_exec (ret->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (d->mutex);
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                       errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (str);
      dupin_linkb_free (ret);
      return NULL;
    }

  sqlite3_free (str);

  g_mutex_unlock (d->mutex);

  if (dupin_linkbase_p_update (ret, error) == FALSE)
    {
      dupin_linkb_free (ret);
      return NULL;
    }

  g_mutex_lock (d->mutex);
  g_hash_table_insert (d->linkbs, g_strdup (linkb), ret);
  g_mutex_unlock (d->mutex);

  return ret;
}

struct dupin_linkbase_p_update_t
{
  gchar *parent;
  gboolean isdb;
};

static int
dupin_linkbase_p_update_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_linkbase_p_update_t *update = data;

  if (argv[0] && *argv[0])
    update->parent = g_strdup (argv[0]);

  if (argv[1] && *argv[1])
    update->isdb = !g_strcmp0 (argv[1], "TRUE") ? TRUE : FALSE;

  return 0;
}

#define DUPIN_LINKBASE_P_SIZE       64

static void
dupin_linkbase_p_update_real (DupinLinkBP * p, DupinLinkB * linkb)
{
  if (p->linkbs == NULL)
    {
      p->linkbs = g_malloc (sizeof (DupinLinkB *) * DUPIN_LINKBASE_P_SIZE);
      p->size = DUPIN_LINKBASE_P_SIZE;
    }

  else if (p->numb == p->size)
    {
      p->size += DUPIN_LINKBASE_P_SIZE;
      p->linkbs = g_realloc (p->linkbs, sizeof (DupinLinkB *) * p->size);
    }

  p->linkbs[p->numb] = linkb;
  p->numb++;
}

gboolean
dupin_linkbase_p_update (DupinLinkB * linkb, GError ** error)
{
  gchar *errmsg;
  struct dupin_linkbase_p_update_t update;
  gchar *query = "SELECT parent, isdb FROM DupinLinkB";

  memset (&update, 0, sizeof (struct dupin_linkbase_p_update_t));

  g_mutex_lock (linkb->d->mutex);

  if (sqlite3_exec (linkb->db, query, dupin_linkbase_p_update_cb, &update, &errmsg)
      != SQLITE_OK)
    {
      g_mutex_unlock (linkb->d->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                   errmsg);
      sqlite3_free (errmsg);
      return FALSE;
    }

  g_mutex_unlock (linkb->d->mutex);

  if (!update.parent)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
                   "Internal error.");
      return FALSE;
    }

  if (update.isdb == TRUE)
    {
      DupinDB *db;

      if (!(db = dupin_database_open (linkb->d, update.parent, error)))
        {
          g_free (update.parent);
          return FALSE;
        }

      g_mutex_lock (db->mutex);
      dupin_linkbase_p_update_real (&db->linkbs, linkb);
      g_mutex_unlock (db->mutex);

      dupin_database_unref (db);
    }
  else
    {
      DupinLinkB *l;

      if (!(l = dupin_linkbase_open (linkb->d, update.parent, error)))
        {
          g_free (update.parent);
          return FALSE;
        }

      g_mutex_lock (l->mutex);
      dupin_linkbase_p_update_real (&l->linkbs, linkb);
      g_mutex_unlock (l->mutex);

      dupin_linkbase_unref (linkb);
    }

  /* make sure parameters are set after dupin server restart on existing link base */

  if (linkb->parent == NULL)
    linkb->parent = update.parent;
  else
    g_free (update.parent);

  linkb->parent_is_db = update.isdb;

  return TRUE;
}

void
dupin_linkbase_p_record_insert (DupinLinkBP * p, gchar * id,
                            JsonObject * obj)
{
  gsize i;

  for (i = 0; i < p->numb; i++)
    {
      DupinLinkB *linkb = p->linkbs[i];

      dupin_linkbase_p_record_insert (&linkb->linkbs, id, obj);
    }
}

void
dupin_linkbase_p_record_delete (DupinLinkBP * p, gchar * pid)
{
  gsize i;

  for (i = 0; i < p->numb; i++)
    {
      DupinLinkB *linkb = p->linkbs[i];

      dupin_linkbase_p_record_delete (&linkb->linkbs, pid);

      //dupin_link_record_delete (linkb, pid);
    }
}

void
dupin_linkbase_ref (DupinLinkB * linkb)
{
  Dupin *d;

  g_return_if_fail (linkb != NULL);

  d = linkb->d;

  g_mutex_lock (d->mutex);
  /* fprintf(stderr,"ref++\n"); */
  linkb->ref++;
  g_mutex_unlock (d->mutex);
}

void
dupin_linkbase_unref (DupinLinkB * linkb)
{
  Dupin *d;

  g_return_if_fail (linkb != NULL);

  d = linkb->d;
  g_mutex_lock (d->mutex);

  if (linkb->ref >= 0) {
    /* fprintf(stderr,"ref--\n"); */
    linkb->ref--;
    };

  if (linkb->ref != 0 && linkb->todelete == TRUE)
    g_warning ("dupin_linkbase_unref: (thread=%p) linkbase %s flagged for deletion but can't free it due ref is %d\n", g_thread_self (), linkb->name, (gint) linkb->ref);

  if (linkb->ref == 0 && linkb->todelete == TRUE)
    g_hash_table_remove (d->linkbs, linkb->name);

  g_mutex_unlock (d->mutex);
}

gboolean
dupin_linkbase_delete (DupinLinkB * linkb, GError ** error)
{
  Dupin *d;

  g_return_val_if_fail (linkb != NULL, FALSE);

  d = linkb->d;

  g_mutex_lock (d->mutex);
  linkb->todelete = TRUE;
  g_mutex_unlock (d->mutex);

  return TRUE;
}

const gchar *
dupin_linkbase_get_name (DupinLinkB * linkb)
{
  g_return_val_if_fail (linkb != NULL, NULL);
  return linkb->name;
}

gsize
dupin_linkbase_get_size (DupinLinkB * linkb)
{
  struct stat st;

  g_return_val_if_fail (linkb != NULL, 0);

  if (g_stat (linkb->path, &st) != 0)
    return 0;

  return (gsize) st.st_size;
}

static void
dupin_linkbase_generate_id_create (DupinLinkB * linkb, gchar id[DUPIN_ID_MAX_LEN])
{
  do
    {
      dupin_util_generate_id (id);
    }
  while (dupin_link_record_exists_real (linkb, id, FALSE) == TRUE);
}

gchar *
dupin_linkbase_generate_id_real (DupinLinkB * linkb, GError ** error, gboolean lock)
{
  gchar id[DUPIN_ID_MAX_LEN];

  if (lock == TRUE)
    g_mutex_lock (linkb->mutex);

  dupin_linkbase_generate_id_create (linkb, id);

  if (lock == TRUE)
    g_mutex_unlock (linkb->mutex);

  return g_strdup (id);
}

gchar *
dupin_linkbase_generate_id (DupinLinkB * linkb, GError ** error)
{
  g_return_val_if_fail (linkb != NULL, NULL);

  return dupin_linkbase_generate_id_real (linkb, error, TRUE);
}

gchar *
dupin_linkbase_get_parent (DupinLinkB * linkb)
{
  g_return_val_if_fail (linkb != NULL, NULL);

  return linkb->parent;
}

gboolean
dupin_linkbase_get_parent_is_db (DupinLinkB * linkb)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  return linkb->parent_is_db;
}

/* Internal: */
void
dupin_linkb_free (DupinLinkB * linkb)
{
  g_message("dupin_linkb_free: total number of changes for '%s' linkbase: %d\n", linkb->name, (gint)sqlite3_total_changes (linkb->db));

  if (linkb->cache_last_context_id)
    g_free (linkb->cache_last_context_id);

  if (linkb->cache_idspath)
    g_hash_table_destroy (linkb->cache_idspath);

  if (linkb->cache_labelspath)
    g_hash_table_destroy (linkb->cache_labelspath);

  if (linkb->db)
    sqlite3_close (linkb->db);

  if (linkb->todelete == TRUE)
    g_unlink (linkb->path);

  if (linkb->name)
    g_free (linkb->name);

  if (linkb->path)
    g_free (linkb->path);

  if (linkb->parent)
    g_free (linkb->parent);

  if (linkb->mutex)
    g_mutex_free (linkb->mutex);

  if (linkb->views.views)
    g_free (linkb->views.views);

  if (linkb->linkbs.linkbs)
    g_free (linkb->linkbs.linkbs);

  if (linkb->error_msg)
    g_free (linkb->error_msg);

  if (linkb->warning_msg)
    g_free (linkb->warning_msg);

  g_free (linkb);
}

DupinLinkB *
dupin_linkb_create (Dupin * d, gchar * name, gchar * path, GError ** error)
{
  gchar *errmsg;
  DupinLinkB *linkb;

  linkb = g_malloc0 (sizeof (DupinLinkB));

  linkb->d = d;

  linkb->name = g_strdup (name);
  linkb->path = g_strdup (path);

  linkb->tocompact = FALSE;
  linkb->compact_processed_count = 0;

  linkb->tocheck = FALSE;
  linkb->check_processed_count = 0;

  /* NOTE - caches for ids and labels path generation, especially useful on bulk inserts */
  linkb->cache_on = FALSE;
  linkb->cache_last_context_id = NULL;
  linkb->cache_idspath = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);
  linkb->cache_labelspath = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);
 
  if (sqlite3_open (linkb->path, &linkb->db) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN,
		   "Linkbase error.");
      dupin_linkb_free (linkb);
      return NULL;
    }

  /* NOTE - set simple collation functions for path columns - see http://wiki.apache.org/couchdb/How_to_store_hierarchical_data */

  if (sqlite3_create_collation (linkb->db, "dupincmp", SQLITE_UTF8,  linkb, dupin_view_collation) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      dupin_linkb_free (linkb);
      return NULL;
    }

  if (sqlite3_exec (linkb->db, DUPIN_LINKB_SQL_MAIN_CREATE, NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (linkb->db, DUPIN_LINKB_SQL_CREATE_INDEX, NULL, NULL, &errmsg) != SQLITE_OK
      || sqlite3_exec (linkb->db, DUPIN_LINKB_SQL_DESC_CREATE, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
		   errmsg);
      sqlite3_free (errmsg);
      dupin_linkb_free (linkb);
      return NULL;
    }

  linkb->mutex = g_mutex_new ();

  return linkb;
}

gsize
dupin_linkbase_count (DupinLinkB * linkb,
		      DupinLinksType links_type,
	              DupinCountType count_type)
{
  g_return_val_if_fail (linkb != NULL, 0);

  struct dupin_link_record_select_total_t count;
  memset (&count, 0, sizeof (count));

  g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, DUPIN_LINKB_SQL_GET_TOTALS, dupin_link_record_select_total_cb, &count, NULL) !=
      SQLITE_OK)
    {
      g_mutex_unlock (linkb->mutex);
      return 0;
    }

  g_mutex_unlock (linkb->mutex);

  if (count_type == DP_COUNT_EXIST)
    {
      if (links_type == DP_LINK_TYPE_WEB_LINK)
        return count.total_webl_ins;

      else if (links_type == DP_LINK_TYPE_RELATIONSHIP)
        return count.total_rel_ins;

      else
        return count.total_webl_ins + count.total_rel_ins;
    }
  else if (count_type == DP_COUNT_DELETE)
    {
      if (links_type == DP_LINK_TYPE_WEB_LINK)
        return count.total_webl_del;

      else if (links_type == DP_LINK_TYPE_RELATIONSHIP)
        return count.total_rel_del;

      else
        return count.total_webl_del + count.total_rel_del;
    }
  else
    {
      return count.total_webl_ins + count.total_rel_ins + count.total_webl_del + count.total_rel_del;
    }
}

static int
dupin_linkbase_get_max_rowid_cb (void *data, int argc, char **argv,
                                  char **col)
{
  gsize *max_rowid = data;

  if (argv[0])
    *max_rowid = atoi (argv[0]);

  return 0;
}

gboolean
dupin_linkbase_get_max_rowid (DupinLinkB * linkb, gsize * max_rowid)
{
  gchar *query;
  gchar * errmsg=NULL;

  g_return_val_if_fail (linkb != NULL, 0);

  *max_rowid=0;

  query = "SELECT max(ROWID) as max_rowid FROM Dupin";

  g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, query, dupin_linkbase_get_max_rowid_cb, max_rowid, &errmsg) !=
      SQLITE_OK)
    {
      g_mutex_unlock (linkb->mutex);

      g_error("dupin_linkbase_get_max_rowid: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (linkb->mutex);

  return TRUE;
}

struct dupin_linkbase_get_changes_list_t
{
  DupinChangesType style;
  GList *list;
};

static int
dupin_linkbase_get_changes_list_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_linkbase_get_changes_list_t *s = data;

  guint rev = 0;
  gsize tm = 0;
  gchar *id = NULL;
  gchar *hash = NULL;
  gchar *obj = NULL;
  gboolean delete = FALSE;
  gsize rowid=0;
  gint i;
  gchar *context_id = NULL;
  gboolean is_weblink = FALSE;
  gchar *href = NULL;
  gchar *rel = NULL;
  gchar *tag = NULL; 
  gchar *label = NULL; 

  for (i = 0; i < argc; i++)
    {
      /* shouldn't this be double and use atof() ?!? */
      if (!g_strcmp0 (col[i], "rev"))
        rev = atoi (argv[i]);

      else if (!g_strcmp0 (col[i], "tm"))
        tm = (gsize)atof(argv[i]);

      else if (!g_strcmp0 (col[i], "hash"))
        hash = argv[i];

      else if (!g_strcmp0 (col[i], "obj"))
        obj = argv[i];

      else if (!g_strcmp0 (col[i], "deleted"))
        delete = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;

      else if (!g_strcmp0 (col[i], "rowid"))
        rowid = atoi(argv[i]);

      else if (!g_strcmp0 (col[i], "id"))
        id = argv[i];

      else if (!g_strcmp0 (col[i], "context_id"))
        context_id = argv[i];

      else if (!g_strcmp0 (col[i], "href"))
        href = argv[i];

      else if (!g_strcmp0 (col[i], "rel"))
        rel = argv[i];

      else if (!g_strcmp0 (col[i], "is_weblink"))
        is_weblink = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;

      else if (!g_strcmp0 (col[i], "tag"))
        tag = argv[i];

      else if (!g_strcmp0 (col[i], "label"))
        label = argv[i];
    }

  if (rev && hash !=NULL)
    {
      JsonNode *change_node=json_node_new (JSON_NODE_OBJECT);
      JsonObject *change=json_object_new();
      json_node_take_object (change_node, change);

      json_object_set_int_member (change,"seq", rowid);
      json_object_set_string_member (change,"id", id);

      if (delete == TRUE)
        json_object_set_boolean_member (change, "deleted", delete);

      json_object_set_int_member (change, "created", tm);

      JsonNode *change_details_node=json_node_new (JSON_NODE_ARRAY);
      JsonArray *change_details=json_array_new();
      json_node_take_array (change_details_node, change_details);
      json_object_set_member (change, "changes", change_details_node);

      JsonNode * node = json_node_new (JSON_NODE_OBJECT);
      JsonObject * node_obj = json_object_new ();
      json_node_take_object (node, node_obj);
      json_array_add_element (change_details, node);  

      gchar mvcc[DUPIN_ID_MAX_LEN];
      dupin_util_mvcc_new (rev, hash, mvcc);

      json_object_set_string_member (node_obj, "rev", mvcc);

      json_object_set_string_member (node_obj, "context_id", context_id);
      json_object_set_string_member (node_obj, "label", label);
      json_object_set_string_member (node_obj, "href", href);
      if (rel != NULL)
        json_object_set_string_member (node_obj, "rel", rel);
      if (is_weblink == TRUE)
        json_object_set_boolean_member (change, "is_weblink", is_weblink);
      if (tag != NULL)
        json_object_set_string_member (node_obj, "tag", tag);

      if (s->style == DP_CHANGES_MAIN_ONLY)
        {
        }
      else if (s->style == DP_CHANGES_ALL_DOCS)
        {
        }

      s->list = g_list_append (s->list, change_node);
    }

  return 0;
}

gboolean
dupin_linkbase_get_changes_list (DupinLinkB *              linkb,
                                 guint                  count,
                                 guint                  offset,
                                 gsize                  since,
                                 gsize                  to,
         			 DupinChangesType	changes_type,
				 DupinCountType         count_type,
                                 DupinOrderByType       orderby_type,
                                 gboolean               descending,
				 gchar *		context_id,
				 gchar *		tag,
                                 GList **               list,
                                 GError **              error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;
  gchar *check_deleted="";
  gchar *check_linktype="";

  struct dupin_linkbase_get_changes_list_t s;

  g_return_val_if_fail (linkb != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  if (context_id != NULL)
    g_return_val_if_fail (dupin_link_record_util_is_valid_context_id (context_id) == TRUE, FALSE);

  memset (&s, 0, sizeof (s));
  s.style = changes_type;

  str = g_string_new ("SELECT id, rev, hash, obj, deleted, tm, ROWID AS rowid, context_id, href, rel, is_weblink, tag, label FROM Dupin as d WHERE d.rev = (select max(rev) as rev FROM Dupin WHERE id=d.id) ");

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  if (changes_type == DP_CHANGES_WEB_LINKS)
    check_linktype = " d.is_weblink = 'TRUE' ";
  else if (changes_type == DP_CHANGES_RELATIONSHIPS)
    check_linktype = " d.is_weblink = 'FALSE' ";

  gchar * op = "AND";

  if (since > 0 && to > 0)
    {
      g_string_append_printf (str, " %s d.ROWID >= %d AND d.ROWID <= %d ", op, (gint)since, (gint)to);
    }
  else if (since > 0)
    {
      g_string_append_printf (str, " %s d.ROWID >= %d ", op, (gint)since);
    }
  else if (to > 0)
    {
      g_string_append_printf (str, " %s d.ROWID <= %d ", op, (gint)to);
    }

  if (g_strcmp0 (check_deleted, ""))
    {
      g_string_append_printf (str, " %s %s ", op, check_deleted);
    }

  if (g_strcmp0 (check_linktype, ""))
    {
      g_string_append_printf (str, " %s %s ", op, check_linktype);
    }

  if (context_id != NULL)
    {
      gchar * tmp2 = sqlite3_mprintf (" %s d.context_id = '%q' ", op, context_id);
      str = g_string_append (str, tmp2);
      sqlite3_free (tmp2);
    }

  if (tag != NULL)
    {
      gchar * tmp2 = sqlite3_mprintf (" %s d.tag = '%q' ", op, tag);
      str = g_string_append (str, tmp2);
      sqlite3_free (tmp2);
    }

  //str = g_string_append (str, " GROUP BY d.id "); 

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

//g_message("dupin_linkbase_get_changes_list() query=%s\n",tmp);

  g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, tmp, dupin_linkbase_get_changes_list_cb, &s, &errmsg) !=
      SQLITE_OK)
    {
      g_mutex_unlock (linkb->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (linkb->mutex);

  g_free (tmp);

  *list = s.list;
  return TRUE;
}

void
dupin_linkbase_get_changes_list_close
				(GList *                list)
{
  while (list)
    {
      json_node_free (list->data);
      list = g_list_remove (list, list->data);
    }
}

static int
dupin_linkbase_get_total_changes_cb
				(void *data, int argc, char **argv, char **col)
{
  gsize *numb = data;

  if (argv[0])
    *numb += 1;

  return 0;
}

gboolean
dupin_linkbase_get_total_changes
				(DupinLinkB *           linkb,
                                 gsize *                total,
                                 gsize                  since,
                                 gsize                  to,
         			 DupinChangesType	changes_type,
			 	 DupinCountType         count_type,
                                 gboolean               inclusive_end,
				 gchar *                context_id,
				 gchar *		tag,
                                 GError **              error)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  gchar *errmsg;
  gchar *tmp;
  GString *str;

  if (context_id != NULL)
    g_return_val_if_fail (dupin_link_record_util_is_valid_context_id (context_id) == TRUE, FALSE);

  *total = 0;

  gchar *check_deleted="";
  gchar *check_linktype="";

  str = g_string_new (DUPIN_LINKB_SQL_TOTAL);

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  if (changes_type == DP_CHANGES_WEB_LINKS)
    check_linktype = " d.is_weblink = 'TRUE' ";
  else if (changes_type == DP_CHANGES_RELATIONSHIPS)
    check_linktype = " d.is_weblink = 'FALSE' ";

  gchar * op = "";

  if (since > 0 && to > 0)
    {
      g_string_append_printf (str, " WHERE d.ROWID >= %d AND d.ROWID <= %d ", (gint)since, (gint)to);
      op = "AND";
    }
  else if (since > 0)
    {
      g_string_append_printf (str, " WHERE d.ROWID >= %d ", (gint)since);
      op = "AND";
    }
  else if (to > 0)
    {
      g_string_append_printf (str, " WHERE d.ROWID <= %d ", (gint)to);
      op = "AND";
    }

  if (g_strcmp0 (check_deleted, ""))
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";

      g_string_append_printf (str, " %s %s ", op, check_deleted);
      op = "AND";
    }

  if (g_strcmp0 (check_linktype, ""))
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";

      g_string_append_printf (str, " %s %s ", op, check_linktype);
      op = "AND";
    }

  if (context_id != NULL)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";

      gchar * tmp2 = sqlite3_mprintf (" %s d.context_id = '%q' ", op, context_id);
      str = g_string_append (str, tmp2);
      sqlite3_free (tmp2);
      op = "AND";
    }

  if (tag != NULL)
    { 
      if (!g_strcmp0 (op, ""))
        op = "WHERE";

      gchar * tmp2 = sqlite3_mprintf (" %s d.tag = '%q' ", op, tag);
      str = g_string_append (str, tmp2);
      sqlite3_free (tmp2);
    }

  str = g_string_append (str, " GROUP BY d.id "); 

  tmp = g_string_free (str, FALSE);

//g_message("dupin_linkbase_get_total_changes() query=%s\n",tmp);

  g_mutex_lock (linkb->mutex);

  *total = 0;

  if (sqlite3_exec (linkb->db, tmp, dupin_linkbase_get_total_changes_cb, total, &errmsg) !=
      SQLITE_OK)
    {
      g_mutex_unlock (linkb->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
                   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (linkb->mutex);

  g_free (tmp);

  return TRUE;
}

/* Linkbase compaction */

static int
dupin_linkbase_compact_cb (void *data, int argc, char **argv, char **col)
{
  gchar **compact_id = data;

  if (argv[0] && *argv[0])
    *compact_id = g_strdup (argv[0]);

  return 0;
}

gboolean
dupin_linkbase_thread_compact (DupinLinkB * linkb, gsize count)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  gchar * compact_id = NULL;
  gsize rowid;
  gchar * errmsg;
  GList *results, *list;

  gboolean ret = TRUE;

  gchar *str;

  /* get last position we compacted and get anything up to count after that */
  gchar * query = "SELECT compact_id as c FROM DupinLinkB";
  g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, query, dupin_linkbase_compact_cb, &compact_id, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (linkb->mutex);

      g_error("dupin_linkbase_thread_compact: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (linkb->mutex);

  gsize start_rowid = (compact_id != NULL) ? atoi(compact_id)+1 : 1;

  if (dupin_link_record_get_list (linkb, count, 0, start_rowid, 0, DP_LINK_TYPE_ANY, DP_COUNT_ALL, DP_ORDERBY_ROWID, FALSE, NULL, NULL, NULL, &results, NULL) ==
      FALSE || !results)
    {
      if (compact_id != NULL)
        g_free(compact_id);

      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

  for (list = results; list; list = list->next)
    {
      DupinLinkRecord * record = list->data;

      guint last_revision = record->last->revision;

      gchar *tmp = sqlite3_mprintf ("DELETE FROM Dupin WHERE id = '%q' AND rev < %d", (gchar *) dupin_link_record_get_id (record), (gint)last_revision);

//g_message("dupin_linkbase_thread_compact: query=%s\n", tmp);

      g_mutex_lock (linkb->mutex);

      if (sqlite3_exec (linkb->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
        {
          g_mutex_unlock (linkb->mutex);

          sqlite3_free (tmp);

          g_error ("dupin_linkbase_thread_compact: %s", errmsg);

          sqlite3_free (errmsg);

          return FALSE;
        }

      linkb->compact_processed_count++;

      g_mutex_unlock (linkb->mutex);

      sqlite3_free (tmp);

      rowid = dupin_link_record_get_rowid (record);

      if (compact_id != NULL)
        g_free(compact_id);

      compact_id = g_strdup_printf ("%i", (gint)rowid);

//g_message("dupin_linkbase_thread_compact(%p) compact_id=%s as fetched",g_thread_self (), compact_id);
    }
  
  dupin_link_record_get_list_close (results);

//g_message("dupin_linkbase_thread_compact() compact_id=%s as to be stored",compact_id);

//g_message("dupin_linkbase_thread_compact(%p)  finished last_compact_rowid=%s - compacted %d\n", g_thread_self (), compact_id, (gint)linkb->compact_processed_count);

  str = sqlite3_mprintf ("UPDATE DupinLinkB SET compact_id = '%q'", compact_id);

  if (compact_id != NULL)
    g_free (compact_id);

  g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (linkb->mutex);
      sqlite3_free (str);

      g_error("dupin_linkbase_thread_compact: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (linkb->mutex);

  sqlite3_free (str);

  return ret;
}

void
dupin_linkbase_compact_func (gpointer data, gpointer user_data)
{
  DupinLinkB * linkb = (DupinLinkB*) data;
  gchar * errmsg;

  dupin_linkbase_ref (linkb);

  g_mutex_lock (linkb->mutex);
  linkb->compact_thread = g_thread_self ();
  g_mutex_unlock (linkb->mutex);

//g_message("dupin_linkbase_compact_func(%p) started\n",g_thread_self ());

  g_mutex_lock (linkb->mutex);
  linkb->compact_processed_count = 0;
  g_mutex_unlock (linkb->mutex);

  while (linkb->todelete == FALSE)
    {
      gboolean compact_operation = dupin_linkbase_thread_compact (linkb, DUPIN_LINKB_COMPACT_COUNT);

      if (compact_operation == FALSE)
        {
//g_message("dupin_linkbase_compact_func(%p) Compacted TOTAL %d records\n", g_thread_self (), (gint)linkb->compact_processed_count);

          /* claim disk space back */

//g_message("dupin_linkbase_compact_func: VACUUM and ANALYZE\n");

          g_mutex_lock (linkb->mutex);

          if (sqlite3_exec (linkb->db, "VACUUM", NULL, NULL, &errmsg) != SQLITE_OK
             || sqlite3_exec (linkb->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK)
            {
              g_mutex_unlock (linkb->mutex);
              g_error ("dupin_linkbase_compact_func: %s", errmsg);
              sqlite3_free (errmsg);
              break;
            }

          g_mutex_unlock (linkb->mutex);

          break;
        }
    }

//g_message("dupin_linkbase_compact_func(%p) finished and linkbase is compacted\n",g_thread_self ());

  g_mutex_lock (linkb->mutex);
  linkb->tocompact = FALSE;
  g_mutex_unlock (linkb->mutex);

  g_mutex_lock (linkb->mutex);
  linkb->compact_thread = NULL;
  g_mutex_unlock (linkb->mutex);

  dupin_linkbase_unref (linkb);
}

void
dupin_linkbase_compact (DupinLinkB * linkb)
{
  g_return_if_fail (linkb != NULL);

  if (dupin_linkbase_is_compacting (linkb))
    {
      g_mutex_lock (linkb->mutex);
      linkb->tocompact = TRUE;
      g_mutex_unlock (linkb->mutex);

//g_message("dupin_linkbase_compact(%p): linkbase is still compacting linkb->compact_thread=%p\n", g_thread_self (), linkb->compact_thread);
    }
  else
    {
//g_message("dupin_linkbase_compact(%p): push to thread pools linkb->compact_thread=%p\n", g_thread_self (), linkb->compact_thread);

      if (!linkb->compact_thread)
        {
          g_thread_pool_push(linkb->d->linkb_compact_workers_pool, linkb, NULL);
        }
    }
}

gboolean
dupin_linkbase_is_compacting (DupinLinkB * linkb)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  if (linkb->compact_thread)
    return TRUE;

  return FALSE;
}

gboolean
dupin_linkbase_is_compacted (DupinLinkB * linkb)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  if (dupin_linkbase_is_compacting (linkb))
    return FALSE;

  return linkb->tocompact ? FALSE : TRUE;
}

/* Links checking */

static int
dupin_linkbase_check_cb (void *data, int argc, char **argv, char **col)
{
  gchar **check_id = data;

  if (argv[0] && *argv[0])
    *check_id = g_strdup (argv[0]);

  return 0;
}

gboolean
dupin_linkbase_thread_check (DupinLinkB * linkb, gsize count)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  DupinDB * parent_db=NULL;
  DupinLinkB * parent_linkb=NULL;
  gchar * check_id = NULL;
  gsize rowid;
  gchar * errmsg;
  GList *results, *list;

  gboolean ret = TRUE;

  gchar *str;

  /* get last position we checked and get anything up to count after that */
  gchar * query = "SELECT check_id as c FROM DupinLinkB";
  g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, query, dupin_linkbase_check_cb, &check_id, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (linkb->mutex);

      g_error("dupin_linkbase_thread_check: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (linkb->mutex);

  /* RULE 1 - for each link record check whether or not the countext_id has been deleted, and delete the link itself (I.e. mark link as deleted only) */

  if (dupin_linkbase_get_parent_is_db (linkb) == TRUE )
    {
      if (! (parent_db = dupin_database_open (linkb->d, dupin_linkbase_get_parent (linkb), NULL)))
        {
          g_error("dupin_linkbase_thread_check: Cannot connect to parent database %s", dupin_linkbase_get_parent (linkb));
          return FALSE;
        }
    }
  else
    {
      if (!(parent_linkb = dupin_linkbase_open (linkb->d, dupin_linkbase_get_parent (linkb), NULL)))
        {
          g_error("dupin_linkbase_thread_check: Cannot connect to parent linkbase %s", dupin_linkbase_get_parent (linkb));
          return FALSE;
        }
    }

  gsize start_rowid = (check_id != NULL) ? atoi(check_id)+1 : 1;

  if (dupin_link_record_get_list (linkb, count, 0, start_rowid, 0, DP_LINK_TYPE_ANY, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE, NULL, NULL, NULL, &results, NULL) ==
      FALSE || !results)
    {
      if (check_id != NULL)
        g_free(check_id);

      return FALSE;
    }

  if (g_list_length (results) != count)
    ret = FALSE;

  for (list = results; list; list = list->next)
    {
      DupinLinkRecord * record = list->data;

      /* we try to be clever in case the below delete will interfer - we really hope not :) */
      if (dupin_link_record_is_deleted (record, NULL) == TRUE)
        continue;

      /* STEP A - check if the context_id of the link record has been deleted */
      gchar * context_id = (gchar *)dupin_link_record_get_context_id (record);

//g_message("dupin_linkbase_thread_check(%p) checking STEP A for context_id=%s\n",g_thread_self (), context_id);

      if (dupin_linkbase_get_parent_is_db (linkb) == TRUE )
        {
          DupinRecord * doc_id_record = NULL;

          if (!(doc_id_record = dupin_record_read (parent_db, context_id, NULL)))
            {
              g_error ("dupin_linkbase_thread_check: Cannot read record from parent database");
              break;
            }

         if (dupin_record_is_deleted (doc_id_record, NULL) == FALSE)
           {
             dupin_record_close (doc_id_record);
	     continue;
           }
         dupin_record_close (doc_id_record);
       }
     else
       {
          DupinLinkRecord * link_id_record = NULL;

          if (!(link_id_record = dupin_link_record_read (parent_linkb, context_id, NULL)))
            {
              g_error ("dupin_linkbase_thread_check: Cannot read record from parent linkbase");
              break;
            }

         if (dupin_link_record_is_deleted (link_id_record, NULL) == FALSE)
           {
             dupin_link_record_close (link_id_record);
	     continue;
           }
         dupin_link_record_close (link_id_record);
       }

     /* STEP B - delete (update) the record */

//g_message("dupin_linkbase_thread_check(%p) STEP B for context_id=%s\n",g_thread_self (), context_id);

     /* NOTE - hopefully this will work and will not generate any problems (I.e. modifiying DB while reading from it
	       with the results cursor - but the ROWID is going to be higher anyway, so we should be safe also for views */

     if (!(dupin_link_record_delete (record, NULL)))
        {
          g_error ("dupin_linkbase_thread_check: Cannot delete link record");
          break;
        }

//g_message("dupin_linkbase_thread_check(%p) STEP B DONE for context_id=%s\n",g_thread_self (), context_id);

     linkb->check_processed_count++;

     rowid = dupin_link_record_get_rowid (record); // NOTE this is the rowid of the previous revision

     if (check_id != NULL)
       g_free(check_id);

     check_id = g_strdup_printf ("%i", (gint)rowid);

//g_message("dupin_linkbase_thread_check(%p) check_id=%s as fetched",g_thread_self (), check_id);
    }
  
  dupin_link_record_get_list_close (results);

  if (parent_db != NULL)
    dupin_database_unref (parent_db);

  if (parent_linkb != NULL)
    dupin_linkbase_unref (parent_linkb);

//g_message("dupin_linkbase_thread_check() check_id=%s as to be stored",check_id);

//g_message("dupin_linkbase_thread_check(%p)  finished last_check_rowid=%s - checked %d\n", g_thread_self (), check_id, (gint)linkb->check_processed_count);

  str = sqlite3_mprintf ("UPDATE DupinLinkB SET check_id = '%q'", check_id);

  if (check_id != NULL)
    g_free (check_id);

  g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, str, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (linkb->mutex);
      sqlite3_free (str);

      g_error("dupin_linkbase_thread_check: %s", errmsg);
      sqlite3_free (errmsg);

      return FALSE;
    }

  g_mutex_unlock (linkb->mutex);

  sqlite3_free (str);

  return ret;
}

void
dupin_linkbase_check_func (gpointer data, gpointer user_data)
{
  DupinLinkB * linkb = (DupinLinkB*) data;

  dupin_linkbase_ref (linkb);

  g_mutex_lock (linkb->mutex);
  linkb->check_thread = g_thread_self ();
  g_mutex_unlock (linkb->mutex);

//g_message("dupin_linkbase_check_func(%p) started\n",g_thread_self ());

  g_mutex_lock (linkb->mutex);
  linkb->check_processed_count = 0;
  g_mutex_unlock (linkb->mutex);

  while (linkb->todelete == FALSE)
    {
      gboolean check_operation = dupin_linkbase_thread_check (linkb, DUPIN_LINKB_CHECK_COUNT);

      if (check_operation == FALSE)
        {
//g_message("dupin_linkbase_check_func(%p) Checked TOTAL %d records\n", g_thread_self (), (gint)linkb->check_processed_count);

/* WE DO NOT DELETE ANYTHING YET with CHECK - add this later eventually if needed */
#if 0
          /* claim disk space back */

//g_message("dupin_linkbase_check_func: VACUUM and ANALYZE\n");

          g_mutex_lock (linkb->mutex);

          if (sqlite3_exec (linkb->db, "VACUUM", NULL, NULL, &errmsg) != SQLITE_OK
             || sqlite3_exec (linkb->db, "ANALYZE Dupin", NULL, NULL, &errmsg) != SQLITE_OK)
            {
              g_mutex_unlock (linkb->mutex);
              g_error ("dupin_linkbase_check_func: %s", errmsg);
              sqlite3_free (errmsg);
              break;
            }

          g_mutex_unlock (linkb->mutex);
#endif

          break;
        }
    }

//g_message("dupin_linkbase_check_func(%p) finished and linkbase is checked\n",g_thread_self ());

  g_mutex_lock (linkb->mutex);
  linkb->tocheck = FALSE;
  g_mutex_unlock (linkb->mutex);

  g_mutex_lock (linkb->mutex);
  linkb->check_thread = NULL;
  g_mutex_unlock (linkb->mutex);

  dupin_linkbase_unref (linkb);
}

void
dupin_linkbase_check (DupinLinkB * linkb)
{
  g_return_if_fail (linkb != NULL);

  if (dupin_linkbase_is_checking (linkb))
    {
      g_mutex_lock (linkb->mutex);
      linkb->tocheck = TRUE;
      g_mutex_unlock (linkb->mutex);

//g_message("dupin_linkbase_check(%p): linkbase is still checking linkb->check_thread=%p\n", g_thread_self (), linkb->check_thread);
    }
  else
    {
//g_message("dupin_linkbase_check(%p): push to thread pools linkb->check_thread=%p\n", g_thread_self (), linkb->check_thread);

      if (!linkb->check_thread)
        {
          g_thread_pool_push(linkb->d->linkb_check_workers_pool, linkb, NULL);
        }
    }
}

gboolean
dupin_linkbase_is_checking (DupinLinkB * linkb)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  if (linkb->check_thread)
    return TRUE;

  return FALSE;
}

gboolean
dupin_linkbase_is_checked (DupinLinkB * linkb)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  if (dupin_linkbase_is_checking (linkb))
    return FALSE;

  return linkb->tocheck ? FALSE : TRUE;
}

void
dupin_linkbase_set_error (DupinLinkB * linkb,
                          gchar * msg)
{
  g_return_if_fail (linkb != NULL);
  g_return_if_fail (msg != NULL);

  dupin_linkbase_clear_error (linkb);

  linkb->error_msg = g_strdup ( msg );

  return;
}

void
dupin_linkbase_clear_error (DupinLinkB * linkb)
{
  g_return_if_fail (linkb != NULL);

  if (linkb->error_msg != NULL)
    g_free (linkb->error_msg);

  linkb->error_msg = NULL;

  return;
}

gchar * dupin_linkbase_get_error (DupinLinkB * linkb)
{
  g_return_val_if_fail (linkb != NULL, NULL);

  return linkb->error_msg;
}

void dupin_linkbase_set_warning (DupinLinkB * linkb,
                                 gchar * msg)
{
  g_return_if_fail (linkb != NULL);
  g_return_if_fail (msg != NULL);

  dupin_linkbase_clear_warning (linkb);

  linkb->warning_msg = g_strdup ( msg );

  return;
}

void dupin_linkbase_clear_warning (DupinLinkB * linkb)
{
  g_return_if_fail (linkb != NULL);

  if (linkb->warning_msg != NULL)
    g_free (linkb->warning_msg);

  linkb->warning_msg = NULL;

  return;
}

gchar * dupin_linkbase_get_warning (DupinLinkB * linkb)
{
  g_return_val_if_fail (linkb != NULL, NULL);

  return linkb->warning_msg;
}

gboolean
dupin_linkbase_is_cache_on (DupinLinkB *   linkb,
			    GError **      error)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  return linkb->cache_on;
}

void
dupin_linkbase_cache_on (DupinLinkB *   linkb,
			 GError **      error)
{
  g_return_if_fail (linkb != NULL);

  linkb->cache_on = TRUE;

  return;
}

void
dupin_linkbase_cache_off (DupinLinkB *   linkb,
			  GError **      error)
{
  g_return_if_fail (linkb != NULL);

  linkb->cache_on = FALSE;

  return;
}

/* EOF */
