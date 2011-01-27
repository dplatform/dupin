#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_link_record.h"
#include "dupin_utils.h"

#include <string.h>
#include <stdlib.h>

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#define DUPIN_LINKB_SQL_EXISTS \
	"SELECT count(id) FROM Dupin WHERE id = '%q' "

#define DUPIN_LINKB_SQL_INSERT \
	"INSERT INTO Dupin (id, rev, hash, obj, tm, context_id, label, href, rel, tag, is_weblink) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%q', %Q, %Q, '%q')"

#define DUPIN_LINKB_SQL_UPDATE \
	"INSERT OR REPLACE INTO Dupin (id, rev, hash, obj, tm, context_id, label, href, rel, tag, is_weblink) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%q', %Q, %Q, '%q')"

#define DUPIN_LINKB_SQL_READ \
	"SELECT rev, hash, obj, deleted, tm, ROWID AS rowid, context_id, label, href, rel, tag, is_weblink FROM Dupin WHERE id='%q'"

#define DUPIN_LINKB_SQL_DELETE \
	"INSERT OR REPLACE INTO Dupin (id, rev, deleted, hash, tm, context_id, label, href, rel, tag, is_weblink) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', 'TRUE', '%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%q', %Q, %Q, '%q')"


DSWeblinkingRelationRegistry DSWeblinkingRelationRegistryList[] = {
  {WEBLINKING_RELNAME_ALTERNATE, "alternate", "Designates a substitute for the link's context."}
  ,
  {WEBLINKING_RELNAME_APPENDIX, "appendix", "Refers to an appendix."}
  ,
  {WEBLINKING_RELNAME_BOOKMARK, "bookmark", "Refers to a bookmark or entry point."}
  ,
  {WEBLINKING_RELNAME_CHAPTER, "chapter", "Refers to a chapter in a collection of resources."}
  ,
  {WEBLINKING_RELNAME_CONTENTS, "contents", "Refers to a table of contents."}
  ,
  {WEBLINKING_RELNAME_COPYRIGHT, "copyright", "Refers to a copyright statement that applies to the link's context."}
  ,
  {WEBLINKING_RELNAME_CURRENT, "current", "Refers to a resource containing the most recent item(s) in a collection of resources."}
  ,
  {WEBLINKING_RELNAME_DESCRIBEDBY, "describedby", "Refers to a resource providing information about the link's context."}
  ,
  {WEBLINKING_RELNAME_EDIT, "edit", "Refers to a resource that can be used to edit the link's context."}
  ,
  {WEBLINKING_RELNAME_EDIT_MEDIA, "edit-media", "Refers to a resource that can be used to edit media associated with the link's context."}
  ,
  {WEBLINKING_RELNAME_ENCLOSURE, "enclosure", "Identifies a related resource that is potentially large and might require special handling."}
  ,
  {WEBLINKING_RELNAME_FIRST, "first", "An IRI that refers to the furthest preceding resource in a series of resources."}
  ,
  {WEBLINKING_RELNAME_GLOSSARY, "glossary", "Refers to a glossary of terms."}
  ,
  {WEBLINKING_RELNAME_HELP, "help", "Refers to a resource offering help (more information, links to other sources information, etc.)"}
  ,
  {WEBLINKING_RELNAME_HUB, "hub", "Refers to a hub that enables registration for notification of updates to the context."}
  ,
  {WEBLINKING_RELNAME_INDEX, "index", "Refers to an index."}
  ,
  {WEBLINKING_RELNAME_LAST, "last", "An IRI that refers to the furthest following resource in a series of resources."}
  ,
  {WEBLINKING_RELNAME_LATEST_VERSION, "latest-version", "Points to a resource containing the latest (e.g., current) version of the context."}
  ,
  {WEBLINKING_RELNAME_LICENSE, "license", "Refers to a license associated with the link's context."}
  ,
  {WEBLINKING_RELNAME_NEXT, "next", "Refers to the next resource in a ordered series of resources."}
  ,
  {WEBLINKING_RELNAME_NEXT_ARCHIVE, "next-archive", "Refers to the immediately following archive resource."}
  ,
  {WEBLINKING_RELNAME_PAYMENT, "payment", "Indicates a resource where payment is accepted."}
  ,
  {WEBLINKING_RELNAME_PREV, "prev", "Refers to the previous resource in an ordered series of resources.  Synonym for \"previous\"."}
  ,
  {WEBLINKING_RELNAME_PREDECESSOR_VERSION, "predecessor-version", "Points to a resource containing the predecessor version in the version history."}
  ,
  {WEBLINKING_RELNAME_PREVIOUS, "previous", "Refers to the previous resource in an ordered series of resources.  Synonym for \"prev\"."}
  ,
  {WEBLINKING_RELNAME_PREV_ARCHIVE, "prev-archive", "Refers to the immediately preceding archive resource."}
  ,
  {WEBLINKING_RELNAME_RELATED, "related", "Identifies a related resource."}
  ,
  {WEBLINKING_RELNAME_REPLIES, "replies", "Identifies a resource that is a reply to the context of the link."}
  ,
  {WEBLINKING_RELNAME_SECTION, "section", "Refers to a section in a collection of resources."}
  ,
  {WEBLINKING_RELNAME_SELF, "self", "Conveys an identifier for the link's context."}
  ,
  {WEBLINKING_RELNAME_SERVICE, "service", "Indicates a URI that can be used to retrieve a service document."}
  ,
  {WEBLINKING_RELNAME_START, "start", "Refers to the first resource in a collection of resources."}
  ,
  {WEBLINKING_RELNAME_STYLESHEET, "stylesheet", "Refers to an external style sheet."}
  ,
  {WEBLINKING_RELNAME_SUBSECTION, "subsection", "Refers to a resource serving as a subsection in a collection of resources."}
  ,
  {WEBLINKING_RELNAME_SUCCESSOR_VERSION, "successor-version", "Points to a resource containing the successor version in the version history."}
  ,
  {WEBLINKING_RELNAME_UP, "up", "Refers to a parent document in a hierarchy of documents."}
  ,
  {WEBLINKING_RELNAME_VERSION_HISTORY, "version-history", "Points to a resource containing the version history for the context."}
  ,
  {WEBLINKING_RELNAME_VIA, "via", "Identifies a resource that is the source of the information in the link's context."}
  ,
  {WEBLINKING_RELNAME_WORKING_COPY, "working-copy", "Points to a working copy for this resource."}
  ,
  {WEBLINKING_RELNAME_WORKING_COPY_OF, "working-copy-of", "Points to the versioned resource from which this working copy was obtained."}
  ,

  {WEBLINKING_RELNAME__END, NULL, NULL}
};

#define DUPIN_LINKB_DEFAULT_REL	DSWeblinkingRelationRegistryList[WEBLINKING_RELNAME_ALTERNATE].rel

static DupinLinkRecord *dupin_link_record_create_with_id_real (DupinLinkB * linkb,
						      JsonNode * obj_node,
						      gchar * id,
						      gchar * context_id,
						      gchar * label,
                                         	      gchar * href,
                                         	      gchar * rel,
                                         	      gchar * tag,
						      GError ** error,
						      gboolean lock);
static DupinLinkRecord *dupin_link_record_read_real (DupinLinkB * linkb, gchar * id,
					    GError ** error, gboolean lock);

static void dupin_link_record_rev_close (DupinLinkRecordRev * rev);

static DupinLinkRecord *dupin_link_record_new (DupinLinkB * linkb, gchar * id);

static void dupin_link_record_add_revision_obj (DupinLinkRecord * record, guint rev,
					   gchar ** hash,
					   JsonNode * obj_node,
					   gchar * context_id,
					   gchar * label,
                                           gchar * href,
                                           gchar * rel,
                                           gchar * tag,
					   gboolean delete,
					   gsize created,
					   gboolean is_weblink);

static void dupin_link_record_add_revision_str (DupinLinkRecord * record, guint rev,
					   gchar * hash, gssize hash_size,
					   gchar * obj, gssize size,
					   gchar * context_id,
					   gchar * label,
                                           gchar * href,
                                           gchar * rel,
                                           gchar * tag,
					   gboolean delete,
					   gsize created,
					   gsize rowid,
					   gboolean is_weblink);

static gboolean
	   dupin_link_record_generate_hash	(DupinLinkRecord * record,
                            		 gchar * obj_serialized, gssize obj_serialized_len,
					 gchar * context_id,
					 gchar * label,
                                         gchar * href,
                                         gchar * rel,
                                         gchar * tag,
			    		 gboolean delete,
					 gboolean is_weblink,
			    		 gchar ** hash, gsize * hash_len);

gboolean
dupin_link_record_exists (DupinLinkB * linkb, gchar * id)
{
  g_return_val_if_fail (linkb != NULL, FALSE);
  g_return_val_if_fail (id != NULL, FALSE);

  return dupin_link_record_exists_real (linkb, id, TRUE);
}

static int
dupin_link_record_exists_real_cb (void *data, int argc, char **argv, char **col)
{
  guint *numb = data;

  if (argv[0])
    *numb = atoi (argv[0]);

  return 0;
}

gboolean
dupin_link_record_exists_real (DupinLinkB * linkb, gchar * id, gboolean lock)
{
  gchar *tmp;
  gchar * errmsg;
  gint numb = 0;

  tmp = sqlite3_mprintf (DUPIN_LINKB_SQL_EXISTS, id);

  if (lock == TRUE)
    g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, tmp, dupin_link_record_exists_real_cb, &numb, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
        g_mutex_unlock (linkb->mutex);

      sqlite3_free (tmp);

      g_error ("dupin_link_record_exists_real: %s", errmsg);

      sqlite3_free (errmsg);

      return FALSE;
    }

  if (lock == TRUE)
    g_mutex_unlock (linkb->mutex);

  sqlite3_free (tmp);

  return numb > 0 ? TRUE : FALSE;
}

DupinLinkRecord *
dupin_link_record_create (DupinLinkB * linkb, JsonNode * obj_node,
			  gchar * context_id,
			  gchar * label,
                          gchar * href,
                          gchar * rel,
                          gchar * tag,
			  GError ** error)
{
  gchar *id;
  DupinLinkRecord *record;

  g_return_val_if_fail (linkb != NULL, NULL);
  g_return_val_if_fail (obj_node != NULL, NULL);
  g_return_val_if_fail (context_id != NULL, NULL);
  g_return_val_if_fail (label != NULL, NULL);
  g_return_val_if_fail (href != NULL, NULL);
  g_return_val_if_fail (dupin_link_record_util_is_valid_context_id (context_id) == TRUE, NULL);
  g_return_val_if_fail (dupin_link_record_util_is_valid_label (label) == TRUE, NULL);
  g_return_val_if_fail (dupin_link_record_util_is_valid_href (href) == TRUE, NULL);
  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  if (rel == NULL)
    rel = DUPIN_LINKB_DEFAULT_REL;
  else
    g_return_val_if_fail (dupin_link_record_util_is_valid_rel (rel) == TRUE, NULL);

  g_mutex_lock (linkb->mutex);

  if (!(id = dupin_linkbase_generate_id_real (linkb, error, FALSE)))
    {
      g_mutex_unlock (linkb->mutex);
      return NULL;
    }

  record = dupin_link_record_create_with_id_real (linkb, obj_node, id,
						  context_id, label, href, rel, tag,
						  error, FALSE);

  g_mutex_unlock (linkb->mutex);
  g_free (id);

  return record;
}

DupinLinkRecord *
dupin_link_record_create_with_id (DupinLinkB * linkb, JsonNode * obj_node,
				  gchar * id,
				  gchar * context_id,
				  gchar * label,
                                  gchar * href,
                                  gchar * rel,
                                  gchar * tag,
			          GError ** error)
{
  g_return_val_if_fail (linkb != NULL, NULL);
  g_return_val_if_fail (obj_node != NULL, NULL);
  g_return_val_if_fail (id != NULL, NULL);
  g_return_val_if_fail (context_id != NULL, NULL);
  g_return_val_if_fail (label != NULL, NULL);
  g_return_val_if_fail (href != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_record_id (id) != FALSE, NULL);
  g_return_val_if_fail (dupin_link_record_util_is_valid_context_id (context_id) == TRUE, NULL);
  g_return_val_if_fail (dupin_link_record_util_is_valid_label (label) == TRUE, NULL);
  g_return_val_if_fail (dupin_link_record_util_is_valid_href (href) == TRUE, NULL);
  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  if (rel == NULL)
    rel = DUPIN_LINKB_DEFAULT_REL;
  else
    g_return_val_if_fail (dupin_link_record_util_is_valid_rel (rel) == TRUE, NULL);

  return dupin_link_record_create_with_id_real (linkb, obj_node, id,
						context_id, label, href, rel, tag,
						error, TRUE);
}

static DupinLinkRecord *
dupin_link_record_create_with_id_real (DupinLinkB * linkb, JsonNode * obj_node,
				       gchar * id,
				       gchar * context_id,
				       gchar * label,
                                       gchar * href,
                                       gchar * rel,
                                       gchar * tag,
				       GError ** error, gboolean lock)
{
  DupinLinkRecord *record;
  gchar *errmsg;
  gchar *tmp;
  gchar * md5=NULL;

  dupin_linkbase_ref (linkb);

  if (lock == TRUE)
    g_mutex_lock (linkb->mutex);

/*
  if (dupin_link_record_exists_real (linkb, id, FALSE) == TRUE)
    {
      g_mutex_unlock (linkb->mutex);
      g_return_val_if_fail (dupin_link_record_exists (linkb, id) == FALSE, NULL);
      return NULL;
    }
*/

  record = dupin_link_record_new (linkb, id);

  gsize created = dupin_util_timestamp_now ();

  dupin_link_record_add_revision_obj (record, 1, &md5, obj_node,
				      context_id, label, href, rel, tag,
				      FALSE, created,
				      dupin_util_is_valid_absolute_uri (href));

  tmp =
    sqlite3_mprintf (DUPIN_LINKB_SQL_INSERT, id, 1, md5,
		     record->last->obj_serialized, created,
		     context_id, label, href, rel, tag,
		     dupin_util_is_valid_absolute_uri (href) ? "TRUE" : "FALSE" );

//g_message("dupin_link_record_create_with_id_real: query=%s\n", tmp);

  if (sqlite3_exec (linkb->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      if (lock == TRUE)
	g_mutex_unlock (linkb->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      dupin_link_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return NULL;
    }

  if (lock == TRUE)
    g_mutex_unlock (linkb->mutex);

  dupin_view_p_record_insert (&linkb->views,
			      (gchar *) dupin_link_record_get_id (record),
			      json_node_get_object (dupin_link_record_get_revision_node (record, NULL)));

  sqlite3_free (tmp);
  return record;
}

static int
dupin_link_record_read_cb (void *data, int argc, char **argv, char **col)
{
  guint rev = 0;
  gsize tm = 0;
  gchar *hash = NULL;
  gchar *obj = NULL;
  gboolean delete = FALSE;
  gsize rowid=0;
  gint i;
  gchar *context_id = NULL;
  gchar *label = NULL;
  gchar *href = NULL;
  gchar *rel = NULL;
  gchar *tag = NULL;
  gboolean is_weblink = FALSE;

  for (i = 0; i < argc; i++)
    {
      /* shouldn't this be double and use atof() ?!? */
      if (!g_strcmp0 (col[i], "rev"))
	rev = atoi (argv[i]);

      else if (!g_strcmp0 (col[i], "tm"))
	tm = (gsize)atof (argv[i]);

      else if (!g_strcmp0 (col[i], "hash"))
	hash = argv[i];

      else if (!g_strcmp0 (col[i], "obj"))
	obj = argv[i];

      else if (!g_strcmp0 (col[i], "deleted"))
	delete = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;

      else if (!g_strcmp0 (col[i], "rowid"))
	rowid = (gsize)atof(argv[i]);

      else if (!g_strcmp0 (col[i], "context_id"))
	context_id = argv[i];

      else if (!g_strcmp0 (col[i], "label"))
	label = argv[i];

      else if (!g_strcmp0 (col[i], "href"))
	href = argv[i];

      else if (!g_strcmp0 (col[i], "rel"))
	rel = argv[i];

      else if (!g_strcmp0 (col[i], "tag"))
	tag = argv[i];

      else if (!g_strcmp0 (col[i], "is_weblink"))
	is_weblink = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;
    }

  if (rev && hash !=NULL)
    dupin_link_record_add_revision_str (data, rev, hash, -1, obj, -1,
					context_id, label, href, rel, tag,
					delete, tm, rowid, is_weblink);

  return 0;
}

DupinLinkRecord *
dupin_link_record_read (DupinLinkB * linkb, gchar * id, GError ** error)
{
  g_return_val_if_fail (linkb != NULL, NULL);
  g_return_val_if_fail (id != NULL, NULL);
  g_return_val_if_fail (dupin_util_is_valid_record_id (id) != FALSE, NULL);

  return dupin_link_record_read_real (linkb, id, error, TRUE);
}

static DupinLinkRecord *
dupin_link_record_read_real (DupinLinkB * linkb, gchar * id, GError ** error,
			gboolean lock)
{
  DupinLinkRecord *record;
  gchar *errmsg;
  gchar *tmp;

  dupin_linkbase_ref (linkb);

  record = dupin_link_record_new (linkb, id);

  tmp = sqlite3_mprintf (DUPIN_LINKB_SQL_READ, id);

  if (lock == TRUE)
    g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, tmp, dupin_link_record_read_cb, record, &errmsg) !=
      SQLITE_OK)
    {
      if (lock == TRUE)
	g_mutex_unlock (linkb->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);
      dupin_link_record_close (record);
      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return NULL;
    }

  if (lock == TRUE)
    g_mutex_unlock (linkb->mutex);

  sqlite3_free (tmp);

  if (!record->last || !record->last->rowid)
    {
      dupin_link_record_close (record);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD,
		   "The record '%s' doesn't exist.", id);
      return NULL;
    }

  return record;
}

struct dupin_link_record_get_list_t
{
  DupinLinkB *linkb;
  GList *list;
};

static int
dupin_link_record_get_list_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_link_record_get_list_t *s = data;
  DupinLinkRecord *record;

  if ((record = dupin_link_record_read_real (s->linkb, argv[0], NULL, FALSE)))
    s->list = g_list_append (s->list, record);

  return 0;
}

gboolean
dupin_link_record_get_list (DupinLinkB * linkb, guint count, guint offset,
                            gsize rowid_start, gsize rowid_end,
			    DupinLinksType         links_type,
			    DupinCountType         count_type,
                            DupinOrderByType       orderby_type,
                            gboolean               descending,
                            gchar *                context_id,
                            gchar *                label,
                            gchar *                tag,
		            GList ** list, GError ** error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;
  gchar *check_deleted="";
  gchar *check_linktype="";

  struct dupin_link_record_get_list_t s;

  g_return_val_if_fail (linkb != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  if (context_id != NULL)
    g_return_val_if_fail (dupin_link_record_util_is_valid_context_id (context_id) == TRUE, FALSE);

  if (label != NULL)
    g_return_val_if_fail (dupin_link_record_util_is_valid_label (label) == TRUE, FALSE);

  memset (&s, 0, sizeof (s));
  s.linkb = linkb;

  str = g_string_new ("SELECT id FROM Dupin as d");

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  if (links_type == DP_LINKS_WEB_LINKS)
    check_linktype = " d.is_weblink = 'TRUE' ";
  else if (links_type == DP_LINKS_RELATIONSHIPS)
    check_linktype = " d.is_weblink = 'FALSE' ";

  gchar * op = "";

  if (rowid_start > 0 && rowid_end > 0)
    {
      g_string_append_printf (str, " WHERE d.ROWID >= %d AND d.ROWID <= %d ", (gint)rowid_start, (gint)rowid_end);
      op = "AND";
    }
  else if (rowid_start > 0)
    {
      g_string_append_printf (str, " WHERE d.ROWID >= %d ", (gint)rowid_start);
      op = "AND";
    }
  else if (rowid_end > 0)
    {
      g_string_append_printf (str, " WHERE d.ROWID <= %d ", (gint)rowid_end);
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

  if (label != NULL)
    {
      if (!g_strcmp0 (op, ""))
        op = "WHERE";

      gchar * tmp2 = sqlite3_mprintf (" %s d.label = '%q' ", op, label);
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

  if (orderby_type == DP_ORDERBY_ROWID)
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

//g_message("dupin_link_record_get_list() query=%s\n",tmp);

  g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, tmp, dupin_link_record_get_list_cb, &s, &errmsg) !=
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
dupin_link_record_get_list_close (GList * list)
{
  while (list)
    {
      dupin_link_record_close (list->data);
      list = g_list_remove (list, list->data);
    }
}

struct dupin_link_record_get_revisions_list_t
{
  GList *list;
};

static int
dupin_link_record_get_revisions_list_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_link_record_get_revisions_list_t *s = data;

  if (argv[0] && argv[1])
    {
      gchar mvcc[DUPIN_ID_MAX_LEN];
      dupin_util_mvcc_new (atoi(argv[0]), argv[1], mvcc);

      s->list = g_list_append (s->list, g_strdup (mvcc));
    }

  return 0;
}

gboolean
dupin_link_record_get_revisions_list (DupinLinkRecord * record,
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

  struct dupin_link_record_get_revisions_list_t s;
  memset (&s, 0, sizeof (s));

  str = g_string_new ("SELECT rev, hash FROM Dupin as d");

  where_id = sqlite3_mprintf (" WHERE d.id = '%q' ", (gchar *) dupin_link_record_get_id (record));
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

//g_message("dupin_link_record_get_revisions_list() query=%s\n",tmp);

  g_mutex_lock (record->linkb->mutex);

  if (sqlite3_exec (record->linkb->db, tmp, dupin_link_record_get_revisions_list_cb, &s, &errmsg) !=
      SQLITE_OK)
    {
      g_mutex_unlock (record->linkb->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (record->linkb->mutex);

  g_free (tmp);

  *list = s.list;

  return TRUE;
}

static int
dupin_link_record_get_total_revisions_cb (void *data, int argc, char **argv,
                                     char **col)
{
  gsize *numb = data;

  if (argv[0])
    *numb = atoi (argv[0]);

  return 0;
}

gboolean
dupin_link_record_get_total_revisions (DupinLinkRecord * record,
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

  where_id = sqlite3_mprintf (" WHERE d.id = '%q' ", (gchar *) dupin_link_record_get_id (record));
  str = g_string_append (str, where_id);
  sqlite3_free (where_id);

  tmp = g_string_free (str, FALSE);

//g_message("dupin_link_record_get_total_revisions() query=%s\n",tmp);

  g_mutex_lock (record->linkb->mutex);

  if (sqlite3_exec (record->linkb->db, tmp, dupin_link_record_get_total_revisions_cb, &total_revisions, &errmsg) !=
      SQLITE_OK)
    {
      g_mutex_unlock (record->linkb->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      g_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (record->linkb->mutex);

  g_free (tmp);

  *total = total_revisions;

  return TRUE;
}

void
dupin_link_record_get_revisions_list_close (GList * list)
{
  while (list)
    {
      g_free (list->data);
      list = g_list_remove (list, list->data);
    }
}

gboolean
dupin_link_record_update (DupinLinkRecord * record, JsonNode * obj_node,
                          gchar * label,
                          gchar * href,
                          gchar * rel,
                          gchar * tag,
			  GError ** error)
{
  guint rev;
  gchar *tmp;
  gchar *errmsg;
  gchar * md5=NULL;

  g_return_val_if_fail (record != NULL, FALSE);
  g_return_val_if_fail (obj_node != NULL, FALSE);
  g_return_val_if_fail (label != NULL, FALSE);
  g_return_val_if_fail (href != NULL, FALSE);
  g_return_val_if_fail (dupin_link_record_util_is_valid_label (label) == TRUE, FALSE);
  g_return_val_if_fail (dupin_link_record_util_is_valid_href (href) == TRUE, FALSE);
  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);

  if (rel == NULL)
    rel = DUPIN_LINKB_DEFAULT_REL;
  else
    g_return_val_if_fail (dupin_link_record_util_is_valid_rel (rel) == TRUE, FALSE);

  g_mutex_lock (record->linkb->mutex);

  rev = record->last->revision + 1;

  gsize created = dupin_util_timestamp_now ();

  dupin_link_record_add_revision_obj (record, rev, &md5, obj_node,
				      (gchar *)dupin_link_record_get_context_id (record),
				      label, href, rel, tag,
				      FALSE, created,
				      dupin_util_is_valid_absolute_uri (href));

  tmp =
    sqlite3_mprintf (DUPIN_LINKB_SQL_INSERT, record->id, rev, md5,
		     record->last->obj_serialized, created,
		     (gchar *)dupin_link_record_get_context_id (record),
		     label, href, rel, tag,
		     dupin_util_is_valid_absolute_uri (href) ? "TRUE" : "FALSE" );

//g_message("dupin_link_record_update: record->last->revision = %d - new rev=%d - query=%s\n", (gint) record->last->revision, (gint) rev, tmp);

  if (sqlite3_exec (record->linkb->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_mutex_unlock (record->linkb->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      sqlite3_free (tmp);
      return FALSE;
    }

  g_mutex_unlock (record->linkb->mutex);

  dupin_view_p_record_delete (&record->linkb->views,
			      (gchar *) dupin_link_record_get_id (record));
  dupin_view_p_record_insert (&record->linkb->views,
			      (gchar *) dupin_link_record_get_id (record),
			      json_node_get_object (dupin_link_record_get_revision_node (record, NULL)));

  sqlite3_free (tmp);
  return TRUE;
}

gboolean
dupin_link_record_delete (DupinLinkRecord * record, GError ** error)
{
  guint rev;
  gchar *tmp;
  gchar *errmsg;
  gchar * md5=NULL;
  gboolean ret = TRUE;

  g_return_val_if_fail (record != NULL, FALSE);

  if (dupin_link_record_is_deleted (record, NULL) == TRUE)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD,
		   "Record already deleted");
      return FALSE;
    }

  g_mutex_lock (record->linkb->mutex);

  rev = record->last->revision + 1;

  gsize created = dupin_util_timestamp_now ();

  dupin_link_record_add_revision_obj (record, rev, &md5, NULL,
				      (gchar *)dupin_link_record_get_context_id (record),
				      (gchar *)dupin_link_record_get_label (record),
				      (gchar *)dupin_link_record_get_href (record),
				      (gchar *)dupin_link_record_get_rel (record),
				      (gchar *)dupin_link_record_get_tag (record),
 				      TRUE, created,
				      dupin_link_record_is_weblink (record));

  tmp = sqlite3_mprintf (DUPIN_LINKB_SQL_DELETE, record->id, rev, md5, created,	
				      (gchar *)dupin_link_record_get_context_id (record),
				      (gchar *)dupin_link_record_get_label (record),
				      (gchar *)dupin_link_record_get_href (record),
				      (gchar *)dupin_link_record_get_rel (record),
				      (gchar *)dupin_link_record_get_tag (record),
				      dupin_link_record_is_weblink (record) ? "TRUE" : "FALSE" );

//g_message("dupin_link_record_delete: query=%s\n", tmp);

  if (sqlite3_exec (record->linkb->db, tmp, NULL, NULL, &errmsg) != SQLITE_OK)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "%s",
		   errmsg);

      sqlite3_free (errmsg);
      ret = FALSE;
    }

  g_mutex_unlock (record->linkb->mutex);

  dupin_view_p_record_delete (&record->linkb->views,
			      (gchar *) dupin_link_record_get_id (record));

  sqlite3_free (tmp);
  return ret;
}

void
dupin_link_record_close (DupinLinkRecord * record)
{
  g_return_if_fail (record != NULL);

  if (record->linkb)
    dupin_linkbase_unref (record->linkb);

  if (record->id)
    g_free (record->id);

  if (record->revisions)
    g_hash_table_destroy (record->revisions);

  g_free (record);
}

const gchar *
dupin_link_record_get_id (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->id;
}

gsize
dupin_link_record_get_rowid (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->rowid;
}

gsize
dupin_link_record_get_created (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->created;
}

const gchar *
dupin_link_record_get_context_id (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->context_id;
}

const gchar *
dupin_link_record_get_label (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->label;
}
const gchar *
dupin_link_record_get_href (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->href;
}

const gchar *
dupin_link_record_get_rel (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->rel;
}

const gchar *
dupin_link_record_get_tag (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->tag;
}

gboolean
dupin_link_record_is_weblink (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->is_weblink;
}

gchar *
dupin_link_record_get_last_revision (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->mvcc;
}

JsonNode *
dupin_link_record_get_revision_node (DupinLinkRecord * record, gchar * mvcc)
{
  DupinLinkRecordRev *r;

  g_return_val_if_fail (record != NULL, NULL);

  if (mvcc != NULL)
    g_return_val_if_fail (dupin_util_is_valid_mvcc (mvcc) == TRUE, NULL);

  if (mvcc == NULL || (!dupin_util_mvcc_revision_cmp (mvcc, record->last->mvcc)))
    r = record->last;
  else
    {
      /* TODO - check if the following check does make any sense ? */
      if (dupin_util_mvcc_revision_cmp (mvcc,record->last->mvcc) > 0)
	g_return_val_if_fail (dupin_util_mvcc_revision_cmp (dupin_link_record_get_last_revision (record), mvcc) >= 0 , NULL);

      if (!(r = g_hash_table_lookup (record->revisions, mvcc)))
	return NULL;
    }

  if (r->deleted == TRUE)
    g_return_val_if_fail (dupin_link_record_is_deleted (record, mvcc) != FALSE,
			  NULL);

  /* r->obj stays owernship of the record revision - the caller eventually need to json_node_copy() it */
  if (r->obj)
    return r->obj;

  JsonParser * parser = json_parser_new();

  /* we do not check any parsing error due we stored earlier, we assume it is sane */
  if (json_parser_load_from_data (parser, r->obj_serialized, r->obj_serialized_len, NULL) == FALSE)
    goto dupin_link_record_get_revision_error;

  r->obj = json_node_copy (json_parser_get_root (parser));

  if (parser != NULL)
    g_object_unref (parser);

  /* r->obj stays owernship of the record revision - the caller eventually need to json_node_copy() it */
  return r->obj;

dupin_link_record_get_revision_error:

  if (parser != NULL)
    g_object_unref (parser);

  return NULL;
}

gboolean
dupin_link_record_is_deleted (DupinLinkRecord * record, gchar * mvcc)
{
  DupinLinkRecordRev *r;

  g_return_val_if_fail (record != NULL, FALSE);

  if (mvcc != NULL)
    g_return_val_if_fail (dupin_util_is_valid_mvcc (mvcc) == TRUE, FALSE);

  if (mvcc == NULL || (!dupin_util_mvcc_revision_cmp (mvcc, record->last->mvcc)))
    r = record->last;
  else
    {
      /* TODO - check if the following check does make any sense ? */
      if (dupin_util_mvcc_revision_cmp (mvcc,record->last->mvcc) > 0)
	g_return_val_if_fail (dupin_util_mvcc_revision_cmp (dupin_link_record_get_last_revision (record), mvcc) >= 0 , FALSE);

      if (!(r = g_hash_table_lookup (record->revisions, mvcc)))
	return FALSE;
    }

  return r->deleted;
}

/* Internal: */
static DupinLinkRecord *
dupin_link_record_new (DupinLinkB * linkb, gchar * id)
{
  DupinLinkRecord *record;

  record = g_malloc0 (sizeof (DupinLinkRecord));
  record->linkb = linkb;
  record->id = g_strdup (id);

  record->revisions =
    g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
			   (GDestroyNotify) dupin_link_record_rev_close);

  return record;
}

static void
dupin_link_record_rev_close (DupinLinkRecordRev * rev)
{
  if (rev->obj_serialized)
    g_free (rev->obj_serialized);

  if (rev->hash)
    g_free (rev->hash);

  if (rev->mvcc)
    g_free (rev->mvcc);

  if (rev->obj)
    json_node_free (rev->obj);

  if (rev->context_id)
    g_free (rev->context_id);

  if (rev->label)
    g_free (rev->label);

  if (rev->href)
    g_free (rev->href);

  if (rev->rel)
    g_free (rev->rel);

  if (rev->tag)
    g_free (rev->tag);

  g_free (rev);
}

static void
dupin_link_record_add_revision_obj (DupinLinkRecord * record, guint rev,
			       gchar ** hash,
			       JsonNode * obj_node,
			       gchar * context_id,
			       gchar * label,
                               gchar * href,
                               gchar * rel,
                               gchar * tag,
			       gboolean delete,
			       gsize created,
			       gboolean is_weblink)
{
  DupinLinkRecordRev *r;
  gchar mvcc[DUPIN_ID_MAX_LEN];

  r = g_malloc0 (sizeof (DupinLinkRecordRev));
  r->revision = rev;

  if (obj_node)
    {
      JsonGenerator * gen = json_generator_new();

      r->obj = json_node_copy (obj_node);

      json_generator_set_root (gen, r->obj );

      r->obj_serialized = json_generator_to_data (gen,&r->obj_serialized_len);

      g_object_unref (gen);
    }

  r->deleted = delete;
  r->created = created;
  r->is_weblink = is_weblink;

  dupin_link_record_generate_hash (record,
			      r->obj_serialized, r->obj_serialized_len,
			      context_id,
			      label,
                              href,
                              rel,
			      tag,
			      delete,
			      is_weblink,
			      &r->hash, &r->hash_len);

  *hash = r->hash; // no need to copy - see caller logic

//g_message("dupin_link_record_add_revision_obj: md5 hash = %s (len=%d)\n", r->hash, (gint)r->hash_len);

  dupin_util_mvcc_new (rev, r->hash, mvcc);

  r->mvcc = g_strdup (mvcc);
  r->mvcc_len = strlen (mvcc);

  r->context_id = g_strdup (context_id);
  r->label = g_strdup (label);
  r->href = g_strdup (href);
  r->rel = g_strdup (rel);
  r->tag = g_strdup (tag);

  /* TODO - double check that the revision record 'r' is freeded properly when hash table disposed */

  g_hash_table_insert (record->revisions, g_strdup (mvcc), r);

  if (!record->last || (dupin_util_mvcc_revision_cmp (dupin_link_record_get_last_revision (record), mvcc) < 0 ))
    record->last = r;
}

static void
dupin_link_record_add_revision_str (DupinLinkRecord * record, guint rev, gchar * hash, gssize hash_size,
 			            gchar * str, gssize size,
		                    gchar * context_id,
		                    gchar * label,
                                    gchar * href,
                                    gchar * rel,
                                    gchar * tag,
				    gboolean delete,
				    gsize created,
				    gsize rowid,
				    gboolean is_weblink)
{
  DupinLinkRecordRev *r;
  gchar mvcc[DUPIN_ID_MAX_LEN];

  r = g_malloc0 (sizeof (DupinLinkRecordRev));
  r->revision = rev;

  if (hash && *hash)
    {
      if (hash_size < 0)
	hash_size = strlen (hash);

      r->hash = g_strndup (hash, hash_size);
      r->hash_len = hash_size;
    }

  if (str && *str)
    {
      if (size < 0)
	size = strlen (str);

      r->obj_serialized = g_strndup (str, size);
      r->obj_serialized_len = size;
    }

  r->deleted = delete;
  r->created = created;
  r->is_weblink = is_weblink;
  r->rowid = rowid;

  dupin_util_mvcc_new (rev, r->hash, mvcc);

  r->mvcc = g_strdup (mvcc);
  r->mvcc_len = strlen (mvcc);

  r->context_id = g_strdup (context_id);
  r->label = g_strdup (label);
  r->href = g_strdup (href);
  r->rel = g_strdup (rel);
  r->tag = g_strdup (tag);

  /* TODO - double check that the revision record 'r' is freeded properly when hash table disposed */

  g_hash_table_insert (record->revisions, g_strdup (mvcc), r);

  if (!record->last || (dupin_util_mvcc_revision_cmp (dupin_link_record_get_last_revision (record), mvcc) < 0 ))
    record->last = r;
}

/* NOTE - compute DUPIN_ID_HASH_ALGO hash of JSON + deleted flag + context_id + label + href + rel + tag + is_weblink */

static gboolean
dupin_link_record_generate_hash (DupinLinkRecord * record,
                            gchar * obj_serialized, gssize obj_serialized_len,
			    gchar * context_id,
			    gchar * label,
                            gchar * href,
                            gchar * rel,
                            gchar * tag,
			    gboolean delete,
			    gboolean is_weblink,
			    gchar ** hash, gsize * hash_len)
{
  g_return_val_if_fail (record != NULL, FALSE);

  GString * str= g_string_new ("");
  gchar * tmp=NULL;

  /* JSON string */
  str = g_string_append_len (str, obj_serialized, (obj_serialized_len < 0) ? strlen(obj_serialized) : obj_serialized_len);

  /* delete flag */
  g_string_append_printf (str, "%d", (gint)delete);

  /* is web link flag */
  g_string_append_printf (str, "%d", (gint)is_weblink);

  g_string_append_printf (str, "%s", context_id);
  g_string_append_printf (str, "%s", label);
  g_string_append_printf (str, "%s", href);
  g_string_append_printf (str, "%s", rel);
  g_string_append_printf (str, "%s", tag);

  tmp = g_string_free (str, FALSE);

  *hash = g_compute_checksum_for_string (DUPIN_ID_HASH_ALGO, tmp, strlen(tmp));
  *hash_len = DUPIN_ID_HASH_ALGO_LEN;

  g_free (tmp);

  return TRUE;
}

gboolean
dupin_link_record_util_is_valid_context_id (gchar * id)
{
  g_return_val_if_fail (id != NULL, FALSE);

  return dupin_util_is_valid_record_id (id);
}

gboolean
dupin_link_record_util_is_valid_label (gchar * label)
{
  g_return_val_if_fail (label != NULL, FALSE);

  return g_strcmp0 (label, "") ? TRUE : FALSE;
}

gboolean
dupin_link_record_util_is_valid_href (gchar * href)
{
  g_return_val_if_fail (href != NULL, FALSE);

  if (dupin_util_is_valid_absolute_uri (href))
    return TRUE;
  else
    return dupin_util_is_valid_record_id (href); /* we only allow relative refs on same parent DB for the moment */
}

gboolean
dupin_link_record_util_is_valid_rel (gchar * rel)
{
  g_return_val_if_fail (rel != NULL, FALSE);

  guint i;

  for (i = 0; DSWeblinkingRelationRegistryList[i].code != WEBLINKING_RELNAME__END; i++)
    if (!g_strcmp0 (DSWeblinkingRelationRegistryList[i].rel, rel))
      break;

  if (DSWeblinkingRelationRegistryList[i].code == WEBLINKING_RELNAME__END)
    {
      /* TODO - check if IRI properly - using libsoup ? */

      return dupin_util_is_valid_absolute_uri (rel);
    }

  return TRUE;
}

/* EOF */
