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
	"INSERT INTO Dupin (id, rev, hash, obj, tm, context_id, label, href, rel, tag, is_weblink, idspath, labelspath) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%q', %Q, %Q, '%q','%q', '%q')"

#define DUPIN_LINKB_SQL_UPDATE \
	"INSERT OR REPLACE INTO Dupin (id, rev, hash, obj, tm, context_id, label, href, rel, tag, is_weblink, idspath, labelspath) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%q', %Q, %Q, '%q', '%q', '%q')"

#define DUPIN_LINKB_SQL_READ \
	"SELECT rev, hash, obj, deleted, tm, ROWID AS rowid, context_id, label, href, rel, tag, is_weblink, idspath, labelspath FROM Dupin WHERE id='%q'"

#define DUPIN_LINKB_SQL_DELETE \
	"INSERT OR REPLACE INTO Dupin (id, rev, deleted, hash, tm, context_id, label, href, rel, tag, is_weblink, idspath, labelspath) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', 'TRUE', '%q', '%" G_GSIZE_FORMAT "', '%q', '%q', '%q', %Q, %Q, '%q', '%q', '%q')"


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

  /* HTML5 rel extra ones - see http://www.whatwg.org/specs/web-apps/current-work/multipage/links.html#linkTypes */

  {WEBLINKING_RELNAME_ARCHIVES, "archives", "Provides a link to a collection of records, documents, or other materials of historical interest."}
  ,
  {WEBLINKING_RELNAME_AUTHOR, "author", "Gives a link to the current document's author."}
  ,
  {WEBLINKING_RELNAME_EXTERNAL, "external", "Indicates that the referenced document is not part of the same site as the current document."}
  ,
  {WEBLINKING_RELNAME_ICON, "icon", "Imports an icon to represent the current document."}
  ,
  {WEBLINKING_RELNAME_NOFOLLOW, "nofollow", "Indicates that the current document's original author or publisher does not endorse the referenced document."}
  ,
  {WEBLINKING_RELNAME_NOREFERRER, "noreferrer", "Requires that the user agent not send an HTTP Referer (sic) header if the user follows the hyperlink."}
  ,
  {WEBLINKING_RELNAME_PINGBACK, "pingback", "Gives the address of the pingback server that handles pingbacks to the current document."}
  ,
  {WEBLINKING_RELNAME_PREFETCH, "prefetch", "Specifies that the target resource should be preemptively cached."}
  ,
  {WEBLINKING_RELNAME_SEARCH, "search", "Gives a link to a resource that can be used to search through the current document and its related pages."}
  ,
  {WEBLINKING_RELNAME_SIDEBAR, "sidebar", "Specifies that the referenced document, if retrieved, is intended to be shown in the browser's sidebar (if it has one)."}
  ,
  {WEBLINKING_RELNAME_TAG, "tag", "Gives a tag (identified by the given address) that applies to the current document."}
  ,

  {WEBLINKING_RELNAME__END, NULL, NULL}
};

#define DUPIN_LINKB_DEFAULT_REL	DSWeblinkingRelationRegistryList[WEBLINKING_RELNAME_ALTERNATE].rel

#define DUPIN_LINKB_DEFAULT_IDSPATH	"[ ]"

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
                                           JsonNode * idspath,
                                           JsonNode * labelspath,
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
                                           gchar * idspath_serialized, gssize idspath_serialized_len, 
                                           gchar * labelspath_serialized, gssize labelspath_serialized_len, 
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

JsonNode * dupin_link_record_util_generate_paths_node
						(DupinLinkB * linkb,
						 gchar * source_id,
						 gchar * target_id,
						 gchar * label,
						 gchar * tag,
						 GError ** error, gboolean lock);

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

  JsonNode * paths = NULL;
  JsonNode * idspath = NULL;
  JsonNode * labelspath = NULL;
  gchar * idspath_serialized = NULL;
  gchar * labelspath_serialized = NULL;

  paths = dupin_link_record_util_generate_paths_node (linkb,
  						      context_id,
						      href,
						      label,
						      tag,
						      error, lock);
  if (paths == NULL)
    return NULL;

  idspath = json_array_get_element (json_node_get_array (paths), 0);   
  labelspath = json_array_get_element (json_node_get_array (paths), 1);   

  idspath_serialized = dupin_util_json_serialize (idspath);
  labelspath_serialized = dupin_util_json_serialize (labelspath);

  dupin_linkbase_ref (linkb);

  if (lock == TRUE)
    g_mutex_lock (linkb->mutex);

  record = dupin_link_record_new (linkb, id);

  gsize created = dupin_util_timestamp_now ();

  dupin_link_record_add_revision_obj (record, 1, &md5, obj_node,
				      context_id, label, href, rel, tag,
				      idspath,
				      labelspath,
				      FALSE, created,
				      dupin_util_is_valid_absolute_uri (href));

  if (paths != NULL)
    json_node_free (paths);

  tmp =
    sqlite3_mprintf (DUPIN_LINKB_SQL_INSERT, id, 1, md5,
		     record->last->obj_serialized, created,
		     context_id, label, href, rel, tag,
		     dupin_util_is_valid_absolute_uri (href) ? "TRUE" : "FALSE",
		     idspath_serialized,
		     labelspath_serialized);

  g_free (idspath_serialized);
  g_free (labelspath_serialized);

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
  gchar *idspath = NULL;
  gchar *labelspath = NULL;
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

      else if (!g_strcmp0 (col[i], "idspath"))
	idspath = argv[i];

      else if (!g_strcmp0 (col[i], "labelspath"))
	labelspath = argv[i];

      else if (!g_strcmp0 (col[i], "is_weblink"))
	is_weblink = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;
    }

  if (rev && hash !=NULL)
    dupin_link_record_add_revision_str (data, rev, hash, -1, obj, -1,
					context_id, label, href, rel, tag,
					idspath, -1,
					labelspath, -1,
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
  gchar *idspath = NULL;
  gchar *labelspath = NULL;
  gboolean is_weblink = FALSE;
  gchar *id = NULL;

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

      else if (!g_strcmp0 (col[i], "idspath"))
	idspath = argv[i];

      else if (!g_strcmp0 (col[i], "labelspath"))
	labelspath = argv[i];

      else if (!g_strcmp0 (col[i], "is_weblink"))
	is_weblink = !g_strcmp0 (argv[i], "TRUE") ? TRUE : FALSE;

      else if (!g_strcmp0 (col[i], "id"))
	id = argv[i];
    }

  if (rev && hash !=NULL)
    {
      dupin_linkbase_ref (s->linkb);

      record = dupin_link_record_new (s->linkb, id);

      dupin_link_record_add_revision_str (record, rev, hash, -1, obj, -1,
					  context_id, label, href, rel, tag,
					  idspath, -1,
					  labelspath, -1,
					  delete, tm, rowid, is_weblink);

      s->list = g_list_append (s->list, record);
    }

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
                            gchar **               labels,
                            gchar *                tag,
		            GList ** list, GError ** error)
{
  GString *str;
  gchar *tmp;
  gchar *errmsg;
  gchar *check_deleted="";
  gchar *check_linktype="";
  gint i=0;

  struct dupin_link_record_get_list_t s;

  g_return_val_if_fail (linkb != NULL, FALSE);
  g_return_val_if_fail (list != NULL, FALSE);

  if (context_id != NULL)
    g_return_val_if_fail (dupin_link_record_util_is_valid_context_id (context_id) == TRUE, FALSE);

  if (labels != NULL)
    {
      for (i = 0; labels[i]; i++)
        {
          g_return_val_if_fail (dupin_link_record_util_is_valid_label (labels[i]) == TRUE, FALSE);
        }
    }

  memset (&s, 0, sizeof (s));
  s.linkb = linkb;

  str = g_string_new ("SELECT *, ROWID as rowid FROM Dupin as d WHERE d.rev = (select max(rev) as rev FROM Dupin WHERE id=d.id) ");

  if (count_type == DP_COUNT_EXIST)
    check_deleted = " d.deleted = 'FALSE' ";
  else if (count_type == DP_COUNT_DELETE)
    check_deleted = " d.deleted = 'TRUE' ";

  if (links_type == DP_LINK_TYPE_WEB_LINK)
    check_linktype = " d.is_weblink = 'TRUE' ";
  else if (links_type == DP_LINK_TYPE_RELATIONSHIP)
    check_linktype = " d.is_weblink = 'FALSE' ";

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

  if (g_strcmp0 (check_deleted, ""))
    {
      g_string_append_printf (str, " %s %s ", op, check_deleted);
      op = "AND";
    }

  if (g_strcmp0 (check_linktype, ""))
    {
      g_string_append_printf (str, " %s %s ", op, check_linktype);
      op = "AND";
    }

  if (context_id != NULL)
    {
      gchar * tmp2 = sqlite3_mprintf (" %s d.context_id = '%q' ", op, context_id);
      str = g_string_append (str, tmp2);
      sqlite3_free (tmp2);
      op = "AND";
    }

  if (labels != NULL)
    {
      if (labels[0])
        {
          gchar * tmp2 = sqlite3_mprintf (" %s ( ", op);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
	}

      for (i = 0; labels[i]; i++)
        {
          gchar * tmp2 = sqlite3_mprintf (" d.label = '%q' ", labels[i]);
          str = g_string_append (str, tmp2);
          sqlite3_free (tmp2);
          if (labels[i+1])
            str = g_string_append (str, " OR ");
        }

      if (labels[0])
        str = g_string_append (str, " ) ");

      op = "AND";
    }

  if (tag != NULL)
    {
      gchar * tmp2 = sqlite3_mprintf (" %s d.tag = '%q' ", op, tag);
      str = g_string_append (str, tmp2);
      sqlite3_free (tmp2);
    }

  if (orderby_type == DP_ORDERBY_ROWID)
    //str = g_string_append (str, " GROUP BY id ORDER BY d.ROWID");
    str = g_string_append (str, " ORDER BY d.ROWID");
  else
    //str = g_string_append (str, " GROUP BY id ORDER BY d.ROWID");
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
  g_return_val_if_fail (href != NULL, FALSE);
  g_return_val_if_fail (dupin_link_record_util_is_valid_href (href) == TRUE, FALSE);
  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);

  if (rel == NULL)
    rel = DUPIN_LINKB_DEFAULT_REL;
  else
    g_return_val_if_fail (dupin_link_record_util_is_valid_rel (rel) == TRUE, FALSE);

  if (label == NULL)
    label = (gchar *)dupin_link_record_get_label (record);
  else
    g_return_val_if_fail (dupin_link_record_util_is_valid_label (label) == TRUE, FALSE);

  g_mutex_lock (record->linkb->mutex);

  rev = record->last->revision + 1;

  gsize created = dupin_util_timestamp_now ();

  JsonNode * paths = NULL;
  JsonNode * idspath = NULL;
  JsonNode * labelspath = NULL;
  gchar * idspath_serialized = NULL;
  gchar * labelspath_serialized = NULL;

  paths = dupin_link_record_util_generate_paths_node (record->linkb,
                                                      (gchar *)dupin_link_record_get_context_id (record),
                                                      href,
                                                      label,
                                                      tag,
                                                      error, FALSE);
  if (paths == NULL)
    return FALSE;

  idspath = json_array_get_element (json_node_get_array (paths), 0);     
  labelspath = json_array_get_element (json_node_get_array (paths), 1);     

  idspath_serialized = dupin_util_json_serialize (idspath);
  labelspath_serialized = dupin_util_json_serialize (labelspath);

  dupin_link_record_add_revision_obj (record, rev, &md5, obj_node,
				      (gchar *)dupin_link_record_get_context_id (record),
				      label, href, rel, tag,
				      idspath,
				      labelspath,
				      FALSE, created,
				      dupin_util_is_valid_absolute_uri (href));

  if (paths != NULL)
    json_node_free (paths);

  tmp =
    sqlite3_mprintf (DUPIN_LINKB_SQL_INSERT, record->id, rev, md5,
		     record->last->obj_serialized, created,
		     (gchar *)dupin_link_record_get_context_id (record),
		     label, href, rel, tag,
		     dupin_util_is_valid_absolute_uri (href) ? "TRUE" : "FALSE",
		     idspath_serialized,
		     labelspath_serialized);

  g_free (idspath_serialized);
  g_free (labelspath_serialized);

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
				      record->last->idspath,
				      record->last->labelspath,
 				      TRUE, created,
				      dupin_link_record_is_weblink (record));

  tmp = sqlite3_mprintf (DUPIN_LINKB_SQL_DELETE, record->id, rev, md5, created,	
				      (gchar *)dupin_link_record_get_context_id (record),
				      (gchar *)dupin_link_record_get_label (record),
				      (gchar *)dupin_link_record_get_href (record),
				      (gchar *)dupin_link_record_get_rel (record),
				      (gchar *)dupin_link_record_get_tag (record),
				      dupin_link_record_is_weblink (record) ? "TRUE" : "FALSE",
				      record->last->idspath_serialized,
				      record->last->labelspath_serialized);

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

JsonNode *
dupin_link_record_get_revision_idspath_node (DupinLinkRecord * record, gchar * mvcc)
{
  g_return_val_if_fail (record != NULL, 0);

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

  /* TODO - check if the following is correct - we do not return an ids_path for deleted links ?! */

  if (r->deleted == TRUE)
    g_return_val_if_fail (dupin_link_record_is_deleted (record, mvcc) != FALSE,
			  NULL);

  /* r->idspath stays owernship of the record revision - the caller eventually need to json_node_copy() it */
  if (r->idspath)
    return r->idspath;

  JsonParser * parser = json_parser_new();

  /* we do not check any parsing error due we stored earlier, we assume it is sane */
  if (json_parser_load_from_data (parser, r->idspath_serialized, r->idspath_serialized_len, NULL) == FALSE)
    goto dupin_link_record_get_revision_idspath_node_error;

  r->idspath = json_node_copy (json_parser_get_root (parser));

  if (parser != NULL)
    g_object_unref (parser);

  /* r->idspath stays owernship of the record revision - the caller eventually need to json_node_copy() it */
  return r->idspath;

dupin_link_record_get_revision_idspath_node_error:

  if (parser != NULL)
    g_object_unref (parser);

  return NULL;
}

JsonNode *
dupin_link_record_get_revision_labelspath_node (DupinLinkRecord * record, gchar * mvcc)
{
  g_return_val_if_fail (record != NULL, 0);

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

  /* TODO - check if the following is correct - we do not return an labels_path for deleted links ?! */

  if (r->deleted == TRUE)
    g_return_val_if_fail (dupin_link_record_is_deleted (record, mvcc) != FALSE,
			  NULL);

  /* r->labelspath stays owernship of the record revision - the caller eventually need to json_node_copy() it */
  if (r->labelspath)
    return r->labelspath;

  JsonParser * parser = json_parser_new();

  /* we do not check any parsing error due we stored earlier, we assume it is sane */
  if (json_parser_load_from_data (parser, r->labelspath_serialized, r->labelspath_serialized_len, NULL) == FALSE)
    goto dupin_link_record_get_revision_labelspath_node_error;

  r->labelspath = json_node_copy (json_parser_get_root (parser));

  if (parser != NULL)
    g_object_unref (parser);

  /* r->labelspath stays owernship of the record revision - the caller eventually need to json_node_copy() it */
  return r->labelspath;

dupin_link_record_get_revision_labelspath_node_error:

  if (parser != NULL)
    g_object_unref (parser);

  return NULL;
}

gboolean
dupin_link_record_is_weblink (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  return record->last->is_weblink;
}

gboolean
dupin_link_record_is_reflexive (DupinLinkRecord * record)
{
  g_return_val_if_fail (record != NULL, 0);

  // TODO - make this stronger eventually

  return (!g_strcmp0 (dupin_link_record_get_context_id (record),
		      dupin_link_record_get_href (record))) ? TRUE : FALSE;
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

  if (rev->idspath)
    json_node_free (rev->idspath);

  if (rev->idspath_serialized)
    g_free (rev->idspath_serialized);

  if (rev->labelspath)
    json_node_free (rev->labelspath);

  if (rev->labelspath_serialized)
    g_free (rev->labelspath_serialized);

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
                               JsonNode * idspath,
                               JsonNode * labelspath,
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

  if (idspath != NULL)
    {
      JsonGenerator * gen = json_generator_new();

      r->idspath = json_node_copy (idspath);

      json_generator_set_root (gen, r->idspath );

      r->idspath_serialized = json_generator_to_data (gen,&r->idspath_serialized_len);

      g_object_unref (gen);
    }

  if (labelspath != NULL)
    {
      JsonGenerator * gen = json_generator_new();

      r->labelspath = json_node_copy (labelspath);

      json_generator_set_root (gen, r->labelspath );

      r->labelspath_serialized = json_generator_to_data (gen,&r->labelspath_serialized_len);

      g_object_unref (gen);
    }

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
                                    gchar * idspath_serialized, gssize idspath_serialized_len,
                                    gchar * labelspath_serialized, gssize labelspath_serialized_len,
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

  if (idspath_serialized && *idspath_serialized)
    {
      if (idspath_serialized_len < 0)
        idspath_serialized_len = strlen (idspath_serialized);

      r->idspath_serialized = g_strndup (idspath_serialized, idspath_serialized_len);
      r->idspath_serialized_len = idspath_serialized_len;
    }

  if (labelspath_serialized && *labelspath_serialized)
    {
      if (labelspath_serialized_len < 0)
        labelspath_serialized_len = strlen (labelspath_serialized);

      r->labelspath_serialized = g_strndup (labelspath_serialized, labelspath_serialized_len);
      r->labelspath_serialized_len = labelspath_serialized_len;
    }

  /* TODO - double check that the revision record 'r' is freeded properly when hash table disposed */

  g_hash_table_insert (record->revisions, g_strdup (mvcc), r);

  if (!record->last || (dupin_util_mvcc_revision_cmp (dupin_link_record_get_last_revision (record), mvcc) < 0 ))
    record->last = r;
}

/* Utility functions */

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

struct dupin_link_record_util_generate_paths_node_t
{
  gchar * parent_idspath;
  gchar * parent_labelspath;
};

static int
dupin_link_record_util_generate_paths_node_cb (void *data, int argc, char **argv, char **col)
{
  struct dupin_link_record_util_generate_paths_node_t *s = data;

  if (argv[0] && *argv[0])
    s->parent_idspath = g_strdup (argv[0]);

  if (argv[1] && *argv[1])
    s->parent_labelspath = g_strdup (argv[1]);

  return 0;
}

JsonNode *
dupin_link_record_util_generate_paths_node (DupinLinkB * linkb,
					    gchar * source_id,
                                            gchar * target_id,
                                            gchar * label,
                                            gchar * tag,
					    GError ** error, gboolean lock)
{
  g_return_val_if_fail (source_id != NULL, FALSE);
  g_return_val_if_fail (dupin_link_record_util_is_valid_context_id (source_id), FALSE);

  gchar *errmsg;
  struct dupin_link_record_util_generate_paths_node_t parent;
  memset (&parent, 0, sizeof (struct dupin_link_record_util_generate_paths_node_t));

  JsonNode * idspath_node = NULL;
  JsonArray * idspath_array = NULL;
  JsonNode * labelspath_node = NULL;
  JsonArray * labelspath_array = NULL;

  gchar *query;
  if (tag != NULL)
    query = sqlite3_mprintf ("SELECT idspath, labelspath, id as link_record_id FROM Dupin WHERE rev = (select max(rev) as rev FROM Dupin WHERE id=link_record_id) AND deleted = 'FALSE' AND href = '%q' AND tag = %Q GROUP BY context_id", source_id, tag);
  else
    query = sqlite3_mprintf ("SELECT idspath, labelspath, id as link_record_id FROM Dupin WHERE rev = (select max(rev) as rev FROM Dupin WHERE id=link_record_id) AND deleted = 'FALSE' AND href = '%q' GROUP BY context_id", source_id);

//g_message("dupin_link_record_util_generate_paths_node: source_id=%s target_id=%s label=%s tag=%s\n", source_id, target_id, label, tag);
//g_message("dupin_link_record_util_generate_paths_node: query=%s\n", query);

  if (lock == TRUE)
    g_mutex_lock (linkb->mutex);

  if (sqlite3_exec (linkb->db, query, dupin_link_record_util_generate_paths_node_cb, &parent, &errmsg)
      != SQLITE_OK)
    {
      if (lock == TRUE)
        g_mutex_unlock (linkb->mutex);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_OPEN, "%s",
                   errmsg);
      sqlite3_free (errmsg);
      sqlite3_free (query);
      return NULL;
    }

  if (lock == TRUE)
    g_mutex_unlock (linkb->mutex);

  sqlite3_free (query);

  JsonParser * parser = json_parser_new();

  if (parent.parent_idspath != NULL)
    {
      if (json_parser_load_from_data (parser, parent.parent_idspath, -1, NULL) == FALSE)
        {
          if (parser != NULL)
            g_object_unref (parser);
          g_free (parent.parent_idspath);
          g_free (parent.parent_labelspath);

          return NULL;
        }

      idspath_node = json_node_copy (json_parser_get_root (parser));

      if (json_node_get_node_type (idspath_node) != JSON_NODE_ARRAY)
        {
          if (parser != NULL)
            g_object_unref (parser);
          json_node_free (idspath_node);
          g_free (parent.parent_idspath);
          g_free (parent.parent_labelspath);

          return NULL;
        }

      idspath_array = json_node_get_array (idspath_node);

      if (json_array_get_length (idspath_array) == 0)
        {
          g_warning ("dupin_link_record_util_generate_paths_node: skipped idspath generation for source_id=%s target_id=%s lael=%s tag=%s - parent path was set to empty JSON array\n",
			source_id, target_id, label, tag);

          if (parser != NULL)
            g_object_unref (parser);
          json_node_free (idspath_node);
          g_free (parent.parent_idspath);
          g_free (parent.parent_labelspath);

          return NULL;
	}
    }
  else
    {
      idspath_node = json_node_new (JSON_NODE_ARRAY);
      idspath_array = json_array_new ();
      json_node_take_array (idspath_node, idspath_array);
    }
  
  if (parent.parent_labelspath != NULL)
    {
      if (json_parser_load_from_data (parser, parent.parent_labelspath, -1, NULL) == FALSE)
        {
          if (parser != NULL)
            g_object_unref (parser);
          g_free (parent.parent_idspath);
          g_free (parent.parent_labelspath);
          json_node_free (idspath_node);

          return NULL;
        }

      labelspath_node = json_node_copy (json_parser_get_root (parser));

      if (json_node_get_node_type (labelspath_node) != JSON_NODE_ARRAY)
        {
          if (parser != NULL)
            g_object_unref (parser);
          json_node_free (labelspath_node);
          g_free (parent.parent_idspath);
          g_free (parent.parent_labelspath);
          json_node_free (idspath_node);

          return NULL;
        }

      labelspath_array = json_node_get_array (labelspath_node);

      if (json_array_get_length (labelspath_array) == 0)
        {
          g_warning ("dupin_link_record_util_generate_paths_node: skipped labelspath generation for source_id=%s target_id=%s label=%s tag=%s - parent path was set to empty JSON array\n",
			source_id, target_id, label, tag);

          if (parser != NULL)
            g_object_unref (parser);
          json_node_free (labelspath_node);
          g_free (parent.parent_idspath);
          g_free (parent.parent_labelspath);
          json_node_free (idspath_node);

          return NULL;
	}
    }
  else
    {
      labelspath_node = json_node_new (JSON_NODE_ARRAY);
      labelspath_array = json_array_new ();
      json_node_take_array (labelspath_node, labelspath_array);
    }
  
  /* NOTE - try to avoid disasters pre-parse source_id, target_id, label and tag to make sure JSON will be sane
 	    even though note that the dupin_view_collation() and the dupin_util_get_collate_type do not use json-glib
	    in any way. But here we might have the user posting nusty IDs */

  /* ids path */

  JsonNode * source_id_node = NULL;
  JsonNode * target_id_node = NULL;
  gchar * escaped_source_id = dupin_util_json_strescape (source_id);
  gchar * escaped_target_id = dupin_util_json_strescape (target_id);

  if (escaped_source_id == NULL || escaped_target_id == NULL)
    {
      if (parser != NULL)
        g_object_unref (parser);
      json_node_free (idspath_node);
      json_node_free (labelspath_node);
      g_free (parent.parent_idspath);
      g_free (parent.parent_labelspath);

      return NULL;
    }

  source_id_node = json_node_new (JSON_NODE_VALUE);
  json_node_set_string (source_id_node, escaped_source_id);

  target_id_node = json_node_new (JSON_NODE_VALUE);
  json_node_set_string (target_id_node, escaped_target_id);

  JsonNode * tag_node = NULL;
  if (tag != NULL)
    {
      /* NOTE - not yet used, but we might want the tag to be a JSON structure too so collation works nicely too */

      if (json_parser_load_from_data (parser, tag, -1, NULL) == FALSE)
        {
          /* NOTE - OK we parse the tag as a string */
          tag_node = json_node_new (JSON_NODE_VALUE);
          gchar * escaped_tag = dupin_util_json_strescape (tag);
          json_node_set_string (tag_node, escaped_tag);
          g_free (escaped_tag);
        }
      else
        {
          tag_node = json_node_copy (json_parser_get_root (parser));
        }

      /* [ [EXISTING-SOURCE_ID-IDSPATH-with-TAG], [ TAG, SOURCE_ID ], [ TAG, TARGET_ID] ] */
      JsonArray * target_id_array = json_array_new ();
      json_array_add_element (target_id_array, tag_node);
      json_array_add_element (target_id_array, target_id_node);

      if (json_array_get_length (idspath_array) == 0)
        {
          JsonNode * source_id_array_node = json_node_new (JSON_NODE_ARRAY);
          JsonArray * source_id_array = json_array_new ();
          json_node_take_array (source_id_array_node, source_id_array);
          json_array_add_element (source_id_array, json_node_copy (tag_node));
          json_array_add_element (source_id_array, source_id_node);

          json_array_add_element (idspath_array, source_id_array_node);
        }
      else
        {
          json_node_free (source_id_node);
        }

      json_array_add_array_element (idspath_array, target_id_array);
    }
  else
    {
      /* [ [EXISTING-SOURCE_ID-IDSPATH], SOURCE_ID, TARGET_ID ] */
      if (json_array_get_length (idspath_array) == 0)
        json_array_add_element (idspath_array, source_id_node);
      else
        json_node_free (source_id_node);

      json_array_add_element (idspath_array, target_id_node);
    }

  g_free (escaped_target_id);
  g_free (escaped_source_id);

  /* labels path */

  JsonNode * label_node = NULL;
  gchar * escaped_label = dupin_util_json_strescape (label);

  if (escaped_label == NULL)
    {
      if (parser != NULL)
        g_object_unref (parser);
      json_node_free (idspath_node);
      json_node_free (labelspath_node);
      g_free (parent.parent_idspath);
      g_free (parent.parent_labelspath);

      return NULL;
    }

  label_node = json_node_new (JSON_NODE_VALUE);
  json_node_set_string (label_node, escaped_label);

  if (tag_node != NULL)
    {
      /* [ [EXISTING-SOURCE_ID-LABELSPATH-with-TAG], [ TAG, LABEL ] ] */
      JsonArray * label_array = json_array_new ();
      json_array_add_element (label_array, json_node_copy (tag_node));
      json_array_add_element (label_array, label_node);

      json_array_add_array_element (labelspath_array, label_array);
    }
  else
    {
      /* [ [EXISTING-SOURCE_ID-LABELSPATH], SOURCE_ID, TARGET_ID ] */
      json_array_add_element (labelspath_array, label_node);
    }

  g_free (escaped_label);

  g_free (parent.parent_idspath);
  g_free (parent.parent_labelspath);

  if (parser != NULL)
    g_object_unref (parser);

  JsonNode *  paths = json_node_new (JSON_NODE_ARRAY);
  JsonArray * paths_array = json_array_new ();
  json_node_take_array (paths, paths_array);

  json_array_add_element (paths_array, idspath_node);
  json_array_add_element (paths_array, labelspath_node);

//{
//gchar * string = dupin_util_json_serialize (paths);
//g_message("dupin_link_record_util_generate_paths_node: paths = %s\n", string);
//g_free (string);
//}

  return paths;
}

/* Insert */

static gchar *
dupin_link_record_insert_extract_label (DupinLinkB * linkb, JsonNode * obj_node)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  gchar *ret = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_LINK_OBJ_LABEL) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_LINK_OBJ_LABEL);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    ret = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_LINK_OBJ_LABEL); 

  return ret;
}

static gchar *
dupin_link_record_insert_extract_href (DupinLinkB * linkb, JsonNode * obj_node)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  gchar *ret = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_LINK_OBJ_HREF) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_LINK_OBJ_HREF);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    ret = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_LINK_OBJ_HREF); 

  return ret;
}

static gchar *
dupin_link_record_insert_extract_rel (DupinLinkB * linkb, JsonNode * obj_node)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  gchar *ret = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_LINK_OBJ_REL) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_LINK_OBJ_REL);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    ret = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_LINK_OBJ_REL); 

  return ret;
}

static gchar *
dupin_link_record_insert_extract_tag (DupinLinkB * linkb, JsonNode * obj_node)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  gchar *ret = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_LINK_OBJ_TAG) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_LINK_OBJ_TAG);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    ret = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_LINK_OBJ_TAG); 

  return ret;
}

static gchar *
dupin_link_record_insert_extract_rev (DupinLinkB * linkb, JsonNode * obj_node)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

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
dupin_link_record_insert_extract_id (DupinLinkB * linkb, JsonNode * obj_node)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

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
      g_string_append_printf (str, "Identifier is of type %s and not string. The system has generated a new ID automaticlaly.", json_node_type_name (node));
      gchar * tmp = g_string_free (str, FALSE);
      dupin_linkbase_set_warning (linkb, tmp);
      g_free (tmp);
    }

  json_object_remove_member (obj, REQUEST_OBJ_ID);

  return id;
}

static gboolean
dupin_link_record_insert_extract_deleted (DupinLinkB * linkb, JsonNode * obj_node)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

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

/* insert = create or update */

gboolean
dupin_link_record_insert_check_context_id (DupinLinkB * linkb,
			                   gchar * context_id)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  /* NOTE - this code is more generic than needed, we will possibly use this in future ... */
  gboolean document_deleted = FALSE;
  gboolean document_exists = TRUE;

  if (dupin_linkbase_get_parent_is_db (linkb) == TRUE )
    {
      DupinDB * parent_db=NULL;

      if (! (parent_db = dupin_database_open (linkb->d, dupin_linkbase_get_parent (linkb), NULL)))
        {
          dupin_linkbase_set_error (linkb, "Cannot connect to parent database");
	  return FALSE;
        }

      DupinRecord * doc_id_record = dupin_record_read (parent_db, context_id, NULL);

      if (doc_id_record == NULL)
        document_exists = FALSE;
      else
        {
          if (dupin_record_is_deleted (doc_id_record, NULL) == TRUE)
            document_deleted = TRUE;

          dupin_record_close (doc_id_record);
        }

      dupin_database_unref (parent_db);
    }
  else
    {
      DupinLinkB * parent_linkb=NULL;

      if (!(parent_linkb = dupin_linkbase_open (linkb->d, dupin_linkbase_get_parent (linkb), NULL)))
        {
          dupin_linkbase_set_error (linkb, "Cannot connect to parent linkbase");
	  return FALSE;
        }

      DupinLinkRecord * link_id_record = dupin_link_record_read (parent_linkb, context_id, NULL);

      if (link_id_record == NULL)
        document_exists = FALSE;
      else
        {
          if (dupin_link_record_is_deleted (link_id_record, NULL) == TRUE)
            document_deleted = TRUE;

          dupin_link_record_close (link_id_record);
        }

      dupin_linkbase_unref (parent_linkb);
    }

  if (document_exists == FALSE )
    {
      //dupin_linkbase_set_warning (linkb, "request_global_post_doc_link: adding a link to a non existing document");
      return TRUE;
    }
  else if (document_deleted == TRUE )
    {
      dupin_linkbase_set_error (linkb,  "Cannot add a link to a document which is marked as deleted.");
      return FALSE;
    }

  return TRUE;
}

/* NOTE - the id of a link must only be used for update or deletion, never possible to set by user */

gboolean
dupin_link_record_insert (DupinLinkB * linkb,
			  JsonNode * obj_node,
			  gchar * id,
			  gchar * caller_mvcc,
			  gchar * context_id,
			  DupinLinksType link_type,
			  GList ** response_list,
			  gboolean strict_links)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  DupinLinkRecord *record=NULL;
  GError * error = NULL;

  gchar * mvcc=NULL;
  gchar * json_record_id;
  gchar * json_record_label;
  gchar * json_record_href;
  gchar * json_record_rel;
  gchar * json_record_tag;

  if (caller_mvcc != NULL)
    {
      g_return_val_if_fail (dupin_util_is_valid_mvcc (caller_mvcc) == TRUE, FALSE);
    }

  if (json_node_get_node_type (obj_node) != JSON_NODE_OBJECT)
    {
      dupin_linkbase_set_error (linkb, "Input must be a JSON object");
      return FALSE;
    }

  if (strict_links == TRUE)
    {
//g_message("dupin_link_record_insert: checking context_id=%s\n", context_id);

      gboolean link_ok = dupin_link_record_insert_check_context_id (linkb, context_id);

      if (link_ok == FALSE )
        return FALSE;
    }

  gboolean to_delete = dupin_link_record_insert_extract_deleted (linkb, obj_node);

  json_record_href = dupin_link_record_insert_extract_href (linkb, obj_node);

  if (json_record_href != NULL)
    {
      gboolean is_weblink = dupin_util_is_valid_absolute_uri (json_record_href);

      if (link_type == DP_LINK_TYPE_WEB_LINK
          && is_weblink == FALSE)
        {
          g_free (json_record_href);
          dupin_linkbase_set_error (linkb, "Expected a web link but a relationship was passed");
          return FALSE;
        }
      else if (link_type == DP_LINK_TYPE_RELATIONSHIP
               && is_weblink == TRUE)
        {
          g_free (json_record_href);
          dupin_linkbase_set_error (linkb, "Expected a relationship but a web link was passed");
          return FALSE;
        }
      /* else is auto, picked by the system */
    }

  JsonNode * record_response_node = json_node_new (JSON_NODE_OBJECT);
  JsonObject * record_response_obj = json_object_new ();
  json_node_take_object (record_response_node, record_response_obj);

  /* fetch the _rev field in the record first, if there */
  mvcc = dupin_link_record_insert_extract_rev (linkb, obj_node);

  if (mvcc == NULL
      && caller_mvcc != NULL)
    mvcc = g_strdup (caller_mvcc);

  if ((json_record_id = dupin_link_record_insert_extract_id (linkb, obj_node)))
    {
      if (id && g_strcmp0 (id, json_record_id))
        {
          if (mvcc != NULL)
            g_free (mvcc);
          g_free (json_record_id);
          
          dupin_linkbase_set_error (linkb, "Specified link record id does not match");
          if (record_response_node != NULL)
            json_node_free (record_response_node);
          if (json_record_href)
            g_free (json_record_href);
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
      
      dupin_linkbase_set_error (linkb, "No valid link record id or MVCC specified");
      if (record_response_node != NULL)
        json_node_free (record_response_node);
      if (json_record_href)
        g_free (json_record_href);
      return FALSE;
    }

  json_record_label = dupin_link_record_insert_extract_label (linkb, obj_node);
  json_record_rel = dupin_link_record_insert_extract_rel (linkb, obj_node);
  json_record_tag = dupin_link_record_insert_extract_tag (linkb, obj_node);

//g_message("dupin_link_record_insert: context_id=%s\n", context_id);
//g_message("dupin_link_record_insert: json_record_label=%s\n", json_record_label);
//g_message("dupin_link_record_insert: json_record_href=%s\n", json_record_href);
//g_message("dupin_link_record_insert: json_record_rel=%s\n", json_record_rel);
//g_message("dupin_link_record_insert: json_record_tag=%s\n", json_record_tag);

  if (mvcc != NULL) // we either try to update an existing link
    {
      record = dupin_link_record_read (linkb, id, &error);

      if ( to_delete == TRUE )
        {
          if (!record || dupin_util_mvcc_revision_cmp (mvcc, dupin_link_record_get_last_revision (record))
              || dupin_link_record_delete (record, &error) == FALSE)
            {
              if (record)
                dupin_link_record_close (record);
              record = NULL;
            }
        }
      else
        {
          if (!record || dupin_util_mvcc_revision_cmp (mvcc, dupin_link_record_get_last_revision (record))
              || dupin_link_record_update (record, obj_node, 
                                       json_record_label, json_record_href, json_record_rel, json_record_tag,
                                        &error) == FALSE)
            {
              if (record)
                dupin_link_record_close (record);
              record = NULL;
            }
        }
    }

  else if (!id) // or we try to create a new link with autogenerated id in a specific context_id
    {
      if ( to_delete == TRUE )
        {
          if (record)
            dupin_link_record_close (record);
          record = NULL;
        }
      else
        {
          /* NOTE - context_id is purely internal and can not be set in any way by the user if not via
            the document is being linked from */

          if (context_id == NULL
              || (dupin_link_record_util_is_valid_context_id (context_id) == FALSE))
            {
              if (context_id == NULL)
                dupin_linkbase_set_error (linkb, "Cannot determine link context.");
              else
                dupin_linkbase_set_error (linkb, "Link context is invalid.");

	      if (json_record_id != NULL)
                g_free (json_record_id);
              if (mvcc != NULL)
                g_free (mvcc);

              if (record_response_node != NULL)
                json_node_free (record_response_node);
              if (json_record_href)
                g_free (json_record_href);

              return FALSE;
            }

          record = dupin_link_record_create (linkb, obj_node,
                                         context_id,
                                         json_record_label, json_record_href, json_record_rel, json_record_tag,
                                         &error);
        }
    }
  else
    {
      dupin_linkbase_set_error (linkb, "Links ids can not be set and they are only atomatically generated by the system.");

      if (json_record_id != NULL)
        g_free (json_record_id);
      if (mvcc != NULL)
        g_free (mvcc);

      if (record_response_node != NULL)
        json_node_free (record_response_node);
      if (json_record_href)
        g_free (json_record_href);

      return FALSE;
    }

  if (json_record_label)
    g_free (json_record_label);

  if (json_record_href)
    g_free (json_record_href);

  if (json_record_rel)
    g_free (json_record_rel);

  if (json_record_tag)
    g_free (json_record_tag);

  if (json_record_id)
    g_free (json_record_id);

  if (!record)
    {
      if (error != NULL)
        dupin_linkbase_set_error (linkb, error->message);
      else
        {
          if (to_delete == TRUE)
            {
              if (mvcc == NULL)
                dupin_linkbase_set_error (linkb, "Deleted flag not allowed on link record creation");
              else
                dupin_linkbase_set_error (linkb, "Cannot delete link record");
            }
          else if (mvcc != NULL)
            dupin_linkbase_set_error (linkb, "Cannot update link record");
          else
            dupin_linkbase_set_error (linkb, "Cannot insert link record");
        }

      if (mvcc != NULL)
        g_free (mvcc);

      if (record_response_node != NULL)
        json_node_free (record_response_node);
      return FALSE;
    }

  if (mvcc != NULL)
    g_free (mvcc);

  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_ID, (gchar *) dupin_link_record_get_id (record));
  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_REV, dupin_link_record_get_last_revision (record));

  dupin_link_record_close (record);
  
  *response_list = g_list_prepend (*response_list, record_response_node);

  return TRUE;
}

/* NOTE - we do insert links only and always if a context_id is set ahead (passed to function) and not read from the JSON ever */

/* NOTE - receive an object containing an array of objects, and return an array of objects as result */

gboolean
dupin_link_record_insert_bulk (DupinLinkB * linkb,
                               JsonNode * bulk_node,
			       gchar * context_id,
                               GList ** response_list,
			       gboolean strict_links)
{
  g_return_val_if_fail (linkb != NULL, FALSE);
  g_return_val_if_fail (context_id != NULL, FALSE);
  g_return_val_if_fail (dupin_link_record_util_is_valid_context_id (context_id) == TRUE, FALSE);

  JsonObject *obj;
  JsonNode *node;
  JsonArray *array;
  GList *nodes, *n;

  /* NOTE - check links once per bulk if requested */

  if (strict_links == TRUE)
    {
//g_message("dupin_link_record_insert_bulk: checking context_id=%s\n", context_id);

      gboolean link_ok = dupin_link_record_insert_check_context_id (linkb, context_id);

      if (link_ok == FALSE )
        return FALSE;
    }

  if (json_node_get_node_type (bulk_node) != JSON_NODE_OBJECT)
    {
      dupin_linkbase_set_error (linkb, "Bulk body must be a JSON object");
      return FALSE;
    }

  obj = json_node_get_object (bulk_node);

  if (json_object_has_member (obj, REQUEST_POST_BULK_LINKS_LINKS) == FALSE)
    {
      dupin_linkbase_set_error (linkb, "Bulk body does not contain a mandatory " REQUEST_POST_BULK_LINKS_LINKS " object memeber");
      return FALSE;
    }

  node = json_object_get_member (obj, REQUEST_POST_BULK_LINKS_LINKS);

  if (node == NULL)
    {
      dupin_linkbase_set_error (linkb, "Bulk body does not contain a valid " REQUEST_POST_BULK_LINKS_LINKS " object memeber");
      return FALSE;
    }

  if (json_node_get_node_type (node) != JSON_NODE_ARRAY)
    {
      dupin_linkbase_set_error (linkb, "Bulk body " REQUEST_POST_BULK_LINKS_LINKS " object memeber is not an array");
      return FALSE;
    }

  array = json_node_get_array (node);

  if (array == NULL)
    {
      dupin_linkbase_set_error (linkb, "Bulk body " REQUEST_POST_BULK_LINKS_LINKS " object memeber is not a valid array");
      return FALSE;
    }

  /* scan JSON array */
  nodes = json_array_get_elements (array);

  for (n = nodes; n != NULL; n = n->next)
    {
      JsonNode *element_node = (JsonNode*)n->data;
      gchar * id = NULL;
      gchar * rev = NULL;

      if (json_node_get_node_type (element_node) != JSON_NODE_OBJECT)
        {
          dupin_linkbase_set_error (linkb, "Bulk body " REQUEST_POST_BULK_LINKS_LINKS " array memebr is not a valid JSON object");
          g_list_free (nodes);
          return FALSE;
        }

      if (json_object_has_member (json_node_get_object (element_node), REQUEST_OBJ_ID) == TRUE)
        id = g_strdup ((gchar *)json_object_get_string_member (json_node_get_object (element_node), REQUEST_OBJ_ID));

      if (json_object_has_member (json_node_get_object (element_node), REQUEST_OBJ_REV) == TRUE)
        rev = g_strdup ((gchar *)json_object_get_string_member (json_node_get_object (element_node), REQUEST_OBJ_REV));

      if (dupin_link_record_insert (linkb, element_node, NULL, NULL, context_id, DP_LINK_TYPE_ANY, response_list, FALSE) == FALSE)
        {
          /* NOTE - we report errors inline in the JSON response */

          JsonNode * error_node = json_node_new (JSON_NODE_OBJECT);
          JsonObject * error_obj = json_object_new ();
          json_node_take_object (error_node, error_obj);

          json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, "bad_request");
          json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, dupin_linkbase_get_error (linkb));

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

          if (dupin_linkbase_get_warning (linkb) != NULL)
            json_object_set_string_member (generated_obj, RESPONSE_STATUS_WARNING, dupin_linkbase_get_warning (linkb));
        }
      if (id != NULL)
        g_free (id);

      if (rev!= NULL)
        g_free (rev);

    }
  g_list_free (nodes);

  return TRUE;
}

/* EOF */
