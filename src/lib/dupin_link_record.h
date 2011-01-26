#ifndef _DUPIN_LINK_RECORD_H_
#define _DUPIN_LINK_RECORD_H_

#include <dupin.h>
#include <tb_json.h>

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

G_BEGIN_DECLS

/* see RFC 5988 - Web Linking  spec */

typedef enum
{
  WEBLINKING_RELNAME_ALTERNATE = 0,
  WEBLINKING_RELNAME_APPENDIX,
  WEBLINKING_RELNAME_BOOKMARK,
  WEBLINKING_RELNAME_CHAPTER,
  WEBLINKING_RELNAME_CONTENTS,
  WEBLINKING_RELNAME_COPYRIGHT,
  WEBLINKING_RELNAME_CURRENT,
  WEBLINKING_RELNAME_DESCRIBEDBY,
  WEBLINKING_RELNAME_EDIT,
  WEBLINKING_RELNAME_EDIT_MEDIA,
  WEBLINKING_RELNAME_ENCLOSURE,
  WEBLINKING_RELNAME_FIRST,
  WEBLINKING_RELNAME_GLOSSARY,
  WEBLINKING_RELNAME_HELP,
  WEBLINKING_RELNAME_HUB,
  WEBLINKING_RELNAME_INDEX,
  WEBLINKING_RELNAME_LAST,
  WEBLINKING_RELNAME_LATEST_VERSION,
  WEBLINKING_RELNAME_LICENSE,
  WEBLINKING_RELNAME_NEXT,
  WEBLINKING_RELNAME_NEXT_ARCHIVE,
  WEBLINKING_RELNAME_PAYMENT,
  WEBLINKING_RELNAME_PREV,
  WEBLINKING_RELNAME_PREDECESSOR_VERSION,
  WEBLINKING_RELNAME_PREVIOUS,
  WEBLINKING_RELNAME_PREV_ARCHIVE,
  WEBLINKING_RELNAME_RELATED,
  WEBLINKING_RELNAME_REPLIES,
  WEBLINKING_RELNAME_SECTION,
  WEBLINKING_RELNAME_SELF,
  WEBLINKING_RELNAME_SERVICE,
  WEBLINKING_RELNAME_START,
  WEBLINKING_RELNAME_STYLESHEET,
  WEBLINKING_RELNAME_SUBSECTION,
  WEBLINKING_RELNAME_SUCCESSOR_VERSION,
  WEBLINKING_RELNAME_UP,
  WEBLINKING_RELNAME_VERSION_HISTORY,
  WEBLINKING_RELNAME_VIA,
  WEBLINKING_RELNAME_WORKING_COPY,
  WEBLINKING_RELNAME_WORKING_COPY_OF,

  WEBLINKING_RELNAME__END
} DSWeblinkingRelationName;

typedef struct ds_weblinking_relation_registry_t DSWeblinkingRelationRegistry;
struct ds_weblinking_relation_registry_t
{
  DSWeblinkingRelationName code;
  gchar *       rel;
  gchar *       description;
};

extern DSWeblinkingRelationRegistry DSWeblinkingRelationRegistryList[];

DupinLinkRecord *	dupin_link_record_create
					(DupinLinkB *		linkb,
					 JsonNode *		obj_node,
					 gchar *                context_id,
                                         gchar *                href,
                                         gchar *                rel,
                                         gchar *                tag,
					 GError **		error);

DupinLinkRecord *	dupin_link_record_create_with_id
					(DupinLinkB *		linkb,
					 JsonNode *		obj_node,
					 gchar *		id,
					 gchar *                context_id,
                                         gchar *                href,
                                         gchar *                rel,
                                         gchar *                tag,
					 GError **		error);

gboolean	dupin_link_record_exists
					(DupinLinkB *		linkb,
					 gchar *		id);

DupinLinkRecord *	dupin_link_record_read
					(DupinLinkB *		linkb,
					 gchar *		id,
					 GError **		error);

/* List of DupinLinkRecord: */
gboolean	dupin_link_record_get_list
					(DupinLinkB *		linkb,
					 guint			count,
					 guint			offset,
				         gsize			rowid_start,
					 gsize			rowid_end,
					 DupinCountType		count_type,
					 DupinOrderByType	orderby_type,
					 gboolean		descending,
					 GList **		list,
					 GError **		error);

void		dupin_link_record_get_list_close
					(GList *		list);

gboolean	dupin_link_record_update
					(DupinLinkRecord *		record,
					 JsonNode *		obj_node,
                                         gchar *                href,
                                         gchar *                rel,
                                         gchar *                tag,
					 GError **		error);

gboolean	dupin_link_record_delete
					(DupinLinkRecord *		record,
					 GError **		error);

void		dupin_link_record_close	(DupinLinkRecord *		record);

const gchar *	dupin_link_record_get_id
					(DupinLinkRecord *		record);

const gchar *	dupin_link_record_get_context_id
					(DupinLinkRecord *		record);

const gchar *	dupin_link_record_get_href
					(DupinLinkRecord *		record);

const gchar *	dupin_link_record_get_rel
					(DupinLinkRecord *		record);

const gchar *	dupin_link_record_get_tag
					(DupinLinkRecord *		record);

gboolean	dupin_link_record_is_weblink
					(DupinLinkRecord *		record);

gsize 	        dupin_link_record_get_rowid
					(DupinLinkRecord *		record);

gsize		dupin_link_record_get_created
					(DupinLinkRecord * record);

/* Public Revision API of DupinLinkRecord: */

gchar *		dupin_link_record_get_last_revision
					(DupinLinkRecord *		record);

JsonNode *
		dupin_link_record_get_revision_node
					(DupinLinkRecord *		record,
					 gchar *		mvcc);

gboolean	dupin_link_record_get_revisions_list
					(DupinLinkRecord *		record,
					 guint			count,
					 guint			offset,
				         gsize			rowid_start,
					 gsize			rowid_end,
					 DupinCountType		count_type,
					 DupinOrderByType	orderby_type,
					 gboolean		descending,
					 GList **		list,
					 GError **		error);

gboolean	dupin_link_record_get_total_revisions
					(DupinLinkRecord * record,
					 gsize * total,
					 GError ** error);

void		dupin_link_record_get_revisions_list_close
					(GList *		list);

gboolean	dupin_link_record_is_deleted
					(DupinLinkRecord *		record,
					 gchar *		mvcc);

gboolean	dupin_link_record_util_is_valid_context_id
					(gchar * id);

gboolean	dupin_link_record_util_is_valid_href 
					(gchar * href);

gboolean	dupin_link_record_util_is_valid_rel
					(gchar * rel);

G_END_DECLS

#endif

/* EOF */
