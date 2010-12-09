#ifndef _DUPIN_RECORD_H_
#define _DUPIN_RECORD_H_

#include <dupin.h>
#include <tb_json.h>

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

G_BEGIN_DECLS

DupinRecord *	dupin_record_create	(DupinDB *		db,
					 JsonNode *		obj_node,
					 GError **		error);

DupinRecord *	dupin_record_create_with_id
					(DupinDB *		db,
					 JsonNode *		obj_node,
					 gchar *		id,
					 GError **		error);

gboolean	dupin_record_exists	(DupinDB *		db,
					 gchar *		id);

DupinRecord *	dupin_record_read	(DupinDB *		db,
					 gchar *		id,
					 GError **		error);

/* List of DupinRecord: */
gboolean	dupin_record_get_list	(DupinDB *		db,
					 guint			count,
					 guint			offset,
				         gsize			rowid_start,
					 gsize			rowid_end,
					 DupinCountType		count_type,
					 DupinOrderByType	orderby_type,
					 gboolean		descending,
					 GList **		list,
					 GError **		error);

void		dupin_record_get_list_close
					(GList *		list);

gboolean	dupin_record_update	(DupinRecord *		record,
					 JsonNode *		obj_node,
					 GError **		error);

gboolean	dupin_record_delete	(DupinRecord *		record,
					 GError **		error);

void		dupin_record_close	(DupinRecord *		record);

const gchar *	dupin_record_get_id	(DupinRecord *		record);

gsize 	        dupin_record_get_rowid	(DupinRecord *		record);

/* Public Revision API of DupinRecord: */

gchar *		dupin_record_get_last_revision
					(DupinRecord *		record);

JsonNode *
		dupin_record_get_revision_node
					(DupinRecord *		record,
					 gchar *		mvcc);

gboolean	dupin_record_get_revisions_list
					(DupinRecord *		record,
					 guint			count,
					 guint			offset,
				         gsize			rowid_start,
					 gsize			rowid_end,
					 DupinCountType		count_type,
					 DupinOrderByType	orderby_type,
					 gboolean		descending,
					 GList **		list,
					 GError **		error);

void		dupin_record_get_revisions_list_close
					(GList *		list);

gboolean	dupin_record_is_deleted	(DupinRecord *		record,
					 gchar *		mvcc);

G_END_DECLS

#endif

/* EOF */
