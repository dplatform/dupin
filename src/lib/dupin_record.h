#ifndef _DUPIN_RECORD_H_
#define _DUPIN_RECORD_H_

#include <dupin.h>

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

/* TODO - avoid using DSHttpStatusCode to report insert and request records */
#include "../httpd/httpd.h"

G_BEGIN_DECLS

struct dupin_record_select_total_t
{
  gsize total_doc_ins;
  gsize total_doc_del;
};

int		dupin_record_select_total_cb 
					(void *data,
					 int argc,
					 char **argv,
					 char **col);

#define DUPIN_DB_SQL_EXISTS \
        "SELECT count(id) FROM Dupin WHERE id = '%q' "

#define DUPIN_DB_SQL_INSERT \
        "INSERT INTO Dupin (id, rev, hash, type, obj, tm, expire_tm) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', '%q', %Q, '%q', '%" G_GSIZE_FORMAT "', '%" G_GSIZE_FORMAT "')"

#define DUPIN_DB_SQL_READ \
        "SELECT rev, hash, type, obj, deleted, tm, expire_tm, ROWID AS rowid FROM Dupin WHERE id='%q'"

#define DUPIN_DB_SQL_DELETE \
        "INSERT OR REPLACE INTO Dupin (id, rev, deleted, hash, type, obj, tm, expire_tm) " \
        "VALUES('%q', '%" G_GSIZE_FORMAT "', 'TRUE', '%q', %Q, '%q', '%" G_GSIZE_FORMAT "', '%" G_GSIZE_FORMAT "')"

#define DUPIN_DB_SQL_UPDATE_REV_HEAD \
        "UPDATE Dupin SET rev_head = 'FALSE' WHERE id = '%q' "

#define DUPIN_DB_SQL_GET_TOTALS \
        "SELECT total_doc_ins, total_doc_del FROM DupinDB"

#define DUPIN_DB_SQL_SET_TOTALS \
        "UPDATE DupinDB SET total_doc_ins = %d, total_doc_del = %d"


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

gsize           dupin_record_get_list_total
                                       	(DupinDB *              db,
				         gsize			rowid_start,
					 gsize			rowid_end,
				         GList *           	keys,
					 gchar *                start_key,
                                         gchar *                end_key,
                                         gboolean               inclusive_end,
                                         DupinCountType         count_type,
					 gchar **      		types,
					 DupinFilterByType	types_op,
					 gchar *           	filter_by,
					 DupinFieldsFormatType  filter_by_format,
                             		 DupinFilterByType	filter_op,
                             		 gchar *           	filter_values,
                                         GError **              error);

gboolean	dupin_record_get_list	(DupinDB *		db,
					 guint			count,
					 guint			offset,
				         gsize			rowid_start,
					 gsize			rowid_end,
				         GList *           	keys,
					 gchar *                start_key,
                                         gchar *                end_key,
                                         gboolean               inclusive_end,
					 DupinCountType		count_type,
					 DupinOrderByType	orderby_type,
					 gboolean		descending,
					 gchar **      		types,
					 DupinFilterByType	types_op,
					 gchar *           	filter_by,
					 DupinFieldsFormatType  filter_by_format,
                                         DupinFilterByType      filter_op,
                             		 gchar *           	filter_values,
					 GList **		list,
					 GError **		error);

void		dupin_record_get_list_close
					(GList *		list);

gboolean	dupin_record_update	(DupinRecord *		record,
					 JsonNode *		obj_node,
					 gboolean 		ignore_updates_if_unmodified,
					 GError **		error);

gboolean	dupin_record_patch	(DupinRecord *		record,
					 JsonNode *		obj_node,
					 gboolean 		ignore_updates_if_unmodified,
					 GError **		error);

gboolean	dupin_record_delete	(DupinRecord *		record,
					 JsonNode * 		preserved_status_obj_node,
					 GError **		error);

void		dupin_record_close	(DupinRecord *		record);

const gchar *	dupin_record_get_id	(DupinRecord *		record);

gchar *		dupin_record_get_type	(DupinRecord *		record);

gsize 	        dupin_record_get_rowid	(DupinRecord *		record);

gsize		dupin_record_get_created
					(DupinRecord * record);

gsize		dupin_record_get_expire
					(DupinRecord * record);

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

gboolean	dupin_record_get_total_revisions
					(DupinRecord * record,
					 gsize * total,
					 GError ** error);

void		dupin_record_get_revisions_list_close
					(GList *		list);

gboolean	dupin_record_is_deleted	(DupinRecord *		record,
					 gchar *		mvcc);

gboolean	dupin_record_is_expired	(DupinRecord *		record,
					 gchar *		mvcc);

/* insert = create or update */

gboolean	dupin_record_insert	(DupinDB * db,
                                         JsonNode * obj_node,
                                         gchar * id, gchar * caller_mvcc,
                                         GList ** response_list,
					 gboolean use_latest_revision,
					 gboolean ignore_updates_if_unmodified,
					 GError ** error);

gboolean	dupin_record_insert_bulk
					(DupinDB * db,
					 JsonNode * bulk_node,
                                         GList ** response_list,
					 gboolean use_latest_revision,
					 gboolean ignore_updates_if_unmodified,
					 GError ** error);

G_END_DECLS

#endif

/* EOF */
