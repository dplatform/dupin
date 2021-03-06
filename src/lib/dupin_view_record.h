#ifndef _DUPIN_VIEW_RECORD_H_
#define _DUPIN_VIEW_RECORD_H_

#include <dupin.h>

G_BEGIN_DECLS

gboolean	dupin_view_record_exists
					(DupinView *		view,
					 gchar *		id);

DupinViewRecord *
		dupin_view_record_read	(DupinView *		view,
					 gchar *		id,
					 GError **		error);

/* get total of records into view */
gboolean        dupin_view_record_get_list_total
    					(DupinView * view,
					 gsize * total,
					 gsize  		rowid_start,
					 gsize  		rowid_end,
					 GList *                keys,
					 gchar *		start_key,
					 gchar *		end_key,
					 gboolean		inclusive_end,
					 gchar *		start_value,
					 gchar *		end_value,
					 gboolean		inclusive_end_value,
					 gchar *                filter_by,
                                         DupinFieldsFormatType  filter_by_format,
                                         DupinFilterByType      filter_op,
                                         gchar *                filter_values,
					 GError **		error);

/* get max rowid for view DB */
gboolean        dupin_view_record_get_max_rowid (DupinView * view,
						 gsize * max_rowid, gboolean lock);

/* List of DupinViewRecord: */
gboolean	dupin_view_record_get_list
					(DupinView *		view,
					 guint			count,
					 guint			offset,
					 gsize  		rowid_start,
					 gsize  		rowid_end,
					 DupinOrderByType	orderby_type,
					 gboolean		descending,
					 GList *                keys,
					 gchar *		start_key,
					 gchar *		end_key,
					 gboolean		inclusive_end,
					 gchar *		start_value,
					 gchar *		end_value,
					 gboolean		inclusive_end_value,
					 gchar *                filter_by,
                                         DupinFieldsFormatType  filter_by_format,
                                         DupinFilterByType      filter_op,
                                         gchar *                filter_values,
					 GList **		list,
					 GError **		error);

void		dupin_view_record_get_list_close
					(GList *		list);

void		dupin_view_record_close	(DupinViewRecord *	record);

const gchar *	dupin_view_record_get_id
					(DupinViewRecord *	record);

gsize 	        dupin_view_record_get_rowid
					(DupinViewRecord *	record);

gsize           dupin_view_record_get_modified
					(DupinViewRecord *      record);

JsonNode *
		dupin_view_record_get_pid
					(DupinViewRecord *	record);

gchar *		dupin_view_record_get_etag
					(DupinViewRecord *	record);

JsonNode *
		dupin_view_record_get_key (DupinViewRecord *	record);

JsonNode *
		dupin_view_record_get	(DupinViewRecord *	record);

G_END_DECLS

#endif

/* EOF */
