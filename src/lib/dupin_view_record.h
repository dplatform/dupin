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

/* List of DupinViewRecord: */
gboolean	dupin_view_record_get_list
					(DupinView *		view,
					 guint			count,
					 guint			offset,
					 gboolean		descending,
					 GList **		list,
					 GError **		error);

void		dupin_view_record_get_list_close
					(GList *		list);

void		dupin_view_record_close	(DupinViewRecord *	record);

const gchar *	dupin_view_record_get_id
					(DupinViewRecord *	record);

const gchar *	dupin_view_record_get_pid
					(DupinViewRecord *	record);

JsonObject *
		dupin_view_record_get	(DupinViewRecord *	record);

G_END_DECLS

#endif

/* EOF */
