#ifndef _DUPIN_RECORD_H_
#define _DUPIN_RECORD_H_

#include <dupin.h>
#include <tb_json.h>

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

G_BEGIN_DECLS

DupinRecord *	dupin_record_create	(DupinDB *		db,
					 JsonObject *	obj,
					 GError **		error);

DupinRecord *	dupin_record_create_with_id
					(DupinDB *		db,
					 JsonObject *	obj,
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
					 gboolean		descending,
					 GList **		list,
					 GError **		error);

void		dupin_record_get_list_close
					(GList *		list);

gboolean	dupin_record_update	(DupinRecord *		record,
					 JsonObject *	obj,
					 GError **		error);

gboolean	dupin_record_delete	(DupinRecord *		record,
					 GError **		error);

void		dupin_record_close	(DupinRecord *		record);

const gchar *	dupin_record_get_id	(DupinRecord *		record);

guint		dupin_record_get_last_revision
					(DupinRecord *		record);

JsonObject *
		dupin_record_get_revision
					(DupinRecord *		record,
					 gint			revision);

#define dupin_record_get( record ) \
		dupin_record_get_revision (record, -1)

gboolean	dupin_record_is_deleted	(DupinRecord *		record,
					 gint			revision);

G_END_DECLS

#endif

/* EOF */
