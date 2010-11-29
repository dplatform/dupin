#ifndef _DUPIN_DB_H_
#define _DUPIN_DB_H_

#include <dupin.h>
#include <dupin_view.h>

G_BEGIN_DECLS

gchar **	dupin_get_databases	(Dupin *	d);

gboolean	dupin_database_exists	(Dupin *	d,
					 gchar *	db);

DupinDB *	dupin_database_open	(Dupin *	d,
					 gchar *	db,
					 GError **	error);

DupinDB *	dupin_database_new	(Dupin *	d,
					 gchar *	db,
					 GError **	error);

void		dupin_database_ref	(DupinDB *	db);

void		dupin_database_unref	(DupinDB *	db);

gboolean	dupin_database_delete	(DupinDB *	db,
					 GError **	error);

const gchar *	dupin_database_get_name	(DupinDB *	db);

gsize		dupin_database_get_size	(DupinDB *	db);

gchar *		dupin_database_generate_id
					(DupinDB *	db,
					 GError **	error);

gchar **	dupin_database_get_views
					(DupinDB *	db);

DupinView *	dupin_database_get_view	(DupinDB *	db,
					 gchar *	view);

gsize		dupin_database_count	(DupinDB *	db,
					 DupinCountType	type);

gboolean	dupin_database_get_max_rowid	(DupinDB *	db,
					         gsize * max_rowid);

G_END_DECLS

#endif

/* EOF */
