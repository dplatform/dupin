#ifndef _DUPIN_VIEW_H_
#define _DUPIN_VIEW_H_

#include <dupin.h>
#include <dupin_mr.h>
#include <dupin_db.h>

G_BEGIN_DECLS

gchar **	dupin_get_views		(Dupin *	d);

gboolean	dupin_view_exists	(Dupin *	d,
					 gchar *	view);

DupinView *	dupin_view_open		(Dupin *	d,
					 gchar *	view,
					 GError **	error);

DupinView *	dupin_view_new		(Dupin *	d,
					 gchar *	view,
					 gchar *	parent,
					 gboolean	is_db,
					 gchar *	map,
					 DupinMRLang	map_language,
					 gchar *	reduce,
					 DupinMRLang	reduce_language,
					 GError **	error);

void		dupin_view_ref		(DupinView *	view);

void		dupin_view_unref	(DupinView *	view);

gboolean	dupin_view_delete	(DupinView *	view,
					 GError **	error);

gboolean        dupin_view_force_quit   (DupinView *    view,
					 GError **	error);

const gchar *	dupin_view_get_name	(DupinView *	view);

const gchar *	dupin_view_get_parent	(DupinView *	view);

gboolean	dupin_view_get_parent_is_db
					(DupinView *	view);

const gchar *	dupin_view_get_map	(DupinView *	view);

DupinMRLang	dupin_view_get_map_language
					(DupinView *	view);

const gchar *	dupin_view_get_reduce	(DupinView *	view);

DupinMRLang	dupin_view_get_reduce_language
					(DupinView *	view);

gsize		dupin_view_get_size	(DupinView *	view);

gsize		dupin_view_count	(DupinView *	view);

gboolean	dupin_view_is_sync	(DupinView *	view);

gboolean	dupin_view_is_syncing	(DupinView *	view);

G_END_DECLS

#endif

/* EOF */
