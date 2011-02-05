#ifndef _DUPIN_LINKB_H_
#define _DUPIN_LINKB_H_

#include <dupin.h>
#include <dupin_view.h>

G_BEGIN_DECLS

gchar **	dupin_get_linkbases	(Dupin *	d);

gboolean	dupin_linkbase_exists	(Dupin *	d,
					 gchar *	linkb);

DupinLinkB *	dupin_linkbase_open	(Dupin *	d,
					 gchar *	linkb,
					 GError **	error);

DupinLinkB *	dupin_linkbase_new	(Dupin *	d,
					 gchar *	linkb,
					 gchar *        parent,
                                         gboolean       is_db,
					 GError **	error);

void		dupin_linkbase_ref	(DupinLinkB *	linkb);

void		dupin_linkbase_unref	(DupinLinkB *	linkb);

gboolean	dupin_linkbase_delete	(DupinLinkB *	linkb,
					 GError **	error);

const gchar *	dupin_linkbase_get_name	(DupinLinkB *	linkb);

gsize		dupin_linkbase_get_size	(DupinLinkB *	linkb);

gchar *		dupin_linkbase_generate_id
					(DupinLinkB *	linkb,
					 GError **	error);

gchar **	dupin_linkbase_get_views
					(DupinLinkB *	linkb);

DupinView *	dupin_linkbase_get_view	(DupinLinkB *	linkb,
					 gchar *	view);

gsize		dupin_linkbase_count	(DupinLinkB * linkb,
					 DupinLinksType links_type,
					 DupinCountType count_type,
					 gchar * context_id,
					 gchar ** labels,
					 gchar * tag);

gboolean	dupin_linkbase_get_max_rowid	(DupinLinkB *	linkb,
					         gsize * max_rowid);

gchar *		dupin_linkbase_get_parent (DupinLinkB * linkb);

gboolean        dupin_linkbase_get_total_changes
                                        (DupinLinkB *           linkb,
                                         gsize *                total,
                                         gsize                  since,
                                         gsize                  to,
					 DupinChangesType       changes_type,
					 DupinCountType         count_type,
                                         gboolean               inclusive_end,
 					 gchar *                context_id,
 					 gchar *                tag,
                                         GError **              error);

gboolean        dupin_linkbase_get_changes_list
					(DupinLinkB *              linkb,
                                         guint                  count,
                                         guint                  offset,
                                         gsize                  since,
                                         gsize                  to,
					 DupinChangesType       changes_type,
					 DupinCountType         count_type,
                                         DupinOrderByType       orderby_type,
                                         gboolean               descending,
 					 gchar *                context_id,
 					 gchar *                tag,
                                         GList **               list,
                                         GError **              error);

void            dupin_linkbase_get_changes_list_close
                                        (GList *                list);

gboolean	dupin_linkbase_thread_compact
					(DupinLinkB * linkb,
					 gsize count);

/* Compaction */

void		dupin_linkbase_compact_func
					(gpointer data,
					gpointer user_data);

void		dupin_linkbase_compact 	(DupinLinkB * linkb);

gboolean	dupin_linkbase_is_compacting
					(DupinLinkB * linkb);

gboolean	dupin_linkbase_is_compacted
					(DupinLinkB * linkb);

/* Links checking */

void		dupin_linkbase_check_func
					(gpointer data,
					gpointer user_data);

void		dupin_linkbase_check 	(DupinLinkB * linkb);

gboolean	dupin_linkbase_is_checking
					(DupinLinkB * linkb);

gboolean	dupin_linkbase_is_checked
					(DupinLinkB * linkb);


gboolean        dupin_linkbase_get_parent_is_db
                                        (DupinLinkB * linkb);

void            dupin_linkbase_set_error
                                        (DupinLinkB * linkb,
                                         gchar * msg);

void            dupin_linkbase_clear_error
                                        (DupinLinkB * linkb);

gchar *         dupin_linkbase_get_error
                                        (DupinLinkB * linkb);

void            dupin_linkbase_set_warning
                                        (DupinLinkB * linkb,
                                         gchar * msg);

void            dupin_linkbase_clear_warning
                                        (DupinLinkB * linkb);

gchar *         dupin_linkbase_get_warning
                                        (DupinLinkB * linkb);

G_END_DECLS

#endif

/* EOF */
