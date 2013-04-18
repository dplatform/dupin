#ifndef _DUPIN_DB_H_
#define _DUPIN_DB_H_

#include <dupin.h>
#include <dupin_view.h>

G_BEGIN_DECLS

struct dupin_database_get_changes_list_t
{
  DupinChangesType style;
  GList *list;
};

gchar **	dupin_get_databases	(Dupin *	d);

gboolean	dupin_database_exists	(Dupin *	d,
					 gchar *	db_name);

DupinDB *	dupin_database_open	(Dupin *	d,
					 gchar *	db,
					 GError **	error);

DupinDB *	dupin_database_new	(Dupin *	d,
					 gchar *	db,
					 GError **	error);

gint		dupin_database_begin_transaction
					(DupinDB * 	db,
					 GError ** 	error);

gint		dupin_database_rollback_transaction
					(DupinDB * 	db,
					 GError ** 	error);

gint		dupin_database_commit_transaction
					(DupinDB * 	db,
					 GError ** 	error);

void		dupin_database_ref	(DupinDB *	db);

void		dupin_database_unref	(DupinDB *	db);

gboolean	dupin_database_delete	(DupinDB *	db,
					 GError **	error);

const gchar *	dupin_database_get_name	(DupinDB *	db);

gsize		dupin_database_get_size	(DupinDB *	db);

gboolean	dupin_database_get_creation_time
					(DupinDB * db,
					 gsize * creation_time);

gchar *		dupin_database_generate_id
					(DupinDB *	db,
					 GError **	error);

gchar **	dupin_database_get_views
					(DupinDB *	db);

DupinView *	dupin_database_get_view	(DupinDB *	db,
					 gchar *	view);

gsize		dupin_database_count	(DupinDB *	db,
					 DupinCountType	type);

gchar *		dupin_database_get_default_attachment_db_name
                                        (DupinDB * db);
DupinAttachmentDB *
		dupin_database_get_default_attachment_db
					(DupinDB *	db);

gchar *		dupin_database_get_default_linkbase_name
					(DupinDB * db);

DupinLinkB *	dupin_database_get_default_linkbase
					(DupinDB *	db);

gboolean	dupin_database_get_max_rowid	(DupinDB *	db,
					         gsize * max_rowid);

gboolean        dupin_database_get_total_changes
                                        (DupinDB *              db,
                                         gsize *                total,
                                         gsize                  since,
                                         gsize                  to,
					 DupinCountType         count_type,
                                         gboolean               inclusive_end,
					 gchar **               types,
					 DupinFilterByType      types_op,
                                         GError **              error);

gboolean        dupin_database_get_changes_list
					(DupinDB *              db,
                                         guint                  count,
                                         guint                  offset,
                                         gsize                  since,
                                         gsize                  to,
					 DupinChangesType       changes_type,
					 DupinCountType         count_type,
                                         DupinOrderByType       orderby_type,
                                         gboolean               descending,
					 gchar **               types,
					 DupinFilterByType      types_op,
                                         GList **               list,
                                         GError **              error);

void            dupin_database_get_changes_list_close
                                        (GList *                list);

gboolean	dupin_database_thread_compact
					(DupinDB * db,
					 gsize count);

void		dupin_database_compact_func
					(gpointer data,
					gpointer user_data);

void		dupin_database_compact 	(DupinDB * db);

gboolean	dupin_database_is_compacting
					(DupinDB * db);

gboolean	dupin_database_is_compacted
					(DupinDB * db);

void		dupin_database_set_error
					(DupinDB * db,
					 gchar * msg);

void		dupin_database_clear_error
					(DupinDB * db);

gchar *		dupin_database_get_error
					(DupinDB * db);

void		dupin_database_set_warning
					(DupinDB * db,
					 gchar * msg);

void		dupin_database_clear_warning
					(DupinDB * db);

gchar *		dupin_database_get_warning
					(DupinDB * db);

G_END_DECLS

#endif

/* EOF */
