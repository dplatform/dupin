#ifndef _DUPIN_VIEW_H_
#define _DUPIN_VIEW_H_

#include <dupin.h>
#include <dupin_mr.h>
#include <dupin_db.h>
#include <dupin_linkb.h>

G_BEGIN_DECLS

struct dupin_view_p_update_t
{
  gchar *parent;
  gchar *map;
  DupinMRLang map_lang;
  gchar *reduce;
  DupinMRLang reduce_lang;
  gboolean isdb;
  gboolean islinkb;
  gchar *output;
  gboolean output_isdb;
  gboolean output_islinkb;
};

struct dupin_view_sync_t
{
  JsonNode *obj;
  gchar *id;
  JsonNode *pid; /* array or null */
  JsonNode *key; /* array or null */
};

struct dupin_view_sync_total_rereduce_t
{
  gsize total;
  gchar * first_matching_key;
};

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
					 gboolean	is_linkb,
					 gchar *	map,
					 DupinMRLang	map_language,
					 gchar *	reduce,
					 DupinMRLang	reduce_language,
					 gchar *	output,
					 gboolean	output_is_db,
					 gboolean	output_is_linkb,
					 GError **	error);

gint            dupin_view_begin_transaction
                                        (DupinView *    view,
					 GError **	error);

gint            dupin_view_rollback_transaction
                                        (DupinView *    view,
					 GError **	error);

gint            dupin_view_commit_transaction
                                        (DupinView *    view,
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

gboolean	dupin_view_get_parent_is_linkb
					(DupinView *	view);

const gchar *	dupin_view_get_output	(DupinView *	view);

gboolean	dupin_view_get_output_is_db
					(DupinView *	view);

gboolean	dupin_view_get_output_is_linkb
					(DupinView *	view);

const gchar *	dupin_view_get_map	(DupinView *	view);

DupinMRLang	dupin_view_get_map_language
					(DupinView *	view);

const gchar *	dupin_view_get_reduce	(DupinView *	view);

DupinMRLang	dupin_view_get_reduce_language
					(DupinView *	view);

gsize		dupin_view_get_size	(DupinView *	view);

gboolean	dupin_view_get_creation_time
					(DupinView * view,
					 gsize * creation_time);

gsize		dupin_view_count	(DupinView *	view);

gboolean	dupin_view_is_sync	(DupinView *	view);

gboolean	dupin_view_is_syncing	(DupinView *	view);

void		dupin_view_sync_map_func (gpointer data, gpointer user_data);

void		dupin_view_sync_reduce_func (gpointer data, gpointer user_data);

int		dupin_view_collation	(void        * ref,
					 int         left_len,
					 const void  *left_void,
					 int         right_len,
					 const void  *right_void);

void            dupin_view_compact_func
                                        (gpointer data,
                                        gpointer user_data);

void            dupin_view_compact      (DupinView * view);

gboolean        dupin_view_is_compacting
                                        (DupinView * view);

gboolean        dupin_view_is_compacted
                                        (DupinView * view);

void            dupin_view_set_error
                                        (DupinView * view,
                                         gchar * msg);

void            dupin_view_clear_error
                                        (DupinView * view);

gchar *         dupin_view_get_error
                                        (DupinView * view);

void            dupin_view_set_warning
                                        (DupinView * view,
                                         gchar * msg);

void            dupin_view_clear_warning
                                        (DupinView * view);

gchar *         dupin_view_get_warning
                                        (DupinView * view);

G_END_DECLS

#endif

/* EOF */
