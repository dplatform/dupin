#ifndef _DUPIN_UTILS_H_
#define _DUPIN_UTILS_H_

#include <dupin.h>

G_BEGIN_DECLS

gboolean	dupin_util_is_valid_db_name	(gchar *	db);

gboolean	dupin_util_is_valid_view_name	(gchar *	db);

gboolean	dupin_util_is_valid_record_id	(gchar *	id);

gboolean	dupin_util_is_valid_obj		(tb_json_object_t *obj);

void		dupin_util_generate_id		(gchar		id[255]);

gboolean	dupin_util_is_valid_mr_lang	(gchar *	lang);

DupinMRLang	dupin_util_mr_lang_to_enum	(gchar *	lang);

const gchar *	dupin_util_mr_lang_to_string	(DupinMRLang	lang);

G_END_DECLS

#endif

/* EOF */
