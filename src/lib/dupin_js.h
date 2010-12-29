#ifndef _DUPIN_JS_H_
#define _DUPIN_JS_H_

#include <dupin.h>

G_BEGIN_DECLS

DupinJs *	dupin_js_new_map	(Dupin *	d,
					 gchar *	js_json_doc,
                                         gchar *        js_code,
              				 gchar**        exception_string);

DupinJs *	dupin_js_new_reduce	(Dupin *	d,
					 gchar *	js_json_keys,
					 gchar *	js_json_values,
                                         gboolean       rereduce,
                                         gchar *        js_code,
              				 gchar**        exception_string);

void		dupin_js_destroy	(DupinJs *	js);

const JsonNode *
		dupin_js_get_reduceResult	(DupinJs *	js);

const JsonArray *
		dupin_js_get_mapResults
					(DupinJs *	js);

G_END_DECLS

#endif

/* EOF */
