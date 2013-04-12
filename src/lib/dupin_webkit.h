#ifndef _DUPIN_WEBKIT_H_
#define _DUPIN_WEBKIT_H_

#include <dupin.h>

G_BEGIN_DECLS

DupinWebKit *	dupin_webkit_new 	(Dupin * d,
					 GError ** error);

void		dupin_webkit_free	(DupinWebKit *	js);

JsonNode *	dupin_webkit_map	(DupinWebKit *	js,
					 gchar *	js_json_doc,
                                         gchar *        js_code,
              				 gchar **       exception_string);

JsonNode *	dupin_webkit_reduce	(DupinWebKit *	js,
					 gchar *	js_json_keys,
					 gchar *	js_json_values,
                                         gboolean       rereduce,
                                         gchar *        js_code,
              				 gchar **       exception_string);

G_END_DECLS

#endif

/* EOF */
