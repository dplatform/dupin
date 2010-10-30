#ifndef _DUPIN_JS_H_
#define _DUPIN_JS_H_

#include <dupin.h>

G_BEGIN_DECLS

DupinJs *	dupin_js_new		(gchar *	js_json,
                                         gchar *        js_code,
                                         gchar *        what,
              				 gchar**        exception_string);

void		dupin_js_destroy	(DupinJs *	js);

const JsonNode *
		dupin_js_get_emit	(DupinJs *	js);

const JsonArray *
		dupin_js_get_emitIntermediate
					(DupinJs *	js);

G_END_DECLS

#endif

/* EOF */
