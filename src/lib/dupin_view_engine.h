#ifndef _DUPIN_VIEW_ENGINE_H_
#define _DUPIN_VIEW_ENGINE_H_

#include <dupin.h>

G_BEGIN_DECLS

DupinViewEngine *
		dupin_view_engine_new   (Dupin * d,
					 DupinViewEngineLang language,
					 gchar * map_code,
					 gchar * reduce_code,
                                         GError ** error);

void            dupin_view_engine_free  (DupinViewEngine * engine);

DupinViewEngineLang
		dupin_view_engine_get_language
					(DupinViewEngine * engine);

gboolean        dupin_view_engine_set_map_code
					(DupinViewEngine * engine,
					 gchar * map_code);

gchar *   dupin_view_engine_get_map_code
					(DupinViewEngine * engine);

gboolean        dupin_view_engine_set_reduce_code
					(DupinViewEngine * engine,
					 gchar * reduce_code);

gchar *   dupin_view_engine_get_reduce_code
					(DupinViewEngine * engine);

JsonNode *	dupin_view_engine_record_map
					(DupinViewEngine * engine,
		 	 	 	 JsonNode * obj);

JsonNode *	dupin_view_engine_record_reduce
					(DupinViewEngine * engine,
		 	 	  	 JsonNode * keys,
		 	 	 	 JsonNode * values,
					 gboolean rereduce);

G_END_DECLS

#endif

/* EOF */
