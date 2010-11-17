#ifndef _DUPIN_MR_H_
#define _DUPIN_MR_H_

#include <dupin.h>

G_BEGIN_DECLS

JsonArray *	dupin_mr_record_map	(DupinView *	view,
		 	 	 		 JsonObject * obj);

JsonNode *	dupin_mr_record_reduce	(DupinView *	view,
		 	 	 		 JsonArray * keys,
		 	 	 		 JsonArray * values,
						 gboolean rereduce);

G_END_DECLS

#endif

/* EOF */
