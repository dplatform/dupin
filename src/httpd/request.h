#ifndef _DS_REQUEST_H_
#define _DS_REQUEST_H_

#include "dupin.h"
#include "httpd.h"

#include <glib.h>

G_BEGIN_DECLS

typedef struct request_type_t RequestType;
struct request_type_t
{
  gchar *		request;
  HttpdRequest		request_type;
  DSHttpStatusCode 	(*func)	(DSHttpdClient *	client,
				 GList *		path,
				 GList *		arguments);
};

extern RequestType request_types[];
extern DSHttpStatus DSHttpStatusList[];

DSHttpStatusCode
		request_global	(DSHttpdClient *	client,
				 GList *		path,
				 GList *		arguments);

gboolean
		request_get_changes_comet_database
				(DSHttpdClient * client,
                           	 gchar *buf,
                           	 gsize count,
                           	 gsize offset, 
                           	 gsize *bytes_read, 
                           	 GError **       error);

gboolean
		request_get_changes_comet_linkbase
				(DSHttpdClient * client,
                           	 gchar *buf,
                           	 gsize count,
                           	 gsize offset, 
                           	 gsize *bytes_read, 
                           	 GError **       error);

G_END_DECLS

#endif
/* EOF */
