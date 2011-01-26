#ifndef _DS_REQUEST_H_
#define _DS_REQUEST_H_

#include "dupin.h"
#include "httpd.h"

#include <glib.h>

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

G_BEGIN_DECLS

typedef struct dupin_keyvalue_t    dupin_keyvalue_t;

struct dupin_keyvalue_t
{
  gchar *       key;
  gchar *       value;
};

dupin_keyvalue_t * dupin_keyvalue_new         (gchar *        key,
                                         gchar *        value) G_GNUC_WARN_UNUSED_RESULT;

void            dupin_keyvalue_destroy     (dupin_keyvalue_t * data);

G_END_DECLS

#endif
/* EOF */
