#ifndef _DS_REQUEST_H_
#define _DS_REQUEST_H_

#include "dupin.h"
#include "httpd.h"

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

#endif
/* EOF */
