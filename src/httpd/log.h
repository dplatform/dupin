#ifndef _DS_LOG_H_
#define _DS_LOG_H_

#include "dupin.h"

#include "configure.h"

typedef enum {
  LOG_STARTUP,
  LOG_QUIT,

  LOG_HTTPD_CLIENT_CONNECT,
  LOG_HTTPD_CLIENT_DISCONNECT,
  LOG_HTTPD_CLIENT_ERROR
} LogType;

typedef enum {
  LOG_TYPE_STRING,
  LOG_TYPE_INTEGER,
  LOG_TYPE_DOUBLE,
  LOG_TYPE_BOOLEAN,
  LOG_TYPE_NULL
} LogTypeValue;

gboolean	log_open	(DSGlobal *	data,
				 GError **	error);

void		log_close	(DSGlobal *	data);

/* Here, a list of argument for the JSON log system. The arguments could be:
 * NULL - the last one, always!!!
 * "string", LogTypeValue, value - triple:
 *   - string is the new json node,
 *   - a LogVerbose level for this node
 *   - the LogTypeValue identifies the value
 *   - value dependes from the LogTypeValue
 */
void		log_write	(DSGlobal *	data,
				 LogVerbose	verbose,
				 LogType	type,
				 ...);

#endif

/* EOF */
