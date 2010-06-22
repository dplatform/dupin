#ifndef _DS_HTTPD_H_
#define _DS_HTTPD_H_

#include "dupin.h"

#define HTTP_MAX_LINE			2048	/* byte */

#define HTTP_CONTENT_LENGTH		"Content-length"
#define HTTP_CONTENT_LENGTH_LEN		14

#define HTTP_WWW_REDIRECT \
"<?xml version=\"1.1\"?>\n" \
"<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\"\n" \
"          \"http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd\">\n" \
"<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\">\n" \
"<body>\n" \
"  <script type=\"text/javascript\"><!--\n " \
"    setTimeout(function() {\n " \
"      location.href = '_www/index.html';\n" \
"    }, 20)\n" \
"    // --></script>\n" \
"  <p>Redirect...</p>\n" \
"</body>\n" \
"</html>\n"

#define HTTP_WWW_REDIRECT_LEN 379

#define HTTP_INDEX_HTML		"index.html"

#define HTTP_MIME_TEXTHTML	"text/html"
#define HTTP_MIME_JSON		"application/json"

typedef enum
{
  HTTP_STATUS_200 = 0,
  HTTP_STATUS_201,
  HTTP_STATUS_400,
  HTTP_STATUS_403,
  HTTP_STATUS_404,
  HTTP_STATUS_409,
  HTTP_STATUS_500,

  HTTP_STATUS_END
} DSHttpStatusCode;

typedef struct ds_http_status_t DSHttpStatus;
struct ds_http_status_t
{
  DSHttpStatusCode code;
  gchar *	header;

  gchar *	body;
  gsize		body_size;

  gchar *	mime;

  gboolean	error;
};

extern DSHttpStatus DSHttpStatusList[];

gboolean	httpd_init		(DSGlobal *	data,
					 GError **	error);

void		httpd_close		(DSGlobal *	data);

#endif

/* EOF */
