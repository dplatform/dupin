#ifndef _DS_HTTPD_H_
#define _DS_HTTPD_H_

#include "dupin.h"

#include "configure.h"

#define HTTP_MAX_LINE			2048	/* byte */

#define HTTP_CONTENT_LENGTH		"Content-Length"
#define HTTP_CONTENT_LENGTH_LEN		14

#define HTTP_CONTENT_TYPE		"Content-Type"
#define HTTP_CONTENT_TYPE_LEN		12

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

#define HTTP_MIME_TEXTPLAIN	"text/plain; charset=utf-8"
#define HTTP_MIME_TEXTHTML	"text/html; charset=utf-8"
#define HTTP_MIME_JSON		"application/json; charset=utf-8"
/* #define HTTP_MIME_PORTABLE_LISTINGS_JSON		"application/listings+json; charset=utf-8; profile=\"http://portablelistings.net/profiles/core/1.0/\"" */
#define HTTP_MIME_PORTABLE_LISTINGS_JSON		HTTP_MIME_JSON

typedef enum
{
  HTTP_STATUS_200 = 0,
  HTTP_STATUS_201,
  HTTP_STATUS_400,
  HTTP_STATUS_403,
  HTTP_STATUS_404,
  HTTP_STATUS_409,
  HTTP_STATUS_412,
  HTTP_STATUS_500,
  HTTP_STATUS_501,
  HTTP_STATUS_503,

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


typedef struct dp_keyvalue_t    dp_keyvalue_t;

struct dp_keyvalue_t
{
  gchar *       key;
  gchar *       value;
};

dp_keyvalue_t * dp_keyvalue_new         (gchar *        key,
                                         gchar *        value) G_GNUC_WARN_UNUSED_RESULT;

void            dp_keyvalue_destroy     (dp_keyvalue_t * data);

#endif

/* EOF */
