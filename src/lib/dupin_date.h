#ifndef _DUPIN_DATE_H_
#define _DUPIN_DATE_H_

#include <dupin.h>
#include <errno.h>
#include <stdlib.h>

#include <libsoup/soup-date.h>

G_BEGIN_DECLS

gsize		dupin_date_timestamp_now	(gint offset_seconds);

gchar *		dupin_date_timestamp_to_iso8601	(gsize timestamp);

gboolean	dupin_date_iso8601_to_timestamp (gchar * iso8601_date,
						 gsize * timestamp);

gchar *		dupin_date_timestamp_to_http_date
						(gsize timestamp);
gboolean	dupin_date_string_to_timestamp
						(gchar * date_string,
						 gsize * timestamp);

gint		dupin_date_timestamp_cmp	(gsize timestamp1,
						 gsize timestamp2);

G_END_DECLS

#endif

/* EOF */
