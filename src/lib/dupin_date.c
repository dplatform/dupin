#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_date.h"

#include <string.h>

gsize
dupin_date_timestamp_now (gint offset_seconds)
{
  gsize ttime=0;
  GTimeVal tnow;

  g_get_current_time(&tnow); 
  ttime= (tnow.tv_sec +offset_seconds) * 1000000 + tnow.tv_usec;

  return ttime;
}

gchar *
dupin_date_timestamp_to_iso8601 (gsize timestamp)
{
  GTimeVal tv;

  tv.tv_sec = timestamp / 1000000;
  tv.tv_usec = timestamp % 1000000;

//g_message ("dupin_util_timestamp_to_iso8601: timestamp=%" G_GSIZE_FORMAT "\n", (long unsigned int)timestamp);
  
  return g_time_val_to_iso8601 (&tv);
}

/* NOTE - we assume dates in UTC absolute time like "2011-06-12T14:10:49.090864Z" */

gboolean
dupin_date_iso8601_to_timestamp (gchar * iso8601_date, gsize * timestamp)
{
  g_return_val_if_fail (iso8601_date != NULL, FALSE);
  g_return_val_if_fail (timestamp != NULL, FALSE);

  GTimeVal date;

  if (g_time_val_from_iso8601 (iso8601_date, &date) == FALSE)
    {
      return FALSE;
    }

  *timestamp = (date.tv_sec*1000000) + date.tv_usec;

//g_message ("dupin_util_iso8601_to_timestamp: timestamp=%" G_GSIZE_FORMAT "\n", (long unsigned int)timestamp);
  
  return TRUE;
}

/* Using libsoup - First, why glib date/time functions do not include this?
   Second, at the moment it is undecided wheteher libsoup will be a dependency in future releases,
   eventually we will need to port the soup-date specific parts */

gchar *
dupin_date_timestamp_to_http_date (gsize timestamp)
{
  time_t t;
  gchar * http_date = NULL;

  t = (time_t) timestamp / 1000000;

  SoupDate * d = soup_date_new_from_time_t (t);

  http_date = soup_date_to_string (d, SOUP_DATE_HTTP);

  soup_date_free (d);

  return http_date;
}

/* NOTE - The following looses usec precision i.e. no usecs */

gboolean
dupin_date_string_to_timestamp (gchar * date_string, gsize * timestamp)
{
  SoupDate *date;
  GTimeVal t;

  g_return_val_if_fail (date_string != NULL, FALSE);
  g_return_val_if_fail (timestamp != NULL, FALSE);

  if (!(date = soup_date_new_from_string (date_string)))
    return FALSE;

  // TODO - convert date to gsize timestamp

  soup_date_to_timeval (date, &t);

  *timestamp = (t.tv_sec*1000000) + t.tv_usec; // soup-date looses precision anyway

  soup_date_free (date);

  return TRUE;
}

gint
dupin_date_timestamp_cmp (gsize timestamp1, gsize timestamp2)
{
  gint t1 = timestamp1 / 1000000;
  gint t2 = timestamp2 / 1000000;

  //g_message ("dupin_date_timestamp_cmp: %d <=> %d\n", t1, t2);

  if (t1 < t2)
    {
      return -1;
    }
  else if (t1 == t2)
    {
      return 0;
    }
  else
    return 1;
}

/* EOF */
