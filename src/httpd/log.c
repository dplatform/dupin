#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin.h"
#include "log.h"

#include <string.h>

gboolean
log_open (DSGlobal * data, GError ** error)
{
  data->logmutex = g_mutex_new ();

  if (!(data->logio = g_io_channel_new_file (data->logfile, "a", error)))
    return FALSE;

  return TRUE;
}

void
log_close (DSGlobal * data)
{
  if (!data->logio)
    return;

  g_io_channel_shutdown (data->logio, TRUE, NULL);
  g_io_channel_unref (data->logio);
  g_mutex_free (data->logmutex);
}

/* WRITE SYSTEM *************************************************************/

/* This function writes something into the log file: */
static void
log_write_internal (DSGlobal * data, LogVerbose verbose, gchar * format, ...)
{
  va_list va;
  gchar *string;
  GError *error = NULL;
  gsize size, written, ret;

  if (data->logverbose < verbose)
    return;

  va_start (va, format);
  string = g_strdup_vprintf (format, va);
  va_end (va);

  size = strlen (string);
  ret = 0;

  while (1)
    {
      GIOStatus status;

      status =
	g_io_channel_write_chars (data->logio, string + ret, size - ret,
				  &written, &error);

      if (status == G_IO_STATUS_ERROR)
	{
	  g_message ("Error writing into the log file: %s", error->message);
	  g_error_free (error);
	  break;
	}

      if (status == G_IO_STATUS_AGAIN)
	continue;

      if (written >= size - ret)
	break;

      ret += written;
    }

  g_free (string);

  g_io_channel_flush (data->logio, NULL);
}

/* This function writes the first part of the JSON object: */
static void
log_write_before (DSGlobal * data)
{
  GTimeVal tv;
  gchar *str;

  if (data->httpd_socket_source)
    {
#if GLIB_CHECK_VERSION (2, 27, 3)
    gint64 timestamp;
    timestamp = g_source_get_time (data->httpd_socket_source);
    tv.tv_sec = timestamp / 1000000;
    tv.tv_usec = timestamp % 1000000;
#else
    g_source_get_current_time (data->httpd_socket_source, &tv);
#endif
    }
  else
    g_get_current_time (&tv);

  str = g_time_val_to_iso8601 (&tv);

  log_write_internal (data, LOG_VERBOSE_ERROR,
		      "{ \"time\" : %d.%d, \"timeIso8601\" : \"%s\"",
		      tv.tv_sec, tv.tv_usec, str);

  g_free (str);
}

/* Ths function writes the end part of the JSON object: */
static void
log_write_after (DSGlobal * data)
{
  log_write_internal (data, LOG_VERBOSE_ERROR, " }\n");
}

/* This function write the node about the type of this message: */
static void
log_write_type (DSGlobal * data, LogType type)
{
  switch (type)
    {
    case LOG_STARTUP:
      log_write_internal (data, LOG_VERBOSE_ERROR,
			  ", \"type\" : \"STARTUP\"");
      break;

    case LOG_QUIT:
      log_write_internal (data, LOG_VERBOSE_ERROR, ", \"type\" : \"QUIT\"");
      break;

    case LOG_HTTPD_CLIENT_CONNECT:
      log_write_internal (data, LOG_VERBOSE_ERROR,
			  ", \"type\" : \"ISTANCE_HTTPD_CLIENT_CONNECT\"");
      break;

    case LOG_HTTPD_CLIENT_DISCONNECT:
      log_write_internal (data, LOG_VERBOSE_ERROR,
			  ", \"type\" : \"ISTANCE_HTTPD_CLIENT_DISCONNECT\"");
      break;

    case LOG_HTTPD_CLIENT_ERROR:
      log_write_internal (data, LOG_VERBOSE_ERROR,
			  ", \"type\" : \"ISTANCE_HTTPD_CLIENT_ERROR\"");
      break;
    }
}

/* This function creates a valid JSON string: */
static gchar *
log_write_parse_string (gchar * ptr)
{
  gunichar unichr;
  GString *ret;

  if (!ptr)
    return g_strdup ("");

  ret = g_string_new (NULL);

  while ((unichr = g_utf8_get_char_validated (ptr, -1)))
    {
      ptr = g_utf8_next_char (ptr);

      switch (unichr)
	{
	case '\b':
	  g_string_append (ret, "\\b");
	  break;
	case '\f':
	  g_string_append (ret, "\\f");
	  break;
	case '\n':
	  g_string_append (ret, "\\n");
	  break;
	case '\r':
	  g_string_append (ret, "\\r");
	  break;
	case '\t':
	  g_string_append (ret, "\\t");
	  break;
	default:
	  if (unichr == '\"')
	    g_string_append_c (ret, '\\');

	  if (g_unichar_isprint (unichr) == TRUE)
	    g_string_append_unichar (ret, unichr);
	  break;
	}
    }

  return g_string_free (ret, FALSE);
}

/* For any nodes, this function writes it into the log file: */
static void
log_write_nodes (DSGlobal * data, va_list va)
{
  gchar *node;
  gchar *string;
  gint integer;
  gdouble doub;
  gboolean boolean;

  while ((node = va_arg (va, gchar *)))
    {
      LogVerbose verbose = va_arg (va, LogVerbose);
      LogTypeValue type = va_arg (va, LogTypeValue);

      switch (type)
	{
	case LOG_TYPE_STRING:
	  string = log_write_parse_string (va_arg (va, gchar *));
	  log_write_internal (data, verbose, ", \"%s\" : \"%s\"", node,
			      string);
	  g_free (string);
	  break;

	case LOG_TYPE_INTEGER:
	  integer = va_arg (va, gint);
	  log_write_internal (data, verbose, ", \"%s\" : %d", node, integer);
	  break;

	case LOG_TYPE_DOUBLE:
	  doub = va_arg (va, gdouble);
	  log_write_internal (data, verbose, ", \"%s\" : %f", node, doub);
	  break;

	case LOG_TYPE_BOOLEAN:
	  boolean = va_arg (va, gboolean);
	  log_write_internal (data, verbose, ", \"%s\" : %s", node,
			      boolean == TRUE ? "true" : "false");
	  break;

	case LOG_TYPE_NULL:
	  log_write_internal (data, verbose, ", \"%s\" : null", node);
	  break;
	}
    }
}

/* Generic log: */
void
log_write (DSGlobal * data, LogVerbose verbose, LogType type, ...)
{
  va_list va;

  if (data->logverbose < verbose)
    return;

  g_mutex_lock (data->logmutex);

  va_start (va, type);

  log_write_before (data);
  log_write_type (data, type);
  log_write_nodes (data, va);
  log_write_after (data);

  va_end (va);

  g_mutex_unlock (data->logmutex);
}

/* EOF */
