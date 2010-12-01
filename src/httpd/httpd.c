#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin.h"
#include "httpd.h"
#include "log.h"
#include "map.h"
#include "request.h"

#include "../tbjson/tb_keyvalue.h"

/* HTTPD: */
#ifdef G_OS_UNIX
#  include <netinet/in.h>
#  include <sys/socket.h>
#  include <netdb.h>
#  include <errno.h>
#  include <unistd.h>
#endif

#include <string.h>
#include <stdlib.h>

DSHttpStatus DSHttpStatusList[] = {
  {HTTP_STATUS_200, "HTTP/1.1 200 OK", "{\"ok\": true}", 12, HTTP_MIME_JSON,
   FALSE}
  ,
  {HTTP_STATUS_201, "HTTP/1.1 201 Created", "{\"ok\": true}", 12,
   HTTP_MIME_JSON, FALSE}
  ,
  {HTTP_STATUS_400, "HTTP/1.1 400 Bad Request",
   "Your client sent a request that this server could not understand.", 65,
   HTTP_MIME_TEXTHTML, TRUE}
  ,
  {HTTP_STATUS_403, "HTTP/1.1 403 Forbidden",
   "You don't have permission to access this resource on this server.", 65,
   HTTP_MIME_TEXTHTML, TRUE}
  ,
  {HTTP_STATUS_404, "HTTP/1.1 404 Not Found",
   "The requested URI was not found on this server.", 47, HTTP_MIME_TEXTHTML,
   TRUE}
  ,
  {HTTP_STATUS_409, "HTTP/1.1 409 Conflict",
   "The request URI generated a conflict.", 37, HTTP_MIME_TEXTHTML, TRUE}
  ,
  {HTTP_STATUS_500, "HTTP/1.1 500 Internal Server Error",
   "Internal Server Error", 21, HTTP_MIME_TEXTHTML, TRUE}
  ,
  {HTTP_STATUS_END, NULL, NULL, 0, NULL, TRUE}
};

static GIOChannel *httpd_create (DSGlobal * data, GError ** error,
				 gint * ret_fd);
static GIOChannel *httpd_create6 (DSGlobal * data, GError ** error,
				  gint * ret_fd);

static gboolean httpd_read (GIOChannel *, GIOCondition, DSGlobal *);
static gboolean httpd_read_timeout (DSGlobal * data);

static DSHttpdThread *httpd_thread_new (DSGlobal * data, GError ** error);
static gpointer httpd_thread (DSHttpdThread * thread);
static void httpd_thread_close (DSHttpdThread * thread);
static gboolean httpd_thread_close_timeout (DSHttpdThread * thread);
static void httpd_thread_free (DSHttpdThread * thread);

static gboolean httpd_client_add (DSGlobal * data, DSHttpdClient * client);
static void httpd_client_close (DSHttpdClient * client);
static void httpd_client_free (DSHttpdClient * client);
static gboolean httpd_client_timeout (DSHttpdClient * client);
static void httpd_client_timeout_refresh (DSHttpdClient * client);
static gboolean httpd_client_read_header (GIOChannel *, GIOCondition,
					  DSHttpdClient * client);
static gboolean httpd_client_read_header_timeout (DSHttpdClient * client);
static gboolean httpd_client_read_body (GIOChannel *, GIOCondition,
					DSHttpdClient * client);
static gboolean httpd_client_read_body_timeout (DSHttpdClient * client);
static gboolean httpd_client_write_header (GIOChannel *, GIOCondition,
					   DSHttpdClient * client);
static gboolean httpd_client_write_header_timeout (DSHttpdClient * client);
static gboolean httpd_client_write_body (GIOChannel *, GIOCondition,
					 DSHttpdClient * client);
static gboolean httpd_client_write_body_timeout (DSHttpdClient * client);

static void httpd_client_send (DSHttpdClient * client, DSHttpStatusCode);

static gboolean httpd_client_header (DSHttpdClient * client,
				     DSHttpStatusCode * error);

static void httpd_client_request (DSHttpdClient * client);

/* INITIALIZE ***************************************************************/

/* Generic init: */
gboolean
httpd_init (DSGlobal * data, GError ** error)
{
  if (data->httpd_ipv6 == FALSE)
    {
      if (!
	  (data->httpd_socket =
	   httpd_create (data, error, &data->httpd_socket_fd)))
	return FALSE;
    }

  else
    if (!
	(data->httpd_socket =
	 httpd_create6 (data, error, &data->httpd_socket_fd)))
    return FALSE;

  /* The callback for the socket input: */
  data->httpd_socket_source = g_io_create_watch (data->httpd_socket, G_IO_IN);
  g_source_set_callback (data->httpd_socket_source, (GSourceFunc) httpd_read,
			 data, NULL);
  g_source_attach (data->httpd_socket_source, g_main_context_default ());

  if (data->limit_threadnumb != 0)
    {
      guint i;

      for (i = 0; i < data->limit_threadnumb; i++)
	if (!httpd_thread_new (data, error))
	  return FALSE;
    }

  return TRUE;
}

/* Close the socket */
void
httpd_close (DSGlobal * data)
{
  g_source_destroy (data->httpd_socket_source);
  g_source_unref (data->httpd_socket_source);
  data->httpd_socket_source = NULL;

  g_io_channel_shutdown (data->httpd_socket, FALSE, NULL);
  g_io_channel_unref (data->httpd_socket);

  while (data->httpd_threads)
    httpd_thread_close (data->httpd_threads->data);
}

/* IPV6 socket: */
static GIOChannel *
httpd_create6 (DSGlobal * data, GError ** error, gint * ret_fd)
{
  gint fd, yes = 1;
  GIOChannel *channel;
  struct sockaddr_in6 sock;

  if ((fd = socket (AF_INET6, SOCK_STREAM, 0)) < 0)
    {
      g_set_error (error, ds_error_quark (), 0,
		   "Error opening the socket: %s\n", g_strerror (errno));
      return NULL;
    }

  if (setsockopt (fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof (int)))
    {
      close (fd);
      g_set_error (error, ds_error_quark (), 0,
		   "Error setting options on the socket: %s\n",
		   g_strerror (errno));
      return NULL;
    }

  memset (&sock, 0, sizeof (struct sockaddr_in6));
  sock.sin6_family = AF_INET6;
  sock.sin6_port = htons (data->httpd_port);

  if (!data->httpd_interface)
    sock.sin6_addr = in6addr_any;

  else
    {
      struct hostent *hp;

      if (!(hp = gethostbyname2 (data->httpd_interface, AF_INET6)))
	{
	  close (fd);
	  g_set_error (error, ds_error_quark (), 0,
		       "Error resolving the interface '%s': %s\n",
		       data->httpd_interface, g_strerror (errno));
	  return NULL;
	}

      sock.sin6_family = hp->h_addrtype;
      memcpy (&sock.sin6_addr, hp->h_addr, hp->h_length);
    }

  if (bind (fd, (struct sockaddr *) &sock, sizeof (struct sockaddr_in6)) < 0)
    {
      close (fd);
      g_set_error (error, ds_error_quark (), 0,
		   "Error binding the socket: %s\n", g_strerror (errno));
      return NULL;
    }

  if (listen (fd, data->httpd_listen) < 0)
    {
      close (fd);
      g_set_error (error, ds_error_quark (), 0,
		   "Error listeining on the socket: %s\n",
		   g_strerror (errno));
      return NULL;
    }

#ifdef G_OS_WIN32
  channel = g_io_channel_win32_new_fd (fd);
#else
  channel = g_io_channel_unix_new (fd);
#endif

  g_io_channel_set_encoding (channel, NULL, NULL);
  *ret_fd = fd;

  return channel;
}

/* IPv4 socket: */
static GIOChannel *
httpd_create (DSGlobal * data, GError ** error, gint * ret_fd)
{
  gint fd, yes = 1;
  struct sockaddr_in sock;
  GIOChannel *channel;

  if ((fd = socket (AF_INET, SOCK_STREAM, 0)) < 0)
    {
      g_set_error (error, ds_error_quark (), 0,
		   "Error creating the socket: %s", g_strerror (errno));
      return NULL;
    }

  if (setsockopt (fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof (int)))
    {
      close (fd);
      g_set_error (error, ds_error_quark (), 0,
		   "Error setting options on the socket: %s\n",
		   g_strerror (errno));
      return NULL;
    }

  memset (&sock, 0, sizeof (struct sockaddr_in));
  sock.sin_family = AF_INET;
  sock.sin_port = htons (data->httpd_port);

  if (!data->httpd_interface)
    sock.sin_addr.s_addr = htonl (INADDR_ANY);

  else
    {
      struct hostent *hp;

      if (!(hp = gethostbyname (data->httpd_interface)))
	{
	  close (fd);
	  g_set_error (error, ds_error_quark (), 0,
		       "Error resolving the interface '%s': %s",
		       data->httpd_interface, g_strerror (errno));
	  return NULL;
	}

      sock.sin_family = hp->h_addrtype;
      memcpy (&sock.sin_addr, hp->h_addr, hp->h_length);
    }

  if (bind (fd, (struct sockaddr *) &sock, sizeof (struct sockaddr_in)) < 0)
    {
      close (fd);
      g_set_error (error, ds_error_quark (), 0,
		   "Error binding the socket: %s\n", g_strerror (errno));
      return NULL;
    }

  if (listen (fd, data->httpd_listen) < 0)
    {
      close (fd);
      g_set_error (error, ds_error_quark (), 0,
		   "Error listeining on the socket: %s\n",
		   g_strerror (errno));
      return NULL;
    }

#ifdef G_OS_WIN32
  channel = g_io_channel_win32_new_fd (fd);
#else
  channel = g_io_channel_unix_new (fd);
#endif

  g_io_channel_set_encoding (channel, NULL, NULL);
  *ret_fd = fd;

  return channel;
}

/* Reading function: */
static gboolean
httpd_read (GIOChannel * source, GIOCondition cond, DSGlobal * data)
{
  struct sockaddr_in sock;
  struct sockaddr_in6 sock6;

  DSHttpdClient *client;

  gchar *ip;
  gint fd;

  /* IPv6: */
  if (data->httpd_ipv6 == TRUE)
    {
      socklen_t size = sizeof (struct sockaddr_in6);
      gint id;
      GString *str;

      /* IPv6 accept: */
      if ((fd =
	   accept (data->httpd_socket_fd, (struct sockaddr *) &sock6,
		   &size)) < 0)
	{
	  g_source_destroy (data->httpd_socket_source);
	  g_source_unref (data->httpd_socket_source);

	  data->httpd_socket_source = g_timeout_source_new (200);
	  g_source_set_callback (data->httpd_socket_source,
				 (GSourceFunc) httpd_read_timeout, data,
				 NULL);
	  g_source_attach (data->httpd_socket_source,
			   g_main_context_default ());

	  return FALSE;
	}

      /* Log information: */
      str = g_string_new (NULL);

      for (id = 0; id < 16; id++)
	{
	  if (sock6.sin6_addr.s6_addr[id])
	    g_string_append_printf (str, "%X%c", sock6.sin6_addr.s6_addr[id],
				    id < 15 ? ':' : ' ');
	  else
	    g_string_append_printf (str, "%c", id < 15 ? ':' : ' ');
	}

      ip = g_string_free (str, FALSE);
    }

  /* IPv4: */
  else
    {
      socklen_t size = sizeof (struct sockaddr_in);
      gint addr;

      /* IPv4 accept: */
      if ((fd =
	   accept (data->httpd_socket_fd, (struct sockaddr *) &sock,
		   &size)) < 0)
	{
	  g_source_destroy (data->httpd_socket_source);
	  g_source_unref (data->httpd_socket_source);

	  data->httpd_socket_source = g_timeout_source_new (200);
	  g_source_set_callback (data->httpd_socket_source,
				 (GSourceFunc) httpd_read_timeout, data,
				 NULL);
	  g_source_attach (data->httpd_socket_source,
			   g_main_context_default ());

	  return FALSE;
	}

      addr = ntohl (sock.sin_addr.s_addr);

      ip =
	g_strdup_printf ("%d.%d.%d.%d", (unsigned int) addr >> 24,
			 (unsigned int) (addr >> 16) % 256,
			 (unsigned int) (addr >> 8) % 256,
			 (unsigned int) addr % 256);
    }

  /* MaxClients check: */
  if (data->limit_maxclients > 0
      && data->httpd_clients_numb >= data->limit_maxclients)
    {
      log_write (data, LOG_VERBOSE_WARNING, LOG_HTTPD_CLIENT_ERROR, "error",
		 LOG_VERBOSE_WARNING, LOG_TYPE_STRING, "Too many clients",
		 NULL);
      g_free (ip);
      close (fd);

      return TRUE;
    }

  /* A new struct for a new client: */
  client = g_malloc0 (sizeof (DSHttpdClient));

  client->ip = ip;
#ifdef G_OS_WIN32
  client->channel = g_io_channel_win32_new (fd);
#else
  client->channel = g_io_channel_unix_new (fd);
#endif
  g_io_channel_set_encoding (client->channel, NULL, NULL);

  if (httpd_client_add (data, client) == FALSE)
    {
      httpd_client_free (client);
      return FALSE;
    }

  return TRUE;
}

static gboolean
httpd_read_timeout (DSGlobal * data)
{
  g_source_destroy (data->httpd_socket_source);
  g_source_unref (data->httpd_socket_source);

  data->httpd_socket_source = g_io_create_watch (data->httpd_socket, G_IO_IN);
  g_source_set_callback (data->httpd_socket_source, (GSourceFunc) httpd_read,
			 data, NULL);
  g_source_attach (data->httpd_socket_source, g_main_context_default ());

  return FALSE;
}

/* CLIENT ******************************************************************/
static gboolean
httpd_client_add (DSGlobal * data, DSHttpdClient * client)
{
  GList *list;
  DSHttpdThread *thread = NULL;

  g_mutex_lock (data->httpd_mutex);
  data->httpd_clients_numb++;

  /* Looking for a empty thread: */
  for (list = data->httpd_threads; list; list = list->next)
    {
      thread = list->data;

      g_mutex_lock (thread->mutex);

      if (thread->clients_numb < data->limit_clientsforthread)
	break;

      g_mutex_unlock (thread->mutex);
      thread = NULL;
    }

  /* Creating a new thread: */
  if (!thread)
    {
      GError *error = NULL;

      if (!(thread = httpd_thread_new (data, &error)))
	{
	  data->httpd_clients_numb--;
	  g_mutex_unlock (data->httpd_mutex);

	  log_write (data, LOG_VERBOSE_ERROR, LOG_HTTPD_CLIENT_ERROR, "error",
		     LOG_VERBOSE_ERROR, LOG_TYPE_STRING, error->message,
		     NULL);
	  g_error_free (error);
	  return FALSE;
	}

      g_mutex_lock (thread->mutex);
    }

  g_mutex_unlock (data->httpd_mutex);

  /* Adding the new client: */
  thread->clients = g_list_prepend (thread->clients, client);
  thread->clients_numb++;

  if (thread->timeout_source)
    {
      g_source_destroy (thread->timeout_source);
      g_source_unref (thread->timeout_source);
      thread->timeout_source = NULL;
    }

  client->thread = thread;

  /* A timer for idle connections: */
  httpd_client_timeout_refresh (client);

  /* A function for the input data: */
  client->channel_source = g_io_create_watch (client->channel, G_IO_IN);
  g_source_set_callback (client->channel_source,
			 (GSourceFunc) httpd_client_read_header, client,
			 NULL);
  g_source_attach (client->channel_source, thread->context);

  g_mutex_unlock (thread->mutex);
  return TRUE;
}

/* After X seconds, the client will be closed: */
static gboolean
httpd_client_timeout (DSHttpdClient * client)
{
  httpd_client_close (client);
  return FALSE;
}

static void
httpd_client_timeout_refresh (DSHttpdClient * client)
{
  /* Removing the timeout: */
  if (client->timeout_source)
    {
      g_source_destroy (client->timeout_source);
      g_source_unref (client->timeout_source);
    }

  /* A new timeout: */
  client->timeout_source =
    g_timeout_source_new (client->thread->data->limit_timeout * 1000);
  g_source_set_callback (client->timeout_source,
			 (GSourceFunc) httpd_client_timeout, client, NULL);
  g_source_attach (client->timeout_source, client->thread->context);
}

/* CLIENT READ HEADER *******************************************************/

/* This function reads something from the client: */
static gboolean
httpd_client_read_header (GIOChannel * source, GIOCondition cond,
			  DSHttpdClient * client)
{
  gchar *line = NULL;
  gsize size, last;
  GIOStatus status;

  status = g_io_channel_read_line (source, &line, &size, &last, NULL);

  /* The status of the read: */
  switch (status)
    {
    case G_IO_STATUS_NORMAL:
      break;

      /* Setting a delay: */
    case G_IO_STATUS_AGAIN:
      g_source_destroy (client->channel_source);
      g_source_unref (client->channel_source);

      client->channel_source = g_timeout_source_new (200);
      g_source_set_callback (client->channel_source,
			     (GSourceFunc) httpd_client_read_header_timeout,
			     client, NULL);
      g_source_attach (client->channel_source, client->thread->context);
      return FALSE;

      /* Close the socket: */
    case G_IO_STATUS_ERROR:
    case G_IO_STATUS_EOF:
      log_write (client->thread->data, LOG_VERBOSE_INFO,
		 LOG_HTTPD_CLIENT_ERROR, "client", LOG_VERBOSE_INFO,
		 LOG_TYPE_STRING, client->ip, "error", LOG_VERBOSE_INFO,
		 LOG_TYPE_STRING, "Client exits", NULL);
      httpd_client_close (client);
      return FALSE;
    }

  /* Removing the timeout: */
  httpd_client_timeout_refresh (client);

  /* If the line is too long: */
  if (size > HTTP_MAX_LINE)
    {
      log_write (client->thread->data, LOG_VERBOSE_INFO,
		 LOG_HTTPD_CLIENT_ERROR, "client", LOG_VERBOSE_INFO,
		 LOG_TYPE_STRING, client->ip, "error", LOG_VERBOSE_INFO,
		 LOG_TYPE_STRING, "Header line too long", NULL);

      httpd_client_send (client, HTTP_STATUS_400);
      g_free (line);
      return FALSE;
    }

  line[last] = 0;

  /* Empty line means the data block: */
  if (line[0] == 0)
    {
      DSHttpStatusCode error;

      g_free (line);

      /* Validation of the HTTP header: */
      if (httpd_client_header (client, &error) == FALSE)
	httpd_client_send (client, error);

      return FALSE;
    }

  client->headers = g_list_append (client->headers, line);
  client->headers_numb++;

  if (client->thread->data->limit_maxheaders > 0
      && client->headers_numb >= client->thread->data->limit_maxheaders)
    {
      log_write (client->thread->data, LOG_VERBOSE_INFO,
		 LOG_HTTPD_CLIENT_ERROR, "client", LOG_VERBOSE_INFO,
		 LOG_TYPE_STRING, client->ip, "error", LOG_VERBOSE_INFO,
		 LOG_TYPE_STRING, "Too many line of header", NULL);
      httpd_client_send (client, HTTP_STATUS_400);
      return FALSE;
    }

  return TRUE;
}

static gboolean
httpd_client_read_header_timeout (DSHttpdClient * client)
{
  g_source_destroy (client->channel_source);
  g_source_unref (client->channel_source);

  client->channel_source = g_io_create_watch (client->channel, G_IO_IN);
  g_source_set_callback (client->channel_source,
			 (GSourceFunc) httpd_client_read_header, client,
			 NULL);
  g_source_attach (client->channel_source, client->thread->context);
  return FALSE;
}

static gboolean httpd_client_header_parse (DSHttpdClient * client,
					   gchar * request, GList ** parts,
					   GList ** keyvalue);
static gchar *httpd_client_header_escape (gchar * string);

/* Header parser: */
static gboolean
httpd_client_header (DSHttpdClient * client, DSHttpStatusCode * error)
{
  GList *list;
  gchar **parts;

  gsize csize = 0;

  if (!client->headers)
    {
      *error = HTTP_STATUS_400;
      return FALSE;
    }

  parts = g_strsplit (client->headers->data, " ", -1);

  if (!parts || !parts[0] || !parts[1] || !parts[2] || parts[3])
    {
      *error = HTTP_STATUS_400;

      if (parts)
	g_strfreev (parts);

      return FALSE;
    }

  if (!strcmp (parts[0], "GET"))
    client->request = DS_HTTPD_REQUEST_GET;

  else if (!strcmp (parts[0], "POST"))
    client->request = DS_HTTPD_REQUEST_POST;

  else if (!strcmp (parts[0], "PUT"))
    client->request = DS_HTTPD_REQUEST_PUT;

  else if (!strcmp (parts[0], "DELETE"))
    client->request = DS_HTTPD_REQUEST_DELETE;

  else
    {
      *error = HTTP_STATUS_400;
      g_strfreev (parts);
      return FALSE;
    }

  if (strcmp (parts[2], "HTTP/1.1") && strcmp (parts[2], "HTTP/1.0"))
    {
      *error = HTTP_STATUS_400;
      g_strfreev (parts);
      return FALSE;
    }

  if (httpd_client_header_parse
      (client, parts[1], &client->request_path,
       &client->request_arguments) == FALSE)
    {
      *error = HTTP_STATUS_400;
      g_strfreev (parts);
      return FALSE;
    }

  g_strfreev (parts);

  for (list = client->headers->next; list; list = list->next)
    {
      gchar *line = list->data;

      if (!strncasecmp (line, HTTP_CONTENT_LENGTH, HTTP_CONTENT_LENGTH_LEN))
	{
	  line += HTTP_CONTENT_LENGTH_LEN;

	  while (line[0] != 0 && (line[0] == ' ' || line[0] == '\t'))
	    line++;

	  if (line[0] != ':')
	    continue;

	  line++;

	  while (line[0] != 0 && (line[0] == ' ' || line[0] == '\t'))
	    line++;

	  if (line[0] != 0)
	    csize = atoi (line);
	}

      if (!strncasecmp (line, HTTP_CONTENT_TYPE, HTTP_CONTENT_TYPE_LEN))
	{
	  line += HTTP_CONTENT_TYPE_LEN;

	  while (line[0] != 0 && (line[0] == ' ' || line[0] == '\t'))
	    line++;

	  if (line[0] != ':')
	    continue;

	  line++;

	  while (line[0] != 0 && (line[0] == ' ' || line[0] == '\t'))
	    line++;

          client->input_mime = g_strdup (line);
	}
    }

  log_write (client->thread->data, LOG_VERBOSE_INFO, LOG_HTTPD_CLIENT_CONNECT,
	     "client", LOG_VERBOSE_INFO, LOG_TYPE_STRING, client->ip,
	     "request", LOG_VERBOSE_INFO, LOG_TYPE_STRING,
	     client->headers->data, "contentTypeLength", LOG_VERBOSE_INFO,
	     LOG_TYPE_INTEGER, (gint) csize, NULL);

  if (csize != 0)
    {
      if (client->thread->data->limit_maxcontentlength != 0
	  && csize >= client->thread->data->limit_maxcontentlength)
	{
	  log_write (client->thread->data, LOG_VERBOSE_INFO,
		     LOG_HTTPD_CLIENT_ERROR, "client", LOG_VERBOSE_INFO,
		     LOG_TYPE_STRING, client->ip, "error", LOG_VERBOSE_INFO,
		     LOG_TYPE_STRING, "Content-Length too big", NULL);

	  *error = HTTP_STATUS_400;
	  return FALSE;
	}

      client->body_size = csize;
      client->body = g_malloc (sizeof (gchar) * csize);

      g_source_destroy (client->channel_source);
      g_source_unref (client->channel_source);

      client->channel_source = g_io_create_watch (client->channel, G_IO_IN);
      g_source_set_callback (client->channel_source,
			     (GSourceFunc) httpd_client_read_body, client,
			     NULL);
      g_source_attach (client->channel_source, client->thread->context);
      return TRUE;
    }

  httpd_client_request (client);
  return TRUE;
}

/* Parsing of the http request: */
static gboolean
httpd_client_header_parse (DSHttpdClient * client, gchar * request,
			   GList ** ret_paths, GList ** keyvalue)
{
  gchar **paths;
  gchar **split;
  gchar **attr;
  gchar *path;
  gint i;

  GList *ret = NULL;

  *keyvalue = NULL;
  *ret_paths = NULL;

  if (request[0] != '/')
    return FALSE;

  if (!(split = g_strsplit (request, "?", 2)) || !split[0]
      || split[0][0] != '/')
    {
      *ret_paths = g_list_append (*ret_paths, g_strdup (request));

      if (split)
	g_strfreev (split);

      return TRUE;
    }

  path = split[0];

  while (path[0] == '/')
    path++;

  paths = g_strsplit (path, "/", -1);

  for (i = 0; paths[i]; i++)
    if (paths[i][0])
      *ret_paths =
	g_list_append (*ret_paths, httpd_client_header_escape (paths[i]));

  g_strfreev (paths);

  if (!split[1] || !(attr = g_strsplit (split[1], "&", -1)))
    {
      g_strfreev (split);
      return TRUE;
    }

  for (i = 0; attr[i]; i++)
    {
      gchar **attribute;
      gchar *key, *value;

      if (!(attribute = g_strsplit (attr[i], "=", 2)) || !attribute[0]
	  || !attribute[1])
	{
	  if (attribute)
	    g_strfreev (attribute);

	  continue;
	}

      key = httpd_client_header_escape (attribute[0]);
      value = httpd_client_header_escape (attribute[1]);

      ret = g_list_prepend (ret, tb_keyvalue_new (key, value));

      g_free (value);
      g_free (key);
      g_strfreev (attribute);
    }

  g_strfreev (attr);
  g_strfreev (split);

  *keyvalue = ret;
  return TRUE;
}

/* URL decode: */
static gchar *
httpd_client_header_escape (gchar * string)
{
  gint i, j, len;
  gchar *ret;

  len = strlen (string);

  ret = g_malloc (sizeof (gchar) * (len + 1));

  for (i = j = 0; i < len; i++)
    {
      if (string[i] == '%' && i < len - 2)
	{
	  gchar byte;

	  byte = string[i + 1] * 16 + string[i + 2];
	  ret[j++] = byte;
	  i += 2;
	}

      else
	ret[j++] = string[i];
    }

  ret[j] = 0;
  return ret;
}

/* CLIENT READ BODY *******************************************************/

/* This function reads something from the client: */
static gboolean
httpd_client_read_body (GIOChannel * source, GIOCondition cond,
			DSHttpdClient * client)
{
  gsize done;
  GIOStatus status;

  status =
    g_io_channel_read_chars (source, client->body + client->body_done,
			     client->body_size - client->body_done, &done,
			     NULL);

  /* The status of the read: */
  switch (status)
    {
    case G_IO_STATUS_NORMAL:
      break;

      /* Setting a delay: */
    case G_IO_STATUS_AGAIN:
      g_source_destroy (client->channel_source);
      g_source_unref (client->channel_source);

      client->channel_source = g_timeout_source_new (200);
      g_source_set_callback (client->channel_source,
			     (GSourceFunc) httpd_client_read_body_timeout,
			     client, NULL);
      g_source_attach (client->channel_source, client->thread->context);
      return FALSE;

      /* Close the socket: */
    case G_IO_STATUS_ERROR:
    case G_IO_STATUS_EOF:
      log_write (client->thread->data, LOG_VERBOSE_INFO,
		 LOG_HTTPD_CLIENT_ERROR, "client", LOG_VERBOSE_INFO,
		 LOG_TYPE_STRING, client->ip, "error", LOG_VERBOSE_INFO,
		 LOG_TYPE_STRING, "Client exits", NULL);

      httpd_client_close (client);
      return FALSE;
    }

  /* Removing the timeout: */
  httpd_client_timeout_refresh (client);

  client->body_done += done;

  /* Some check: */
  if (client->body_done >= client->body_size)
    {
      httpd_client_request (client);
      return FALSE;
    }

  return TRUE;
}

static gboolean
httpd_client_read_body_timeout (DSHttpdClient * client)
{
  g_source_destroy (client->channel_source);
  g_source_unref (client->channel_source);

  client->channel_source = g_io_create_watch (client->channel, G_IO_IN);
  g_source_set_callback (client->channel_source,
			 (GSourceFunc) httpd_client_read_body, client, NULL);
  g_source_attach (client->channel_source, client->thread->context);
  return FALSE;
}

/* CLIENT REQUEST ***********************************************************/
static void
httpd_client_request (DSHttpdClient * client)
{
  guint i;
  DSHttpStatusCode status;

  if (!client->request_path)
    {
      client->output_type = DS_HTTPD_OUTPUT_STRING;
      client->output.string.string = g_strdup (HTTP_WWW_REDIRECT);
      client->output_size = HTTP_WWW_REDIRECT_LEN;
      client->output_mime = g_strdup (HTTP_MIME_TEXTHTML);

      httpd_client_send (client, HTTP_STATUS_200);
      return;
    }

  for (i = 0; request_types[i].request; i++)
    {
      if (client->request == request_types[i].request_type
	  && !strcmp (client->request_path->data, request_types[i].request))
	{
	  status =
	    request_types[i].func (client, client->request_path->next,
				   client->request_arguments);
	  break;
	}
    }

  if (!request_types[i].request)
    status =
      request_global (client, client->request_path,
		      client->request_arguments);

  httpd_client_send (client, status);
}

/* CLIENT WRITE HEADER ****************************************************/
/* This function writes something to the client: */
static gboolean
httpd_client_write_header (GIOChannel * source, GIOCondition cond,
			   DSHttpdClient * client)
{
  gsize done;
  GIOStatus status;

  if ((status =
       g_io_channel_write_chars (source,
				 client->output_header +
				 client->output_header_done,
				 client->output_header_size -
				 client->output_header_done, &done,
				 NULL)) == G_IO_STATUS_NORMAL)
    status = g_io_channel_flush (client->channel, NULL);

  /* The status of the read: */
  switch (status)
    {
    case G_IO_STATUS_NORMAL:
      client->output_header_done += done;

      if (client->output_header_done >= client->output_header_size)
	{
	  g_source_destroy (client->channel_source);
	  g_source_unref (client->channel_source);

	  client->channel_source =
	    g_io_create_watch (client->channel, G_IO_OUT);
	  g_source_set_callback (client->channel_source,
				 (GSourceFunc) httpd_client_write_body,
				 client, NULL);
	  g_source_attach (client->channel_source, g_main_context_default ());
	  return FALSE;
	}

      break;

      /* Setting a delay: */
    case G_IO_STATUS_AGAIN:
      g_source_destroy (client->channel_source);
      g_source_unref (client->channel_source);

      client->channel_source = g_timeout_source_new (200);
      g_source_set_callback (client->channel_source,
			     (GSourceFunc) httpd_client_write_header_timeout,
			     client, NULL);
      g_source_attach (client->channel_source, g_main_context_default ());
      return FALSE;

      /* Close the socket: */
    case G_IO_STATUS_ERROR:
    case G_IO_STATUS_EOF:
      httpd_client_close (client);
      return FALSE;
    }

  /* Removing the timeout: */
  httpd_client_timeout_refresh (client);

  return TRUE;
}

static gboolean
httpd_client_write_header_timeout (DSHttpdClient * client)
{
  g_source_destroy (client->channel_source);
  g_source_unref (client->channel_source);

  client->channel_source = g_io_create_watch (client->channel, G_IO_OUT);
  g_source_set_callback (client->channel_source,
			 (GSourceFunc) httpd_client_write_header, client,
			 NULL);
  g_source_attach (client->channel_source, g_main_context_default ());
  return FALSE;
}

/* CLIENT WRITE BODY *******************************************************/

static gboolean httpd_client_write_body_string (GIOChannel * source,
						GIOCondition cond,
						DSHttpdClient * client);
static gboolean httpd_client_write_body_io (GIOChannel * source,
					    GIOCondition cond,
					    DSHttpdClient * client);
static gboolean httpd_client_write_body_map (GIOChannel * source,
					     GIOCondition cond,
					     DSHttpdClient * client);

/* This function writes something to the client: */
static gboolean
httpd_client_write_body (GIOChannel * source, GIOCondition cond,
			 DSHttpdClient * client)
{
  switch (client->output_type)
    {
    case DS_HTTPD_OUTPUT_NONE:
      break;

    case DS_HTTPD_OUTPUT_STRING:
      return httpd_client_write_body_string (source, cond, client);

    case DS_HTTPD_OUTPUT_IO:
      return httpd_client_write_body_io (source, cond, client);

    case DS_HTTPD_OUTPUT_MAP:
      return httpd_client_write_body_map (source, cond, client);
    }

  return TRUE;
}

static gboolean
httpd_client_write_body_string (GIOChannel * source, GIOCondition cond,
				DSHttpdClient * client)
{
  gsize done;
  GIOStatus status;

  if ((status =
       g_io_channel_write_chars (source,
				 client->output.string.string +
				 client->output.string.done,
				 client->output_size -
				 client->output.string.done, &done,
				 NULL)) == G_IO_STATUS_NORMAL)
    status = g_io_channel_flush (client->channel, NULL);

  /* The status of the read: */
  switch (status)
    {
    case G_IO_STATUS_NORMAL:
      client->output.string.done += done;

      if (client->output.string.done >= client->output_size)
	{
	  httpd_client_close (client);
	  return FALSE;
	}

      break;

      /* Setting a delay: */
    case G_IO_STATUS_AGAIN:
      g_source_destroy (client->channel_source);
      g_source_unref (client->channel_source);

      client->channel_source = g_timeout_source_new (200);
      g_source_set_callback (client->channel_source,
			     (GSourceFunc) httpd_client_write_body_timeout,
			     client, NULL);
      g_source_attach (client->channel_source, g_main_context_default ());
      return FALSE;

      /* Close the socket: */
    case G_IO_STATUS_ERROR:
    case G_IO_STATUS_EOF:
      httpd_client_close (client);
      return FALSE;
    }

  /* Removing the timeout: */
  httpd_client_timeout_refresh (client);

  return TRUE;
}

static gboolean httpd_client_write_body_io_read (DSHttpdClient * client);

static gboolean
httpd_client_write_body_io (GIOChannel * source, GIOCondition cond,
			    DSHttpdClient * client)
{
  gsize done;
  GIOStatus status;

  if (client->output.io.size == 0)
    {
      if (httpd_client_write_body_io_read (client) == FALSE)
	{
	  httpd_client_close (client);
	  return FALSE;
	}
    }

  if ((status =
       g_io_channel_write_chars (source,
				 client->output.io.string +
				 client->output.io.done,
				 client->output.io.size -
				 client->output.io.done, &done,
				 NULL)) == G_IO_STATUS_NORMAL)
    status = g_io_channel_flush (client->channel, NULL);

  /* The status of the read: */
  switch (status)
    {
    case G_IO_STATUS_NORMAL:
      client->output.io.done += done;

      if (client->output.io.done >= client->output.io.size)
	client->output.io.size = client->output.io.done = 0;

      break;

      /* Setting a delay: */
    case G_IO_STATUS_AGAIN:
      g_source_destroy (client->channel_source);
      g_source_unref (client->channel_source);

      client->channel_source = g_timeout_source_new (200);
      g_source_set_callback (client->channel_source,
			     (GSourceFunc) httpd_client_write_body_timeout,
			     client, NULL);
      g_source_attach (client->channel_source, g_main_context_default ());
      return FALSE;

      /* Close the socket: */
    case G_IO_STATUS_ERROR:
    case G_IO_STATUS_EOF:
      httpd_client_close (client);
      return FALSE;
    }

  /* Removing the timeout: */
  httpd_client_timeout_refresh (client);

  return TRUE;
}

static gboolean
httpd_client_write_body_io_read (DSHttpdClient * client)
{
  GIOStatus status;

  status =
    g_io_channel_read_chars (client->output.io.io, client->output.io.string,
			     sizeof (client->output.io.string),
			     &client->output.io.size, NULL);

  /* The status of the read: */
  switch (status)
    {
    case G_IO_STATUS_NORMAL:
      return TRUE;

      /* Setting a delay: */
    case G_IO_STATUS_AGAIN:
      g_source_destroy (client->channel_source);
      g_source_unref (client->channel_source);

      client->channel_source = g_timeout_source_new (200);
      g_source_set_callback (client->channel_source,
			     (GSourceFunc) httpd_client_write_body_timeout,
			     client, NULL);
      g_source_attach (client->channel_source, client->thread->context);
      return TRUE;

    case G_IO_STATUS_ERROR:
    case G_IO_STATUS_EOF:
      return FALSE;
    }
  return FALSE;
}

static gboolean
httpd_client_write_body_map (GIOChannel * source, GIOCondition cond,
			     DSHttpdClient * client)
{
  gsize done;
  GIOStatus status;

  if ((status =
       g_io_channel_write_chars (source,
				 client->output.map.contents +
				 client->output.map.done,
				 client->output_size -
				 client->output.map.done, &done,
				 NULL)) == G_IO_STATUS_NORMAL)
    status = g_io_channel_flush (client->channel, NULL);

  /* The status of the read: */
  switch (status)
    {
    case G_IO_STATUS_NORMAL:
      client->output.map.done += done;

      if (client->output.map.done >= client->output_size)
	{
	  httpd_client_close (client);
	  return FALSE;
	}

      break;

      /* Setting a delay: */
    case G_IO_STATUS_AGAIN:
      g_source_destroy (client->channel_source);
      g_source_unref (client->channel_source);

      client->channel_source = g_timeout_source_new (200);
      g_source_set_callback (client->channel_source,
			     (GSourceFunc) httpd_client_write_body_timeout,
			     client, NULL);
      g_source_attach (client->channel_source, g_main_context_default ());
      return FALSE;

      /* Close the socket: */
    case G_IO_STATUS_ERROR:
    case G_IO_STATUS_EOF:
      httpd_client_close (client);
      return FALSE;
    }

  /* Removing the timeout: */
  httpd_client_timeout_refresh (client);

  return TRUE;
}

static gboolean
httpd_client_write_body_timeout (DSHttpdClient * client)
{
  g_source_destroy (client->channel_source);
  g_source_unref (client->channel_source);

  client->channel_source = g_io_create_watch (client->channel, G_IO_OUT);
  g_source_set_callback (client->channel_source,
			 (GSourceFunc) httpd_client_write_body, client, NULL);
  g_source_attach (client->channel_source, g_main_context_default ());
  return FALSE;
}

/* CLIENT SEND *************************************************************/
/* This function writes to the socket: */
static void
httpd_client_send (DSHttpdClient * client, DSHttpStatusCode code)
{
  gchar buf[26];
  GString *str;
  guint i, j;
  time_t t;

  for (i = 0; DSHttpStatusList[i].code != HTTP_STATUS_END; i++)
    if (DSHttpStatusList[i].code == code)
      break;

  if (DSHttpStatusList[i].code == HTTP_STATUS_END)
    {
      httpd_client_send (client, HTTP_STATUS_400);
      return;
    }

  /* The body: */
  if (client->output_type == DS_HTTPD_OUTPUT_NONE)
    {
      client->output_type = DS_HTTPD_OUTPUT_STRING;
      client->output_size = DSHttpStatusList[i].body_size;
      client->output.string.string = g_strdup (DSHttpStatusList[i].body);
    }

  /* Header: */
  str = g_string_new (NULL);
  str = g_string_append (str, DSHttpStatusList[i].header);

  /* End of the string: */
  str = g_string_append (str, "\r\n");

  /* the server: */
  str = g_string_append (str, "Server: " PACKAGE " " VERSION "\r\n");

  /* the date: */
  t = time (NULL);
  asctime_r (localtime (&t), buf);
  for (j = sizeof (buf) - 1; j >= 0; j--)
    if (buf[j] == '\n')
      {
	buf[j] = 0;
	break;
      }

  g_string_append_printf (str, "Date: %s\r\n", buf);

  /* Body length: */
  g_string_append_printf (str, "Content-Length: %" G_GSIZE_FORMAT "\r\n",
			  client->output_size);

  /* Body type: */
  g_string_append_printf (str, "Content-Type: %s; charset=UTF-8\r\n",
			  client->output_mime ? client->output_mime :
			  DSHttpStatusList[i].mime);

  /* Connection Close: */
  g_string_append (str, "Connection: close\r\n");

  /* Empty line: */
  str = g_string_append (str, "\r\n");

  client->output_header_size = str->len;
  client->output_header = g_string_free (str, FALSE);

  /* Creating the writing function: */
  g_source_destroy (client->channel_source);
  g_source_unref (client->channel_source);

  client->channel_source = g_io_create_watch (client->channel, G_IO_OUT);
  g_source_set_callback (client->channel_source,
			 (GSourceFunc) httpd_client_write_header, client,
			 NULL);
  g_source_attach (client->channel_source, g_main_context_default ());
}

/* CLIENT CLOSE *************************************************************/
static void
httpd_client_close (DSHttpdClient * client)
{
  g_mutex_lock (client->thread->data->httpd_mutex);
  g_mutex_lock (client->thread->mutex);

  client->thread->clients = g_list_remove (client->thread->clients, client);
  client->thread->clients_numb--;

  if (client->thread->clients_numb == 0 && !client->thread->timeout_source)
    {
      if (!client->thread->data->limit_threadnumb
	  || client->thread->data->limit_threadnumb <
	  client->thread->data->httpd_threads_numb)
	{
	  client->thread->timeout_source =
	    g_timeout_source_new (client->thread->data->
				  limit_timeoutforthread * 1000);
	  g_source_set_callback (client->thread->timeout_source,
				 (GSourceFunc) httpd_thread_close_timeout,
				 client->thread, NULL);
	  g_source_attach (client->thread->timeout_source,
			   g_main_context_default ());
	}
    }

  g_mutex_unlock (client->thread->mutex);

  client->thread->data->httpd_clients_numb--;
  g_mutex_unlock (client->thread->data->httpd_mutex);

  httpd_client_free (client);
}

static void
httpd_client_free (DSHttpdClient * client)
{
  if (!client)
    return;

  if (client->ip)
    g_free (client->ip);

  if (client->channel)
    {
      g_io_channel_shutdown (client->channel, FALSE, NULL);
      g_io_channel_unref (client->channel);
    }

  if (client->channel_source)
    {
      g_source_destroy (client->channel_source);
      g_source_unref (client->channel_source);
    }

  if (client->timeout_source)
    {
      g_source_destroy (client->timeout_source);
      g_source_unref (client->timeout_source);
    }

  if (client->headers)
    {
      g_list_foreach (client->headers, (GFunc) g_free, NULL);
      g_list_free (client->headers);
    }

  if (client->request_path)
    {
      g_list_foreach (client->request_path, (GFunc) g_free, NULL);
      g_list_free (client->request_path);
    }

  if (client->request_arguments)
    {
      g_list_foreach (client->request_arguments, (GFunc) tb_keyvalue_destroy,
		      NULL);
      g_list_free (client->request_arguments);
    }

  if (client->body)
    g_free (client->body);

  if (client->output_header)
    g_free (client->output_header);

  if (client->input_mime)
    g_free (client->input_mime);

  if (client->output_mime)
    g_free (client->output_mime);

  switch (client->output_type)
    {
    case DS_HTTPD_OUTPUT_NONE:
      break;

    case DS_HTTPD_OUTPUT_STRING:
      if (client->output.string.string)
	g_free (client->output.string.string);
      break;

    case DS_HTTPD_OUTPUT_IO:
      if (client->output.io.io)
	{
	  g_io_channel_shutdown (client->output.io.io, FALSE, NULL);
	  g_io_channel_unref (client->output.io.io);
	}
      break;

    case DS_HTTPD_OUTPUT_MAP:
      if (client->output.map.map)
	map_unref (client->thread->data, client->output.map.map);
      break;
    }

  g_free (client);
}

/* THREADS *****************************************************************/
static DSHttpdThread *
httpd_thread_new (DSGlobal * data, GError ** error)
{
  DSHttpdThread *thread;

  thread = g_malloc0 (sizeof (DSHttpdThread));
  thread->data = data;

  thread->mutex = g_mutex_new ();

  thread->context = g_main_context_new ();
  thread->loop = g_main_loop_new (thread->context, FALSE);

  if (!
      (thread->thread =
       g_thread_create ((GThreadFunc) httpd_thread, thread, TRUE, error)))
    {
      httpd_thread_free (thread);
      return NULL;
    }

  data->httpd_threads = g_list_prepend (data->httpd_threads, thread);
  data->httpd_threads_numb++;

  return thread;
}

static gboolean
httpd_thread_close_timeout (DSHttpdThread * thread)
{
  g_source_destroy (thread->timeout_source);
  g_source_unref (thread->timeout_source);
  thread->timeout_source = NULL;

  httpd_thread_close (thread);
  return FALSE;
}

static void
httpd_thread_close (DSHttpdThread * thread)
{
  g_mutex_lock (thread->mutex);
  g_main_loop_quit (thread->loop);
  g_mutex_unlock (thread->mutex);

  g_thread_join (thread->thread);

  g_mutex_lock (thread->data->httpd_mutex);

  thread->data->httpd_threads =
    g_list_remove (thread->data->httpd_threads, thread);
  thread->data->httpd_threads_numb--;

  g_mutex_unlock (thread->data->httpd_mutex);

  httpd_thread_free (thread);
}

static gpointer
httpd_thread (DSHttpdThread * thread)
{
  g_main_loop_run (thread->loop);

  while (thread->clients)
    httpd_client_close (thread->clients->data);

  g_thread_exit (NULL);
  return NULL;
}

static void
httpd_thread_free (DSHttpdThread * thread)
{
  if (thread->timeout_source)
    {
      g_source_destroy (thread->timeout_source);
      g_source_unref (thread->timeout_source);
    }

  if (thread->loop)
    {
      if (g_main_loop_is_running (thread->loop) == TRUE)
	g_main_loop_quit (thread->loop);

      g_main_loop_unref (thread->loop);
    }

  if (thread->context)
    g_main_context_unref (thread->context);

  if (thread->mutex)
    g_mutex_free (thread->mutex);

  g_free (thread);
}

/* EOF */
