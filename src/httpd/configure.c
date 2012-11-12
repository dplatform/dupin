#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "configure.h"
#include "dupin_server_common.h"

#include <libxml/parser.h>

/* BASE CONFIGURE PARSER ***************************************************/
static gboolean configure_parser (xmlDocPtr xml, DSGlobal * data,
				  GError ** error);
static gboolean configure_log_verbose (xmlChar * string, LogVerbose * verbose,
				       GError ** error);
static gboolean configure_httpd_parser (xmlDocPtr xml, DSGlobal * data,
					GError ** error);
static gboolean configure_limit_parser (xmlDocPtr xml, DSGlobal * data,
					GError ** error);

DSGlobal *
configure_init (int argc, char **argv, GError ** error)
{
  xmlDocPtr xml;
  xmlNodePtr node;

  DSGlobal *data;
  gchar **items = NULL;

  GOptionContext *context;
  GOptionEntry entries[] = {
    {G_OPTION_REMAINING, 0, 0, G_OPTION_ARG_FILENAME_ARRAY, &items, NULL,
     NULL},
    {NULL}
  };

  /* Menu */
  context = g_option_context_new ("configFile");
  g_option_context_add_main_entries (context, entries, NULL);

  if (g_option_context_parse (context, &argc, &argv, error) == FALSE)
    {
      g_option_context_free (context);
      return NULL;
    }

  g_option_context_free (context);

  /* New data struct: */
  data = g_malloc0 (sizeof (DSGlobal));

  if (!items || !items[0])
    data->configfile = g_strdup (DS_CONFIG_FILE);

  else if (!items[1])
    data->configfile = g_strdup (items[0]);

  else
    {
      g_strfreev (items);
      configure_free (data);

      g_set_error (error, dupin_server_common_error_quark (), 0, "Only one configFile");
      return NULL;
    }

  g_strfreev (items);

  /* Parsing del file */
  if (!(xml = xmlParseFile (data->configfile))
      || !(node = xmlDocGetRootElement (xml))
      || xmlStrcmp (node->name, (xmlChar *) DS_ROOT_TAG))
    {
      g_set_error (error, dupin_server_common_error_quark (), 0,
		   "Error parsing the XML file: %s", data->configfile);

      if (xml)
	xmlFreeDoc (xml);

      configure_free (data);
      return NULL;
    }

  /* Global section: */
  if (configure_parser (xml, data, error) == FALSE)
    {
      xmlFreeDoc (xml);
      configure_free (data);
      return NULL;
    }

  /* Httpd section: */
  if (configure_httpd_parser (xml, data, error) == FALSE)
    {
      xmlFreeDoc (xml);
      configure_free (data);
      return NULL;
    }

  /* Limit section: */
  if (configure_limit_parser (xml, data, error) == FALSE)
    {
      xmlFreeDoc (xml);
      configure_free (data);
      return NULL;
    }

  xmlFreeDoc (xml);

#if GLIB_CHECK_VERSION (2,31,3)
  data->httpd_mutex = g_new0 (GMutex, 1);
  g_mutex_init (data->httpd_mutex);
#else
  data->httpd_mutex = g_mutex_new ();
#endif

  return data;
}

static gboolean
configure_parser (xmlDocPtr xml, DSGlobal * data, GError ** error)
{
  xmlNodePtr cur;
  xmlChar *tmp;

  cur = xmlDocGetRootElement (xml);

  data->sqlite_db_mode = DP_SQLITE_OPEN_READWRITE;
  data->sqlite_linkb_mode = DP_SQLITE_OPEN_READWRITE;
  data->sqlite_attachment_db_mode = DP_SQLITE_OPEN_READWRITE;
  data->sqlite_view_mode = DP_SQLITE_OPEN_READWRITE;

  for (cur = cur->children; cur; cur = cur->next)
    {
      if (cur->type == XML_ELEMENT_NODE
	  && !xmlStrcmp (cur->name, (xmlChar *) DS_GENERAL_TAG))
	{
	  for (cur = cur->children; cur; cur = cur->next)
	    {
	      if (cur->type == XML_ELEMENT_NODE)
		{
		  /* Log file: */
		  if (!xmlStrcmp (cur->name, (xmlChar *) DS_LOGFILE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->logfile = g_strdup ((gchar *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* Log Verbose: */
		  else
		    if (!xmlStrcmp (cur->name, (xmlChar *) DS_LOGVERBOSE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  if (configure_log_verbose
			      (tmp, &data->logverbose, error) == FALSE)
			    {
			      xmlFree (tmp);
			      return FALSE;
			    }

			  xmlFree (tmp);
			}
		    }

		  /* Background: */
		  else
		    if (!xmlStrcmp (cur->name, (xmlChar *) DS_BACKGROUND_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  if (!xmlStrcmp (tmp, (xmlChar *) "true"))
			    data->background = TRUE;

			  xmlFree (tmp);
			}
		    }

		  /* Pid file: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_PIDFILE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->pidfile = g_strdup ((gchar *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* User: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_USER_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->user = g_strdup ((gchar *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* Group: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_GROUP_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->group = g_strdup ((gchar *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* NOTE - general SQLite params */

		  /* SQLite path: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_PATH_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
                        {
                          data->sqlite_path = g_strdup ((gchar *) tmp);
                          xmlFree (tmp);
                        }
		    }

		  /* SQLite mode: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_MODE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  if (!xmlStrcmp (tmp, (xmlChar *) "readonly"))
			    {
			      data->sqlite_db_mode = DP_SQLITE_OPEN_READONLY;
			      data->sqlite_linkb_mode = DP_SQLITE_OPEN_READONLY;
			      data->sqlite_attachment_db_mode = DP_SQLITE_OPEN_READONLY;
			      data->sqlite_view_mode = DP_SQLITE_OPEN_READONLY;
			    }

			  else if (!xmlStrcmp (tmp, (xmlChar *) "readwrite"))
			    {
			      data->sqlite_db_mode = DP_SQLITE_OPEN_READWRITE;
			      data->sqlite_linkb_mode = DP_SQLITE_OPEN_READWRITE;
			      data->sqlite_attachment_db_mode = DP_SQLITE_OPEN_READWRITE;
			      data->sqlite_view_mode = DP_SQLITE_OPEN_READWRITE;
			    }

			  else if (!xmlStrcmp (tmp, (xmlChar *) "create"))
			    {
			      data->sqlite_db_mode = DP_SQLITE_OPEN_CREATE;
			      data->sqlite_linkb_mode = DP_SQLITE_OPEN_CREATE;
			      data->sqlite_attachment_db_mode = DP_SQLITE_OPEN_CREATE;
			      data->sqlite_view_mode = DP_SQLITE_OPEN_CREATE;
			    }

			  xmlFree (tmp);
			}
		    }

		  /* SQLite allow: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_ALLOW_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_allow = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite deny: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_DENY_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_deny = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite connect: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_CONNECT_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_connect = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* NOTE - database specific SQLite params */

		  /* SQLite database mode: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_DB_MODE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  if (!xmlStrcmp (tmp, (xmlChar *) "readonly"))
			    data->sqlite_db_mode = DP_SQLITE_OPEN_READONLY;

			  else if (!xmlStrcmp (tmp, (xmlChar *) "readwrite"))
			    data->sqlite_db_mode = DP_SQLITE_OPEN_READWRITE;

			  else if (!xmlStrcmp (tmp, (xmlChar *) "create"))
			    data->sqlite_db_mode = DP_SQLITE_OPEN_CREATE;

			  xmlFree (tmp);
			}
		    }

		  /* SQLite database allow: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_DB_ALLOW_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_db_allow = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite database deny: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_DB_DENY_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_db_deny = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite database connect: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_DB_CONNECT_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_db_connect = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* NOTE - linkbase specific SQLite params */

		  /* SQLite linkbase mode: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_LINKB_MODE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  if (!xmlStrcmp (tmp, (xmlChar *) "readonly"))
			    data->sqlite_linkb_mode = DP_SQLITE_OPEN_READONLY;

			  else if (!xmlStrcmp (tmp, (xmlChar *) "readwrite"))
			    data->sqlite_linkb_mode = DP_SQLITE_OPEN_READWRITE;

			  else if (!xmlStrcmp (tmp, (xmlChar *) "create"))
			    data->sqlite_linkb_mode = DP_SQLITE_OPEN_CREATE;

			  xmlFree (tmp);
			}
		    }

		  /* SQLite linkbase allow: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_LINKB_ALLOW_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_linkb_allow = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite linkbase deny: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_LINKB_DENY_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_linkb_deny = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite linkbase connect: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_LINKB_CONNECT_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_linkb_connect = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* NOTE - attachment database specific SQLite params */

		  /* SQLite attachment database mode: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_ATTACHMENT_DB_MODE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  if (!xmlStrcmp (tmp, (xmlChar *) "readonly"))
			    data->sqlite_attachment_db_mode = DP_SQLITE_OPEN_READONLY;

			  else if (!xmlStrcmp (tmp, (xmlChar *) "readwrite"))
			    data->sqlite_attachment_db_mode = DP_SQLITE_OPEN_READWRITE;

			  else if (!xmlStrcmp (tmp, (xmlChar *) "create"))
			    data->sqlite_attachment_db_mode = DP_SQLITE_OPEN_CREATE;

			  xmlFree (tmp);
			}
		    }

		  /* SQLite attachment database allow: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_ATTACHMENT_DB_ALLOW_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_attachment_db_allow = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite attachment database deny: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_ATTACHMENT_DB_DENY_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_attachment_db_deny = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite attachment database connect: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_ATTACHMENT_DB_CONNECT_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_attachment_db_connect = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* NOTE - view specific SQLite params */

		  /* SQLite view mode: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_VIEW_MODE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  if (!xmlStrcmp (tmp, (xmlChar *) "readonly"))
			    data->sqlite_view_mode = DP_SQLITE_OPEN_READONLY;

			  else if (!xmlStrcmp (tmp, (xmlChar *) "readwrite"))
			    data->sqlite_view_mode = DP_SQLITE_OPEN_READWRITE;

			  else if (!xmlStrcmp (tmp, (xmlChar *) "create"))
			    data->sqlite_view_mode = DP_SQLITE_OPEN_CREATE;

			  xmlFree (tmp);
			}
		    }

		  /* SQLite view allow: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_VIEW_ALLOW_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_view_allow = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite view deny: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_VIEW_DENY_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_view_deny = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		  /* SQLite view connect: */
		  else if (!xmlStrcmp (cur->name, (xmlChar *) DS_SQLITE_VIEW_CONNECT_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->sqlite_view_connect = g_regex_new ((gchar *) tmp, 0, 0, NULL);

			  xmlFree (tmp);
			}
		    }

		}
	    }

	  break;
	}
    }

  if (!data->logfile)
    {
      g_set_error (error, dupin_server_common_error_quark (), 0,
		   "No LogFile tag in the config file.");
      return FALSE;
    }

  if (!data->pidfile)
    {
      g_set_error (error, dupin_server_common_error_quark (), 0,
		   "No PidFile tag in the config file.");
      return FALSE;
    }

  return TRUE;
}

static gboolean
configure_log_verbose (xmlChar * string, LogVerbose * verbose,
		       GError ** error)
{
  if (!xmlStrcmp (string, (xmlChar *) "error"))
    *verbose = LOG_VERBOSE_ERROR;

  else if (!xmlStrcmp (string, (xmlChar *) "warning"))
    *verbose = LOG_VERBOSE_WARNING;

  else if (!xmlStrcmp (string, (xmlChar *) "info"))
    *verbose = LOG_VERBOSE_INFO;

  else if (!xmlStrcmp (string, (xmlChar *) "debug"))
    *verbose = LOG_VERBOSE_DEBUG;

  else
    {
      g_set_error (error, dupin_server_common_error_quark (), 0,
		   "LogVerbose can be 'error', 'warning', 'info', 'debug' and not '%s'.",
		   string);
      return FALSE;
    }

  return TRUE;
}

static gboolean
configure_httpd_parser (xmlDocPtr xml, DSGlobal * data, GError ** error)
{
  xmlNodePtr cur;
  xmlChar *tmp;

  cur = xmlDocGetRootElement (xml);

  for (cur = cur->children; cur; cur = cur->next)
    {
      if (cur->type == XML_ELEMENT_NODE
	  && !xmlStrcmp (cur->name, (xmlChar *) DS_HTTPD_TAG))
	{
	  for (cur = cur->children; cur; cur = cur->next)
	    {
	      if (cur->type == XML_ELEMENT_NODE)
		{
		  /* Interface: */
		  if (!xmlStrcmp
		      (cur->name, (xmlChar *) DS_HTTPD_INTERFACE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->httpd_interface = g_strdup ((gchar *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* Port: */
		  else
		    if (!xmlStrcmp (cur->name, (xmlChar *) DS_HTTPD_PORT_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->httpd_port = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* Listen: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_HTTPD_LISTEN_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->httpd_listen = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* ipv6: */
		  else
		    if (!xmlStrcmp (cur->name, (xmlChar *) DS_HTTPD_IPV6_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  if (!xmlStrcmp (tmp, (xmlChar *) "true"))
			    data->httpd_ipv6 = TRUE;

			  xmlFree (tmp);
			}
		    }
		}
	    }

	  break;
	}
    }

  if (!data->httpd_port)
    {
      g_set_error (error, dupin_server_common_error_quark (), 0,
		   "No Port tag in the config file.");
      return FALSE;
    }

  if (!data->httpd_listen)
    data->httpd_listen = DS_HTTPD_LISTEN_DEFAULT;

  return TRUE;
}

static gboolean
configure_limit_parser (xmlDocPtr xml, DSGlobal * data, GError ** error)
{
  xmlNodePtr cur;
  xmlChar *tmp;

  cur = xmlDocGetRootElement (xml);

  for (cur = cur->children; cur; cur = cur->next)
    {
      if (cur->type == XML_ELEMENT_NODE
	  && !xmlStrcmp (cur->name, (xmlChar *) DS_LIMIT_TAG))
	{
	  for (cur = cur->children; cur; cur = cur->next)
	    {
	      if (cur->type == XML_ELEMENT_NODE)
		{
		  /* ClientsForThread: */
		  if (!xmlStrcmp
		      (cur->name, (xmlChar *) DS_LIMIT_CLIENTSFORTHREAD_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_clientsforthread = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* MaxHeaders: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_MAXHEADERS_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_maxheaders = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* MaxClients: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_MAXCLIENTS_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_maxclients = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* MaxContentLength: */
		  else
		    if (!xmlStrcmp
			(cur->name,
			 (xmlChar *) DS_LIMIT_MAXCONTENTLENGTH_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_maxcontentlength = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* timeout: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_TIMEOUT_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_timeout = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* ThreadNumb: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_THREADNUMB_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_threadnumb = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* timeoutForThread: */
		  else
		    if (!xmlStrcmp
			(cur->name,
			 (xmlChar *) DS_LIMIT_TIMEOUTFORTHREAD_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_timeoutforthread = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* cacheSize: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_CACHESIZE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_cachesize = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* cacheMaxFileSize: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_CACHEMAXFILE_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_cachemaxfilesize = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* MapMaxThreads: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_MAP_MAXTHREADS_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_map_max_threads = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* ReduceMaxThreads: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_REDUCE_MAXTHREADS_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_reduce_max_threads = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* ReduceTimeoutForThread: */
		  else
		    if (!xmlStrcmp
			(cur->name,
			 (xmlChar *) DS_LIMIT_REDUCE_TIMEOUTFORTHREAD_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_reduce_timeoutforthread = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* SyncInterval: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_SYNC_INTERVAL_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_sync_interval = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* CompactMaxThreads: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_COMPACT_MAXTHREADS_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_compact_max_threads = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }

		  /* CheckLinksMaxThreads: */
		  else
		    if (!xmlStrcmp
			(cur->name, (xmlChar *) DS_LIMIT_CHECKLINKS_MAXTHREADS_TAG))
		    {
		      if ((tmp = xmlNodeGetContent (cur)))
			{
			  data->limit_checklinks_max_threads = atoi ((char *) tmp);
			  xmlFree (tmp);
			}
		    }
		}
	    }

	  break;
	}
    }

  if (!data->limit_timeout)
    data->limit_timeout = DS_LIMIT_TIMEOUT_DEFAULT;

  if (!data->limit_clientsforthread)
    data->limit_clientsforthread = DS_LIMIT_CLIENTSFORTHREAD_DEFAULT;

  if (!data->limit_timeoutforthread)
    data->limit_timeoutforthread = DS_LIMIT_TIMEOUTFORTHREAD_DEFAULT;

  if (!data->limit_map_max_threads)
    data->limit_map_max_threads = DS_LIMIT_MAP_MAXTHREADS_DEFAULT;

  if (!data->limit_reduce_max_threads)
    data->limit_reduce_max_threads = DS_LIMIT_REDUCE_MAXTHREADS_DEFAULT;

  if (!data->limit_reduce_timeoutforthread)
    data->limit_reduce_timeoutforthread = DS_LIMIT_REDUCE_TIMEOUTFORTHREAD_DEFAULT;

  if (!data->limit_sync_interval)
    data->limit_sync_interval = DS_LIMIT_SYNC_INTERVAL_DEFAULT;

  if (!data->limit_compact_max_threads)
    data->limit_compact_max_threads = DS_LIMIT_COMPACT_MAXTHREADS_DEFAULT;

  if (!data->limit_checklinks_max_threads)
    data->limit_checklinks_max_threads = DS_LIMIT_CHECKLINKS_MAXTHREADS_DEFAULT;

  if (!data->sqlite_path)
    data->sqlite_path = g_strdup (DUPIN_DB_PATH);

  return TRUE;
}

/* FREE ********************************************************************/
void
configure_free (DSGlobal * data)
{
  if (!data)
    return;

  if (data->configfile)
    g_free (data->configfile);

  if (data->logfile)
    g_free (data->logfile);

  if (data->pidfile)
    g_free (data->pidfile);

  if (data->user)
    g_free (data->user);

  if (data->group)
    g_free (data->group);

  if (data->httpd_interface)
    g_free (data->httpd_interface);

  if (data->httpd_mutex)
    {
#if GLIB_CHECK_VERSION (2,31,3)
      g_mutex_clear (data->httpd_mutex);
      g_free (data->httpd_mutex);
#else
      g_mutex_free (data->httpd_mutex);
#endif
    }

  if (data->sqlite_path)
    g_free (data->sqlite_path);

  if (data->sqlite_allow)
    g_regex_unref (data->sqlite_allow);

  if (data->sqlite_deny)
    g_regex_unref (data->sqlite_deny);

  if (data->sqlite_connect)
    g_regex_unref (data->sqlite_connect);

  if (data->sqlite_db_allow)
    g_regex_unref (data->sqlite_db_allow);

  if (data->sqlite_db_deny)
    g_regex_unref (data->sqlite_db_deny);

  if (data->sqlite_db_connect)
    g_regex_unref (data->sqlite_db_connect);

  if (data->sqlite_linkb_allow)
    g_regex_unref (data->sqlite_linkb_allow);

  if (data->sqlite_linkb_deny)
    g_regex_unref (data->sqlite_linkb_deny);

  if (data->sqlite_linkb_connect)
    g_regex_unref (data->sqlite_linkb_connect);

  if (data->sqlite_attachment_db_allow)
    g_regex_unref (data->sqlite_attachment_db_allow);

  if (data->sqlite_attachment_db_deny)
    g_regex_unref (data->sqlite_attachment_db_deny);

  if (data->sqlite_attachment_db_connect)
    g_regex_unref (data->sqlite_attachment_db_connect);

  if (data->sqlite_view_allow)
    g_regex_unref (data->sqlite_view_allow);

  if (data->sqlite_view_deny)
    g_regex_unref (data->sqlite_view_deny);

  if (data->sqlite_view_connect)
    g_regex_unref (data->sqlite_view_connect);

  g_free (data);
}

/* EOF */
