#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin.h"
#include "dupin_internal.h"

#include "map.h"
#include "request.h"

#include "../tbjsonpath/tb_jsonpath.h"

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include <string.h>
#include <stdlib.h>

void request_set_error (DSHttpdClient * client, gchar * msg);
void request_clear_error (DSHttpdClient * client);
gchar * request_get_error (DSHttpdClient * client);

void request_set_warning (DSHttpdClient * client, gchar * msg);
void request_clear_warning (DSHttpdClient * client);
gchar * request_get_warning (DSHttpdClient * client);

static JsonNode *request_record_revision_obj (DSHttpdClient * client,
					      GList * arguments,
					      DupinRecord * record, gchar * id,
					      gchar * mvcc,
				              gboolean visit_links);

static JsonNode *request_link_record_revision_obj (DSHttpdClient * client,
						   GList * arguments,
						   DupinLinkRecord * record, gchar * id,
						   gchar * mvcc,
						   gboolean visit_docs);

static JsonNode *request_view_record_obj (DSHttpdClient * client,
					  GList * arguments,
					  DupinViewRecord * record,
				 	  gchar * id,
					  gboolean visit_docs);

static gboolean request_record_response (DSHttpdClient * client,
					 GList * response_list,
			 		 gboolean is_bulk);

/* WWW FUNCTION *************************************************************/
static DSHttpStatusCode
request_www (DSHttpdClient * client, GList * paths, GList * arguments)
{
  gchar *path = NULL;
  struct stat st;
  DSMap *map;

  if (!paths)
    {
      path =
	g_build_path (G_DIR_SEPARATOR_S, DS_WWW_PATH, HTTP_INDEX_HTML, NULL);

      if (g_file_test (path, G_FILE_TEST_EXISTS | G_FILE_TEST_IS_REGULAR) ==
	  FALSE)
	{
          request_set_error (client, "www path does not exist or is invalid");
	  g_free (path);
	  return HTTP_STATUS_403;
	}
    }

  else
    {
      gchar *newpath;

      for (; paths; paths = paths->next)
	{
	  if (!g_strcmp0 (paths->data, ".."))
	    {
              request_set_error (client, "www path is not '..'");
	      if (path)
		g_free (path);
	      return HTTP_STATUS_403;
	    }

	  if (!path)
	    newpath =
	      g_build_path (G_DIR_SEPARATOR_S, DS_WWW_PATH, paths->data,
			    NULL);
	  else
	    {
	      newpath =
		g_build_path (G_DIR_SEPARATOR_S, path, paths->data, NULL);
	      g_free (path);
	    }

	  if (g_file_test (newpath, G_FILE_TEST_EXISTS) == FALSE)
	    {
              request_set_error (client, "www path does not exist");
	      g_free (newpath);
	      return HTTP_STATUS_403;
	    }

	  path = newpath;
	}

      if (g_file_test (path, G_FILE_TEST_IS_DIR) == TRUE)
	{
	  gchar *newpath =
	    g_build_path (G_DIR_SEPARATOR_S, path, HTTP_INDEX_HTML, NULL);
	  g_free (path);

	  if (g_file_test (path, G_FILE_TEST_EXISTS | G_FILE_TEST_IS_REGULAR)
	      == FALSE)
	    {
              request_set_error (client, "www path does not exist or is invalid");
	      g_free (newpath);
	      return HTTP_STATUS_403;
	    }

	  path = newpath;
	}
    }

  if (g_stat (path, &st) != 0)
    {
      request_set_error (client, "www path does match");
      g_free (path);
      return HTTP_STATUS_403;
    }

  /* Try with a map: */
  if ((map = map_find (client->thread->data, path, st.st_mtime)))
    {
      client->output_type = DS_HTTPD_OUTPUT_MAP;

      client->output.map.contents = g_mapped_file_get_contents (map->map);
      client->output_size = g_mapped_file_get_length (map->map);
      client->output_mime = g_strdup (map->mime);
      client->output.map.map = map;
    }

  else
    {
      client->output_type = DS_HTTPD_OUTPUT_IO;
      client->output_size = st.st_size;

      if (!(client->output_mime = g_content_type_guess (path, NULL, 0, NULL)))
	client->output_mime = g_strdup (HTTP_MIME_TEXTHTML);

      if (!(client->output.io.io = g_io_channel_new_file (path, "r", NULL)))
	{
          request_set_error (client, "cannot open www path for reading");
	  g_free (path);
	  return HTTP_STATUS_403;
	}

      g_io_channel_set_encoding (client->output.io.io, NULL, NULL);
    }

  g_free (path);
  return HTTP_STATUS_200;
}

/* QUIT FUNCTION ***********************************************************/
static DSHttpStatusCode
request_quit (DSHttpdClient * client, GList * paths, GList * arguments)
{
  if (g_strcmp0 (client->ip, "127.0.0.1")
      && g_strcmp0 (client->ip, "localhost"))
    {
      request_set_error (client, "Quit command forbidden");
      return HTTP_STATUS_403;
    }

  /* TODO - return an OK status but need to shuffle this into httpd.c probably on httpd_client_free() */

  g_main_loop_quit (client->thread->data->loop);

  return HTTP_STATUS_200;
}

/* STATUS FUNCTION *********************************************************/
static DSHttpStatusCode
request_status (DSHttpdClient * client, GList * paths, GList * arguments)
{
  JsonObject *obj;
  JsonObject *nobj;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  GTimeVal tv;

  obj = json_object_new ();

  if (obj == NULL)
    {
      request_set_error (client, "Cannot return status");
      return HTTP_STATUS_500;
    }

  g_mutex_lock (client->thread->data->httpd_mutex);

  /* Start TimeVal: */
  nobj = json_object_new ();

  if (nobj == NULL)
    goto request_status_quit;

  /* timeval:tv_sec: */
  json_object_set_int_member (nobj, "sec", client->thread->data->start_timeval.tv_sec );

  /* timeval:tv_usec: */
  json_object_set_int_member (nobj, "usec", client->thread->data->start_timeval.tv_usec );

  json_object_set_object_member (obj, "startTimeVal", nobj );

  /* This TimeVal: */
  nobj = json_object_new ();

  if (nobj == NULL)
    goto request_status_quit;

#if GLIB_CHECK_VERSION (2, 27, 3)
  gint64 timestamp;
  timestamp = g_source_get_time (client->channel_source);
  tv.tv_sec = timestamp / 1000000;
  tv.tv_usec = timestamp % 1000000;
#else
  g_source_get_current_time (client->channel_source, &tv);
#endif

  /* timeval:tv_sec: */
  json_object_set_int_member (nobj, "sec", tv.tv_sec );

  /* timeval:tv_usec: */
  json_object_set_int_member (nobj, "usec", tv.tv_usec );

  json_object_set_object_member (obj, "thisTimeVal", nobj );

  /* Number of threads: */
  json_object_set_int_member (obj, "threads", client->thread->data->httpd_threads_numb);

  /* Number of clients: */
  json_object_set_int_member (obj, "clients", client->thread->data->httpd_clients_numb);

  /* Limit obj: */
  nobj = json_object_new ();

  if (nobj == NULL)
    goto request_status_quit;

  /* Max Headers: */
  json_object_set_int_member (nobj, "maxHeaders", client->thread->data->limit_maxheaders);

  /* Max Clients: */
  json_object_set_int_member (nobj, "maxClients", client->thread->data->limit_maxclients);

  /* Max Content-Length: */
  json_object_set_int_member (nobj, "maxContentLength", client->thread->data->limit_maxcontentlength);

  /* Number of clients for Thread: */
  json_object_set_int_member (nobj, "clientsForThread", client->thread->data->limit_clientsforthread);

  /* Timeout: */
  json_object_set_int_member (nobj, "timeout", client->thread->data->limit_timeout);

  /* Timeout for thread: */
  json_object_set_int_member (nobj, "timeoutForThread", client->thread->data->limit_timeoutforthread);

  json_object_set_object_member (obj, "limits", nobj );

  /* Httpd obj: */
  nobj = json_object_new ();

  if (nobj == NULL)
    goto request_status_quit;

  /* Interface: */
  if (client->thread->data->httpd_interface != NULL)
    json_object_set_string_member (nobj, "interface", client->thread->data->httpd_interface);
  else
    json_object_set_null_member (nobj, "interface");

  /* port: */
  json_object_set_int_member (nobj, "port", client->thread->data->httpd_port);

  /* listen: */
  json_object_set_int_member (nobj, "listen", client->thread->data->httpd_listen);

  /* ipv6: */
  json_object_set_boolean_member (nobj, "ipv6", client->thread->data->httpd_ipv6);

  json_object_set_object_member (obj, "httpd", nobj );

  /* Serialize: */
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_status_quit;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_status_quit;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_status_quit;

  client->output_mime = g_strdup (HTTP_MIME_JSON);

  g_mutex_unlock (client->thread->data->httpd_mutex);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  return HTTP_STATUS_200;

request_status_quit:
  g_mutex_unlock (client->thread->data->httpd_mutex);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  request_set_error (client, "Cannot return status");
  return HTTP_STATUS_500;
}

/* GLOBAL ******************************************************************/
static DSHttpStatusCode request_global_get (DSHttpdClient * client,
					    GList * path, GList * arguments);
static DSHttpStatusCode request_global_post (DSHttpdClient * client,
					     GList * path, GList * arguments);
static DSHttpStatusCode request_global_put (DSHttpdClient * client,
					    GList * path, GList * arguments);
static DSHttpStatusCode request_global_delete (DSHttpdClient * client,
					       GList * path,
					       GList * arguments);

DSHttpStatusCode
request_global (DSHttpdClient * client, GList * path, GList * arguments)
{
  switch (client->request)
    {
    case DS_HTTPD_REQUEST_GET:
      return request_global_get (client, path, arguments);

    case DS_HTTPD_REQUEST_POST:
      return request_global_post (client, path, arguments);
      break;

    case DS_HTTPD_REQUEST_PUT:
      return request_global_put (client, path, arguments);

    case DS_HTTPD_REQUEST_DELETE:
      return request_global_delete (client, path, arguments);
    }

  request_set_error (client, "Allowed methods: GET, POST, PUT or DELETE");

  return HTTP_STATUS_400;
}

/* GET *********************************************************************/

static DSHttpStatusCode request_global_get_server_info (DSHttpdClient * client,
							GList * path,
                             				GList * arguments);
static DSHttpStatusCode request_global_get_all_dbs (DSHttpdClient * client,
						    GList * paths,
						    GList * arguments);
static DSHttpStatusCode request_global_get_all_linkbs (DSHttpdClient * client,
						    GList * paths,
						    GList * arguments);
static DSHttpStatusCode request_global_get_all_attachment_dbs (DSHttpdClient * client,
						    GList * paths,
						    GList * arguments);
static DSHttpStatusCode request_global_get_all_views (DSHttpdClient * client,
						      GList * paths,
						      GList * arguments);
static DSHttpStatusCode request_global_get_all_docs (DSHttpdClient * client,
						     GList * paths,
						     GList * arguments);
static DSHttpStatusCode request_global_get_database (DSHttpdClient * client,
						     GList * path,
						     GList * arguments);
static DSHttpStatusCode request_global_get_record (DSHttpdClient * client,
						   GList * paths,
						   GList * arguments);
static DSHttpStatusCode request_global_get_view (DSHttpdClient * client,
						 GList * path,
						 GList * arguments);
static DSHttpStatusCode request_global_get_changes_database (DSHttpdClient * client,
						    GList * paths,
						    GList * arguments);

static DSHttpStatusCode request_global_get_linkbase (DSHttpdClient * client,
						    GList * paths,
						    GList * arguments);
static DSHttpStatusCode request_global_get_all_links_linkbase (DSHttpdClient * client,
						    GList * paths,
						    GList * arguments);

static DSHttpStatusCode request_global_get_changes_linkbase (DSHttpdClient * client,
						    GList * paths,
						    GList * arguments);

static DSHttpStatusCode request_global_get_record_linkbase (DSHttpdClient * client,
						    GList * paths,
						    GList * arguments);
static DSHttpStatusCode request_global_get_linkbase_query (DSHttpdClient *
							   client,
							   GList * path,
							   gchar * query);

static DSHttpStatusCode request_global_get_all_docs_view (DSHttpdClient *
							  client,
							  GList * paths,
							  GList * arguments);
static DSHttpStatusCode request_global_get_record_view (DSHttpdClient *
							client, GList * paths,
							GList * arguments);
static DSHttpStatusCode request_global_get_database_query (DSHttpdClient *
							   client,
							   GList * path,
							   gchar * query);
static DSHttpStatusCode request_global_get_view_query (DSHttpdClient * client,
						       GList * path,
						       gchar * query);

static DSHttpStatusCode request_global_view_sync (DSHttpdClient * client,
						  GList * path,
						  GList * arguments);

static DSHttpStatusCode request_global_get_uuids (DSHttpdClient * client,
						   GList * path,
						   GList * arguments);

/* PORTABLE LISTINGS */

static DSHttpStatusCode request_global_get_portable_listings (DSHttpdClient * client,
						     	      GList * paths,
						     	      GList * arguments);

static DSHttpStatusCode request_global_get_portable_listings_record (DSHttpdClient * client,
						     	      	     GList * paths,
						     	      	     GList * arguments);

static DSHttpStatusCode request_global_get_portable_listings_record_relationship (DSHttpdClient * client,
						     	      	     		  GList * paths,
						     	      	     		  GList * arguments);

static DSHttpStatusCode
request_global_get (DSHttpdClient * client, GList * path, GList * arguments)
{
  if (!path)
    return request_global_get_server_info (client, path, arguments);

  /* GET /_all_dbs */
  if (!g_strcmp0 (path->data, REQUEST_ALL_DBS))
    return request_global_get_all_dbs (client, path, arguments);

  /* GET /_all_linkbs */
  if (!g_strcmp0 (path->data, REQUEST_ALL_LINKBS))
    return request_global_get_all_linkbs (client, path, arguments);

  /* GET /_all_attachment_dbs */
  if (!g_strcmp0 (path->data, REQUEST_ALL_ATTACH_DBS))
    return request_global_get_all_attachment_dbs (client, path, arguments);

  /* GET /_all_views */
  if (!g_strcmp0 (path->data, REQUEST_ALL_VIEWS))
    return request_global_get_all_views (client, path, arguments);

  /* GET /_uuids */
  if (!g_strcmp0 (path->data, REQUEST_UUIDS))
    return request_global_get_uuids (client, path, arguments);

  /* GET /database */
  if (!path->next)
    return request_global_get_database (client, path, arguments);

  /* GET /database/_changes */
  if (!path->next->next && !g_strcmp0 (path->next->data, REQUEST_ALL_CHANGES))
    return request_global_get_changes_database (client, path, arguments);

  /* GET /database/_all_docs */
  if (!path->next->next && !g_strcmp0 (path->next->data, REQUEST_ALL_DOCS))
    return request_global_get_all_docs (client, path, arguments);

  if (!g_strcmp0 (path->next->data, REQUEST_PORTABLE_LISTINGS))
    {
      if (path->next->next)
        {
          if (path->next->next->next)
            {
              /* GET /database/_portable_listings/id/relationship */
              return request_global_get_portable_listings_record_relationship (client, path, arguments);
	    }
	  else
	    {
              /* GET /database/_portable_listings/id */
              return request_global_get_portable_listings_record (client, path, arguments);
	    }
	}
      else
	{
          /* GET /database/_portable_listings */
          return request_global_get_portable_listings (client, path, arguments);
	}
    }

  if (!g_strcmp0 (path->data, REQUEST_LINKBS))
    {
      /* GET /_linkbs/linkbase */
      if (!path->next->next)
	return request_global_get_linkbase (client, path->next, arguments);

      /* GET /_linkbs/linkbase/_all_links */
      if (!g_strcmp0 (path->next->next->data, REQUEST_ALL_LINKS))
        return request_global_get_all_links_linkbase (client, path->next, arguments);

      /* GET /_linkbs/linkbase/_changes */
      if (!g_strcmp0 (path->next->next->data, REQUEST_ALL_CHANGES))
        return request_global_get_changes_linkbase (client, path->next, arguments);

      /* GET /_linkbs/linkbase/id */
      return request_global_get_record_linkbase (client, path->next, arguments);

      request_set_error (client, "Linkbases GET allowed commands: /_linkbs/linkbase, /_linkbs/linkbase/_all_links, /_linkbs/linkbase/_changes or /_linkbs/linkbase/id");

      return HTTP_STATUS_400;
    }

  if (!g_strcmp0 (path->data, REQUEST_VIEWS))
    {
      /* GET /_views/view */
      if (!path->next->next)
	return request_global_get_view (client, path, arguments);

      else if (!path->next->next->next)
	{
	  /* GET /_views/view/_all_docs */
	  if (!g_strcmp0 (path->next->next->data, REQUEST_ALL_DOCS))
	    return request_global_get_all_docs_view (client, path, arguments);

	  /* GET /_views/view/_sync */
	  if (!g_strcmp0 (path->next->next->data, REQUEST_SYNC))
	    return request_global_view_sync (client, path, arguments);

	  /* GET /_views/view/id */
	  return request_global_get_record_view (client, path, arguments);
	}

      request_set_error (client, "Views GET allowed commands: /_views/view/_all_docs, /_views/view/_sync or /_views/view/id");

      return HTTP_STATUS_400;
    }

  /* GET /database/id */
  return request_global_get_record (client, path, arguments);

  request_set_error (client, "Record GET allowed commands: /database/id");

  return HTTP_STATUS_400;
}

static DSHttpStatusCode
request_global_get_uuids (DSHttpdClient * client, GList * path,
			   GList * arguments)
{
  GList *list;

  guint count = 1;
  guint i;

  JsonArray *array;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_UUIDS_COUNT))
	count = atoi (kv->value);
    }

  if (!count)
    {
      request_set_error (client, "Cannot return UUIDs");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      request_set_error (client, "Cannot return UUIDs");
      return HTTP_STATUS_500;
    }

  for (i = 0; i < count; i++)
    {
      gchar id[DUPIN_ID_MAX_LEN];

      dupin_util_generate_id (id);

      json_array_add_string_element (array, id);
    }

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    {
      json_array_unref (array);
      goto request_global_get_uuids_error;
    }

  JsonObject * node_obj = json_object_new ();

  json_node_take_object (node, node_obj);
 
  json_object_set_array_member (node_obj, "uuids", array);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_uuids_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_uuids_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  return HTTP_STATUS_200;

request_global_get_uuids_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  request_set_error (client, "Cannot return UUIDs");
  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_all_dbs (DSHttpdClient * client, GList * paths,
			    GList * arguments)
{
  guint i;
  gchar **dbs;
  JsonArray *array;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  if (client->request != DS_HTTPD_REQUEST_GET)
    {
      request_set_error (client, "Database /_all_dbs allows only GET method");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      request_set_error (client, "Cannot return list of databases");
      return HTTP_STATUS_500;
    }

  dbs = dupin_get_databases (client->thread->data->dupin);

  for (i = 0; dbs && dbs[i]; i++)
    {
      json_array_add_string_element (array, dbs[i]);
    }

  client->output_type = DS_HTTPD_OUTPUT_STRING;
  client->output_mime = g_strdup (HTTP_MIME_JSON);

  node = json_node_new (JSON_NODE_ARRAY);

  if (node == NULL)
    goto request_global_get_all_dbs;

  json_node_set_array (node, array);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_all_dbs;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_all_dbs;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  g_strfreev (dbs);
  return HTTP_STATUS_200;

request_global_get_all_dbs:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  g_strfreev (dbs);
  request_set_error (client, "Cannot return list of databases");
  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_all_linkbs (DSHttpdClient * client, GList * paths,
			    GList * arguments)
{
  guint i;
  gchar **linkbs;
  JsonArray *array;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  if (client->request != DS_HTTPD_REQUEST_GET)
    {
      request_set_error (client, "Linkabse /_all_linkbs allows only GET method");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      request_set_error (client, "Cannot return list of linkbases");
      return HTTP_STATUS_500;
    }

  linkbs = dupin_get_linkbases (client->thread->data->dupin);

  for (i = 0; linkbs && linkbs[i]; i++)
    {
      json_array_add_string_element (array, linkbs[i]);
    }

  client->output_type = DS_HTTPD_OUTPUT_STRING;
  client->output_mime = g_strdup (HTTP_MIME_JSON);

  node = json_node_new (JSON_NODE_ARRAY);

  if (node == NULL)
    goto request_global_get_all_linkbs;

  json_node_set_array (node, array);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_all_linkbs;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_all_linkbs;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  g_strfreev (linkbs);
  return HTTP_STATUS_200;

request_global_get_all_linkbs:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  g_strfreev (linkbs);
  request_set_error (client, "Cannot return list of linkbases");
  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_all_attachment_dbs (DSHttpdClient * client, GList * paths,
			    GList * arguments)
{
  guint i;
  gchar **attachment_dbs;
  JsonArray *array;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  if (client->request != DS_HTTPD_REQUEST_GET)
    {
      request_set_error (client, "Attachment db /_all_attachment_dbs allows only GET method");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      request_set_error (client, "Cannot return list of attachment databases");
      return HTTP_STATUS_500;
    }

  attachment_dbs = dupin_get_attachment_dbs (client->thread->data->dupin);

  for (i = 0; attachment_dbs && attachment_dbs[i]; i++)
    {
      json_array_add_string_element (array, attachment_dbs[i]);
    }

  client->output_type = DS_HTTPD_OUTPUT_STRING;
  client->output_mime = g_strdup (HTTP_MIME_JSON);

  node = json_node_new (JSON_NODE_ARRAY);

  if (node == NULL)
    goto request_global_get_all_attachment_dbs;

  json_node_set_array (node, array);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_all_attachment_dbs;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_all_attachment_dbs;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  g_strfreev (attachment_dbs);
  return HTTP_STATUS_200;

request_global_get_all_attachment_dbs:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  g_strfreev (attachment_dbs);
  request_set_error (client, "Cannot return list of attachment databases");
  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_all_views (DSHttpdClient * client, GList * paths,
			      GList * arguments)
{
  guint i;
  gchar **views;
  JsonArray *array;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  if (client->request != DS_HTTPD_REQUEST_GET)
    {
      request_set_error (client, "Views /_all_views allows only GET method");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      request_set_error (client, "Cannot return list of views");
      return HTTP_STATUS_500;
    }

  views = dupin_get_views (client->thread->data->dupin);

  for (i = 0; views && views[i]; i++)
    {
      json_array_add_string_element  (array, views[i]);
    }

  client->output_type = DS_HTTPD_OUTPUT_STRING;
  client->output_mime = g_strdup (HTTP_MIME_JSON);

  node = json_node_new (JSON_NODE_ARRAY);

  if (node == NULL)
    goto request_global_get_all_views;

  json_node_set_array (node, array);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_all_views;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_all_views;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  g_strfreev (views);
  return HTTP_STATUS_200;

request_global_get_all_views:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  g_strfreev (views);
  request_set_error (client, "Cannot return list of views");
  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_changes_database (DSHttpdClient * client, GList * path,
			    GList * arguments)
{
  DupinDB *db;

  GList *list;
  GList *results;

  gboolean descending = FALSE;
  guint count = DUPIN_DB_MAX_CHANGES_COUNT;

  guint heartbeat=REQUEST_GET_ALL_CHANGES_HEARTBEAT_DEFAULT;
  guint timeout=REQUEST_GET_ALL_CHANGES_TIMEOUT_DEFAULT;

  gsize total_rows = 0;

  gsize last_seq=0;
  gint since = 0;
  DupinChangesType style = DP_CHANGES_MAIN_ONLY;
  DupinChangesFeedType feed = DP_CHANGES_FEED_POLL;
  gboolean include_docs = FALSE;
  gchar ** types = NULL;
  DupinFilterByType types_op = DP_FILTERBY_EQUALS;

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_LIMIT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_SINCE))
	since = (gint) g_ascii_strtoll (kv->value, NULL, 10);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_HEARTBEAT))
	heartbeat = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_TIMEOUT))
	timeout = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_STYLE)
	       && !g_strcmp0 (kv->value, "all_docs"))
        {
	  style = DP_CHANGES_ALL_DOCS;
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_FEED))
        {
	  if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_FEED_LONGPOLL))
            {
	      feed = DP_CHANGES_FEED_LONGPOLL;
            }
	  else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_FEED_CONTINUOUS))
            {
	      feed = DP_CHANGES_FEED_CONTINUOUS;
            }
	  else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_FEED_POLL))
            {
	      feed = DP_CHANGES_FEED_POLL;
            }
          else
            {
	      if (types)
                g_strfreev (types);
              request_set_error (client, "Invalid " REQUEST_GET_ALL_CHANGES_FEED " parameter. Allowed values are: " REQUEST_GET_ALL_CHANGES_FEED_LONGPOLL ", " REQUEST_GET_ALL_CHANGES_FEED_CONTINUOUS ", " REQUEST_GET_ALL_CHANGES_FEED_POLL);
              return HTTP_STATUS_400;
            }
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_INCLUDE_DOCS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
	      if (types)
                g_strfreev (types);
              request_set_error (client, "Invalid " REQUEST_GET_ALL_CHANGES_INCLUDE_DOCS " parameter. Allowed values are: true, false");
              return HTTP_STATUS_400;
            }

          include_docs = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_TYPES))
        {
          types = g_strsplit (kv->value, ",", -1);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_TYPES_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            types_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            types_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            types_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            types_op = DP_FILTERBY_PRESENT;
        }

    }

  if (feed == DP_CHANGES_FEED_LONGPOLL
      || feed == DP_CHANGES_FEED_CONTINUOUS)
    {
      if (!  (client->output.changes_comet.db =
			dupin_database_open (client->thread->data->dupin, path->data, NULL)))
        {
	  if (types)
	    g_strfreev (types);
          request_set_error (client, "Cannot connect to changes database");
          return HTTP_STATUS_404;
        }

      if (dupin_database_get_max_rowid (client->output.changes_comet.db, &client->output.changes_comet.change_max_rowid) == FALSE)
        {
          dupin_database_unref (client->output.changes_comet.db);
	  if (types)
            g_strfreev (types);
          request_set_error (client, "Cannot get last seq number from changes database");
          return HTTP_STATUS_500;
        }

      client->output.changes_comet.change_size = 0;
      client->output.changes_comet.change_string = NULL;
      client->output.changes_comet.change_errors = 0;
      client->output.changes_comet.change_results_offset = 0;
      client->output.changes_comet.param_heartbeat = heartbeat;
      client->output.changes_comet.param_timeout = timeout;
      client->output.changes_comet.param_descending = descending;
      client->output.changes_comet.param_style = style;

      /* NOTE: special case since "now" for continuous and longpoll - see https://issues.apache.org/jira/browse/COUCHDB-1501 */
      if (since == -1)
        {
          client->output.changes_comet.param_since = client->output.changes_comet.change_max_rowid;
	}
      else
        {
          client->output.changes_comet.param_since = (gsize)since;
        }

      client->output.changes_comet.param_feed = feed;
      client->output.changes_comet.param_include_docs = include_docs;
      client->output.changes_comet.param_types = types;
      client->output.changes_comet.param_types_op = types_op;
      client->output.changes_comet.change_generated = FALSE;
      client->output.changes_comet.change_last_seq = 0;

      client->output_type = DS_HTTPD_OUTPUT_CHANGES_COMET;

      if (feed == DP_CHANGES_FEED_LONGPOLL)
        client->output_mime = g_strdup (HTTP_MIME_JSON);
      else
        client->output_mime = g_strdup (HTTP_MIME_TEXTPLAIN); /* this will not be valid JSON { ...} { ... } ... */

      // we can not set output size and we use Transfer-Encoding: chunked instead - see httpd.c

      return HTTP_STATUS_200;
    }

  /* feed type poll */

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      if (types)
        g_strfreev (types);
      request_set_error (client, "Cannot connect to changes database");
      return HTTP_STATUS_404;
    }

  if (dupin_database_get_max_rowid (db, &last_seq) == FALSE)
    {
      if (types)
        g_strfreev (types);
      request_set_error (client, "Cannot get last change from database");
      return HTTP_STATUS_404;
    }

  if (since == -1)
    {
      /* NOTE: This is only for completness, due we expect to always return an empty changes when feed poll with since=-1 */
      since = (gint)last_seq;
    }

  if (since == 0)
    { 
      total_rows = dupin_database_count (db, DP_COUNT_CHANGES);
    }
  else
    {
      if (dupin_database_get_total_changes (db, &total_rows, (gsize)since, 0, DP_COUNT_CHANGES, TRUE, types, types_op, NULL) == FALSE)
        {
          dupin_database_unref (db);
          if (types)
            g_strfreev (types);
          request_set_error (client, "Cannot get last seq number from changes database");
          return HTTP_STATUS_500;
        }
   }

  if (dupin_database_get_changes_list (db, count, 0, (gsize)since, 0, style, DP_COUNT_CHANGES, DP_ORDERBY_ROWID, descending, types, types_op, &results, NULL) ==
      FALSE)
    {
      dupin_database_unref (db);
      if (types)
        g_strfreev (types);
      request_set_error (client, "Cannot list changes from database");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_database_get_changes_list_close (results);
      dupin_database_unref (db);
      if (types)
        g_strfreev (types);
      request_set_error (client, "Cannot list changes from database");
      return HTTP_STATUS_500;
    }

  json_object_set_int_member (obj, "total_rows", total_rows);
  json_object_set_int_member (obj, "rows_per_page", count);

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_changes_database_error;

  for (list = results; list; list = list->next)
    {
      JsonNode * change = json_node_copy (list->data);

      if (include_docs == TRUE)
        {
          JsonObject * on_obj = json_node_get_object (change);

          gchar * record_id   = (gchar *) json_object_get_string_member (on_obj, RESPONSE_OBJ_ID);
	  gchar * record_mvcc = (gchar *) json_object_get_string_member (
						json_array_get_object_element (json_object_get_array_member (on_obj, "changes"), 0)
						, RESPONSE_OBJ_REV);

          DupinRecord * db_record=NULL;
          if (!(db_record = dupin_record_read (db, record_id, NULL)))
            {
              json_node_free (change);
              json_array_unref (array);
              goto request_global_get_changes_database_error;
            }

          JsonNode * doc = NULL;

          if (! (doc = request_record_revision_obj (client, arguments,
						    db_record, record_id, record_mvcc, FALSE)))
            {
              json_node_free (change);
              json_array_unref (array);
              goto request_global_get_changes_database_error;
            }

          dupin_record_close (db_record);

          json_object_set_member (on_obj, RESPONSE_OBJ_DOC, doc);
        }

      json_array_add_element (array, change);
    }

  json_object_set_array_member (obj, "results", array );

  if (total_rows > 0)
    {
      json_object_set_int_member (obj, "last_seq", last_seq);
    }

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_changes_database_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_changes_database_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_changes_database_error;

  if( results )
    dupin_database_get_changes_list_close (results);

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);

  json_object_unref (obj);

  if (types)
    g_strfreev (types);

  return HTTP_STATUS_200;

request_global_get_changes_database_error:

  if( results )
    dupin_database_get_changes_list_close (results);

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (types)
    g_strfreev (types);

  request_set_error (client, "Cannot list changes from database");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_all_docs (DSHttpdClient * client, GList * path,
			     GList * arguments)
{
  DupinDB *db;

  GList *list;
  GList *results;

  gboolean descending = FALSE;
  guint count = DUPIN_DB_MAX_DOCS_COUNT;
  guint offset = 0;
  gsize total_rows = 0;

  gchar * startkey = NULL;
  gchar * endkey = NULL;
  gboolean inclusive_end = TRUE;
  gboolean include_docs = FALSE;

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  gsize created = 0;
  DupinCreatedType created_op = DP_CREATED_SINCE;

  gchar ** types = NULL;
  DupinFilterByType types_op = DP_FILTERBY_EQUALS;

  gchar * filter_by = NULL;
  DupinFieldsFormatType filter_by_format = DP_FIELDS_FORMAT_DOTTED;
  DupinFilterByType filter_op = DP_FILTERBY_UNDEF;
  gchar * filter_values = NULL;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_LIMIT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_OFFSET))
	offset = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_CREATED_SINCE))
        {
          created = (gsize) g_ascii_strtoll (kv->value, NULL, 10);
          created_op = DP_CREATED_SINCE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_CREATED_UNTIL))
        {
          created = (gsize) g_ascii_strtoll (kv->value, NULL, 10);
          created_op = DP_CREATED_UNTIL;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_DOCS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              dupin_database_unref (db);
	      
	      if (types)
                g_strfreev (types);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_INCLUDE_DOCS " parameter. Allowed values are: true, false");

              return HTTP_STATUS_400;
            }

          include_docs = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUSIVEEND))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              dupin_database_unref (db);

	      if (types)
                g_strfreev (types);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_INCLUSIVEEND " parameter. Allowed values are: true, false");

              return HTTP_STATUS_400;
            }

          inclusive_end = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_KEY))
        {
          if (startkey != NULL)
            g_free (startkey);

	  startkey = dupin_util_json_string_normalize_docid (kv->value);
          if (startkey == NULL)
            {
              if (endkey != NULL)
                g_free (endkey);

              dupin_database_unref (db);

	      if (types)
                g_strfreev (types);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_KEY " parameter. It must be a valid JSON string.");

              return HTTP_STATUS_400;
            }

	  endkey = g_strdup (startkey);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_STARTKEY))
        {
          if (startkey != NULL)
            g_free (startkey);

	  startkey = dupin_util_json_string_normalize_docid (kv->value);
          if (startkey == NULL)
            {
              if (endkey != NULL)
                g_free (endkey);

              dupin_database_unref (db);

	      if (types)
                g_strfreev (types);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_STARTKEY " parameter. It must be a valid JSON string.");

              return HTTP_STATUS_400;
            }
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_ENDKEY))
        {
          if (endkey != NULL)
            g_free (endkey);

	  endkey = dupin_util_json_string_normalize_docid (kv->value);
          if (endkey == NULL)
            {
              if (startkey != NULL)
                g_free (startkey);

              dupin_database_unref (db);

	      if (types)
                g_strfreev (types);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_ENDKEY " parameter. It must be a valid JSON string.");

              return HTTP_STATUS_400;
            }
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_TYPES))
        {
          types = g_strsplit (kv->value, ",", -1);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_TYPES_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            types_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            types_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            types_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            types_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_BY))
        {
          filter_by = kv->value;
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_DOTTED))
            filter_by_format = DP_FIELDS_FORMAT_DOTTED;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_JSONPATH))
            filter_by_format = DP_FIELDS_FORMAT_JSONPATH;
        }
 
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            filter_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            filter_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            filter_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            filter_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_VALUES))
        {
          filter_values = kv->value;
        }
    }

  if (startkey != NULL
      || endkey != NULL
      || types != NULL
      || filter_by != NULL)
      total_rows = dupin_record_get_list_total (db, 0, 0, startkey, endkey, inclusive_end, DP_COUNT_EXIST, types, types_op, 
						filter_by, filter_by_format, filter_op, filter_values, NULL);
  else
      total_rows = dupin_database_count (db, DP_COUNT_EXIST);

  /* NOTE - bear in mind we are cheating bad (on our side) and we do a full fetch from underlying DB, always even if include_docs=false */

  if (dupin_record_get_list (db, count, offset, 0, 0, startkey, endkey, inclusive_end, DP_COUNT_EXIST,
				DP_ORDERBY_ID, descending, types, types_op,
				filter_by, filter_by_format, filter_op, filter_values, &results, NULL) == FALSE)
    {
      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      dupin_database_unref (db);
      if (types)
        g_strfreev (types);

      request_set_error (client, "Cannot list documents from database");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_record_get_list_close (results);

      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      dupin_database_unref (db);
      if (types)
        g_strfreev (types);
      request_set_error (client, "Cannot list documents from database");
      return HTTP_STATUS_500;
    }

  json_object_set_int_member (obj, "total_rows", total_rows);
  json_object_set_int_member (obj, "offset", offset);
  json_object_set_int_member (obj, "rows_per_page", count);

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_all_docs_error;

  for (list = results; list; list = list->next)
    {
      DupinRecord *record = list->data;
      JsonNode *kvd = json_node_new (JSON_NODE_OBJECT);
      JsonObject *kvd_obj = json_object_new ();
      json_node_take_object (kvd, kvd_obj);

      json_object_set_string_member (kvd_obj, RESPONSE_OBJ_ID, (gchar *)dupin_record_get_id (record));
      json_object_set_string_member (kvd_obj, RESPONSE_VIEW_OBJ_KEY, (gchar *)dupin_record_get_id (record));

      JsonObject *value_obj = json_object_new ();
      json_object_set_string_member (value_obj, RESPONSE_OBJ_REV, (gchar *)dupin_record_get_last_revision (record));
      gchar * created = dupin_util_timestamp_to_iso8601 (dupin_record_get_created (record));
      json_object_set_string_member (value_obj, RESPONSE_OBJ_CREATED, created);
      g_free (created);
      gchar * type = (gchar *)dupin_record_get_type (record);
      if (type != NULL)
        json_object_set_string_member (value_obj, RESPONSE_OBJ_TYPE, type);

      json_object_set_object_member (kvd_obj, RESPONSE_VIEW_OBJ_VALUE, value_obj);

      if (include_docs == TRUE)
        {
          JsonNode *on;
          if (!  (on = request_record_revision_obj (client, arguments,
					record, (gchar *) dupin_record_get_id (record),
					dupin_record_get_last_revision (record), TRUE)))
            {
	      json_array_unref (array);
	      json_node_free (kvd);
	      goto request_global_get_all_docs_error;
            }

          json_object_set_member (kvd_obj, RESPONSE_VIEW_OBJ_DOC, on);
        }

      json_array_add_element( array, kvd);
    }

  json_object_set_array_member (obj, "rows", array );

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_all_docs_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_all_docs_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_all_docs_error;

  if( results )
    dupin_record_get_list_close (results);

  if (startkey != NULL)
    g_free (startkey);

  if (endkey != NULL)
    g_free (endkey);

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (types)
    g_strfreev (types);

  return HTTP_STATUS_200;

request_global_get_all_docs_error:

  if( results )
    dupin_record_get_list_close (results);

  if (startkey != NULL)
    g_free (startkey);

  if (endkey != NULL)
    g_free (endkey);

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (types)
    g_strfreev (types);

  request_set_error (client, "Cannot list documents from database");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_server_info (DSHttpdClient * client, GList * path,
			     GList * arguments)
{
  JsonObject *obj;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  obj = json_object_new ();

  if (obj == NULL)
    {
      request_set_error (client, "Cannot get server information");
      return HTTP_STATUS_500;
    }

  json_object_set_string_member (obj, "dupindb", "Welcome to Dupin");
  json_object_set_string_member (obj, "version", PACKAGE " " VERSION);

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_server_info_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_server_info_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_server_info_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  return HTTP_STATUS_200;

request_global_get_server_info_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  request_set_error (client, "Cannot get server information");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_database (DSHttpdClient * client, GList * path,
			     GList * arguments)
{
  DupinDB *db;
  GList *list;
  JsonObject *obj;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_QUERY))
	return request_global_get_database_query (client, path, kv->value);
    }

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      return HTTP_STATUS_404;
    }

  gsize max_rowid;
  if (dupin_database_get_max_rowid (db, &max_rowid) == FALSE)
    {
      dupin_database_unref (db);
      request_set_error (client, "Cannot get last seq number from changes database");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      dupin_database_unref (db);
      request_set_error (client, "Cannot read database information");
      return HTTP_STATUS_500;
    }

  json_object_set_string_member (obj, "db_name", (gchar *) dupin_database_get_name (db));
  json_object_set_int_member (obj, "doc_count", dupin_database_count (db, DP_COUNT_EXIST));
  json_object_set_int_member (obj, "doc_del_count", dupin_database_count (db, DP_COUNT_DELETE));

  /* FIXME: not really a lot of documentation about this stuff (update_seq)... - see also
     http://guide.couchdb.org/draft/replication.html and http://ayende.com/Blog/archive/2008/10/04/erlang-reading-couchdb-digging-down-to-disk.aspx
     and https://issues.apache.org/jira/browse/COUCHDB-576?page=com.atlassian.jira.plugin.system.issuetabpanels:all-tabpanel */

  json_object_set_int_member (obj, "update_seq", max_rowid);

  json_object_set_int_member (obj, "disk_size", dupin_database_get_size (db));

  gsize creation_time;
  dupin_database_get_creation_time (db, &creation_time);
  json_object_set_string_member (obj, "instance_start_time", g_strdup_printf ("%" G_GSIZE_FORMAT, creation_time));

  /* FIXME: this does not make sense for dupin yet, see also http://blog.couchone.com/post/632718824/simple-document-versioning-with-couchdb */
  /* NOTE - Compaction removes old revs, only the latest rev is represented in view queries, and only the latest revision is replicated. */
  json_object_set_boolean_member (obj, "compact_running", dupin_database_is_compacting (db));

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_database_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_database_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_database_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  dupin_database_unref (db);
  return HTTP_STATUS_200;

request_global_get_database_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  dupin_database_unref (db);

  request_set_error (client, "Cannot read database information");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_record (DSHttpdClient * client, GList * path,
			   GList * arguments)
{
  gchar * mvcc = NULL;
  gboolean allrevs = FALSE;
  gchar * request_fields=NULL;

  GList *list, *results=NULL;

  DupinDB *db=NULL;
  DupinAttachmentDB *attachment_db=NULL;
  DupinRecord *record=NULL;

  gboolean descending = FALSE;
  guint count = DUPIN_REVISIONS_COUNT;
  guint offset = 0;

  gboolean attachments_descending = FALSE;
  guint attachments_count = DUPIN_ATTACHMENTS_COUNT;
  guint attachments_offset = 0;
  
  JsonNode *node=NULL;

  gchar * doc_id=NULL;
  GList * title_parts=NULL;

  gchar * dbname = (gchar *) path->data;

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* GET _special_document/document_ID */
      if (path->next->next)
        {
          doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);

          if (path->next->next->next)
            {
              /* GET _special_document/document_ID/_fields/field */
              if (!g_strcmp0 (path->next->next->next->data, REQUEST_FIELDS))
                {
                  if (!path->next->next->next->next
                       || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ATTACHMENTS))
                    {
                      request_set_error (client, "Cannot GET record field");
                      return HTTP_STATUS_400;
                    }

                  request_fields=(gchar *)path->next->next->next->next->data;
                }
              else
                {
                  /* GET _special_document/document_ID/attachment  */
                  title_parts = path->next->next->next;
                }
            }
        }
    }
  else
    {
      if (path->next->next)
        {
          /* GET document_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->data, REQUEST_FIELDS))
            {
              if (!path->next->next->next
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS))
                {
                  request_set_error (client, "Cannot GET record field");
                  return HTTP_STATUS_400;
                }

              request_fields=(gchar *)path->next->next->next->data;
            }

          /* NOTE - the following two works becuase the database and the default linkbase
		    are named the same - we should rewrite path really */

          /* GET document_ID/_links and document_ID/_relationships */
          else if ((!g_strcmp0 (path->next->next->data, REQUEST_OBJ_LINKS))
                  || (!g_strcmp0 (path->next->next->data, REQUEST_OBJ_RELATIONSHIPS)))
            {
	      if (client->request_arguments == NULL)
	        client->request_arguments = NULL;

              client->request_arguments = g_list_prepend (client->request_arguments,
				dp_keyvalue_new (REQUEST_GET_ALL_LINKS_LINKBASE,
						 dbname));
              client->request_arguments = g_list_prepend (client->request_arguments,
				dp_keyvalue_new (REQUEST_GET_ALL_LINKS_CONTEXT_ID,
						 (gchar *)path->next->data));

              if (path->next->next->next)
                  client->request_arguments = g_list_prepend (client->request_arguments,
				dp_keyvalue_new (REQUEST_GET_ALL_LINKS_LABELS,
						 (gchar *)path->next->next->next->data));

              if (!g_strcmp0 (path->next->next->data, REQUEST_OBJ_LINKS))
                {
                  client->request_arguments = g_list_prepend (client->request_arguments,
				dp_keyvalue_new (REQUEST_GET_ALL_LINKS_LINK_TYPE,
						 REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS));
                }
              else
                {
                  client->request_arguments = g_list_prepend (client->request_arguments,
				dp_keyvalue_new (REQUEST_GET_ALL_LINKS_LINK_TYPE,
						 REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS));
                }

	      return request_global_get_all_links_linkbase (client, path, client->request_arguments);
	    }

          /* GET /document_ID/attachment */
          else
            {
              title_parts = path->next->next;
            }
        }

      /* GET document_ID */
      doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
              g_free (doc_id);
              request_set_error (client, "Invalid record MVCC revision");
              return HTTP_STATUS_400;
            }
	  mvcc = kv->value;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REVS)
	       && !g_strcmp0 (kv->value, "true"))
	allrevs = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_LIMIT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_OFFSET))
	offset = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ATTACHMENTS_DESCENDING)
	       && !g_strcmp0 (kv->value, "true"))
	attachments_descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ATTACHMENTS_LIMIT))
	attachments_count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ATTACHMENTS_OFFSET))
	attachments_offset = atoi (kv->value);

    }

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, dbname, NULL)))
    {
      g_free (doc_id);
      request_set_error (client, "Cannot connect to database");
      return HTTP_STATUS_404;
    }

  if (!  (attachment_db =
                dupin_attachment_db_open (client->thread->data->dupin, dbname, NULL)))
    {
      dupin_database_unref (db);
      g_free (doc_id);
      request_set_error (client, "Cannot connect to attachments database");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_record_read (db, doc_id, NULL)))
    {
      dupin_attachment_db_unref (attachment_db);
      dupin_database_unref (db);
      g_free (doc_id);
      request_set_error (client, "Cannot read record from database");
      return HTTP_STATUS_404;
    }

  if ((dupin_record_is_deleted (record, NULL) == TRUE) && (allrevs == FALSE))
    {
      dupin_record_close (record);
      dupin_attachment_db_unref (attachment_db);
      dupin_database_unref (db);
      g_free (doc_id);
      request_set_error (client, "Record is deleted");
      return HTTP_STATUS_404;
    }

  /* process input attachment name parameter */
  if (title_parts != NULL)
    {
      gchar * title = NULL;
      GString *str = g_string_new (title_parts->data);
      GList * l=NULL;

      for (l=title_parts->next ; l != NULL ; l=l->next)
        {
          g_string_append_printf (str, "/%s", (gchar *)l->data);
        }

      title = g_string_free (str, FALSE);

      if (title == NULL)
        {
          dupin_record_close (record);
          dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          request_set_error (client, "Cannot get valid attachment title");
          return HTTP_STATUS_400;
        }

//g_message("request_global_get_record: title=%s\n", title);

      if ( dupin_attachment_record_exists (attachment_db, doc_id, title) == FALSE)
        {
          dupin_record_close (record);
          g_free (title);
          dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          request_set_error (client, "Attachment does not exist");
          return HTTP_STATUS_404;
        }

      if ( (!(client->output.blob.record = dupin_attachment_record_read (attachment_db,
							       doc_id, title,
							       NULL)))
	  || (dupin_attachment_record_blob_open (client->output.blob.record) == FALSE))
        {
          dupin_record_close (record);
          g_free (title);
          dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          request_set_error (client, "Cannot read attachment from database");
          return HTTP_STATUS_500;
        }
      
      client->output_type = DS_HTTPD_OUTPUT_BLOB;
      client->output_mime = g_strdup (dupin_attachment_record_get_type (client->output.blob.record));
      client->output_size = dupin_attachment_record_get_length (client->output.blob.record);

      dupin_record_close (record);
      g_free (title);
      dupin_database_unref (db);
      dupin_attachment_db_unref (attachment_db);

      g_free (doc_id);

      return HTTP_STATUS_200;
    }

  /* Show all revisions: */
  if (allrevs == TRUE)
    {
      GList * revisions=NULL;
      GList * list=NULL;
      JsonObject *obj;
      JsonArray *array;
      gsize total_rows = 0;

      node = json_node_new (JSON_NODE_OBJECT);

      if (node == NULL)
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          request_set_error (client, "Cannot get list of record revisions");
	  return HTTP_STATUS_500;
	}

      obj = json_object_new ();

      if (obj == NULL)
        goto request_global_get_record_error;

      json_node_take_object (node, obj);

      array = json_array_new ();

      if (array == NULL)
        goto request_global_get_record_error;

      if ((dupin_record_get_revisions_list (record,
				           count,
					   //offset, 1, 0, DP_COUNT_ALL, DP_ORDERBY_REV, descending,
					   offset, 0, 0, DP_COUNT_ALL, DP_ORDERBY_REV, descending,
					   &revisions, NULL) == FALSE)
         || (dupin_record_get_total_revisions (record, &total_rows, NULL) == FALSE))
	{
          if (node != NULL)
            json_node_free (node);
	  json_array_unref (array);
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          request_set_error (client, "Cannot get list of record revisions");
	  return HTTP_STATUS_500;
	}

      for (list = revisions; list; list = list->next)
        {
	  JsonNode *on;

	  if (!
	      (on =
	       request_record_revision_obj (client, arguments,
					    record,
					    (gchar *) dupin_record_get_id (record),
					    (gchar *) list->data,
					    FALSE)))
            {
              dupin_record_get_revisions_list_close (revisions);
	      json_array_unref (array);
	      goto request_global_get_record_error;
            }

          if (request_fields != NULL)
            {
              JsonObject * on_obj = json_node_get_object (on);

              if (json_object_has_member (on_obj, (const gchar *)request_fields) == FALSE)
                {
                  dupin_record_get_revisions_list_close (revisions);
                  if (node != NULL)
                    json_node_free (node);
	          json_array_unref (array);
	          dupin_record_close (record);
	          dupin_database_unref (db);
                  dupin_attachment_db_unref (attachment_db);
                  g_free (doc_id);
                  request_set_error (client, "Cannot get field for record revisions list");
	          return HTTP_STATUS_404;
                }

              json_array_add_element( array, json_node_copy (json_object_get_member (on_obj, (const gchar *)request_fields)));
              json_node_free (on);
            }
          else
            {
              json_array_add_element( array, on);
            }
        }

      dupin_record_get_revisions_list_close (revisions);

      json_object_set_int_member (obj, "total_rows", total_rows);
      json_object_set_int_member (obj, "offset", offset);
      json_object_set_int_member (obj, "rows_per_page", count);

      json_object_set_array_member (obj, "_revs_info", array );
    }

  /* Show a single revision: */
  else
    {
      JsonNode *node_temp=NULL;

      if (mvcc == NULL)
	mvcc = dupin_record_get_last_revision (record);

      else if (dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record)) > 0)
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          request_set_error (client, "Requested record MVCC revision newer that latest revision");
	  return HTTP_STATUS_404;
	}

      if (!
	  (node_temp =
	   request_record_revision_obj (client, arguments,
					record, (gchar *) dupin_record_get_id (record),
					mvcc,
					TRUE)))
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          request_set_error (client, "Cannot get record revision");
	  return HTTP_STATUS_404;
	}

      if (request_fields != NULL)
        {
          JsonObject * node_obj = json_node_get_object (node_temp);

          if (json_object_has_member (node_obj, (const gchar *)request_fields) == FALSE)
            {
              if (node_temp != NULL)
                json_node_free (node_temp);
	      dupin_record_close (record);
	      dupin_database_unref (db);
              dupin_attachment_db_unref (attachment_db);
              g_free (doc_id);
              request_set_error (client, "Cannot get record revision field");
	      return HTTP_STATUS_404;
            }

          node = json_node_copy (json_object_get_member (node_obj, (const gchar *)request_fields));
        }
      else
        {
          node = json_node_copy (node_temp);
        }

      json_node_free (node_temp);
    }

  if (request_fields == NULL)
    {
      JsonObject * attachments_obj = json_object_new ();

      gsize total_attachments = dupin_attachment_record_get_list_total (attachment_db, 1, 0,
									(gchar *) dupin_record_get_id (record), NULL, NULL, TRUE, NULL);

      if (dupin_attachment_record_get_list (attachment_db, attachments_count,
					    attachments_offset, 1, 0, DP_ORDERBY_TITLE, attachments_descending,
					    (gchar *) dupin_record_get_id (record), NULL, NULL, TRUE,
					    &results, NULL) == FALSE)
        {
          if (node != NULL)
            json_node_free (node);
          json_object_unref (attachments_obj);
          dupin_record_close (record);
          dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          request_set_error (client, "Cannot get record list of attachments");
          return HTTP_STATUS_500;
        }

//g_message("request_global_get_record: g_list_length (results) = %d \n", (gint) g_list_length (results));

      for (list = results; list; list = list->next)
        {
          JsonNode * attachment_node = NULL;
          if (! (attachment_node = dupin_attachment_record_get (list->data)))
            {
              if (node != NULL)
                json_node_free (node);
              json_object_unref (attachments_obj);
	      dupin_record_close (record);
	      dupin_database_unref (db);
              dupin_attachment_db_unref (attachment_db);
              g_free (doc_id);
              request_set_error (client, "Cannot get record attachment");
	      return HTTP_STATUS_500;
	    }

          json_object_set_member (attachments_obj, dupin_attachment_record_get_title (list->data), attachment_node);
       }

     if (json_object_get_size (attachments_obj) > 0
         && json_object_has_member (attachments_obj, RESPONSE_OBJ_ATTACHMENTS_PAGING) == FALSE)
       {
         JsonNode * paging_info_node = json_node_new (JSON_NODE_OBJECT);
         JsonObject * paging_info = json_object_new ();
	 json_node_take_object (paging_info_node, paging_info);
         json_object_set_int_member (paging_info, "total_attachments", total_attachments);
	 json_object_set_int_member (paging_info, "offset", attachments_offset);
	 json_object_set_int_member (paging_info, "attachments_per_document", attachments_count);
	 json_object_set_member (attachments_obj, RESPONSE_OBJ_ATTACHMENTS_PAGING, paging_info_node);
       }
     dupin_attachment_record_get_list_close (results);

     if (results)
       {
         if (allrevs == TRUE
             || (allrevs == FALSE && (!dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record)))))
           {
             json_object_remove_member (json_node_get_object (node), RESPONSE_OBJ_ATTACHMENTS);
             json_object_set_object_member (json_node_get_object (node), RESPONSE_OBJ_ATTACHMENTS, attachments_obj);
           }
       }
     else
       {
         json_object_unref (attachments_obj);
       }
    }

  /* Writing: */

  client->output.string.string = dupin_util_json_serialize (node);
  client->output_size = strlen(client->output.string.string);

  if (client->output.string.string == NULL)
    goto request_global_get_record_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (node != NULL)
    json_node_free (node);

  dupin_record_close (record);
  dupin_database_unref (db);
  dupin_attachment_db_unref (attachment_db);

  g_free (doc_id);

  return HTTP_STATUS_200;

request_global_get_record_error:

  if (node != NULL)
    json_node_free (node);

  dupin_record_close (record);
  dupin_database_unref (db);
  dupin_attachment_db_unref (attachment_db);

  g_free (doc_id);

  request_set_error (client, "Cannot get record");

  return HTTP_STATUS_500;
}

/* GET link bases */

static DSHttpStatusCode
request_global_get_linkbase (DSHttpdClient * client, GList * path,
			     GList * arguments)
{
  DupinLinkB *linkb;
  GList *list;
  JsonObject *obj;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_QUERY))
	return request_global_get_linkbase_query (client, path, kv->value);
    }

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to linkabse");
      return HTTP_STATUS_404;
    }

  gsize max_rowid;
  if (dupin_linkbase_get_max_rowid (linkb, &max_rowid) == FALSE)
    {
      dupin_linkbase_unref (linkb);
      request_set_error (client, "Cannot get last seq number from changes linkbase");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      dupin_linkbase_unref (linkb);
      request_set_error (client, "Cannot get linkabse information");
      return HTTP_STATUS_500;
    }

  json_object_set_string_member (obj, "db_name", (gchar *) dupin_linkbase_get_name (linkb));
  json_object_set_string_member (obj, "parent", (gchar *) dupin_linkbase_get_parent (linkb));
  json_object_set_int_member (obj, "links_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_ANY, DP_COUNT_EXIST));
  json_object_set_int_member (obj, "web_links_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_WEB_LINK, DP_COUNT_EXIST));
  json_object_set_int_member (obj, "relationships_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_RELATIONSHIP, DP_COUNT_EXIST));
  json_object_set_int_member (obj, "links_del_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_ANY, DP_COUNT_DELETE));
  json_object_set_int_member (obj, "web_links_del_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_WEB_LINK, DP_COUNT_DELETE));
  json_object_set_int_member (obj, "relationships_del_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_RELATIONSHIP, DP_COUNT_DELETE));

  json_object_set_int_member (obj, "update_seq", max_rowid);

  json_object_set_int_member (obj, "disk_size", dupin_linkbase_get_size (linkb));

  gsize creation_time;
  dupin_linkbase_get_creation_time (linkb, &creation_time);
  json_object_set_string_member (obj, "instance_start_time", g_strdup_printf ("%" G_GSIZE_FORMAT, creation_time));

  json_object_set_boolean_member (obj, "compact_running", dupin_linkbase_is_compacting (linkb));
  json_object_set_boolean_member (obj, "check_running", dupin_linkbase_is_checking (linkb));

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_linkbase_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_linkbase_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_linkbase_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  dupin_linkbase_unref (linkb);
  return HTTP_STATUS_200;

request_global_get_linkbase_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  dupin_linkbase_unref (linkb);

  request_set_error (client, "Cannot get linkabse information");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_all_links_linkbase (DSHttpdClient * client, GList * path,
			     	       GList * arguments)
{
  DupinLinkB *linkb;

  GList *list;
  GList *results;

  gboolean descending = FALSE;
  guint count = DUPIN_LINKB_MAX_LINKS_COUNT;
  guint offset = 0;
  gsize total_rows = 0;

  gchar * startkey = NULL;
  gchar * endkey = NULL;
  gboolean inclusive_end = TRUE;
  gboolean include_docs = FALSE;

  gchar * context_id = NULL;
  gchar ** link_rels = NULL;
  DupinFilterByType link_rels_op = DP_FILTERBY_EQUALS;
  gchar ** link_labels = NULL;
  DupinFilterByType link_labels_op = DP_FILTERBY_EQUALS;
  gchar ** link_hrefs = NULL;
  DupinFilterByType link_hrefs_op = DP_FILTERBY_EQUALS;
  gchar ** link_tags = NULL;
  DupinFilterByType link_tags_op = DP_FILTERBY_EQUALS;
  DupinLinksType link_type = DP_LINK_TYPE_ANY;

  gchar * filter_by = NULL;
  DupinFieldsFormatType filter_by_format = DP_FIELDS_FORMAT_DOTTED;
  DupinFilterByType filter_op = DP_FILTERBY_UNDEF;
  gchar * filter_values = NULL;

  gsize created = 0;
  DupinCreatedType created_op = DP_CREATED_SINCE;

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  gchar * linkbase_name = path->data;

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, linkbase_name, NULL)))
    {
      request_set_error (client, "Cannot connect to linkabse");
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

//g_message("request_global_get_all_links_linkbase: k=%s v=%s\n", (gchar *)kv->key, (gchar *)kv->value);

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LIMIT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_OFFSET))
	offset = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_CONTEXT_ID))
        context_id = kv->value;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_CREATED_SINCE))
        {
          created = (gsize) g_ascii_strtoll (kv->value, NULL, 10);
          created_op = DP_CREATED_SINCE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_CREATED_UNTIL))
        {
          created = (gsize) g_ascii_strtoll (kv->value, NULL, 10);
          created_op = DP_CREATED_UNTIL;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_RELS))
        {
          link_rels = g_strsplit (kv->value, ",", -1);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_RELS_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            link_rels_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            link_rels_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            link_rels_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            link_rels_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LABELS))
        {
          link_labels = g_strsplit (kv->value, ",", -1);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LABELS_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            link_labels_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            link_labels_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            link_labels_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            link_labels_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_HREFS))
        {
          link_hrefs = g_strsplit (kv->value, ",", -1);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_HREFS_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            link_hrefs_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            link_hrefs_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            link_hrefs_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            link_hrefs_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_TAGS))
        {
          link_tags = g_strsplit (kv->value, ",", -1);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_TAGS_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            link_tags_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            link_tags_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            link_tags_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            link_tags_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LINKBASE))
        linkbase_name = kv->value;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LINK_TYPE))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS))
            link_type = DP_LINK_TYPE_WEB_LINK;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS))
            link_type = DP_LINK_TYPE_RELATIONSHIP;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_DOCS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              dupin_linkbase_unref (linkb);
             
              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_INCLUDE_DOCS " parameter. Allowed values are: true, false");

              return HTTP_STATUS_400;
            }

          include_docs = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUSIVEEND))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              dupin_linkbase_unref (linkb);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_INCLUSIVEEND " parameter. Allowed values are: true, false");

              return HTTP_STATUS_400;
            }

          inclusive_end = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_KEY))
        {
          if (startkey != NULL)
            g_free (startkey);

         startkey = dupin_util_json_string_normalize_docid (kv->value);
          if (startkey == NULL)
            {
              if (endkey != NULL)
                g_free (endkey);

              dupin_linkbase_unref (linkb);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_KEY " parameter. It must be a valid JSON string.");

              return HTTP_STATUS_400;
            }

         endkey = g_strdup (startkey);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_STARTKEY))
        {
          if (startkey != NULL)
            g_free (startkey);

         startkey = dupin_util_json_string_normalize_docid (kv->value);
          if (startkey == NULL)
            {
              if (endkey != NULL)
                g_free (endkey);

              dupin_linkbase_unref (linkb);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_STARTKEY " parameter. It must be a valid JSON string.");

              return HTTP_STATUS_400;
            }
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_ENDKEY))
        {
          if (endkey != NULL)
            g_free (endkey);

         endkey = dupin_util_json_string_normalize_docid (kv->value);
          if (endkey == NULL)
            {
              if (startkey != NULL)
                g_free (startkey);

              dupin_linkbase_unref (linkb);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_ENDKEY " parameter. It must be a valid JSON string.");

              return HTTP_STATUS_400;
            }
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_BY))
        {
          filter_by = kv->value;
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_DOTTED))
            filter_by_format = DP_FIELDS_FORMAT_DOTTED;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_JSONPATH))
            filter_by_format = DP_FIELDS_FORMAT_JSONPATH;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            filter_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            filter_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            filter_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            filter_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_VALUES))
        {
          filter_values = kv->value;
        }
    }

  /* NOTE - try to optimize here */
  if (startkey != NULL
      || endkey != NULL
      || context_id != NULL
      || link_rels != NULL
      || link_labels != NULL
      || link_hrefs != NULL
      || link_tags != NULL
      || filter_by != NULL)
    total_rows = dupin_link_record_get_list_total (linkb, 0, 0, link_type, startkey, endkey, inclusive_end, DP_COUNT_EXIST, 
						   context_id, link_rels, link_rels_op,
						   link_labels, link_labels_op, link_hrefs, link_hrefs_op, link_tags, link_tags_op,
						   filter_by, filter_by_format, filter_op, filter_values);
  else
    total_rows = dupin_linkbase_count (linkb, link_type, DP_COUNT_EXIST);


  if (dupin_link_record_get_list (linkb, count, offset, 0, 0, link_type, startkey, endkey, inclusive_end, DP_COUNT_EXIST, DP_ORDERBY_ID, descending, 
				  context_id, link_rels, link_rels_op, link_labels, link_labels_op,
				  link_hrefs, link_hrefs_op, link_tags, link_tags_op,
				  filter_by, filter_by_format, filter_op, filter_values, &results, NULL) == FALSE)
    {
      if (link_rels)
        g_strfreev (link_rels);

      if (link_labels)
        g_strfreev (link_labels);

      if (link_hrefs)
        g_strfreev (link_hrefs);

      if (link_tags)
        g_strfreev (link_tags);

      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      dupin_linkbase_unref (linkb);
      request_set_error (client, "Cannot get list of links from linkabse");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_link_record_get_list_close (results);

      if (link_rels)
        g_strfreev (link_rels);

      if (link_labels)
        g_strfreev (link_labels);

      if (link_hrefs)
        g_strfreev (link_hrefs);

      if (link_tags)
        g_strfreev (link_tags);

      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      dupin_linkbase_unref (linkb);
      request_set_error (client, "Cannot get list of links from linkabse");
      return HTTP_STATUS_500;
    }

  json_object_set_int_member (obj, "total_rows", total_rows);
  json_object_set_int_member (obj, "offset", offset);
  json_object_set_int_member (obj, "rows_per_page", count);

  /* add base */
  if (link_type == DP_LINK_TYPE_ANY
      || link_type == DP_LINK_TYPE_RELATIONSHIP)
    {
      /* NOTE - we assume that the linkbase is named after the documents database */
      gchar * escaped_base = g_uri_escape_string (linkb->parent, NULL, TRUE);
      GString * str = g_string_new (NULL);
      if (dupin_linkbase_get_parent_is_db (linkb) == TRUE)
        g_string_append_printf (str, "/%s/", escaped_base);
      else
        g_string_append_printf (str, "/_linkbs/%s/", escaped_base);
      g_free (escaped_base);
      gchar * tmp = g_string_free (str, FALSE);
      json_object_set_string_member (obj, "base", tmp);
      g_free (tmp);
    }

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_all_links_linkbase_error;

  for (list = results; list; list = list->next)
    {
      DupinLinkRecord *record = list->data;
      JsonNode *kvd = json_node_new (JSON_NODE_OBJECT);
      JsonObject *kvd_obj = json_object_new ();
      json_node_take_object (kvd, kvd_obj);

      json_object_set_string_member (kvd_obj, RESPONSE_LINK_OBJ_ID, (gchar *)dupin_link_record_get_id (record));
      json_object_set_string_member (kvd_obj, RESPONSE_VIEW_OBJ_KEY, (gchar *)dupin_link_record_get_id (record));

      JsonObject *value_obj = json_object_new ();
      json_object_set_string_member (value_obj, RESPONSE_LINK_OBJ_REV, (gchar *)dupin_link_record_get_last_revision (record));

      json_object_set_string_member (value_obj, RESPONSE_LINK_OBJ_CONTEXT_ID, dupin_link_record_get_context_id (record));
      json_object_set_string_member (value_obj, RESPONSE_LINK_OBJ_LABEL, dupin_link_record_get_label (record));
      json_object_set_string_member (value_obj, RESPONSE_LINK_OBJ_HREF, dupin_link_record_get_href (record));

      gchar * rel = (gchar *)dupin_link_record_get_rel (record);

      if (rel != NULL)
        json_object_set_string_member (value_obj, RESPONSE_LINK_OBJ_REL, rel);

      gchar * tag = (gchar *)dupin_link_record_get_tag (record);

      if (tag != NULL)
        json_object_set_string_member (value_obj, RESPONSE_LINK_OBJ_TAG, tag);

      gchar * created = dupin_util_timestamp_to_iso8601 (dupin_link_record_get_created (record));
      json_object_set_string_member (value_obj, RESPONSE_OBJ_CREATED, created);
      g_free (created);

      json_object_set_object_member (kvd_obj, RESPONSE_VIEW_OBJ_VALUE, value_obj);

      if (include_docs == TRUE)
        {
          JsonNode *on;
          if (!  (on = request_link_record_revision_obj (client, arguments,
					     record, (gchar *) dupin_link_record_get_id (record),
			       		     dupin_link_record_get_last_revision (record),
					     TRUE)))
            {
	      json_array_unref (array);
              json_node_free (kvd);
	      goto request_global_get_all_links_linkbase_error;
            }

          json_object_set_member (kvd_obj, RESPONSE_LINK_OBJ_DOC, on);
        }

      json_array_add_element( array, kvd);
    }

  json_object_set_array_member (obj, "rows", array );

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_all_links_linkbase_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_all_links_linkbase_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_all_links_linkbase_error;

  if( results )
    dupin_link_record_get_list_close (results);

  dupin_linkbase_unref (linkb);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (link_rels)
    g_strfreev (link_rels);

  if (link_labels)
    g_strfreev (link_labels);

  if (link_hrefs)
    g_strfreev (link_hrefs);

  if (link_tags)
    g_strfreev (link_tags);

  if (startkey != NULL)
    g_free (startkey);

  if (endkey != NULL)
    g_free (endkey);

  return HTTP_STATUS_200;

request_global_get_all_links_linkbase_error:

  if( results )
    dupin_link_record_get_list_close (results);

  dupin_linkbase_unref (linkb);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (link_rels)
    g_strfreev (link_rels);

  if (link_labels)
    g_strfreev (link_labels);

  if (link_hrefs)
    g_strfreev (link_hrefs);

  if (link_tags)
    g_strfreev (link_tags);

  if (startkey != NULL)
    g_free (startkey);

  if (endkey != NULL)
    g_free (endkey);

  request_set_error (client, "Cannot get list of links from linkabse");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_changes_linkbase (DSHttpdClient * client, GList * path,
			    	     GList * arguments)
{
  DupinLinkB *linkb;

  GList *list;
  GList *results;

  gboolean descending = FALSE;
  guint count = DUPIN_DB_MAX_CHANGES_COUNT;

  guint heartbeat=REQUEST_GET_ALL_CHANGES_HEARTBEAT_DEFAULT;
  guint timeout=REQUEST_GET_ALL_CHANGES_TIMEOUT_DEFAULT;

  gchar * context_id = NULL;
  gchar ** tags = NULL;
  DupinFilterByType tags_op = DP_FILTERBY_EQUALS;

  gsize total_rows = 0;

  gsize last_seq=0;
  gint since = 0;
  DupinChangesType style = DP_CHANGES_MAIN_ONLY;
  DupinChangesFeedType feed = DP_CHANGES_FEED_POLL;
  gboolean include_links = FALSE;

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LIMIT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_SINCE))
	since = (gint) g_ascii_strtoll (kv->value, NULL, 10);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_HEARTBEAT))
	heartbeat = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_TIMEOUT))
	timeout = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_CONTEXT_ID))
	context_id = kv->value;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_TAGS))
        {
          tags = g_strsplit (kv->value, ",", -1);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_TAGS_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            tags_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            tags_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            tags_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            tags_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_STYLE))
        {
	  if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_STYLE_ALL_LINKS))
	    style = DP_CHANGES_ALL_LINKS;
	  else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_STYLE_WEBLINKS))
	    style = DP_CHANGES_WEB_LINKS;
	  else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_STYLE_RELATIONSHIPS))
	    style = DP_CHANGES_RELATIONSHIPS;
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_FEED))
        {
	  if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_FEED_LONGPOLL))
            {
	      feed = DP_CHANGES_FEED_LONGPOLL;
            }
	  else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_FEED_CONTINUOUS))
            {
	      feed = DP_CHANGES_FEED_CONTINUOUS;
            }
	  else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_FEED_POLL))
            {
	      feed = DP_CHANGES_FEED_POLL;
            }
          else
            {
              request_set_error (client, "Invalid " REQUEST_GET_ALL_CHANGES_FEED " parameter. Allowed values are: " REQUEST_GET_ALL_CHANGES_FEED_LONGPOLL ", " REQUEST_GET_ALL_CHANGES_FEED_CONTINUOUS ", " REQUEST_GET_ALL_CHANGES_FEED_POLL);
              return HTTP_STATUS_400;
            }
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_INCLUDE_LINKS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              request_set_error (client, "Invalid " REQUEST_GET_ALL_CHANGES_INCLUDE_LINKS " parameter. Allowed values are: true, false");
              return HTTP_STATUS_400;
            }

          include_links = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }
    }

  if (feed == DP_CHANGES_FEED_LONGPOLL
      || feed == DP_CHANGES_FEED_CONTINUOUS)
    {
      if (!  (client->output.changes_comet.linkb =
			dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
        {
          request_set_error (client, "Cannot connect to changes linkbase");
          return HTTP_STATUS_404;
        }

      if (dupin_linkbase_get_max_rowid (client->output.changes_comet.linkb, &client->output.changes_comet.change_max_rowid) == FALSE)
        {
          dupin_linkbase_unref (client->output.changes_comet.linkb);
          request_set_error (client, "Cannot get last seq number from changes linkbase");
          return HTTP_STATUS_500;
        }

      client->output.changes_comet.change_size = 0;
      client->output.changes_comet.change_string = NULL;
      client->output.changes_comet.change_errors = 0;
      client->output.changes_comet.change_results_offset = 0;
      client->output.changes_comet.param_heartbeat = heartbeat;
      client->output.changes_comet.param_timeout = timeout;
      client->output.changes_comet.param_descending = descending;
      client->output.changes_comet.param_style = style;

      /* NOTE: special case since "now" for continuous and longpoll - see https://issues.apache.org/jira/browse/COUCHDB-1501 */
      if (since == -1)
        {
          client->output.changes_comet.param_since = client->output.changes_comet.change_max_rowid;
        }
      else
        {
          client->output.changes_comet.param_since = (gsize)since;
        }

      client->output.changes_comet.param_feed = feed;
      client->output.changes_comet.param_include_links = include_links;
      client->output.changes_comet.param_context_id = context_id;
      client->output.changes_comet.param_tags = tags;
      client->output.changes_comet.param_tags_op = tags_op;
      client->output.changes_comet.change_generated = FALSE;
      client->output.changes_comet.change_last_seq = 0;

      client->output_type = DS_HTTPD_OUTPUT_CHANGES_COMET;

      if (feed == DP_CHANGES_FEED_LONGPOLL)
        client->output_mime = g_strdup (HTTP_MIME_JSON);
      else
        client->output_mime = g_strdup (HTTP_MIME_TEXTPLAIN); /* this will not be valid JSON { ...} { ... } ... */

      // we can not set output size and we use Transfer-Encoding: chunked instead - see httpd.c

      return HTTP_STATUS_200;
    }

  /* feed type poll */

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
    {
      if (tags != NULL)
        g_strfreev (tags);

      request_set_error (client, "Cannot connect to changes linkbase");
      return HTTP_STATUS_404;
    }

  if (dupin_linkbase_get_max_rowid (linkb, &last_seq) == FALSE)
    {
      if (tags != NULL)
        g_strfreev (tags);

      request_set_error (client, "Cannot get last change from linkbase");
      return HTTP_STATUS_404;
    }

  if (since == -1)
    {
      /* NOTE: This is only for completness, due we expect to always return an empty changes when feed poll with since=-1 */
      since = (gint)last_seq;
    }

  if (since == 0)
    {
      total_rows = dupin_linkbase_count (linkb, DP_LINK_TYPE_ANY, DP_COUNT_CHANGES);
    }
  else
    {
      if (dupin_linkbase_get_total_changes (linkb, &total_rows, (gsize)since, 0, style, DP_COUNT_CHANGES, TRUE, context_id, tags, tags_op, NULL) == FALSE)
        {
          if (tags != NULL)
            g_strfreev (tags);

          dupin_linkbase_unref (linkb);
          request_set_error (client, "Cannot get last seq number from changes linkbase");
          return HTTP_STATUS_500;
        }
    }

  if (dupin_linkbase_get_changes_list (linkb, count, 0, (gsize)since, 0, style, DP_COUNT_CHANGES, DP_ORDERBY_ROWID, descending, context_id, tags, tags_op, &results, NULL) ==
      FALSE)
    {
      if (tags != NULL)
        g_strfreev (tags);

      dupin_linkbase_unref (linkb);
      request_set_error (client, "Cannot list changes from linkbase");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_linkbase_get_changes_list_close (results);

      if (tags != NULL)
        g_strfreev (tags);

      dupin_linkbase_unref (linkb);
      request_set_error (client, "Cannot list changes from linkbase");
      return HTTP_STATUS_500;
    }

  json_object_set_int_member (obj, "total_rows", total_rows);
  json_object_set_int_member (obj, "rows_per_page", count);

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_changes_linkbase_error;

  for (list = results; list; list = list->next)
    {
      JsonNode * change = json_node_copy (list->data);

      if (include_links == TRUE)
        {
          JsonObject * on_obj = json_node_get_object (change);

          gchar * record_id   = (gchar *) json_object_get_string_member (on_obj, RESPONSE_OBJ_ID);
	  gchar * record_mvcc = (gchar *) json_object_get_string_member (
						json_array_get_object_element (json_object_get_array_member (on_obj, "changes"), 0)
						, RESPONSE_OBJ_REV);

          DupinLinkRecord * link_record=NULL;
          if (!(link_record = dupin_link_record_read (linkb, record_id, NULL)))
            {
              json_node_free (change);
              json_array_unref (array);
              goto request_global_get_changes_linkbase_error;
            }

          JsonNode * link = NULL;

          if (! (link = request_link_record_revision_obj (client, arguments,
							  link_record, record_id, record_mvcc,
							  FALSE)))
            {
              json_node_free (change);
              json_array_unref (array);
              goto request_global_get_changes_linkbase_error;
            }

          dupin_link_record_close (link_record);

          json_object_set_member (on_obj, RESPONSE_LINK_OBJ_LINK, link);
        }

      json_array_add_element (array, change);
    }

  json_object_set_array_member (obj, "results", array );

  if (total_rows > 0)
    {
      json_object_set_int_member (obj, "last_seq", last_seq);
    }

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_changes_linkbase_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_changes_linkbase_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_changes_linkbase_error;

  if( results )
    dupin_linkbase_get_changes_list_close (results);

  if (tags != NULL)
    g_strfreev (tags);

  dupin_linkbase_unref (linkb);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);

  json_object_unref (obj);

  return HTTP_STATUS_200;

request_global_get_changes_linkbase_error:

  if( results )
    dupin_linkbase_get_changes_list_close (results);

  if (tags != NULL)
    g_strfreev (tags);

  dupin_linkbase_unref (linkb);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  request_set_error (client, "Cannot list changes from linkbase");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_record_linkbase (DSHttpdClient * client, GList * path,
			   	    GList * arguments)
{
  gchar * mvcc = NULL;
  gboolean allrevs = FALSE;
  gchar * request_fields=NULL;

  GList *list=NULL;

  DupinLinkB *linkb;
  DupinLinkRecord *record;

  gboolean descending = FALSE;
  guint count = DUPIN_LINKB_MAX_LINKS_COUNT;
  guint offset = 0;

  JsonNode *node=NULL;

  gchar * link_id=NULL;

  /* check if special linkument name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* GET _special_link/link_ID */
      if (path->next->next)
        {
          link_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);

          if (path->next->next->next)
            {
              /* GET _special_link/link_ID/_fields/field */
              if (!g_strcmp0 (path->next->next->next->data, REQUEST_FIELDS))
                {
                  if (!path->next->next->next->next
                       || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_LINKS))
                    {
                      request_set_error (client, "Cannot GET link record field");
                      return HTTP_STATUS_400;
                    }

                  request_fields=(gchar *)path->next->next->next->next->data;
                }
            }
        }
    }
  else
    {
      if (path->next->next)
        {
          /* GET link_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->data, REQUEST_FIELDS))
            {
              if (!path->next->next->next
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_LINKS))
                {
		  request_set_error (client, "Cannot GET link record field");
                  return HTTP_STATUS_400;
                }

              request_fields=(gchar *)path->next->next->next->data;
            }
        }

      /* GET link_ID */
      link_id = g_strdup_printf ("%s", (gchar *)path->next->data);
    }

//g_message("request_global_get_record_linkbase: link_id=%s request_fields=%s\n", link_id, request_fields);

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
              g_free (link_id);
	      request_set_error (client, "Invalid link record MVCC revision");
              return HTTP_STATUS_400;
            }
	  mvcc = kv->value;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REVS)
	       && !g_strcmp0 (kv->value, "true"))
	allrevs = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LIMIT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_OFFSET))
	offset = atoi (kv->value);
    }

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
    {
      g_free (link_id);
      request_set_error (client, "Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_link_record_read (linkb, link_id, NULL)))
    {
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      request_set_error (client, "Cannot read link record from linkbase");
      return HTTP_STATUS_404;
    }

  if ((dupin_link_record_is_deleted (record, NULL) == TRUE) && (allrevs == FALSE))

    {   
      dupin_link_record_close (record);
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      request_set_error (client, "Link record is deleted");
      return HTTP_STATUS_404;
    } 

  /* Show all revisions: */
  if (allrevs == TRUE)
    {
      GList * revisions=NULL;
      GList * list=NULL;
      JsonObject *obj;
      JsonArray *array;
      gsize total_rows = 0;

      node = json_node_new (JSON_NODE_OBJECT);

      if (node == NULL)
	{
	  dupin_link_record_close (record);
	  dupin_linkbase_unref (linkb);
          g_free (link_id);
	  request_set_error (client, "Cannot get list of link record revisions");
	  return HTTP_STATUS_500;
	}

      obj = json_object_new ();

      if (obj == NULL)
        goto request_global_get_record_error;

      json_node_take_object (node, obj);

      array = json_array_new ();

      if (array == NULL)
        goto request_global_get_record_error;

      if ((dupin_link_record_get_revisions_list (record,
				           count,
					   //offset, 1, 0, DP_COUNT_ALL, DP_ORDERBY_REV, descending,
					   offset, 0, 0, DP_COUNT_ALL, DP_ORDERBY_REV, descending,
					   &revisions, NULL) == FALSE)
         || (dupin_link_record_get_total_revisions (record, &total_rows, NULL) == FALSE))
	{
	  dupin_link_record_close (record);
	  dupin_linkbase_unref (linkb);
          g_free (link_id);
	  request_set_error (client, "Cannot get list of link record revisions");
	  return HTTP_STATUS_500;
	}

      for (list = revisions; list; list = list->next)
        {
	  JsonNode *on;

	  if (!
	      (on =
	       request_link_record_revision_obj (client, arguments,
						 record,
						 (gchar *) dupin_link_record_get_id (record),
				   		 (gchar *) list->data,
						 FALSE)))
            {
              dupin_link_record_get_revisions_list_close (revisions);
	      json_array_unref (array);
	      goto request_global_get_record_error;
            }

          if (request_fields != NULL)
            {
              JsonObject * on_obj = json_node_get_object (on);

              if (json_object_has_member (on_obj, (const gchar *)request_fields) == FALSE)
                {
                  dupin_link_record_get_revisions_list_close (revisions);
	          json_array_unref (array);
	          dupin_link_record_close (record);
	          dupin_linkbase_unref (linkb);
                  g_free (link_id);
		  request_set_error (client, "Cannot get field for link record revisions list");
	          return HTTP_STATUS_404;
                }

              json_array_add_element( array, json_node_copy (json_object_get_member (on_obj, (const gchar *)request_fields)));
              json_node_free (on);
            }
          else
            {
              json_array_add_element( array, on);
            }
        }

      dupin_link_record_get_revisions_list_close (revisions);

      json_object_set_int_member (obj, "total_rows", total_rows);
      json_object_set_int_member (obj, "offset", offset);
      json_object_set_int_member (obj, "rows_per_page", count);

      json_object_set_array_member (obj, "_revs_info", array );
    }

  /* Show a single revision: */
  else
    {
      JsonNode *node_temp=NULL;

      if (mvcc == NULL)
	mvcc = dupin_link_record_get_last_revision (record);

      else if (dupin_util_mvcc_revision_cmp (mvcc, dupin_link_record_get_last_revision (record)) > 0)
	{
	  dupin_link_record_close (record);
	  dupin_linkbase_unref (linkb);
          g_free (link_id);
	  request_set_error (client, "Requested link record MVCC revision newer that latest revision");
	  return HTTP_STATUS_404;
	}

      if (!
	  (node_temp =
	   request_link_record_revision_obj (client, arguments,
					     record, (gchar *) dupin_link_record_get_id (record),
					     mvcc,
					     TRUE)))
	{
	  dupin_link_record_close (record);
	  dupin_linkbase_unref (linkb);
          g_free (link_id);
	  request_set_error (client, "Cannot get link record revision");
	  return HTTP_STATUS_404;
	}

      if (request_fields != NULL)
        {
          JsonObject * node_obj = json_node_get_object (node_temp);

          if (json_object_has_member (node_obj, (const gchar *)request_fields) == FALSE)
            {
              if (node_temp != NULL)
                json_node_free (node_temp);
	      dupin_link_record_close (record);
	      dupin_linkbase_unref (linkb);
              g_free (link_id);
	      request_set_error (client, "Cannot get link record revision field");
	      return HTTP_STATUS_404;
            }

          node = json_node_copy (json_object_get_member (node_obj, (const gchar *)request_fields));
        }
      else
        {
          node = json_node_copy (node_temp);
        }

//g_message("request_global_get_record_linkbase: link_id=%s request_fields=%s\n", link_id, request_fields);

      json_node_free (node_temp);
    }

  /* Writing: */

  client->output.string.string = dupin_util_json_serialize (node);
  client->output_size = strlen(client->output.string.string);

  if (client->output.string.string == NULL)
    goto request_global_get_record_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (node != NULL)
    json_node_free (node);

  dupin_link_record_close (record);
  dupin_linkbase_unref (linkb);

  g_free (link_id);

  return HTTP_STATUS_200;

request_global_get_record_error:

  if (node != NULL)
    json_node_free (node);

  dupin_link_record_close (record);
  dupin_linkbase_unref (linkb);

  g_free (link_id);

  request_set_error (client, "Cannot get link record");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_view (DSHttpdClient * client, GList * path,
			 GList * arguments)
{
  GList *list;
  DupinView *view;

  JsonObject *obj;
  JsonObject *subobj;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_QUERY))
	return request_global_get_view_query (client, path, kv->value);
    }

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    {
      request_set_error (client, "Cannot connect to view");
      return HTTP_STATUS_404;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      dupin_view_unref (view);
      request_set_error (client, "Cannot get view information");
      return HTTP_STATUS_500;
    }

  json_object_set_string_member (obj, "view_name", (gchar *) dupin_view_get_name (view));

  subobj = json_object_new ();

  if (subobj == NULL)
    goto request_global_get_view_error;

  json_object_set_string_member (subobj, "name", (gchar *) dupin_view_get_parent (view));
  json_object_set_boolean_member (subobj, "is_db", dupin_view_get_parent_is_db (view));
  json_object_set_boolean_member (subobj, "is_linkb", dupin_view_get_parent_is_linkb (view));
  json_object_set_object_member (obj, "parent", subobj );

  subobj = json_object_new ();

  if (subobj == NULL)
    goto request_global_get_view_error;

  json_object_set_string_member (subobj, "name", (gchar *) dupin_view_get_output (view));
  json_object_set_boolean_member (subobj, "is_db", dupin_view_get_output_is_db (view));
  json_object_set_boolean_member (subobj, "is_linkb", dupin_view_get_output_is_linkb (view));
  json_object_set_object_member (obj, "output", subobj );

  subobj = json_object_new ();

  if (subobj == NULL)
    goto request_global_get_view_error;

  /* TODO - double check that the actual Javascript code does not need any special escaping or anything here */
  json_object_set_string_member (subobj, "code", (gchar *) dupin_view_get_map (view));
  json_object_set_string_member (subobj, "language", (gchar *) dupin_util_mr_lang_to_string (dupin_view_get_map_language (view)));
  json_object_set_object_member (obj, "map", subobj );

  gchar * reduce = (gchar *) dupin_view_get_reduce (view);
  gchar * reduce_lang = (gchar *) dupin_util_mr_lang_to_string (dupin_view_get_reduce_language (view));

  if (reduce != NULL)
    {
      subobj = json_object_new ();

      if (subobj == NULL)
        goto request_global_get_view_error;

      /* TODO - double check that the actual Javascript code does not need any special escaping or anything here */
      json_object_set_string_member (subobj, "code", reduce);
      json_object_set_string_member (subobj, "language", reduce_lang);
      json_object_set_object_member (obj, "reduce", subobj );
    }

  json_object_set_int_member (obj, "doc_count", dupin_view_count (view));
  json_object_set_int_member (obj, "disk_size", dupin_view_get_size (view));

  gsize creation_time;
  dupin_view_get_creation_time (view, &creation_time);
  json_object_set_string_member (obj, "instance_start_time", g_strdup_printf ("%" G_GSIZE_FORMAT, creation_time));

  json_object_set_boolean_member (obj, "sync", dupin_view_is_sync (view));
  json_object_set_boolean_member (obj, "sync_running", dupin_view_is_syncing (view));
  json_object_set_boolean_member (obj, "sync_map_running", (view->sync_map_thread) ? TRUE : FALSE);
  json_object_set_boolean_member (obj, "sync_reduce_running", (view->sync_reduce_thread) ? TRUE : FALSE);

  /* Writing: */
  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_view_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_view_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_view_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  dupin_view_unref (view);
  return HTTP_STATUS_200;

request_global_get_view_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  dupin_view_unref (view);

  request_set_error (client, "Cannot get view information");

  return HTTP_STATUS_500;
}

/* TODO - probably useless to ask sync on demand */

static DSHttpStatusCode
request_global_view_sync (DSHttpdClient * client, GList * path,
			  GList * arguments)
{
  DupinView *view;

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    {
      request_set_error (client, "Cannot connect to view");
      return HTTP_STATUS_404;
    }

  dupin_view_sync (view);

  if (dupin_view_get_error (view))
    {
      /* TODO - we do not fail yet, due either map or reduce threads map be already started
		and thread poll will queue up the requests anyway - we just tell user/caller
		we had an issue with thread/s eventually */

      request_set_warning (client, dupin_view_get_error (view));
    }

  dupin_view_unref (view);

  return HTTP_STATUS_200;
}

static DSHttpStatusCode
request_global_get_all_docs_view (DSHttpdClient * client, GList * path,
				  GList * arguments)
{
  DupinView *view=NULL;
  DupinDB *docs_db=NULL;
  DupinLinkB *docs_linkb=NULL;
  DupinView *docs_view=NULL;

  GList *list;
  GList *results;

  gsize total_rows = 0;
  gboolean descending = FALSE;
  guint count = DUPIN_VIEW_MAX_DOCS_COUNT;
  guint offset = 0;
  gchar * startkey = NULL;
  gchar * endkey = NULL;
  gboolean inclusive_end = TRUE;
  gboolean include_docs = FALSE;
  gchar * startvalue = NULL;
  gchar * endvalue = NULL;
  gboolean inclusive_end_value = TRUE;

  gchar * filter_by = NULL;
  DupinFieldsFormatType filter_by_format = DP_FIELDS_FORMAT_DOTTED;
  DupinFilterByType filter_op = DP_FILTERBY_UNDEF;
  gchar * filter_values = NULL;

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    {
      request_set_error (client, "Cannot connect to view");
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_LIMIT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_OFFSET))
	offset = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_DOCS))
        {
          if (view->reduce != NULL
             || (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
                 g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE")))
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              if (startvalue != NULL)
                g_free (startvalue);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);
	      
      	      if (view->reduce != NULL)
                request_set_error (client, "The " REQUEST_GET_ALL_DOCS_INCLUDE_DOCS " parameter can only be used on map only views.");
              else
                request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_INCLUDE_DOCS " parameter. Allowed values are: true, false");

              return HTTP_STATUS_400;
            }

          if (view->reduce == NULL)
            {
              include_docs = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
            }
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUSIVEEND))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

	      if (startvalue != NULL)
                g_free (startvalue);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_INCLUSIVEEND " parameter. Allowed values are: true, false");

              return HTTP_STATUS_400;
            }

          inclusive_end = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUSIVEEND_VALUE))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

	      if (startvalue != NULL)
                g_free (startvalue);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_INCLUSIVEEND_VALUE " parameter. Allowed values are: true, false");

              return HTTP_STATUS_400;
            }

          inclusive_end_value = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_KEY))
        {
          if (startkey != NULL)
            g_free (startkey);

	  startkey = dupin_util_json_string_normalize (kv->value);
          if (startkey == NULL)
            {
              if (endkey != NULL)
                g_free (endkey);

	      if (startvalue != NULL)
                g_free (startvalue);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_KEY " parameter. It must be a valid JSON node.");

              return HTTP_STATUS_400;
            }

	  endkey = g_strdup (startkey);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_STARTKEY))
        {
          if (startkey != NULL)
            g_free (startkey);

	  startkey = dupin_util_json_string_normalize (kv->value);
          if (startkey == NULL)
            {
              if (endkey != NULL)
                g_free (endkey);

	      if (startvalue != NULL)
                g_free (startvalue);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_STARTKEY " parameter. It must be a valid JSON node.");

              return HTTP_STATUS_400;
            }
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_ENDKEY))
        {
          if (endkey != NULL)
            g_free (endkey);

	  endkey = dupin_util_json_string_normalize (kv->value);
          if (endkey == NULL)
            {
              if (startkey != NULL)
                g_free (startkey);

	      if (startvalue != NULL)
                g_free (startvalue);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_ENDKEY " parameter. It must be a valid JSON node.");

              return HTTP_STATUS_400;
            }
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_VALUE))
        {
          if (startvalue != NULL)
            g_free (startvalue);

	  startvalue = dupin_util_json_string_normalize (kv->value);
          if (startvalue == NULL)
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_VALUE " parameter. It must be a valid JSON node.");

              return HTTP_STATUS_400;
            }

	  endvalue = g_strdup (startvalue);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_STARTVALUE))
        {
          if (startvalue != NULL)
            g_free (startvalue);

	  startvalue = dupin_util_json_string_normalize (kv->value);
          if (startvalue == NULL)
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_STARTVALUE " parameter. It must be a valid JSON node.");

              return HTTP_STATUS_400;
            }
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_ENDVALUE))
        {
          if (endvalue != NULL)
            g_free (endvalue);

	  endvalue = dupin_util_json_string_normalize (kv->value);
          if (endvalue == NULL)
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

	      if (startvalue != NULL)
                g_free (startvalue);

              dupin_view_unref (view);

              request_set_error (client, "Invalid " REQUEST_GET_ALL_DOCS_ENDVALUE " parameter. It must be a valid JSON node.");

              return HTTP_STATUS_400;
            }
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_BY))
        {
          filter_by = kv->value;
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_DOTTED))
            filter_by_format = DP_FIELDS_FORMAT_DOTTED;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_JSONPATH))
            filter_by_format = DP_FIELDS_FORMAT_JSONPATH;
        }
 
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
            filter_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
            filter_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
            filter_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
            filter_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_VALUES))
        {
          filter_values = kv->value;
        }
    }

  if (include_docs == TRUE)
    {
      if (dupin_view_get_parent_is_db (view) == TRUE)
        {
          if (!(docs_db = dupin_database_open (view->d, view->parent, NULL)))
            {
	      if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

	      if (startvalue != NULL)
                g_free (startvalue);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);
              request_set_error (client, "Cannot connect to parent database");
              return HTTP_STATUS_404;
            }
        }
      else if (dupin_view_get_parent_is_linkb (view) == TRUE)
        {
          if (!(docs_linkb = dupin_linkbase_open (view->d, view->parent, NULL)))
            {
             if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

	      if (startvalue != NULL)
                g_free (startvalue);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);
              request_set_error (client, "Cannot connect to parent linkbase");
              return HTTP_STATUS_404;
            }
        }
      else
        {
          if (!(docs_view = dupin_view_open (view->d, view->parent, NULL)))
            {
	      if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

	      if (startvalue != NULL)
                g_free (startvalue);

              if (endvalue != NULL)
                g_free (endvalue);

              dupin_view_unref (view);
              request_set_error (client, "Cannot connect to parent view");
    	      return HTTP_STATUS_404;
            }
        }
    }

  if (dupin_view_record_get_list_total (view, &total_rows, 0, 0, startkey, endkey, inclusive_end, 
						startvalue, endvalue, inclusive_end_value,
						filter_by, filter_by_format, filter_op, filter_values, NULL) == FALSE)
    {
      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      if (startvalue != NULL)
        g_free (startvalue);

      if (endvalue != NULL)
        g_free (endvalue);

      if (docs_db != NULL)
        dupin_database_unref (docs_db);

      if (docs_linkb != NULL)
        dupin_linkbase_unref (docs_linkb);

      if (docs_view != NULL)
        dupin_view_unref (docs_view);

      dupin_view_unref (view);

      request_set_error (client, "Cannot get total of records from view");

      return HTTP_STATUS_500;
    }

  if (dupin_view_record_get_list (view, count, offset, 0, 0, DP_ORDERBY_KEY, descending,
				  startkey, endkey, inclusive_end,
				  startvalue, endvalue, inclusive_end_value,
				  filter_by, filter_by_format, filter_op, filter_values,
				  &results, NULL) == FALSE)
    {
      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      if (startvalue != NULL)
        g_free (startvalue);
      
      if (endvalue != NULL)
        g_free (endvalue);

      if (docs_db != NULL)
        dupin_database_unref (docs_db);

      if (docs_linkb != NULL)
        dupin_linkbase_unref (docs_linkb);

      if (docs_view != NULL)
        dupin_view_unref (docs_view);

      dupin_view_unref (view);

      request_set_error (client, "Cannot get list of records from view");

      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if (docs_db != NULL)
        dupin_database_unref (docs_db);

      if (docs_linkb != NULL)
        dupin_linkbase_unref (docs_linkb);

      if (docs_view != NULL)
        dupin_view_unref (docs_view);

      if( results )
        dupin_view_record_get_list_close(results);

      dupin_view_unref (view);

      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      if (startvalue != NULL)
        g_free (startvalue);
      
      if (endvalue != NULL)
        g_free (endvalue);

      request_set_error (client, "Cannot get list of records from view");

      return HTTP_STATUS_500;
    }

  json_object_set_int_member (obj, "total_rows", total_rows);
  json_object_set_int_member (obj, "offset", offset);
  json_object_set_int_member (obj, "rows_per_page", count);

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_all_docs_view_error;

  for (list = results; list; list = list->next)
    {
      DupinViewRecord *record = list->data;
      JsonNode *on = NULL;

      JsonNode *result_node=json_node_new (JSON_NODE_OBJECT);
      JsonObject *result_obj=json_object_new ();
      json_node_take_object (result_node, result_obj);

      if (!
	  (on =
	   request_view_record_obj (client, arguments,
				    record,
				    (gchar *)
				    dupin_view_record_get_id (record),
				    TRUE)))
        {
	  json_node_free (result_node);
          json_array_unref (array);
	  goto request_global_get_all_docs_view_error;
        }

      /* TODO - need to make sure two includes are done if DP_LINKBASE_INCLUDE_DOC_TYPE_ALL is used */

      if (include_docs == TRUE)
        {
          gchar * record_id;
	  JsonNode * doc = NULL;
	  JsonObject * on_obj = NULL;

          if (json_node_get_node_type (on) == JSON_NODE_OBJECT)
            on_obj = json_node_get_object (on);

          if (on_obj != NULL
	      && json_object_has_member (on_obj, REQUEST_OBJ_ID))
            record_id = (gchar *) json_object_get_string_member (on_obj, REQUEST_OBJ_ID);
          else if (on_obj != NULL
		   && json_object_has_member (on_obj, RESPONSE_OBJ_ID))
            record_id = (gchar *) json_object_get_string_member (on_obj, RESPONSE_OBJ_ID);
	  else
	    {
	      JsonNode * pid = dupin_view_record_get_pid (record);
              record_id = (gchar *)json_array_get_string_element (json_node_get_array (pid), 0);
            }

          if (docs_db != NULL)
            {
	      DupinRecord * db_record=NULL;
              if (!(db_record = dupin_record_read (docs_db, record_id, NULL)))
                {
                  // TODO - log error
                  doc = json_node_new (JSON_NODE_NULL);
                }
              else
                {
                  if (! (doc = request_record_revision_obj (client,
                                                           arguments,
                                                           db_record,
                                                           record_id,
                                                           (gchar *)dupin_record_get_last_revision (db_record),
							   TRUE)))
                    {
                      // TODO - log error
                      doc = json_node_new (JSON_NODE_NULL);
                    }

	          dupin_record_close (db_record);
                }
            }
          else if (docs_linkb != NULL)
            {
	      DupinLinkRecord * linkb_record=NULL;
              if (!(linkb_record = dupin_link_record_read (docs_linkb, record_id, NULL)))
                {
                  // TODO - log error
                  doc = json_node_new (JSON_NODE_NULL);
                }
              else
                {
                  if (!(doc = request_link_record_revision_obj (client, arguments,
                                                                linkb_record,
								record_id,
								(gchar *)dupin_link_record_get_last_revision (linkb_record),
								TRUE)))
                    {
                      // TODO - log error
                      doc = json_node_new (JSON_NODE_NULL);
                    }

	          dupin_link_record_close (linkb_record);
                }
            }
          else if (docs_view != NULL)
            {
              DupinViewRecord * view_record=NULL;
              if (!(view_record = dupin_view_record_read (docs_view, record_id, NULL)))
                {
                  // TODO - log error
                  doc = json_node_new (JSON_NODE_NULL);
                }
              else
                {
                  if (!  (node = request_view_record_obj (client, arguments,
							  view_record,
							  (gchar *) dupin_view_record_get_id (view_record),
							  TRUE)))
                    {
                      // TODO - log error
                      doc = json_node_new (JSON_NODE_NULL);
                    }

	          dupin_view_record_close (view_record);
                }
            }

          json_object_set_member (result_obj, RESPONSE_VIEW_OBJ_DOC, doc);

        }

      json_object_set_member (result_obj, RESPONSE_VIEW_OBJ_VALUE, on);
      json_object_set_member (result_obj, RESPONSE_VIEW_OBJ_KEY, json_node_copy (dupin_view_record_get_key (record)));

      json_array_add_element( array, result_node);
   }

  json_object_set_array_member (obj, "rows", array );

  /* Writing: */
  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_all_docs_view_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_all_docs_view_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_all_docs_view_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (startkey != NULL)
    g_free (startkey);

  if (endkey != NULL)
    g_free (endkey);

  if (startvalue != NULL)
    g_free (startvalue);
      
  if (endvalue != NULL)
    g_free (endvalue);

  if (docs_db != NULL)
    dupin_database_unref (docs_db);

  if (docs_linkb != NULL)
    dupin_linkbase_unref (docs_linkb);

  if (docs_view != NULL)
    dupin_view_unref (docs_view);

  if( results )
    dupin_view_record_get_list_close(results);

  dupin_view_unref (view);

  return HTTP_STATUS_200;

request_global_get_all_docs_view_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (startkey != NULL)
    g_free (startkey);

  if (endkey != NULL)
    g_free (endkey);

  if (startvalue != NULL)
    g_free (startvalue);
      
  if (endvalue != NULL)
    g_free (endvalue);

  if (docs_db != NULL)
    dupin_database_unref (docs_db);

  if (docs_linkb != NULL)
    dupin_linkbase_unref (docs_linkb);

  if (docs_view != NULL)
    dupin_view_unref (docs_view);

  if( results )
    dupin_view_record_get_list_close(results);

  dupin_view_unref (view);

  request_set_error (client, "Cannot get list of records from view");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_record_view (DSHttpdClient * client, GList * path,
				GList * arguments)
{
  DupinView *view;
  DupinViewRecord *record;

  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    {
      request_set_error (client, "Cannot connect to view");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_view_record_read (view, path->next->next->data, NULL)))
    {
      dupin_view_unref (view);
      request_set_error (client, "Cannot get view information");
      return HTTP_STATUS_404;
    }

  if (!
      (node =
       request_view_record_obj (client, arguments, record,
				(gchar *) dupin_view_record_get_id (record),
				TRUE)))
    {
      dupin_view_record_close (record);
      dupin_view_unref (view);
      request_set_error (client, "Cannot get view record");
      return HTTP_STATUS_404;
    }

  /* Writing: */
  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_view_record_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_view_record_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);

  dupin_view_record_close (record);
  dupin_view_unref (view);

  return HTTP_STATUS_200;

request_global_get_view_record_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);

  dupin_view_record_close (record);
  dupin_view_unref (view);

  request_set_error (client, "Cannot get view record");

  return HTTP_STATUS_500;
}

#define QUERY_BLOCK	100

static DSHttpStatusCode
request_global_get_database_query (DSHttpdClient * client, GList * path,
				   gchar * query)
{
  DupinDB *db;
  GList *results;
  gsize offset = 0;

  JsonArray *array;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      return HTTP_STATUS_404;
    }

  array = json_array_new ();

  while (dupin_record_get_list (db, QUERY_BLOCK, offset, 0, 0, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID,
					FALSE, NULL, DP_FILTERBY_EQUALS, 
					NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == TRUE && results)
    {
      GList *list;

      for (list = results; list; list = list->next)
	{
	  DupinRecord *record = list->data;
	  tb_jsonpath_result_t *ret = NULL;

	  /* TODO - check if we need to json_node_copy ( dupin_record_get_revision_node() ) or not */
	  if (tb_jsonpath_exec
	      (query, -1, json_node_get_object (dupin_record_get_revision_node (record, NULL)), &ret, NULL,
	       NULL) == TRUE && ret)
	    {
	      JsonNode *value;

	      while (tb_jsonpath_result_next (ret, &value) == TRUE)
		{
		  json_array_add_element (array, json_node_copy (value));
		}

	      tb_jsonpath_result_free (ret);
	    }
	}

      offset += g_list_length (results);
      dupin_record_get_list_close (results);
    }

  node = json_node_new (JSON_NODE_ARRAY);

  if (node == NULL)
    goto request_global_get_database_query_error;

  json_node_set_array (node, array);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_database_query_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_database_query_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  dupin_database_unref (db);
  return HTTP_STATUS_200;

request_global_get_database_query_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  dupin_database_unref (db);

  request_set_error (client, "Cannot query database");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_linkbase_query (DSHttpdClient * client, GList * path,
				   gchar * query)
{
  DupinLinkB *linkb;
  GList *results;
  gsize offset = 0;

  JsonArray *array;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  array = json_array_new ();

  while (dupin_link_record_get_list (linkb, QUERY_BLOCK, offset, 0, 0, DP_LINK_TYPE_ANY, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE,
					NULL, NULL, DP_FILTERBY_EQUALS, NULL, DP_FILTERBY_EQUALS, NULL, 
					DP_FILTERBY_EQUALS, NULL, DP_FILTERBY_EQUALS,
					NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == TRUE && results)
    {
      GList *list;

      for (list = results; list; list = list->next)
	{
	  DupinLinkRecord *record = list->data;
	  tb_jsonpath_result_t *ret = NULL;

	  /* TODO - check if we need to json_node_copy ( dupin_link_record_get_revision_node() ) or not */
	  if (tb_jsonpath_exec
	      (query, -1, json_node_get_object (dupin_link_record_get_revision_node (record, NULL)), &ret, NULL,
	       NULL) == TRUE && ret)
	    {
	      JsonNode *value;

	      while (tb_jsonpath_result_next (ret, &value) == TRUE)
		{
		  json_array_add_element (array, json_node_copy (value));
		}

	      tb_jsonpath_result_free (ret);
	    }
	}

      offset += g_list_length (results);
      dupin_link_record_get_list_close (results);
    }

  node = json_node_new (JSON_NODE_ARRAY);

  if (node == NULL)
    goto request_global_get_linkbase_query_error;

  json_node_set_array (node, array);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_linkbase_query_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_linkbase_query_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  dupin_linkbase_unref (linkb);
  return HTTP_STATUS_200;

request_global_get_linkbase_query_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  dupin_linkbase_unref (linkb);

  request_set_error (client, "Cannot query linkbase");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_view_query (DSHttpdClient * client, GList * path,
			       gchar * query)
{
  DupinView *view;
  GList *results;
  gsize offset = 0;

  JsonArray *array;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    {
      request_set_error (client, "Cannot connect to view");
      return HTTP_STATUS_404;
    }

  array = json_array_new ();

  while (dupin_view_record_get_list (view, QUERY_BLOCK, offset, 0, 0, DP_ORDERBY_KEY, FALSE, NULL, NULL, TRUE, NULL, NULL, TRUE,
					NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, NULL) == TRUE
	 && results)
    {
      GList *list;

      for (list = results; list; list = list->next)
	{
	  DupinViewRecord *record = list->data;
	  tb_jsonpath_result_t *ret;

	  /* TODO - check if we need to json_node_copy ( dupin_view_record_get() ) or not */
	  if (tb_jsonpath_exec
	      (query, -1, json_node_get_object (dupin_view_record_get (record)), &ret, NULL,
	       NULL) == TRUE && ret)
	    {
	      JsonNode *value;

	      while (tb_jsonpath_result_next (ret, &value) == TRUE)
		{
		  json_array_add_element (array, json_node_copy (value));
		}

	      tb_jsonpath_result_free (ret);
	    }
	}

      offset += g_list_length (results);
      dupin_view_record_get_list_close (results);
    }

  node = json_node_new (JSON_NODE_ARRAY);

  if (node == NULL)
    goto request_global_get_view_query_error;

  json_node_set_array (node, array);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_view_query_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_view_query_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  dupin_view_unref (view);
  return HTTP_STATUS_200;

request_global_get_view_query_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_array_unref (array);
  dupin_view_unref (view);

  request_set_error (client, "Cannot query view");

  return HTTP_STATUS_500;
}

/* POST ********************************************************************/

static DSHttpStatusCode request_global_post_record (DSHttpdClient * client,
						    GList * path,
						    GList * arguments);
static DSHttpStatusCode request_global_post_bulk_docs (DSHttpdClient * client,
						       GList * path,
						       GList * arguments);
static DSHttpStatusCode request_global_post_doc_link (DSHttpdClient * client,
						      GList * path,
						      GList * arguments,
						      DupinLinksType link_type);

static DSHttpStatusCode request_global_post_bulk_doc_links (DSHttpdClient * client,
						            GList * path,
						            GList * arguments);

static DSHttpStatusCode request_global_post_compact_database (DSHttpdClient * client,
						     	      GList * path,
						     	      GList * arguments);

static DSHttpStatusCode request_global_post_compact_linkbase (DSHttpdClient * client,
						     	      GList * path,
						     	      GList * arguments);

static DSHttpStatusCode request_global_post_check_linkbase (DSHttpdClient * client,
						     	    GList * path,
						     	    GList * arguments);

static DSHttpStatusCode
request_global_post (DSHttpdClient * client, GList * path, GList * arguments)
{
  if (!path)
    {
      request_set_error (client, "Missing URL path");
      return HTTP_STATUS_400;
    }

  if (!g_strcmp0 (path->data, REQUEST_LINKBS))
    {
      if (path->next && path->next->next)
        {
          /* POST /_linkbs/linkbase/_compact */
	  if (!g_strcmp0 (path->next->next->data, REQUEST_POST_COMPACT_LINKBASE))
            return request_global_post_compact_linkbase (client, path->next, arguments);

          /* POST /_linkbs/linkbase/_check */
          if (!g_strcmp0 (path->next->next->data, REQUEST_POST_CHECK_LINKBASE))
            return request_global_post_check_linkbase (client, path->next, arguments);
        }

      request_set_error (client, "POST /_linkbs allowed commands are: /_linkbs/linkbase/_compact and /_linkbs/linkbase/_check");

      return HTTP_STATUS_400;
    }

  /* POST /database */
  if (!path->next)
    return request_global_post_record (client, path, arguments);

  /* POST /database/_bulk_docs */
  if (!g_strcmp0 (path->next->data, REQUEST_POST_BULK_DOCS) && !path->next->next)
    return request_global_post_bulk_docs (client, path, arguments);

  /* POST /database/_compact */
  if (!g_strcmp0 (path->next->data, REQUEST_POST_COMPACT_DATABASE) && !path->next->next)
    return request_global_post_compact_database (client, path, arguments);

  /* POST /database/doc_id/_links */
  if (path->next
      && path->next->next && !g_strcmp0 (path->next->next->data, REQUEST_OBJ_LINKS)
      && !path->next->next->next)
    return request_global_post_doc_link (client, path, arguments, DP_LINK_TYPE_WEB_LINK);

  /* POST /database/doc_id/_relationships */
  if (path->next
      && path->next->next && !g_strcmp0 (path->next->next->data, REQUEST_OBJ_RELATIONSHIPS)
      && !path->next->next->next)
    return request_global_post_doc_link (client, path, arguments, DP_LINK_TYPE_RELATIONSHIP);

  /* POST /database/doc_id */
  if (path->next
      && !path->next->next )
    return request_global_post_doc_link (client, path, arguments, DP_LINK_TYPE_ANY);

  /* POST /database/doc_id/_bulk_links */
  if (path->next
      && path->next->next && !g_strcmp0 (path->next->next->data, REQUEST_POST_BULK_LINKS)
      && !path->next->next->next)
    return request_global_post_bulk_doc_links (client, path, arguments);

  request_set_error (client, "POST /database allowed commands are: /database, /database/_bulk_docs, /database/_compact, /database/doc_id, /database/doc_id/_bulk_links, /database/doc_id/_links, /database/doc_id/_relationships");

  return HTTP_STATUS_400;
}

static DSHttpStatusCode
request_global_post_record (DSHttpdClient * client, GList * path,
			    GList * arguments)
{
  DupinDB * db=NULL;
  DSHttpStatusCode code;
  GList *response_list=NULL;
  GError *error = NULL;

  if (!client->body)
    {
      request_set_error (client, "Missing POST body");
      return HTTP_STATUS_400;
    }

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      request_set_error (client, "Cannot parse POST body");
      code = HTTP_STATUS_500;
      goto request_global_post_record_end;
    }

  /* TODO - add options to GET methods to pass escape/unescape Unicode if needed by client accordingly to RFC4627
            we currently read escaped JSON documents and stored them as UTF-8 - also check UTF-16 and UTF-32 encodings if needed/supported */

  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      if (error)
        {
          request_set_error (client, error->message);
          g_error_free (error);
        }
      code = HTTP_STATUS_400;
      goto request_global_post_record_end;
    }

  JsonNode * node = json_parser_get_root (parser);

  if (node == NULL)
    {
      request_set_error (client, "Cannot parse POST body");
      code = HTTP_STATUS_500;
      goto request_global_post_record_end;
    }

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      code = HTTP_STATUS_404;
      goto request_global_post_record_end;
    }

  if (dupin_record_insert (db, node, NULL, NULL, &response_list, FALSE, &error) == TRUE)
    {
      if (request_record_response (client, response_list, FALSE) == FALSE)
        {
	  code = HTTP_STATUS_500;
          request_set_error (client, "Cannot generate JSON output response");
        }
      else
        {
          code = HTTP_STATUS_201;
        }
    }
  else
    {
      if (error != NULL && error->code == DUPIN_ERROR_RECORD_CONFLICT)
        code = HTTP_STATUS_409;
      else
        code = HTTP_STATUS_400;
      request_set_error (client, dupin_database_get_error (db));
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  dupin_database_unref (db);

request_global_post_record_end:

  if (parser != NULL)
    g_object_unref (parser);

  if (error)
    g_error_free (error);

  return code;
}

static DSHttpStatusCode
request_global_post_doc_link (DSHttpdClient * client, GList * path,
			      GList * arguments,
			      DupinLinksType link_type)
{
  DSHttpStatusCode code;
  GError *error = NULL;
  GList * response_list = NULL;
  DupinDB * db=NULL; 
  DupinLinkB * linkb=NULL;
  gboolean strict_links = FALSE;
  GList * l=NULL;

  if (!client->body
      || !path->next)
    {
      request_set_error (client, "POST body or path missing");
      return HTTP_STATUS_400;
    }

  for (l = arguments; l; l = l->next)
    {
      dupin_keyvalue_t *kv = l->data;

      if (!g_strcmp0 (kv->key, REQUEST_STRICT))
        {
	  if (!g_strcmp0 (kv->value, REQUEST_STRICT_LINKS))
            {
	      strict_links = true;
            }
        }
    }

  gchar * context_id = path->next->data;

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      request_set_error (client, "Cannot parse POST body");
      code = HTTP_STATUS_500;
      goto request_global_post_doc_link_end;
    }

  /* TODO - add options to GET methods to pass escape/unescape Unicode if needed by client accordingly to RFC4627
            we currently read escaped JSON documents and stored them as UTF-8 - also check UTF-16 and UTF-32 encodings if needed/supported */

  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      if (error)
        {
          request_set_error (client, error->message);
          g_error_free (error);
        }
      code = HTTP_STATUS_400;
      goto request_global_post_doc_link_end;
    }

  JsonNode * node = json_parser_get_root (parser);

  if (node == NULL)
    {
      request_set_error (client, "Cannot parse POST body");
      code = HTTP_STATUS_500;
      goto request_global_post_doc_link_end;
    }

  /* NOTE - need to get the right linkbase to post to */
  if (!  (db = dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      code = HTTP_STATUS_404;
      goto request_global_post_doc_link_end;
    }

  if (!(linkb = dupin_linkbase_open (client->thread->data->dupin, dupin_database_get_default_linkbase_name (db), NULL)))
    {
      dupin_database_unref (db);
      request_set_error (client, "Cannot connect to linkbase");
      code = HTTP_STATUS_404;
      goto request_global_post_doc_link_end;
    }

  dupin_database_unref (db);

  if (dupin_link_record_insert (linkb, node, NULL, NULL, context_id, link_type, &response_list, strict_links, FALSE, &error) == TRUE)
    {
      if (request_record_response (client, response_list, FALSE) == FALSE)
        {
          code = HTTP_STATUS_500;
          request_set_error (client, "Cannot generate JSON output response");
        }
      else
        {
          code = HTTP_STATUS_201;
        }
    }
  else
    {
      if (error != NULL && error->code == DUPIN_ERROR_RECORD_CONFLICT)
        code = HTTP_STATUS_409;
      else
        code = HTTP_STATUS_400;
      request_set_error (client, dupin_linkbase_get_error (linkb));
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  dupin_linkbase_unref (linkb);

request_global_post_doc_link_end:

  if (parser != NULL)
    g_object_unref (parser);

  if (error)
    g_error_free (error);

  return code;
}

static DSHttpStatusCode
request_global_post_bulk_docs (DSHttpdClient * client, GList * path,
			       GList * arguments)
{
  JsonNode *node;
  GError *error = NULL;
  GList *response_list=NULL;
  DupinDB * db=NULL;

  DSHttpStatusCode code;

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      request_set_error (client, "Cannot parse POST body");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_end;
    }

  /* TODO - check any parsing error */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      if (error)
        {
          request_set_error (client, error->message);
          g_error_free (error);
        }
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_end;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      request_set_error (client, "Cannot parse POST body");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_end;
    }

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      code = HTTP_STATUS_404;
      goto request_global_post_bulk_docs_end;
    }

  if (dupin_record_insert_bulk (db, node, &response_list, FALSE, &error) == TRUE)
    {
      if (request_record_response (client, response_list, TRUE) == FALSE)
        {
	  code = HTTP_STATUS_500;
          request_set_error (client, "Cannot generate JSON output response");
        }
      else
        {
	  code = HTTP_STATUS_201;
        }
    }
  else
    {
      code = HTTP_STATUS_400;
      request_set_error (client, dupin_database_get_error (db));
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  dupin_database_unref (db);

request_global_post_bulk_docs_end:

  if (parser != NULL)
    g_object_unref (parser);

  return code;
}

static DSHttpStatusCode
request_global_post_bulk_doc_links (DSHttpdClient * client, GList * path,
			            GList * arguments)
{
  JsonNode *node;
  GError *error = NULL;
  GList *response_list=NULL;
  DupinDB * db=NULL; 
  DupinLinkB * linkb = NULL;
  gboolean strict_links = FALSE;
  GList * l=NULL;

  DSHttpStatusCode code;

  if (!client->body
      || !path->next)
    {
      request_set_error (client, "POST body or path missing");
      return HTTP_STATUS_400;
    }

  for (l = arguments; l; l = l->next)
    {
      dupin_keyvalue_t *kv = l->data;

      if (!g_strcmp0 (kv->key, REQUEST_STRICT))
        {
          if (!g_strcmp0 (kv->value, REQUEST_STRICT_LINKS))
            {
              strict_links = true;
            }
        }
    }

  gchar * context_id = path->next->data;

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      request_set_error (client, "Cannot parse POST body");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_doc_links_end;
    }

  /* TODO - check any parsing error */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      if (error)
        {
          request_set_error (client, error->message);
          g_error_free (error);
        }
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_doc_links_end;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      request_set_error (client, "Cannot parse POST body");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_doc_links_end;
    }

  /* NOTE - need to get the right linkbase to post to */
  if (!  (db = dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      code = HTTP_STATUS_404;
      goto request_global_post_bulk_doc_links_end;
    }

  if (!(linkb = dupin_linkbase_open (client->thread->data->dupin, dupin_database_get_default_linkbase_name (db), NULL)))
    {
      dupin_database_unref (db);
      request_set_error (client, "Cannot connect to linkbase");
      code = HTTP_STATUS_404;
      goto request_global_post_bulk_doc_links_end;
    }

  dupin_database_unref (db);

  if (dupin_link_record_insert_bulk (linkb, node, context_id, &response_list, strict_links, FALSE, &error) == TRUE)
    {
      if (request_record_response (client, response_list, TRUE) == FALSE)
        {
	  code = HTTP_STATUS_500;
          request_set_error (client, "Cannot generate JSON output response");
        }
      else
        {
	  code = HTTP_STATUS_201;
        }
    }
  else
    {
      code = HTTP_STATUS_400;
      request_set_error (client, dupin_linkbase_get_error (linkb));
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  dupin_linkbase_unref (linkb);

request_global_post_bulk_doc_links_end:

  if (parser != NULL)
    g_object_unref (parser);

  return code;
}

static DSHttpStatusCode
request_global_post_compact_database (DSHttpdClient * client, GList * path,
				      GList * arguments)
{
  DupinDB *db;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      return HTTP_STATUS_404;
    }

  dupin_database_compact (db);

  if (dupin_database_get_error (db))
    {
      request_set_warning (client, dupin_database_get_error (db));
    }

  dupin_database_unref (db);

  return HTTP_STATUS_200;
}

static DSHttpStatusCode
request_global_post_compact_linkbase (DSHttpdClient * client, GList * path,
				      GList * arguments)
{
  DupinLinkB *linkb;

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  dupin_linkbase_compact (linkb);

  if (dupin_linkbase_get_error (linkb))
    {
      request_set_warning (client, dupin_linkbase_get_error (linkb));
    }

  dupin_linkbase_unref (linkb);

  return HTTP_STATUS_200;
}

static DSHttpStatusCode
request_global_post_check_linkbase (DSHttpdClient * client, GList * path,
				    GList * arguments)
{
  DupinLinkB *linkb;

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  dupin_linkbase_check (linkb);

  if (dupin_linkbase_get_error (linkb))
    {
      request_set_warning (client, dupin_linkbase_get_error (linkb));
    }

  dupin_linkbase_unref (linkb);

  return HTTP_STATUS_200;
}

/* PUT *********************************************************************/
static DSHttpStatusCode request_global_put_database (DSHttpdClient * client,
						     GList * path,
						     GList * arguments);
static DSHttpStatusCode request_global_put_view (DSHttpdClient * client,
						 GList * path,
						 GList * arguments);
static DSHttpStatusCode request_global_put_record (DSHttpdClient * client,
						   GList * path,
						   GList * arguments);

static DSHttpStatusCode request_global_put_link_record (DSHttpdClient * client,
						   	GList * path,
						   	GList * arguments);

static DSHttpStatusCode request_global_put_record_attachment (DSHttpdClient * client,
						   	      GList * path,
						              GList * arguments);

static DSHttpStatusCode
request_global_put (DSHttpdClient * client, GList * path, GList * arguments)
{
  if (!path)
    {
      request_set_error (client, "Path missing");
      return HTTP_STATUS_400;
    }

  if (!g_strcmp0 (path->data, REQUEST_LINKBS))
    {
      /* PUT /_linkbs/linkbase/id */
      if (path->next && path->next->next)
	return request_global_put_link_record (client, path->next, arguments);

      request_set_error (client, "PUT /_linkbs allowed commands: /_linkbs/linkbase/id");

      return HTTP_STATUS_400;
    }

  if (!g_strcmp0 (path->data, REQUEST_VIEWS))
    {
      /* PUT /_views/view */
      if (path->next && !path->next->next)
	return request_global_put_view (client, path, arguments);

      request_set_error (client, "PUT /_views allowed commands: /_views/view");

      return HTTP_STATUS_400;
    }

  /* PUT /database */
  if (!path->next)
    {
      return request_global_put_database (client, path, arguments);
    }
  else
    {
      /* PUT /document_ID */
      return request_global_put_record (client, path, arguments);
    }
}

static DSHttpStatusCode
request_global_put_database (DSHttpdClient * client, GList * path,
			     GList * arguments)
{
  DupinDB *db;

  if (!
      (db =
       dupin_database_new (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "The database could not be created, the file already exists.");
      return HTTP_STATUS_412;
    }

  dupin_database_unref (db);

  return HTTP_STATUS_201;
}

static DSHttpStatusCode
request_global_put_view (DSHttpdClient * client, GList * path,
			 GList * arguments)
{
  JsonParser *parser;
  JsonObject *obj;
  JsonNode *node;

  DSHttpStatusCode code;
  DupinView *view;

  GList *nodes, *n;

  const gchar *parent = NULL;
  gboolean parent_is_db = FALSE;
  gboolean parent_is_linkb = FALSE;
  const gchar *map = NULL;
  const gchar *map_lang = "javascript";
  const gchar *reduce = NULL;
  const gchar *reduce_lang = "javascript";
  const gchar *output = NULL;
  gboolean output_is_db = FALSE;
  gboolean output_is_linkb = FALSE;
  GError *error = NULL;

  if (!client->body)
    {
      request_set_error (client, "Missing or invalid PUT body");
      return HTTP_STATUS_400;
    }

  parser = json_parser_new ();

  if (parser == NULL)
    {
      request_set_error (client, "Cannot parse PUT body");
      code = HTTP_STATUS_500;
      goto request_global_put_view_error;
    }

  /* TODO - check any parsing error */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      if (error)
        {
          request_set_error (client, error->message);
          g_error_free (error);
        }
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      request_set_error (client, "Cannot parse PUT body");
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      request_set_error (client, "PUT body must be a JSON object");
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  obj = json_node_get_object (node); /* it is a volatile object part of parser as well as node - see docs */

  nodes = json_object_get_members (obj);

  for (n = nodes; n != NULL; n = n->next)
    {
      gchar *member_name = (gchar *) n->data;
      JsonNode *subnode = json_object_get_member (obj, member_name);

      if (!g_strcmp0 (member_name, "parent")
	  && json_node_get_node_type (subnode) == JSON_NODE_OBJECT)
	{
	  JsonObject *subobj = json_node_get_object (subnode);
	  GList *subnodes = json_object_get_members (subobj);
	  GList *sn;

          for (sn = subnodes; sn != NULL; sn = sn->next)
	    {
              gchar *sub_member_name = (gchar *) sn->data;
              JsonNode *sub_subnode = json_object_get_member (subobj, sub_member_name);

	      if (!g_strcmp0 (sub_member_name, "name")
		  && json_node_get_value_type (sub_subnode) == G_TYPE_STRING) /* check this is correct type */
		parent = json_node_get_string (sub_subnode);

	      else if (!g_strcmp0 (sub_member_name, "is_db")
		       && json_node_get_value_type (sub_subnode) == G_TYPE_BOOLEAN)
		parent_is_db = json_node_get_boolean (sub_subnode);

	      else if (!g_strcmp0 (sub_member_name, "is_linkb")
		       && json_node_get_value_type (sub_subnode) == G_TYPE_BOOLEAN)
		parent_is_linkb = json_node_get_boolean (sub_subnode);
	    }
          g_list_free (subnodes);
	}

      else if (!g_strcmp0 (member_name, "output")
	       && json_node_get_node_type (subnode) == JSON_NODE_OBJECT)
	{
	  JsonObject *subobj = json_node_get_object (subnode);
	  GList *subnodes = json_object_get_members (subobj);
	  GList *sn;

          for (sn = subnodes; sn != NULL; sn = sn->next)
	    {
              gchar *sub_member_name = (gchar *) sn->data;
              JsonNode *sub_subnode = json_object_get_member (subobj, sub_member_name);

	      if (!g_strcmp0 (sub_member_name, "name")
		  && json_node_get_value_type (sub_subnode) == G_TYPE_STRING) /* check this is correct type */
		output = json_node_get_string (sub_subnode);

	      else if (!g_strcmp0 (sub_member_name, "is_db")
		       && json_node_get_value_type (sub_subnode) == G_TYPE_BOOLEAN)
		output_is_db = json_node_get_boolean (sub_subnode);

	      else if (!g_strcmp0 (sub_member_name, "is_linkb")
		       && json_node_get_value_type (sub_subnode) == G_TYPE_BOOLEAN)
		output_is_linkb = json_node_get_boolean (sub_subnode);
	    }
          g_list_free (subnodes);
	}

      else if (!g_strcmp0 (member_name, "map")
	       && json_node_get_node_type (subnode) == JSON_NODE_OBJECT)
	{
          JsonObject *subobj = json_node_get_object (subnode);
          GList *subnodes = json_object_get_members (subobj);
	  GList *sn;

          for (sn = subnodes; sn != NULL; sn = sn->next)
	    {
              gchar *sub_member_name = (gchar *) sn->data;
              JsonNode *sub_subnode = json_object_get_member (subobj, sub_member_name);

	      if (!g_strcmp0 (sub_member_name, "code")
		  && json_node_get_value_type (sub_subnode) == G_TYPE_STRING) /* check this is correct type */
		map = json_node_get_string (sub_subnode);

	      else if (!g_strcmp0 (sub_member_name, "language")
		  && json_node_get_value_type (sub_subnode) == G_TYPE_STRING) /* check this is correct type */
		map_lang = json_node_get_string (sub_subnode);
	    }
          g_list_free (subnodes);
	}

      else if (!g_strcmp0 (member_name, "reduce")
	       && json_node_get_node_type (subnode) == JSON_NODE_OBJECT)
	{
          JsonObject *subobj = json_node_get_object (subnode);
          GList *subnodes = json_object_get_members (subobj);
	  GList *sn;

          for (sn = subnodes; sn != NULL; sn = sn->next)
	    {
              gchar *sub_member_name = (gchar *) sn->data;
              JsonNode *sub_subnode = json_object_get_member (subobj, sub_member_name);

	      if (!g_strcmp0 (sub_member_name, "code")
		  && json_node_get_value_type (sub_subnode) == G_TYPE_STRING) /* check this is correct type */
		reduce = json_node_get_string (sub_subnode);

	      else if (!g_strcmp0 (sub_member_name, "language")
		  && json_node_get_value_type (sub_subnode) == G_TYPE_STRING) /* check this is correct type */
		reduce_lang = json_node_get_string (sub_subnode);
	    }
          g_list_free (subnodes);
	}
    }
  g_list_free (nodes);

  if (!map || !map_lang || !parent)
    {
      request_set_error (client, "No map, map language or parent fields defined");
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  if (!
      (view =
       dupin_view_new (client->thread->data->dupin, path->next->data, (gchar *)parent,
		       parent_is_db, parent_is_linkb, (gchar *)map,
		       dupin_util_mr_lang_to_enum ((gchar *)map_lang), (gchar *)reduce,
		       dupin_util_mr_lang_to_enum ((gchar *)reduce_lang),
		       (gchar *)output, output_is_db, output_is_linkb, &error)))
    {
      if (error)
        {
          request_set_error (client, error->message);
          g_error_free (error);
        }
      code = HTTP_STATUS_409;
      goto request_global_put_view_error;
    }

  code = HTTP_STATUS_201;

  dupin_view_unref (view);
  if (parser != NULL)
    g_object_unref (parser);
  return code;

request_global_put_view_error:

  if (parser != NULL)
    g_object_unref (parser);
  return code;
}

static DSHttpStatusCode
request_global_put_record (DSHttpdClient * client, GList * path,
			   GList * arguments)
{
  JsonNode *node=NULL;
  JsonParser *parser;
  DSHttpStatusCode code;
  gchar * doc_id=NULL; 
  gchar * request_fields=NULL;
  GList *response_list=NULL;
  GError *error = NULL;
  DupinDB *db;
  gchar * mvcc=NULL;
  GList * l=NULL;
  gboolean res;
 
  if (!client->body
      || !path->next->data)
    {
      request_set_error (client, "Missing or invalid PUT body");
      return HTTP_STATUS_400;
    }

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
//g_message("request_global_put_record: dbname=%s id=%s\n", (gchar *) path->data, (gchar *)path->next->data);

      if (path->next->next && path->next->next->next)
        {
          /* PUT _special_document/document_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->next->data, REQUEST_FIELDS))
            {
	      if (!path->next->next->next->next
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ID)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_REV)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ATTACHMENTS)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_PATCHED))
	        {
      		  request_set_error (client, "Cannot update record field");
                  return HTTP_STATUS_400;
		}

              request_fields=path->next->next->next->next->data;
            }

          /* PUT _special_document/document_ID/attachment */
          else
            {
	      return request_global_put_record_attachment (client, path, arguments);
            }
        }

      /* PUT _special_document/document_ID */
      if (path->next->next)
        doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);
    }
  else
    {
      if (path->next->next)
        {
          /* PUT /document_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->data, REQUEST_FIELDS))
            {
	      if (!path->next->next->next
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ID)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_REV)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_PATCHED))
                {
      		  request_set_error (client, "Cannot update record field");
                  return HTTP_STATUS_400;
	        }

              request_fields=path->next->next->next->data;
            }

          /* PUT /document_ID/attachment */
          else
            {
              return request_global_put_record_attachment (client, path, arguments);
            }
        }

      /* PUT /document_ID */
      doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
    }

//g_message("request_global_put_record: dbname=%s doc_id=%s\n", (gchar *) path->data, doc_id);

  parser = json_parser_new ();

  if (parser == NULL)
    {
      request_set_error (client, "Cannot parse PUT body");
      code = HTTP_STATUS_500;
      goto request_global_put_record_end;
    }

  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      if (error)
        {
          request_set_error (client, error->message);
          g_error_free (error);
        }
      code = HTTP_STATUS_400;
      goto request_global_put_record_end;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      request_set_error (client, "Cannot parse PUT body");
      code = HTTP_STATUS_400;
      goto request_global_put_record_end;
    }

  for (l = arguments; l; l = l->next)
    {
      dupin_keyvalue_t *kv = l->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
              request_set_error (client, "Invalid record MVCC revision");
              code = HTTP_STATUS_400;
              goto request_global_put_record_end;
            }
          mvcc = kv->value;
        }
    }

  if (!  (db = dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot open database");
      code = HTTP_STATUS_404;
      goto request_global_put_record_end;
    }

  if (request_fields != NULL)
    {
      if (mvcc == NULL)
        {
          request_set_error (client, "Record MVCC revision is missing");
          code = HTTP_STATUS_400;
          goto request_global_put_record_end;
        }

      JsonNode * patch = json_node_new (JSON_NODE_OBJECT);
      JsonObject * patch_obj = json_object_new ();
      json_node_take_object (patch, patch_obj);

      /* Setting _id and _rev: */
      json_object_set_string_member (patch_obj, REQUEST_OBJ_ID, doc_id);
      json_object_set_string_member (patch_obj, REQUEST_OBJ_REV, mvcc);

      /* set field */
      json_object_set_member (patch_obj, (const gchar *)request_fields, json_node_copy (node));
      json_object_set_boolean_member (patch_obj, REQUEST_OBJ_PATCHED, TRUE);

      res = dupin_record_insert (db, patch, doc_id, mvcc, &response_list, FALSE, &error);

      json_node_free (patch);
    }
  else
    {
      res = dupin_record_insert (db, node, doc_id, mvcc, &response_list, FALSE, &error);
    }

  if (res == TRUE)
    {
      if (request_record_response (client, response_list, FALSE) == FALSE)
        {
          code = HTTP_STATUS_500;
          request_set_error (client, "Cannot generate JSON output response");
        }
      else
        {
	  code = HTTP_STATUS_201;
	}
    }
  else
    {
      if (error != NULL && error->code == DUPIN_ERROR_RECORD_CONFLICT)
        code = HTTP_STATUS_409;
      else
        code = HTTP_STATUS_400;
      request_set_error (client, dupin_database_get_error (db));
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  dupin_database_unref (db);

request_global_put_record_end:

  if (parser != NULL)
    g_object_unref (parser);

  g_free (doc_id);

  if (error)
    g_error_free (error);

  return code;
}

static DSHttpStatusCode
request_global_put_link_record (DSHttpdClient * client, GList * path,
			   	GList * arguments)
{
  JsonNode *node=NULL;
  JsonParser *parser;
  DSHttpStatusCode code;
  gchar * link_id=NULL; 
  gchar * request_fields=NULL;
  GError *error = NULL;
  GList * response_list = NULL;
  DupinDB * db=NULL;
  DupinLinkB * linkb=NULL;
  gchar * mvcc=NULL;
  gboolean strict_links = FALSE;
  GList * l=NULL;
  gboolean res;
 
  if (!client->body
      || !path->next->data)
    {
      request_set_error (client, "Missing or invalid PUT body");
      return HTTP_STATUS_400;
    }

  /* check if special link name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
//g_message("request_global_put_link_record: linkbname=%s id=%s\n", (gchar *) path->data, (gchar *)path->next->data);

      if (path->next->next && path->next->next->next)
        {
          /* PUT _special_link/link_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->next->data, REQUEST_FIELDS))
            {
	      if (!path->next->next->next->next
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ID)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_REV)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ATTACHMENTS)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_PATCHED)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_LINKS))
                {
      		  request_set_error (client, "Cannot update link record field");
                  return HTTP_STATUS_400;
	        }

              request_fields=path->next->next->next->next->data;
            }
        }

      /* PUT _special_link/link_ID */
      if (path->next->next)
        link_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);
    }
  else
    {
      if (path->next->next)
        {
          /* PUT link_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->data, REQUEST_FIELDS))
            {
	      if (!path->next->next->next
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ID)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_REV)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_PATCHED)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_LINKS))
                {
      		  request_set_error (client, "Cannot update link record field");
                  return HTTP_STATUS_400;
	        }

              request_fields=path->next->next->next->data;
            }
        }

      /* PUT /link_ID */
      link_id = g_strdup_printf ("%s", (gchar *)path->next->data);
    }

//g_message("request_global_put_link_record: linkbname=%s link_id=%s\n", (gchar *) path->data, link_id);

  parser = json_parser_new ();

  if (parser == NULL)
    {
      request_set_error (client, "Cannot parse PUT body");
      code = HTTP_STATUS_500;
      goto request_global_put_link_record_end;
    }

  /* TODO - check any parsing error */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      if (error)
        {
          request_set_error (client, error->message);
          g_error_free (error);
        }
      code = HTTP_STATUS_400;
      goto request_global_put_link_record_end;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      request_set_error (client, "Cannot parse PUT body");
      code = HTTP_STATUS_400;
      goto request_global_put_link_record_end;
    }

  for (l = arguments; l; l = l->next)
    {
      dupin_keyvalue_t *kv = l->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
              request_set_error (client, "Invalid record MVCC revision");
              code = HTTP_STATUS_404;
              goto request_global_put_link_record_end;
            }
          mvcc = kv->value;
        }
      else if (!g_strcmp0 (kv->key, REQUEST_STRICT))
        {
          if (!g_strcmp0 (kv->value, REQUEST_STRICT_LINKS))
            {
              strict_links = true;
            }
        }
    }

  /* NOTE - need to get the right linkbase to put/update to */
  if (!  (db = dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      code = HTTP_STATUS_404;
      goto request_global_put_link_record_end;
    }

  if (!(linkb = dupin_linkbase_open (client->thread->data->dupin, dupin_database_get_default_linkbase_name (db), NULL)))
    {
      dupin_database_unref (db);
      request_set_error (client, "Cannot connect to linkbase");
      code = HTTP_STATUS_404;
      goto request_global_put_link_record_end;
    }

  dupin_database_unref (db);

  if (request_fields != NULL)
    {
      if (mvcc == NULL)
        {
          request_set_error (client, "Link record MVCC revision is missing");
          code = HTTP_STATUS_404;
          goto request_global_put_link_record_end;
        }

      JsonNode * patch = json_node_new (JSON_NODE_OBJECT);
      JsonObject * patch_obj = json_object_new ();
      json_node_take_object (patch, patch_obj);

      /* Setting _id and _rev: */
      json_object_set_string_member (patch_obj, REQUEST_OBJ_ID, link_id);
      json_object_set_string_member (patch_obj, REQUEST_OBJ_REV, mvcc);

      /* set field */
      json_object_set_member (patch_obj, (const gchar *)request_fields, json_node_copy (node));
      json_object_set_boolean_member (patch_obj, REQUEST_OBJ_PATCHED, TRUE);

      res = dupin_link_record_insert (linkb, patch, link_id, mvcc, NULL, DP_LINK_TYPE_ANY, &response_list, strict_links, FALSE, &error);

      json_node_free (patch);
    }
  else
    {
      res = dupin_link_record_insert (linkb, node, link_id, mvcc, NULL, DP_LINK_TYPE_ANY, &response_list, strict_links, FALSE, &error);
    }

  if (res == TRUE)
    {
      if (request_record_response (client, response_list, FALSE) == FALSE)
        {
          code = HTTP_STATUS_500;
          request_set_error (client, "Cannot generate JSON output response");
        }
      else
        {
	  code = HTTP_STATUS_201;
	}
    }
  else
    {
      if (error != NULL && error->code == DUPIN_ERROR_RECORD_CONFLICT)
        code = HTTP_STATUS_409;
      else
        code = HTTP_STATUS_400;
      request_set_error (client, dupin_linkbase_get_error (linkb));
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  dupin_linkbase_unref (linkb);

request_global_put_link_record_end:

  if (parser != NULL)
    g_object_unref (parser);

  g_free (link_id);

  if (error)
    g_error_free (error);

  return code;
}

static DSHttpStatusCode
request_global_put_record_attachment (DSHttpdClient * client, GList * path,
				      GList * arguments)
{
  DSHttpStatusCode code;
  gchar * doc_id=NULL;
  GList * title_parts=NULL;
  GList * response_list = NULL;
  DupinDB * db=NULL;
  DupinAttachmentDB * attachment_db=NULL;
  gchar * mvcc=NULL;
  GList * l=NULL;
  GError *error = NULL;

  if (!client->body
      || !client->input_mime
      || !path->next->data
      || !path->next->next)
    {
      request_set_error (client, "Missing or invalid PUT body attachment file, input Content-Type header or attachment name not specified");
      return HTTP_STATUS_400;
    }

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* PUT _special_document/document_ID/attachment */
      doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);

      if (path->next->next && path->next->next->next)
        {
          title_parts = path->next->next->next;
        }
    }
  else
    {
       if (path->next->next)
         {
           /* PUT /document_ID/attachment */
          doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
          title_parts = path->next->next;
         }
       else
         {
           request_set_error (client, "PUT /document_ID/attachment is the only allowed command");
           return HTTP_STATUS_400;
         }
    }

  for (l = arguments; l; l = l->next)
    {
      dupin_keyvalue_t *kv = l->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
              request_set_error (client, "Invalid record MVCC revision");
              code = HTTP_STATUS_404;
              goto request_global_put_record_attachment_end;
            }
          mvcc = kv->value;
        }
    }

  /* NOTE - we do not force MVCC due if not specified we can create document if it does not exist */

//g_message("request_global_put_record_attachment: dbname=%s doc_id=%s title_parts=%s\n", (gchar *) path->data, doc_id, (gchar *)title_parts->data);

  /* NOTE - need to get the right attachment database to put to */
  if (!  (db = dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      code = HTTP_STATUS_404;
      goto request_global_put_record_attachment_end;
    }

  if (!  (attachment_db = dupin_attachment_db_open (client->thread->data->dupin, dupin_database_get_default_attachment_db_name (db), NULL)))
    {
      dupin_database_unref (db);
      request_set_error (client, "Cannot connect to attachments database");
      code = HTTP_STATUS_404;
      goto request_global_put_record_attachment_end;
    }

  dupin_database_unref (db);

  const void * client_body_ref = (const void *)client->body;

  if (dupin_attachment_record_insert (attachment_db, doc_id, mvcc, title_parts,
				      client->body_size, client->input_mime,
				      &client_body_ref, &response_list, &error) == TRUE)
    {
      if (request_record_response (client, response_list, FALSE) == FALSE)
        {
          code = HTTP_STATUS_500;
          request_set_error (client, "Cannot generate JSON output response");
        }
      else
        code = HTTP_STATUS_201;
    }
  else
    {
      code = HTTP_STATUS_400;
      request_set_error (client, dupin_attachment_db_get_error (attachment_db));
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  dupin_attachment_db_unref (attachment_db);

request_global_put_record_attachment_end:

  g_free (doc_id);

  return code;
}

/* DELETE*******************************************************************/
static DSHttpStatusCode request_global_delete_database (DSHttpdClient *
							client, GList * path,
							GList * arguments);
static DSHttpStatusCode request_global_delete_view (DSHttpdClient * client,
						    GList * path,
						    GList * arguments);
static DSHttpStatusCode request_global_delete_record (DSHttpdClient * client,
						      GList * path,
						      GList * arguments);

static DSHttpStatusCode request_global_delete_link_record (DSHttpdClient * client,
						           GList * path,
						           GList * arguments);

static DSHttpStatusCode
request_global_delete (DSHttpdClient * client, GList * path,
		       GList * arguments)
{
  if (!path)
    {
      request_set_error (client, "Path missing");
      return HTTP_STATUS_400;
    }

  if (!g_strcmp0 (path->data, REQUEST_LINKBS))
    {
      /* DELETE /_linkbs/linkbase/id */
      if (path->next && path->next->next)
	return request_global_delete_link_record (client, path->next, arguments);

      request_set_error (client, "DELETE /_linkbs allowed commands: /_linkbs/linkbase/id");

      return HTTP_STATUS_400;
    }

  /* DELETE /database */
  if (!path->next)
    return request_global_delete_database (client, path, arguments);

  if (!g_strcmp0 (path->data, REQUEST_VIEWS))
    {
      /* DELETE /_views/view */
      if (!path->next->next)
	return request_global_delete_view (client, path, arguments);

      request_set_error (client, "DELETE /_views allowed commands: /_views/view");

      return HTTP_STATUS_400;
    }

  /* DELETE /database/id */
  return request_global_delete_record (client, path, arguments);
}

static DSHttpStatusCode
request_global_delete_database (DSHttpdClient * client, GList * path,
				GList * arguments)
{
  DupinDB *db;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      return HTTP_STATUS_404;
    }

  if (dupin_database_delete (db, NULL) == FALSE)
    {
      request_set_error (client, "Cannot delete database");
      dupin_database_unref (db);
      return HTTP_STATUS_404;
    }

  dupin_database_unref (db);

  return HTTP_STATUS_200;
}

static DSHttpStatusCode
request_global_delete_view (DSHttpdClient * client, GList * path,
			    GList * arguments)
{
  DupinView *view;

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    {
      request_set_error (client, "Cannot connect to view");
      return HTTP_STATUS_404;
    }

  if (dupin_view_delete (view, NULL) == FALSE)
    {
      request_set_error (client, "Cannot delete view");
      dupin_view_unref (view);
      return HTTP_STATUS_409;
    }

  dupin_view_unref (view);
  return HTTP_STATUS_200;
}

static DSHttpStatusCode
request_global_delete_record (DSHttpdClient * client, GList * path,
			      GList * arguments)
{
  DupinDB *db;
  DupinAttachmentDB *attachment_db=NULL;
  DupinRecord *record;
  gchar * mvcc=NULL;
  gchar * title = NULL;
  GList * title_parts=NULL;
  GList * l=NULL;
  GString *str;
  gchar * doc_id=NULL;
  gchar * request_fields=NULL;

  if (!path->next->data)
    {
      request_set_error (client, "Cannot delete record field");
      return HTTP_STATUS_400;
    }

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* TODO - shouldn't we stop/avoid user to delete /_design/something ?! */

      /* DELETE _special_document/document_ID */
      if (path->next->next)
        {
          doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);

          /* DELETE _special_document/document_ID/attachment */
          if (path->next->next->next)
            {
              title_parts = path->next->next->next;
            }
        }

      /* NOTE - we deliberately do not implement field delete of special documents - of course general PUT of whole doc still works */
    }
  else
    {
      if (path->next->next)
        {
          /* DELETE /document_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->data, REQUEST_FIELDS))
            {
              if (!path->next->next->next
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ID)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_REV)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_PATCHED))
	        {
      		  request_set_error (client, "Cannot delete record field");
                  return HTTP_STATUS_400;
		}

              request_fields=path->next->next->next->data;
            }

          /* DELETE /document_ID/attachment */
          else
            {
              title_parts = path->next->next;
            }
        }

      /* DELETE /document_ID */
      doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
    }

  /* process input attachment name parameter */
  if (title_parts != NULL)
    {
      str = g_string_new (title_parts->data);

      for (l=title_parts->next ; l != NULL ; l=l->next)
        {
          g_string_append_printf (str, "/%s", (gchar *)l->data);
        }

      title = g_string_free (str, FALSE);

      if (title == NULL)
        {
      	  request_set_error (client, "Cannot get attachment name");
          g_free (doc_id);
          return HTTP_STATUS_400;
        }

//g_message("request_global_delete_record: doc_id=%s title=%s\n", doc_id, title);
    }

  for (l = arguments; l; l = l->next)
    {
      dupin_keyvalue_t *kv = l->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
	      request_set_error (client, "Invalid record MVCC revision");
              g_free (doc_id);
              return HTTP_STATUS_400;
            }
          mvcc = kv->value;
        }
    }

  if (mvcc == NULL)
    {
      request_set_error (client, "Record MVCC revision is missing");
      g_free (doc_id);
      return HTTP_STATUS_400;
    }

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      if (title != NULL)
        g_free (title);
      g_free (doc_id);
      request_set_error (client, "Cannot connect to database");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_record_read (db, doc_id, NULL)))
    {
      if (title != NULL)
        g_free (title);
      dupin_database_unref (db);
      g_free (doc_id);
      request_set_error (client, "Cannot read record");
      return HTTP_STATUS_404;
    }

  if (dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record)))
    {
      if (title != NULL)
        g_free (title);
      dupin_record_close (record);
      dupin_database_unref (db);
      g_free (doc_id);
      request_set_error (client, "Record MVCC revision missing or not matching latest revision");
      return HTTP_STATUS_404;
    }

  if (title_parts != NULL)
    {
      JsonNode * obj_node = NULL;

      if (!(attachment_db =
       		dupin_attachment_db_open (client->thread->data->dupin, path->data, NULL)))
        {
          if (title != NULL)
            g_free (title);
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
	  request_set_error (client, "Cannot open attachments database");
          return HTTP_STATUS_404;
        }

      if (!(obj_node = request_record_revision_obj (client, arguments,
						    record, doc_id, mvcc, FALSE)))
        {
          if (title != NULL)
            g_free (title);
          if (title_parts != NULL)
            dupin_attachment_db_unref (attachment_db);
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
	  request_set_error (client, "Cannot get record JSON object");
          return HTTP_STATUS_404;
        }

      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_REV);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_ID);

      if (dupin_attachment_record_exists (attachment_db, doc_id, title) == FALSE
          || dupin_attachment_record_delete (attachment_db, doc_id, title) == FALSE)
        {
          if (title != NULL)
            g_free (title);
          if (obj_node != NULL)
            json_node_free (obj_node);
          dupin_record_close (record);
          if (title_parts != NULL)
            dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          g_free (doc_id);
	  request_set_error (client, "Cannot find record attachment or cannot delete attachment");
          return HTTP_STATUS_404;
        }

      if (dupin_record_update (record, obj_node, NULL) == FALSE)
        {
          if (title != NULL)
            g_free (title);
          if (obj_node != NULL)
            json_node_free (obj_node);
          dupin_record_close (record);
          if (title_parts != NULL)
            dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          g_free (doc_id);
	  request_set_error (client, "Cannot update record");
          return HTTP_STATUS_404;
        }

      if (obj_node != NULL)
        json_node_free (obj_node);

      if (title_parts != NULL)
        dupin_attachment_db_unref (attachment_db);

      if (title != NULL)
        g_free (title);
    }

  else if (request_fields != NULL)
    {
      JsonNode * patch = json_node_new (JSON_NODE_OBJECT);
      JsonObject * patch_obj = json_object_new ();
      json_node_take_object (patch, patch_obj);

      /* flag field to be deleted */
      JsonObject * to_delete_obj = json_object_new ();
      json_object_set_boolean_member (to_delete_obj, REQUEST_OBJ_DELETED, TRUE);
      json_object_set_object_member (patch_obj, (const gchar *)request_fields, to_delete_obj);

      if (dupin_record_patch (record, patch, NULL) == FALSE)
        {
          json_node_free (patch);
          if (title != NULL)
            g_free (title);
          dupin_record_close (record);
          if (title_parts != NULL)
            dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          g_free (doc_id);
	  request_set_error (client, "Cannot patch record");
          return HTTP_STATUS_400;
        }

      json_node_free (patch);
    }

  else
    {
      if (!(dupin_record_delete (record, NULL)))
        {
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
	  request_set_error (client, "Cannot delete record");
          return HTTP_STATUS_400;
        }

      /* NOTE - we do *NOT* delete all attachments to document - which can be individually deleted with above fetch revs=true of deleted records */
    }

  JsonNode * record_response_node = json_node_new (JSON_NODE_OBJECT);
  JsonObject * record_response_obj = json_object_new ();
  json_node_take_object (record_response_node, record_response_obj);

  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_ID, (gchar *) dupin_record_get_id (record));
  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_REV, dupin_record_get_last_revision (record));

  GList * response_list = NULL;
  response_list = g_list_prepend (response_list, record_response_node);

  if (request_record_response (client, response_list, FALSE) == FALSE)
    {
      while (response_list)
        {
          json_node_free (response_list->data);
          response_list = g_list_remove (response_list, response_list->data);
        }
      dupin_record_close (record);
      dupin_database_unref (db);
      g_free (doc_id);
      request_set_error (client, "Cannot generate JSON output response");
      return HTTP_STATUS_500;
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  dupin_record_close (record);
  dupin_database_unref (db);

  g_free (doc_id);

  return HTTP_STATUS_200;
}

static DSHttpStatusCode
request_global_delete_link_record (DSHttpdClient * client, GList * path,
			           GList * arguments)
{
  DupinLinkB *linkb;
  DupinLinkRecord *record;
  gchar * mvcc=NULL;
  GList * l=NULL;
  gchar * link_id=NULL;
  gchar * request_fields=NULL;

  if (!path->next->data)
    {
      request_set_error (client, "Cannot delete link record field");
      return HTTP_STATUS_400;
    }

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* TODO - shouldn't we stop/avoid user to delete /_design/something ?! */

      /* DELETE _special_link/link_ID */
      if (path->next->next)
        {
          link_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);
        }

      /* NOTE - we deliberately do not implement field delete of special documents - of course general PUT of whole doc still works */
    }
  else
    {
      if (path->next->next)
        {
          /* DELETE /link_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->data, REQUEST_FIELDS))
            {
              if (!path->next->next->next
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ID)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_REV)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_LINKS)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_PATCHED)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_LINK_OBJ_LABEL)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_LINK_OBJ_HREF)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_LINK_OBJ_REL)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_LINK_OBJ_TAG))
	        {
      		  request_set_error (client, "Cannot delete link record field");
                  return HTTP_STATUS_400;
		}

              request_fields=path->next->next->next->data;
            }
        }

      /* DELETE /link_ID */
      link_id = g_strdup_printf ("%s", (gchar *)path->next->data);
    }

//g_message("request_global_delete_link_record: link_id=%s request_fields=%s\n", link_id, request_fields);

  for (l = arguments; l; l = l->next)
    {
      dupin_keyvalue_t *kv = l->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
              request_set_error (client, "Invalid link record MVCC revision");
              g_free (link_id);
              return HTTP_STATUS_400;
            }
          mvcc = kv->value;
        }
    }

  if (mvcc == NULL)
    {
      request_set_error (client, "Link record MVCC revision is missing");
      g_free (link_id);
      return HTTP_STATUS_400;
    }

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
    {
      g_free (link_id);
      request_set_error (client, "Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_link_record_read (linkb, link_id, NULL)))
    {
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      request_set_error (client, "Cannot read link record");
      return HTTP_STATUS_404;
    }

  if (dupin_util_mvcc_revision_cmp (mvcc, dupin_link_record_get_last_revision (record)))
    {
      dupin_link_record_close (record);
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      request_set_error (client, "Link record MVCC revision missing or not matching latest revision");
      return HTTP_STATUS_404;
    }

  if (request_fields != NULL)
    {
      JsonNode * patch = json_node_new (JSON_NODE_OBJECT);
      JsonObject * patch_obj = json_object_new ();
      json_node_take_object (patch, patch_obj);

      /* flag field to be deleted */
      JsonObject * to_delete_obj = json_object_new ();
      json_object_set_boolean_member (to_delete_obj, REQUEST_OBJ_DELETED, TRUE);
      json_object_set_object_member (patch_obj, (const gchar *)request_fields, to_delete_obj);

      if (dupin_link_record_patch (record, patch,
      				    (gchar *)dupin_link_record_get_label (record),
      				    (gchar *)dupin_link_record_get_href (record),
      				    (gchar *)dupin_link_record_get_rel (record),
      				    (gchar *)dupin_link_record_get_tag (record), NULL) == FALSE)
        {
          json_node_free (patch);
          dupin_link_record_close (record);
          dupin_linkbase_unref (linkb);
          g_free (link_id);
	  request_set_error (client, "Cannot patch link record");
          return HTTP_STATUS_404;
        }

      json_node_free (patch);
    }
  else
    {
      if (!(dupin_link_record_delete (record, NULL)))
        {
          dupin_link_record_close (record);
          dupin_linkbase_unref (linkb);
          g_free (link_id);
	  request_set_error (client, "Cannot delete link record");
          return HTTP_STATUS_400;
        }
    }

  JsonNode * record_response_node = json_node_new (JSON_NODE_OBJECT);
  JsonObject * record_response_obj = json_object_new ();
  json_node_take_object (record_response_node, record_response_obj);

  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_ID, (gchar *) dupin_link_record_get_id (record));
  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_REV, dupin_link_record_get_last_revision (record));

  GList * response_list = NULL;
  response_list = g_list_prepend (response_list, record_response_node);

  if (request_record_response (client, response_list, FALSE) == FALSE)
    {
      while (response_list)
        {
          json_node_free (response_list->data);
          response_list = g_list_remove (response_list, response_list->data);
        }
      dupin_link_record_close (record);
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      request_set_error (client, "Cannot generate JSON output response");
      return HTTP_STATUS_500;
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  dupin_link_record_close (record);
  dupin_linkbase_unref (linkb);

  g_free (link_id);

  return HTTP_STATUS_200;

}

/* DATA STRUCT *************************************************************/

RequestType request_types[] = {
  {REQUEST_WWW, DS_HTTPD_REQUEST_GET, request_www}
  ,
  {REQUEST_QUIT, DS_HTTPD_REQUEST_GET, request_quit}
  ,
  {REQUEST_STATUS, DS_HTTPD_REQUEST_GET, request_status}
  ,
  {NULL}
};

/* RECORD *********************************************************************/

static gboolean
request_record_response (DSHttpdClient * client,
			 GList * response_list,
			 gboolean is_bulk)
{
  JsonGenerator *gen=NULL;
  JsonNode *response_node=NULL;

/*
  if (g_list_length (response_list) == 0)
    return FALSE;
*/

  if (g_list_length (response_list) == 1 && is_bulk == FALSE)
    {
      response_node = (JsonNode *) response_list->data;

      if (json_node_get_node_type (response_node) != JSON_NODE_OBJECT)
        return FALSE;
 
      response_node = json_node_copy (response_node);

      if (json_object_has_member (json_node_get_object (response_node), RESPONSE_STATUS_ERROR) == FALSE)
        json_object_set_boolean_member (json_node_get_object (response_node), "ok", TRUE);
    }
  else
    {
      response_node = json_node_new (JSON_NODE_ARRAY);
      if (response_node == NULL)
        return FALSE;
      JsonArray * response_array = json_array_new ();
      json_node_take_array (response_node, response_array);

      if (response_array == NULL)
        {
          json_node_free (response_node);
          return FALSE;
        }

      for (; response_list; response_list = response_list->next)
        {
          JsonNode * r_node = (JsonNode *)response_list->data;
	  json_array_add_element (response_array, json_node_copy (r_node));
        }
    }

  gen = json_generator_new();

  if (gen == NULL)
    goto request_record_response_error;
 
  json_generator_set_root (gen, response_node );

  client->output.string.string = json_generator_to_data (gen,&client->output_size);
 
  if (client->output.string.string == NULL)
    goto request_record_response_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (response_node != NULL)
    json_node_free (response_node);

  return TRUE;

request_record_response_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (response_node != NULL)
    json_node_free (response_node);

  return FALSE;
}

/* TODO - the following will need to be moved as well into record level API on top of
          dupin_record, dupin_link_record and dupin_attachment_record modules */
 
static JsonNode *
request_record_revision_obj (DSHttpdClient * client,
                             GList * arguments,
			     DupinRecord * record,
			     gchar * id, gchar * mvcc,
			     gboolean visit_links)
{
  g_return_val_if_fail (client != NULL, NULL);
  g_return_val_if_fail (record != NULL, NULL);

  client->request_included_docs_level++;

  JsonNode *obj_node=NULL;
  JsonObject *obj=NULL;
  GList * list = NULL;

  if (dupin_record_is_deleted (record, mvcc) == TRUE)
    {
      obj_node = json_node_new (JSON_NODE_OBJECT);

      if (obj_node == NULL)
        {
	  client->request_included_docs_level--;

          return NULL;
        }

      obj = json_object_new ();

      if (obj == NULL)
        {
          json_node_free (obj_node);
	  client->request_included_docs_level--;

          return NULL;
        }

      json_node_take_object (obj_node, obj);

      /* Setting _id, _rev and _deleted: */
      json_object_set_string_member (obj, REQUEST_OBJ_ID, id);
      json_object_set_string_member (obj, REQUEST_OBJ_REV, mvcc);
      json_object_set_boolean_member (obj, RESPONSE_OBJ_DELETED, TRUE);

      client->request_included_docs_level--;

      return obj_node;
    }

  JsonNode *record_obj_node = dupin_record_get_revision_node (record, mvcc);

  if (record_obj_node == NULL)
    {
      client->request_included_docs_level--;

      return NULL;
    }

  /* filter fields */
  gchar * fields = NULL;
  gchar ** fields_splitted = NULL;
  DupinFieldsFormatType fields_format = DP_FIELDS_FORMAT_DOTTED;
  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_FIELDS))
        {
	  fields = kv->value;
	}
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED))
	    fields_format = DP_FIELDS_FORMAT_DOTTED;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_JSONPATH))
	    fields_format = DP_FIELDS_FORMAT_JSONPATH;
	}
    }

//g_message("request_record_revision_obj: fields=%s fields_format=%d", fields, fields_format);
 
  if (fields != NULL)
    {
      fields_splitted = g_strsplit (fields, ",", -1);
      gint i;
      gboolean any = FALSE;
      for (i = 0; fields_splitted[i]; i++)
        {
          if ((!g_strcmp0 (fields_splitted[i],REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL))
	      || (!g_strcmp0 (fields_splitted[i],REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL_FIELDS)))
	    {
	      any = TRUE;
	      break;
	    }
	}
      if (any == FALSE)
        obj_node = dupin_util_json_node_object_filter_fields (record_obj_node, fields_format, fields_splitted, FALSE, NULL);
      else
        obj_node = json_node_copy (record_obj_node);
    }
  else
    obj_node = json_node_copy (record_obj_node);

  if (obj_node == NULL)
    {
      if (fields_splitted != NULL)
        g_strfreev (fields_splitted);

      client->request_included_docs_level--;

      return NULL;
    }

  obj = json_node_get_object (obj_node);

  /* Setting _id and _rev: */
  json_object_set_string_member (obj, REQUEST_OBJ_ID, id);
  json_object_set_string_member (obj, REQUEST_OBJ_REV, mvcc);

  gchar * type = (gchar *)dupin_record_get_type (record);
  if (type != NULL)
    json_object_set_string_member (obj, REQUEST_OBJ_TYPE, type);

  /* Setting links and relationships if requested */
  if (visit_links == TRUE)
    {
      DupinLinkB * linkb=NULL;

      DupinLinksType include_links_type = DP_LINK_TYPE_NONE;
      gboolean include_links_weblinks_descending = FALSE;
      guint include_links_weblinks_count = DUPIN_LINKB_MAX_LINKS_COUNT;
      guint include_links_weblinks_offset = 0;
      gboolean include_links_relationships_descending = FALSE;
      guint include_links_relationships_count = DUPIN_LINKB_MAX_LINKS_COUNT;
      guint include_links_relationships_offset = 0;

      gchar ** include_links_rels = NULL;
      DupinFilterByType include_links_rels_op = DP_FILTERBY_EQUALS;
      gchar ** include_links_labels = NULL;
      DupinFilterByType include_links_labels_op = DP_FILTERBY_EQUALS;
      gchar ** include_links_hrefs = NULL;
      DupinFilterByType include_links_hrefs_op = DP_FILTERBY_EQUALS;
      gchar ** include_links_tags = NULL;
      DupinFilterByType include_links_tags_op = DP_FILTERBY_EQUALS;

      gchar * include_links_filter_by = NULL;
      DupinFieldsFormatType include_links_filter_by_format = DP_FIELDS_FORMAT_DOTTED;
      DupinFilterByType include_links_filter_op = DP_FILTERBY_UNDEF;
      gchar * include_links_filter_values = NULL;

      gsize include_links_created = 0;
      DupinCreatedType include_links_created_op = DP_CREATED_SINCE;

      gint include_linked_docs_level = DUPIN_INCLUDE_DEFAULT_LEVEL;

      for (list = arguments; list; list = list->next)
        {
          dupin_keyvalue_t *kv = list->data;

          if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE))
            {
              if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_ALL_LINKS))
                include_links_type = DP_LINK_TYPE_ANY;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_WEBLINKS))
                include_links_type = DP_LINK_TYPE_WEB_LINK;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_RELATIONSHIPS))
                include_links_type = DP_LINK_TYPE_RELATIONSHIP;
            }

	  /* NOTE - we do not process any parameters for sub-requests but the link type and
		    do a depth-first visit of the whole tree, considering to reach the limits of max web-links
		    and max relationships as needed - this is basically a "fetch object" */

 	  if (client->request_included_links_level >= 1)
	    continue;

          if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_DESCENDING)
	           && !g_strcmp0 (kv->value, "true"))
	    include_links_weblinks_descending = TRUE;

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_LIMIT))
	    include_links_weblinks_count = atoi (kv->value);

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_OFFSET))
	    include_links_weblinks_offset = atoi (kv->value);

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_DESCENDING)
	           && !g_strcmp0 (kv->value, "true"))
	    include_links_relationships_descending = TRUE;

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_LIMIT))
	    include_links_relationships_count = atoi (kv->value);

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_OFFSET))
	    include_links_relationships_offset = atoi (kv->value);

	  else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_CREATED_SINCE))
            {
	      include_links_created = (gsize) g_ascii_strtoll (kv->value, NULL, 10);
              include_links_created_op = DP_CREATED_SINCE;
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_CREATED_SINCE))
            {
	      include_links_created = (gsize) g_ascii_strtoll (kv->value, NULL, 10);
              include_links_created_op = DP_CREATED_UNTIL;
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELS))
            {
              include_links_rels = g_strsplit (kv->value, ",", -1);
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELS_OP))
            {
              if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                include_links_rels_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                include_links_rels_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                include_links_rels_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                include_links_rels_op = DP_FILTERBY_PRESENT;
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS))
            {
              include_links_labels = g_strsplit (kv->value, ",", -1);

      	      gint i;
      	      gboolean any = FALSE;
              for (i = 0; include_links_labels[i]; i++)
                {   
                  if ((!g_strcmp0 (include_links_labels[i],REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL))
                      || (!g_strcmp0 (include_links_labels[i],REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL_RELATIONSHIPS)))
                    {
                      any = TRUE;
                      break;
                    }
                }

              if (any == TRUE)
	        {
                  if (include_links_labels != NULL)
                    g_strfreev (include_links_labels);

		  include_links_labels=NULL;
		}
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS_OP))
            {
              if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                include_links_labels_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                include_links_labels_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                include_links_labels_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                include_links_labels_op = DP_FILTERBY_PRESENT;
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_HREFS))
            {
              include_links_hrefs = g_strsplit (kv->value, ",", -1);
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_HREFS_OP))
            {
              if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                include_links_hrefs_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                include_links_hrefs_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                include_links_hrefs_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                include_links_hrefs_op = DP_FILTERBY_PRESENT;
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TAGS))
            {
              include_links_tags = g_strsplit (kv->value, ",", -1);
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TAGS_OP))
            {
              if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                include_links_tags_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                include_links_tags_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                include_links_tags_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                include_links_tags_op = DP_FILTERBY_PRESENT;
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_BY))
            {
              include_links_filter_by = kv->value;
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_BY_FORMAT))
            {
              if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_BY_FORMAT_DOTTED))
                include_links_filter_by_format = DP_FIELDS_FORMAT_DOTTED;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_BY_FORMAT_JSONPATH))
                include_links_filter_by_format = DP_FIELDS_FORMAT_JSONPATH;
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_OP))
            {
              if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                include_links_filter_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                include_links_filter_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                include_links_filter_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                include_links_filter_op = DP_FILTERBY_PRESENT;
           }

         else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_VALUES))
           {
             include_links_filter_values = kv->value;
           }

         else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_LEVEL))
            {
              include_linked_docs_level = (gint) atoi (kv->value);

              if (include_linked_docs_level > DUPIN_INCLUDE_MAX_LEVEL)
                {
                  include_linked_docs_level = DUPIN_INCLUDE_MAX_LEVEL;
                }
            }
        }

      if (include_links_type != DP_LINK_TYPE_NONE
	  && json_object_has_member (json_node_get_object (obj_node), RESPONSE_OBJ_LINKS) == FALSE
	  && json_object_has_member (json_node_get_object (obj_node), RESPONSE_OBJ_RELATIONSHIPS) == FALSE)
        {
          GList *list;
          GList *results;

          if (! (linkb = dupin_linkbase_open (client->thread->data->dupin, record->db->name, NULL)))
            {
	      if (obj_node != NULL)
	        json_node_free (obj_node);

              if (include_links_rels != NULL)
                g_strfreev (include_links_rels);

              if (include_links_labels != NULL)
                g_strfreev (include_links_labels);

              if (include_links_hrefs != NULL)
                g_strfreev (include_links_hrefs);

              if (include_links_tags != NULL)
                g_strfreev (include_links_tags);

              request_set_error (client, "Cannot connect to linkbase");

	      if (fields_splitted != NULL)
	        g_strfreev (fields_splitted);

	      /* TODO - pass the ret code here as well ? */

	      client->request_included_docs_level--;

              return NULL;
            }

	  if ((include_links_type == DP_LINK_TYPE_ANY
	       || include_links_type == DP_LINK_TYPE_WEB_LINK)
		&& client->request_included_links_level <= include_linked_docs_level)
            {
	      JsonNode * links_node = json_node_new (JSON_NODE_OBJECT);
	      JsonObject * links_obj = json_object_new ();
	      json_node_take_object (links_node, links_obj);

              gsize total_links = dupin_link_record_get_list_total (linkb, 0, 0, DP_LINK_TYPE_WEB_LINK, NULL, NULL, TRUE, DP_COUNT_EXIST,
						    (gchar *) dupin_record_get_id (record), include_links_rels, include_links_rels_op,
						    include_links_labels, include_links_labels_op, include_links_hrefs, include_links_hrefs_op, include_links_tags, include_links_tags_op,
						    include_links_filter_by, include_links_filter_by_format, include_links_filter_op, include_links_filter_values);

              if (dupin_link_record_get_list (linkb, include_links_weblinks_count, include_links_weblinks_offset,
					      0, 0, DP_LINK_TYPE_WEB_LINK, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, include_links_weblinks_descending,
				              (gchar *) dupin_record_get_id (record), include_links_rels, include_links_rels_op,
					      include_links_labels, include_links_labels_op, include_links_hrefs, include_links_hrefs_op, include_links_tags, include_links_tags_op,
					      include_links_filter_by, include_links_filter_by_format, include_links_filter_op, include_links_filter_values, &results, NULL) == FALSE)
                {
		  // just log the error and reason into JSON
		  gchar * msg = g_strdup_printf ("Cannot get list of web links for record %s\n", (gchar *)dupin_record_get_id (record));
                  request_set_error (client, msg);
		  fprintf (stderr, "%s", msg);
		  g_free (msg);
		  json_node_free (links_node);
		  goto request_record_revision_obj_relationships;
                }

              for (list = results; list; list = list->next)
                {
                  DupinLinkRecord *link_record = list->data;
      		  JsonNode *on=NULL;

		  gchar * label = (gchar *) dupin_link_record_get_label (link_record);

		  if (json_object_has_member (links_obj, label) == FALSE)
		    {
	              JsonNode * links_label_node = json_node_new (JSON_NODE_ARRAY);
	              JsonArray * links_label_array = json_array_new ();
	              json_node_take_array (links_label_node, links_label_array);
		      json_object_set_member (links_obj, label, links_label_node);
                    }

                  /* we do no never return deleted links */
      		  if (!  (on = request_link_record_revision_obj (client, arguments,
								 link_record,
								 (gchar *) dupin_link_record_get_id (link_record),
								 dupin_link_record_get_last_revision (link_record),
								 TRUE)))
        	    {
		      // just log the error and reason into JSON
		      gchar * msg = g_strdup_printf ("Cannot get web link with id %s and label %s in record %s\n",
								(gchar *)dupin_link_record_get_id (link_record),
								label, (gchar *)dupin_record_get_id (record));
                      request_set_error (client, msg);
		      fprintf (stderr, "%s", msg);
		      g_free (msg);
		      continue;
        	    }

                  // remove context_id and label due they are implied

                  if (on != NULL)
                    json_array_add_element( json_node_get_array (
						json_object_get_member (links_obj, label)), on);
                }

	      if (json_object_get_size (links_obj) > 0
		  && json_object_has_member (links_obj, RESPONSE_OBJ_LINKS_PAGING) == FALSE)
                {
                  JsonNode * paging_info_node = json_node_new (JSON_NODE_OBJECT);
                  JsonObject * paging_info = json_object_new ();
		  json_node_take_object (paging_info_node, paging_info);
                  json_object_set_int_member (paging_info, "total_links", total_links);
	          json_object_set_int_member (paging_info, "offset", include_links_weblinks_offset);
	          json_object_set_int_member (paging_info, "links_per_document", include_links_weblinks_count);
		  json_object_set_member (links_obj, RESPONSE_OBJ_LINKS_PAGING, paging_info_node);
                }

	      if (results)
	        dupin_link_record_get_list_close (results);

	      json_object_set_member (json_node_get_object (obj_node), RESPONSE_OBJ_LINKS, links_node);
            }

request_record_revision_obj_relationships:

	  if ((include_links_type == DP_LINK_TYPE_ANY
	       || include_links_type == DP_LINK_TYPE_RELATIONSHIP)
		&& client->request_included_links_level <= include_linked_docs_level)
            {
	      JsonNode * relationships_node = json_node_new (JSON_NODE_OBJECT);
	      JsonObject * relationships_obj = json_object_new ();
	      json_node_take_object (relationships_node, relationships_obj);

              gsize total_relationships = dupin_link_record_get_list_total (linkb, 0, 0, DP_LINK_TYPE_RELATIONSHIP, NULL, NULL, TRUE, DP_COUNT_EXIST,
				              (gchar *) dupin_record_get_id (record), include_links_rels, include_links_rels_op,
					      include_links_labels, include_links_labels_op, include_links_hrefs, include_links_hrefs_op, include_links_tags, include_links_tags_op,
					      include_links_filter_by, include_links_filter_by_format, include_links_filter_op, include_links_filter_values);

              if (dupin_link_record_get_list (linkb, include_links_relationships_count, include_links_relationships_offset,
					      0, 0, DP_LINK_TYPE_RELATIONSHIP, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, include_links_relationships_descending,
				 	      (gchar *) dupin_record_get_id (record), include_links_rels, include_links_rels_op,
					      include_links_labels, include_links_labels_op, include_links_hrefs, include_links_hrefs_op, include_links_tags, include_links_tags_op,
					      include_links_filter_by, include_links_filter_by_format, include_links_filter_op, include_links_filter_values, &results, NULL) == FALSE)
                {
		  // just log the error and reason into JSON
		  gchar * msg = g_strdup_printf ("Cannot get list of relationships for record %s\n", (gchar *)dupin_record_get_id (record));
                  request_set_error (client, msg);
		  fprintf (stderr, "%s", msg);
		  g_free (msg);
		  json_node_free (relationships_node);
		  goto request_record_revision_obj_end;
                }

              for (list = results; list; list = list->next)
                {
                  DupinLinkRecord *link_record = list->data;
      		  JsonNode *on;

		  gchar * label = (gchar *) dupin_link_record_get_label (link_record);

		  if (json_object_has_member (relationships_obj, label) == FALSE)
		    {
	              JsonNode * relationships_label_node = json_node_new (JSON_NODE_ARRAY);
	              JsonArray * relationships_label_array = json_array_new ();
	              json_node_take_array (relationships_label_node, relationships_label_array);
		      json_object_set_member (relationships_obj, label, relationships_label_node);
                    }

                  /* we do no never return deleted relationships */
      		  if (!  (on = request_link_record_revision_obj (client, arguments,
								 link_record,
								 (gchar *) dupin_link_record_get_id (link_record),
								 dupin_link_record_get_last_revision (link_record),
								 TRUE)))
        	    {
		      // just log the error and reason into JSON
		      gchar * msg = g_strdup_printf ("Cannot get relationship with id %s and label %s in record %s\n",
								(gchar *)dupin_link_record_get_id (link_record),
								label, (gchar *)dupin_record_get_id (record));
                      request_set_error (client, msg);
		      fprintf (stderr, "%s", msg);
		      g_free (msg);
		      continue;
        	    }

                  // remove context_id and label due they are implied

                  if (on != NULL)
                    json_array_add_element( json_node_get_array (
						json_object_get_member (relationships_obj, label)), on);
                }

	      if (json_object_get_size (relationships_obj) > 0
	          && json_object_has_member (relationships_obj, RESPONSE_OBJ_LINKS_PAGING) == FALSE)
                {
                  JsonNode * paging_info_node = json_node_new (JSON_NODE_OBJECT);
                  JsonObject * paging_info = json_object_new ();
		  json_node_take_object (paging_info_node, paging_info);
                  json_object_set_int_member (paging_info, "total_relationships", total_relationships);
	          json_object_set_int_member (paging_info, "offset", include_links_relationships_offset);
	          json_object_set_int_member (paging_info, "relationships_per_document", include_links_relationships_count);

		  /* add base */
		  gchar * escaped_base = g_uri_escape_string (record->db->name, NULL, TRUE);
                  GString * str = g_string_new (NULL);
                  g_string_append_printf (str, "/%s/", escaped_base);
		  g_free (escaped_base);
		  gchar * tmp = g_string_free (str, FALSE);
	          json_object_set_string_member (paging_info, "base", tmp);
		  g_free (tmp);

		  json_object_set_member (relationships_obj, RESPONSE_OBJ_LINKS_PAGING, paging_info_node);
                }

	      if (results)
	        dupin_link_record_get_list_close (results);

	      json_object_set_member (json_node_get_object (obj_node), RESPONSE_OBJ_RELATIONSHIPS, relationships_node);
            }

request_record_revision_obj_end:

          if (linkb != NULL)
            dupin_linkbase_unref (linkb);
        }

      if (include_links_rels != NULL)
        g_strfreev (include_links_rels);

      if (include_links_labels != NULL)
        g_strfreev (include_links_labels);

      if (include_links_hrefs != NULL)
        g_strfreev (include_links_hrefs);

      if (include_links_tags != NULL)
        g_strfreev (include_links_tags);
    }

  if (fields_splitted != NULL)
    g_strfreev (fields_splitted);

//DUPIN_UTIL_DUMP_JSON ("request_record_revision_obj: obj_node:", obj_node);

  client->request_included_docs_level--;

  return obj_node;
}

static JsonNode *
request_link_record_revision_obj (DSHttpdClient * client, GList * arguments,
				  DupinLinkRecord * record,
				  gchar * id, gchar * mvcc,
			          gboolean visit_docs)
{

  client->request_included_links_level++;

  JsonNode *obj_node;
  JsonObject *obj;
  GList * list = NULL;

  if (dupin_link_record_is_deleted (record, mvcc) == TRUE)
    {
      obj_node = json_node_new (JSON_NODE_OBJECT);

      if (obj_node == NULL)
        {
          client->request_included_links_level--;

          return NULL;
        }

      obj = json_object_new ();

      if (obj == NULL)
        {
          json_node_free (obj_node);

          client->request_included_links_level--;

          return NULL;
        }

      json_node_take_object (obj_node, obj);

      /* Setting _id, _rev and _deleted: */
      json_object_set_string_member (obj, REQUEST_OBJ_ID, id);
      json_object_set_string_member (obj, REQUEST_OBJ_REV, mvcc);
      json_object_set_boolean_member (obj, RESPONSE_OBJ_DELETED, TRUE);

      client->request_included_links_level--;

      return obj_node;
    }

  JsonNode * record_obj_node = dupin_link_record_get_revision_node (record, mvcc);

  if (record_obj_node == NULL)
    {
      client->request_included_links_level--;

      return NULL;
    }

  /* filter fields */
  gchar * fields = NULL;
  gchar ** fields_splitted = NULL;
  DupinFieldsFormatType fields_format = DP_FIELDS_FORMAT_DOTTED;
  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_LINK_FIELDS))
        {
          fields = kv->value;
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED))
            fields_format = DP_FIELDS_FORMAT_DOTTED;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_JSONPATH))
            fields_format = DP_FIELDS_FORMAT_JSONPATH;
        }
    }

//g_message("request_link_record_revision_obj: fields=%s fields_format=%d", fields, fields_format);

  if (fields != NULL)
    {
      fields_splitted = g_strsplit (fields, ",", -1);
      gint i;
      gboolean any = FALSE;
      for (i = 0; fields_splitted[i]; i++)
        {   
          if ((!g_strcmp0 (fields_splitted[i],REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL))
              || (!g_strcmp0 (fields_splitted[i],REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL_FIELDS)))
            {
              any = TRUE;
              break;
            }
        }
      if (any == FALSE)
        obj_node = dupin_util_json_node_object_filter_fields (record_obj_node, fields_format, fields_splitted, FALSE, NULL);
      else
        obj_node = json_node_copy (record_obj_node);
    }
  else
    obj_node = json_node_copy (record_obj_node);

  if (obj_node == NULL)
    {
      if (fields_splitted != NULL)
        g_strfreev (fields_splitted);

      client->request_included_links_level--;

      return NULL;
    }

  obj = json_node_get_object (obj_node);

  json_object_set_string_member (obj, REQUEST_LINK_OBJ_CONTEXT_ID, dupin_link_record_get_context_id (record));
  json_object_set_string_member (obj, REQUEST_LINK_OBJ_LABEL, dupin_link_record_get_label (record));
  json_object_set_string_member (obj, REQUEST_LINK_OBJ_HREF, dupin_link_record_get_href (record));

  gchar * rel = (gchar *)dupin_link_record_get_rel (record);

  if (rel != NULL)
    json_object_set_string_member (obj, REQUEST_LINK_OBJ_REL, rel);

  gchar * tag = (gchar *)dupin_link_record_get_tag (record);

  if (tag != NULL)
    json_object_set_string_member (obj, REQUEST_LINK_OBJ_TAG, tag);

  /* Setting _id and _rev: */
  json_object_set_string_member (obj, REQUEST_LINK_OBJ_ID, id);
  json_object_set_string_member (obj, REQUEST_LINK_OBJ_REV, mvcc);

  /* Include any docs if requested */

  if (visit_docs == TRUE
      && arguments != NULL)
    {
      DupinLinkbaseIncludeDocsType include_linked_docs = DP_LINKBASE_INCLUDE_DOC_TYPE_NONE;
      gint include_linked_docs_level = DUPIN_INCLUDE_DEFAULT_LEVEL;

      for (list = arguments; list; list = list->next)
        {
	  dupin_keyvalue_t *kv = list->data;

          if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS))
            {
              if (g_strcmp0 (kv->value, REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_IN)
                   && g_strcmp0 (kv->value, REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_OUT)
                   && g_strcmp0 (kv->value, REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_ALL))
                {
		  if (obj_node != NULL)
                    json_node_free (obj_node);

                  request_set_error (client, "Invalid " REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS " parameter. Allowed values are: " REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_IN ", " REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_OUT " or " REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_ALL ".");

      		  if (fields_splitted != NULL)
        	    g_strfreev (fields_splitted);

                  /* TODO - pass the ret code here as well ? */

      		  client->request_included_links_level--;

                  return NULL;
                }

              if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_IN))
                include_linked_docs = DP_LINKBASE_INCLUDE_DOC_TYPE_IN;
              else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_OUT))
                include_linked_docs = DP_LINKBASE_INCLUDE_DOC_TYPE_OUT;
              else
                include_linked_docs = DP_LINKBASE_INCLUDE_DOC_TYPE_ALL;
            }
          if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_LEVEL))
            {
	      include_linked_docs_level = (gint) atoi (kv->value);

              if (include_linked_docs_level > DUPIN_INCLUDE_MAX_LEVEL)
                {
	          include_linked_docs_level = DUPIN_INCLUDE_MAX_LEVEL;
                }
	    }
        }

    if (include_linked_docs != DP_LINKBASE_INCLUDE_DOC_TYPE_NONE)
      {
        DupinDB * parent_db=NULL;
        DupinLinkB * parent_linkb=NULL;
        gboolean document_in_exists = TRUE;
        gboolean document_in_deleted = FALSE;
        gboolean document_out_exists = TRUE;
        gboolean document_out_deleted = FALSE;
        JsonNode * node_in = NULL;
        JsonNode * node_out = NULL;
        gchar * href = (gchar *)dupin_link_record_get_href (record);
        gchar * context_id = (gchar *)dupin_link_record_get_context_id (record);

//g_message("request_link_record_revision_obj: generating links for link record id=%s with mvcc=%s for context_id=%s href=%s\n", id, mvcc, context_id, href);

        if (dupin_linkbase_get_parent_is_db (record->linkb) == TRUE )
          {
	    if (! (parent_db = dupin_database_open (client->thread->data->dupin, record->linkb->parent, NULL)))
	      {
	        if (obj_node != NULL)
	          json_node_free (obj_node);

	        request_set_error (client, "Cannot connect to parent database");

      		if (fields_splitted != NULL)
        	  g_strfreev (fields_splitted);

	        /* TODO - pass the ret code here as well ? */

      		client->request_included_links_level--;

	        return NULL;
	      }

            DupinRecord * doc_id_record = NULL;

            if ((include_linked_docs == DP_LINKBASE_INCLUDE_DOC_TYPE_IN
		 || include_linked_docs == DP_LINKBASE_INCLUDE_DOC_TYPE_ALL)
		&& client->request_included_docs_level <= include_linked_docs_level)
              {
                doc_id_record = dupin_record_read (parent_db, context_id, NULL);

	        if (doc_id_record == NULL)
	          document_in_exists = FALSE;
                else
                  {
		    if (dupin_record_is_deleted (doc_id_record, NULL) == FALSE)
                      {
                        if (! (node_in = request_record_revision_obj (client,
							   arguments,
							   doc_id_record,
							   context_id,
							   (gchar *)dupin_record_get_last_revision (doc_id_record),
							   TRUE)))
                          {
	                    if (obj_node != NULL)
	                      json_node_free (obj_node);
                            dupin_record_close (doc_id_record);
                            dupin_database_unref (parent_db);
                            request_set_error (client, "Cannot read ancestor record from parent database");

      			    if (fields_splitted != NULL)
        	  	      g_strfreev (fields_splitted);

	                    /* TODO - pass the ret code here as well ? */

      			    client->request_included_links_level--;

	                    return NULL;
                          }
                      }
                    else
                      document_in_deleted = TRUE;

                    dupin_record_close (doc_id_record);
                  }
	      }

            if (dupin_link_record_is_weblink (record) == FALSE
                && dupin_link_record_is_reflexive (record) == FALSE
		&& (include_linked_docs == DP_LINKBASE_INCLUDE_DOC_TYPE_OUT
		    || include_linked_docs == DP_LINKBASE_INCLUDE_DOC_TYPE_ALL)
		&& client->request_included_docs_level <= include_linked_docs_level)
              {
		doc_id_record = NULL;
                doc_id_record = dupin_record_read (parent_db, href, NULL);

	        if (doc_id_record == NULL)
	          document_out_exists = FALSE;
                else
                  {
		    if (dupin_record_is_deleted (doc_id_record, NULL) == FALSE)
                      {
                        if (! (node_out = request_record_revision_obj (client,
							   arguments,
							   doc_id_record,
							   href,
							   (gchar *)dupin_record_get_last_revision (doc_id_record),
							   TRUE)))
                          {
	                    if (obj_node != NULL)
	                      json_node_free (obj_node);
	                    if (node_in != NULL)
	                      json_node_free (node_in);
                            dupin_record_close (doc_id_record);
                            dupin_database_unref (parent_db);
                            request_set_error (client, "Cannot read child record from parent database");

      			    if (fields_splitted != NULL)
        	  	      g_strfreev (fields_splitted);

	                    /* TODO - pass the ret code here as well ? */

      			    client->request_included_links_level--;

	                    return NULL;
                          }
                      }
                    else
                      document_out_deleted = TRUE;

                    dupin_record_close (doc_id_record);
                  }
	      }

            dupin_database_unref (parent_db);
          }
        else
          {
            if (!(parent_linkb = dupin_linkbase_open (client->thread->data->dupin, record->linkb->parent, NULL)))
              {
	        if (obj_node != NULL)
	          json_node_free (obj_node);

                request_set_error (client, "Cannot connect to parent linkbase");

      		if (fields_splitted != NULL)
        	  g_strfreev (fields_splitted);

	        /* TODO - pass the ret code here as well ? */

    		client->request_included_links_level--;

	        return NULL;
              }

            DupinLinkRecord * link_id_record = NULL;

            if ((include_linked_docs == DP_LINKBASE_INCLUDE_DOC_TYPE_IN
		 || include_linked_docs == DP_LINKBASE_INCLUDE_DOC_TYPE_ALL)
		&& client->request_included_links_level <= include_linked_docs_level)
              {
                link_id_record = dupin_link_record_read (parent_linkb, context_id, NULL);

	        if (link_id_record == NULL)
	          document_in_exists = FALSE;
                else
                  {
                    if (dupin_link_record_is_deleted (link_id_record, NULL) == FALSE)
                      {
                        if (! (node_in = request_link_record_revision_obj (client,
							   arguments,
							   link_id_record,
							   context_id,
							   (gchar *)dupin_link_record_get_last_revision (link_id_record),
							   TRUE)))
                          {
	                    if (obj_node != NULL)
	                      json_node_free (obj_node);
                            dupin_link_record_close (link_id_record);
                            dupin_linkbase_unref (parent_linkb);
                            request_set_error (client, "Cannot read record from parent linkbase");

      			    if (fields_splitted != NULL)
        	  	      g_strfreev (fields_splitted);

	                    /* TODO - pass the ret code here as well ? */

    			    client->request_included_links_level--;

	                    return NULL;
                          }
                      }
                    else
                      document_in_deleted = TRUE;

                    dupin_link_record_close (link_id_record);
                  }
              }

            if (dupin_link_record_is_weblink (record) == FALSE
                && dupin_link_record_is_reflexive (record) == FALSE
		&& (include_linked_docs == DP_LINKBASE_INCLUDE_DOC_TYPE_OUT
		    || include_linked_docs == DP_LINKBASE_INCLUDE_DOC_TYPE_ALL)
		&& client->request_included_links_level <= include_linked_docs_level)
              {
		link_id_record = NULL;
                link_id_record = dupin_link_record_read (parent_linkb, href, NULL);

	        if (link_id_record == NULL)
	          document_out_exists = FALSE;
                else
                  {
                    if (dupin_link_record_is_deleted (link_id_record, NULL) == FALSE)
                      {
                        if (! (node_out = request_link_record_revision_obj (client,
							   arguments,
							   link_id_record,
							   href,
							   (gchar *)dupin_link_record_get_last_revision (link_id_record),
							   TRUE)))
                          {
	                    if (obj_node != NULL)
	                      json_node_free (obj_node);
	                    if (node_in != NULL)
	                      json_node_free (node_in);
                            dupin_link_record_close (link_id_record);
                            dupin_linkbase_unref (parent_linkb);
                            request_set_error (client, "Cannot read record from parent linkbase");

      			    if (fields_splitted != NULL)
        	  	      g_strfreev (fields_splitted);

	                    /* TODO - pass the ret code here as well ? */

    			    client->request_included_links_level--;

	                    return NULL;
                          }
                      }
                    else
                      document_out_deleted = TRUE;

                    dupin_link_record_close (link_id_record);
                  }
	      }

            dupin_linkbase_unref (parent_linkb);
          }

        if (document_in_deleted == TRUE )
          {
            node_in = json_node_new (JSON_NODE_OBJECT);
            JsonObject * doc_obj = json_object_new ();
            json_node_take_object (node_in, doc_obj);
            json_object_set_boolean_member (doc_obj, RESPONSE_OBJ_DELETED, TRUE);
          }
        else if (document_in_exists == FALSE )
          {
            node_in = json_node_new (JSON_NODE_OBJECT);
            JsonObject * doc_obj = json_object_new ();
            json_node_take_object (node_in, doc_obj);
            json_object_set_boolean_member (doc_obj, RESPONSE_OBJ_EMPTY, TRUE);
          }

        if (document_out_deleted == TRUE )
          {
            node_out = json_node_new (JSON_NODE_OBJECT);
            JsonObject * doc_obj = json_object_new ();
            json_node_take_object (node_out, doc_obj);
            json_object_set_boolean_member (doc_obj, RESPONSE_OBJ_DELETED, TRUE);
          }
        else if (document_out_exists == FALSE )
          {
            node_out = json_node_new (JSON_NODE_OBJECT);
            JsonObject * doc_obj = json_object_new ();
            json_node_take_object (node_out, doc_obj);
            json_object_set_boolean_member (doc_obj, RESPONSE_OBJ_EMPTY, TRUE);
          }

        if (node_in != NULL)
          json_object_set_member (obj, RESPONSE_LINK_OBJ_DOC_IN, node_in);

        if (node_out != NULL)
          json_object_set_member (obj, RESPONSE_LINK_OBJ_DOC_OUT, node_out);
      }
    }

  if (fields_splitted != NULL)
    g_strfreev (fields_splitted);

//DUPIN_UTIL_DUMP_JSON ("request_link_record_revision_obj: obj_node:", obj_node);

  client->request_included_links_level--;

  return obj_node;
}

static JsonNode *
request_view_record_obj (DSHttpdClient * client,
			 GList * arguments,
			 DupinViewRecord * record, gchar * id,
			 gboolean visit_docs)
{
  JsonNode * node = dupin_view_record_get (record);

  if (node == NULL)
    return NULL;

  return json_node_copy (node);
}

/* Changes API */

gboolean
request_get_changes_comet_database (DSHttpdClient * client,
			   gchar *buf,
			   gsize count,
                           gsize offset,
                           gsize *bytes_read, 
			   GError **       error)
{
  g_return_val_if_fail (client->output.changes_comet.db != NULL, FALSE);
  g_return_val_if_fail (client != NULL, FALSE);
  g_return_val_if_fail (   client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL
                        || client->output.changes_comet.param_feed == DP_CHANGES_FEED_CONTINUOUS , FALSE);

  GList * results=NULL;
  GList * list=NULL;
  JsonNode * change=NULL;
  JsonObject * on_obj=NULL;
  gboolean ret = TRUE;
  GString * str = NULL;

request_get_changes_comet_database_next:

  if (client->output.changes_comet.db->todelete == TRUE)
    {
      goto request_get_changes_comet_database_error;
    }

//g_message("request_get_changes_comet_database: count=%d offset=%d\n", (gint)count, (gint)offset);

  if (client->output.changes_comet.change_generated == FALSE)
    {
      if (dupin_database_get_changes_list (client->output.changes_comet.db,
                                       DUPIN_DB_MAX_CHANGES_COMET_COUNT,
                                       client->output.changes_comet.change_results_offset,
                                       client->output.changes_comet.param_since,
                                       0,
                                       client->output.changes_comet.param_style,
                                       DP_COUNT_CHANGES, DP_ORDERBY_ROWID,
                                       client->output.changes_comet.param_descending,
				       client->output.changes_comet.param_types,
                                       client->output.changes_comet.param_types_op,
				       &results, NULL) == FALSE)
        {
          goto request_get_changes_comet_database_error;
        }

//g_message("request_get_changes_comet_database: param_since=%d results_count=%d\n", (gint)client->output.changes_comet.param_since, (gint)g_list_length (results));

      str = g_string_new (NULL);

      if (client->output.changes_comet.change_size == 0
          && client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL)
        {
          g_string_append (str,"{ \"results\": [\n");
        }
      else if (g_list_length (results) == 0)
        {
          /* heartbeat */
//g_message("request_get_changes_comet_database: heat beat\n");

          g_string_append (str,"\n");
        }
      else
        {
          for (list = results; list; list = list->next)
            {
              change = list->data;
              on_obj = json_node_get_object (change);

              if (client->output.changes_comet.param_include_docs == TRUE)
                {
                  gchar * record_id   = (gchar *) json_object_get_string_member (on_obj, "id");
                  gchar * record_mvcc = (gchar *) json_object_get_string_member (
                                                json_array_get_object_element (json_object_get_array_member (on_obj, "changes"), 0)
                                                , "rev");

//g_message("request_get_changes_comet_database: record_id=%s record_mvcc=%s\n", record_id, record_mvcc);

                  DupinRecord * db_record=NULL;
                  if (!(db_record = dupin_record_read (client->output.changes_comet.db, record_id, NULL)))
                    {
                      json_node_free (change);
                      goto request_get_changes_comet_database_error;
                    }

                  JsonNode * doc = NULL;

                  if (! (doc = request_record_revision_obj (client, client->request_arguments,
							    db_record, record_id, record_mvcc,
							    FALSE)))
                    {
                      json_node_free (change);
                      goto request_get_changes_comet_database_error;
                    }

                  dupin_record_close (db_record);

                  json_object_set_member (on_obj, RESPONSE_OBJ_DOC, doc);
                }

                client->output.changes_comet.change_last_seq = (gsize)json_object_get_int_member (on_obj, "seq");

                gchar * change_str = dupin_util_json_serialize (change);
                g_string_append (str, change_str);
		g_free (change_str);

                if ((client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL)
                    && ((client->output.changes_comet.change_last_seq < client->output.changes_comet.change_max_rowid)
                         || (client->output.changes_comet.change_last_seq > client->output.changes_comet.change_max_rowid
                             && client->output.changes_comet.change_last_seq < (client->output.changes_comet.param_since+DUPIN_DB_MAX_CHANGES_COMET_COUNT))))
                  g_string_append (str, ",");

                g_string_append (str, "\n");
            }

//g_message("request_get_changes_comet_database: last_seq=%d max_rowid=%d\n", (gint)client->output.changes_comet.change_last_seq , (gint) client->output.changes_comet.change_max_rowid);

            if ((client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL)
                && ((client->output.changes_comet.change_last_seq == client->output.changes_comet.change_max_rowid)
                     || (client->output.changes_comet.change_last_seq > client->output.changes_comet.change_max_rowid
                         && client->output.changes_comet.change_last_seq == (client->output.changes_comet.param_since))))
              g_string_append_printf (str,"], \"last_seq\": %" G_GSIZE_FORMAT " }", client->output.changes_comet.change_last_seq);

            client->output.changes_comet.change_generated = TRUE;

            client->output.changes_comet.change_results_offset += g_list_length (results)-1;
        }

      if (results)
        dupin_database_get_changes_list_close (results);

      client->output.changes_comet.change_string = g_string_free (str, FALSE);
      client->output.changes_comet.change_size = strlen (client->output.changes_comet.change_string);

//g_message("request_get_changes_comet_database: generated string=%s of len=%d\n",client->output.changes_comet.change_string, (gint)client->output.changes_comet.change_size);
//g_message("request_get_changes_comet_database: generated string of len=%d\n",(gint)client->output.changes_comet.change_size);
    }

  /* NOTE - cursors stuff - we have got one bunch of results/changes from longpoll or continous
            in client->output.changes_comet.changes_string of client->output.changes_comet.changes_size
	    now need to slice it as count requested by the caller - once finished if longpoll terminates,
	    otherwise read next bunch for continous */

  gint left = client->output.changes_comet.change_size - offset;

//g_message("request_get_changes_comet_database: left=%d count=%d\n", (gint)left, (gint)count);

  if (left <= 0)
    {
      client->output.changes_comet.change_generated = FALSE;
      *bytes_read = 0;

      if (client->output.changes_comet.change_last_seq < client->output.changes_comet.change_max_rowid
          || client->output.changes_comet.param_feed == DP_CHANGES_FEED_CONTINUOUS)
        {
	  offset = 0;
          client->output.changes_comet.offset = 0;

//g_message("request_get_changes_comet_database: NEXT -> last_seq=%d < max_rowid=%d\n", (gint)client->output.changes_comet.change_last_seq, (gint)client->output.changes_comet.change_max_rowid);

          goto request_get_changes_comet_database_next;
        }
      else if (client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL)
        {
          return FALSE; /* done */
        }
    }

  if (left > 0 && left < count)
    count = left;

  /* copy count bytes to buf from change_string starting from offset */

//g_message("request_get_changes_comet_database: memcpy %d bytes from change_string of len %d to buf starting at %d\n",(gint)count, (gint)client->output.changes_comet.change_size, (gint)offset);

/* TODO - we get garbage copied over !! probably when string is bigger than buffer and need to chunk it up I.e. cursoring */

  memcpy (buf, client->output.changes_comet.change_string+offset, count);

  *bytes_read = count;

//g_message("request_get_changes_comet_database: bytes_read=%d (=count)\n", (gint)*bytes_read);

  if (left == count)
    {
//g_message("request_get_changes_comet_database: freeing string generated buffer of bytes=%d\n", (gint)client->output.changes_comet.change_size);

      if (client->output.changes_comet.change_string != NULL)
        g_free (client->output.changes_comet.change_string);
      client->output.changes_comet.change_string = NULL;
    }

  return ret;

request_get_changes_comet_database_error:

  if (results)
    dupin_database_get_changes_list_close (results);

  client->output.changes_comet.change_errors++;

  return FALSE;
}

gboolean
request_get_changes_comet_linkbase (DSHttpdClient * client,
			   gchar *buf,
			   gsize count,
                           gsize offset,
                           gsize *bytes_read, 
			   GError **       error)
{
  g_return_val_if_fail (client->output.changes_comet.linkb != NULL, FALSE);
  g_return_val_if_fail (client != NULL, FALSE);
  g_return_val_if_fail (   client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL
                        || client->output.changes_comet.param_feed == DP_CHANGES_FEED_CONTINUOUS , FALSE);

  GList * results=NULL;
  GList * list=NULL;
  JsonNode * change=NULL;
  JsonObject * on_obj=NULL;
  gboolean ret = TRUE;
  GString * str = NULL;

request_get_changes_comet_linkbase_next:

  if (client->output.changes_comet.linkb->todelete == TRUE)
    {
      goto request_get_changes_comet_linkbase_error;
    }

//g_message("request_get_changes_comet_linkbase: count=%d offset=%d\n", (gint)count, (gint)offset);

  if (client->output.changes_comet.change_generated == FALSE)
    {
      if (dupin_linkbase_get_changes_list (client->output.changes_comet.linkb,
                                       DUPIN_DB_MAX_CHANGES_COMET_COUNT,
                                       client->output.changes_comet.change_results_offset,
                                       client->output.changes_comet.param_since,
                                       0,
                                       client->output.changes_comet.param_style,
                                       DP_COUNT_CHANGES, DP_ORDERBY_ROWID,
                                       client->output.changes_comet.param_descending,
                                       client->output.changes_comet.param_context_id,
                                       client->output.changes_comet.param_tags,
                                       client->output.changes_comet.param_tags_op,
					&results, NULL) == FALSE)
        {
          goto request_get_changes_comet_linkbase_error;
        }

//g_message("request_get_changes_comet_linkbase: param_since=%d results_count=%d\n", (gint)client->output.changes_comet.param_since, (gint)g_list_length (results));

      str = g_string_new (NULL);

      if (client->output.changes_comet.change_size == 0
          && client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL)
        {
          g_string_append (str,"{ \"results\": [\n");
        }
      else if (g_list_length (results) == 0)
        {
          /* heartbeat */
//g_message("request_get_changes_comet_linkbase: heat beat\n");

          g_string_append (str,"\n");
        }
      else
        {
          for (list = results; list; list = list->next)
            {
              change = list->data;
              on_obj = json_node_get_object (change);

              if (client->output.changes_comet.param_include_links == TRUE)
                {
                  gchar * record_id   = (gchar *) json_object_get_string_member (on_obj, "id");
                  gchar * record_mvcc = (gchar *) json_object_get_string_member (
                                                json_array_get_object_element (json_object_get_array_member (on_obj, "changes"), 0)
                                                , "rev");

//g_message("request_get_changes_comet_linkbase: record_id=%s record_mvcc=%s\n", record_id, record_mvcc);

                  DupinLinkRecord * linkb_record=NULL;
                  if (!(linkb_record = dupin_link_record_read (client->output.changes_comet.linkb, record_id, NULL)))
                    {
                      json_node_free (change);
                      goto request_get_changes_comet_linkbase_error;
                    }

                  JsonNode * link = NULL;

                  if (! (link = request_link_record_revision_obj (client, client->request_arguments,
								  linkb_record, record_id, record_mvcc,
								  FALSE)))
                    {
                      json_node_free (change);
                      goto request_get_changes_comet_linkbase_error;
                    }

                  dupin_link_record_close (linkb_record);

                  json_object_set_member (on_obj, RESPONSE_LINK_OBJ_LINK, link);
                }

                client->output.changes_comet.change_last_seq = (gsize)json_object_get_int_member (on_obj, "seq");

                gchar * change_str = dupin_util_json_serialize (change);
                g_string_append (str, change_str);
		g_free (change_str);

                if ((client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL)
                    && ((client->output.changes_comet.change_last_seq < client->output.changes_comet.change_max_rowid)
                         || (client->output.changes_comet.change_last_seq > client->output.changes_comet.change_max_rowid
                             && client->output.changes_comet.change_last_seq < (client->output.changes_comet.param_since+DUPIN_DB_MAX_CHANGES_COMET_COUNT))))
                  g_string_append (str, ",");

                g_string_append (str, "\n");
            }

//g_message("request_get_changes_comet_linkbase: last_seq=%d max_rowid=%d\n", (gint)client->output.changes_comet.change_last_seq , (gint) client->output.changes_comet.change_max_rowid);

            if ((client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL)
                && ((client->output.changes_comet.change_last_seq == client->output.changes_comet.change_max_rowid)
                     || (client->output.changes_comet.change_last_seq > client->output.changes_comet.change_max_rowid
                         && client->output.changes_comet.change_last_seq == (client->output.changes_comet.param_since))))
              g_string_append_printf (str,"], \"last_seq\": %" G_GSIZE_FORMAT " }", client->output.changes_comet.change_last_seq);

            client->output.changes_comet.change_generated = TRUE;

            client->output.changes_comet.change_results_offset += g_list_length (results)-1;
        }

      if (results)
        dupin_linkbase_get_changes_list_close (results);

      client->output.changes_comet.change_string = g_string_free (str, FALSE);
      client->output.changes_comet.change_size = strlen (client->output.changes_comet.change_string);

//g_message("request_get_changes_comet_linkbase: generated string=%s of len=%d\n",client->output.changes_comet.change_string, (gint)client->output.changes_comet.change_size);
//g_message("request_get_changes_comet_linkbase: generated string of len=%d\n",(gint)client->output.changes_comet.change_size);
    }

  /* NOTE - cursors stuff - we have got one bunch of results/changes from longpoll or continous
            in client->output.changes_comet.changes_string of client->output.changes_comet.changes_size
	    now need to slice it as count requested by the caller - once finished if longpoll terminates,
	    otherwise read next bunch for continous */

  gint left = client->output.changes_comet.change_size - offset;

//g_message("request_get_changes_comet_linkbase: left=%d count=%d\n", (gint)left, (gint)count);

  if (left <= 0)
    {
      client->output.changes_comet.change_generated = FALSE;
      *bytes_read = 0;

      if (client->output.changes_comet.change_last_seq < client->output.changes_comet.change_max_rowid
          || client->output.changes_comet.param_feed == DP_CHANGES_FEED_CONTINUOUS)
        {
	  offset = 0;
          client->output.changes_comet.offset = 0;

//g_message("request_get_changes_comet_linkbase: NEXT -> last_seq=%d < max_rowid=%d\n", (gint)client->output.changes_comet.change_last_seq, (gint)client->output.changes_comet.change_max_rowid);

          goto request_get_changes_comet_linkbase_next;
        }
      else if (client->output.changes_comet.param_feed == DP_CHANGES_FEED_LONGPOLL)
        {
          return FALSE; /* done */
        }
    }

  if (left > 0 && left < count)
    count = left;

  /* copy count bytes to buf from change_string starting from offset */

//g_message("request_get_changes_comet_linkbase: memcpy %d bytes from change_string %s of len %d to buf starting at %d --> %s \n",(gint)count, client->output.changes_comet.change_string, (gint)client->output.changes_comet.change_size, (gint)offset, client->output.changes_comet.change_string+offset);

/* TODO - we get garbage copied over !! probably when string is bigger than buffer and need to chunk it up I.e. cursoring */

  memcpy (buf, client->output.changes_comet.change_string+offset, count);

  *bytes_read = count;

//g_message("request_get_changes_comet_linkbase: bytes_read=%d (=count)\n", (gint)*bytes_read);

  if (left == count)
    {
//g_message("request_get_changes_comet_linkbase: freeing string generated buffer of bytes=%d\n", (gint)client->output.changes_comet.change_size);

      if (client->output.changes_comet.change_string != NULL)
        g_free (client->output.changes_comet.change_string);
      client->output.changes_comet.change_string = NULL;
    }

  return ret;

request_get_changes_comet_linkbase_error:

  if (results)
    dupin_linkbase_get_changes_list_close (results);

  client->output.changes_comet.change_errors++;

  return FALSE;
}

/* Utility functions */

void request_set_error (DSHttpdClient * client,
			gchar * msg)
{
  g_return_if_fail (client != NULL);
  g_return_if_fail (msg != NULL);

  request_clear_error (client);

  client->dupin_error_msg = g_strdup ( msg );

  return;
}

void request_clear_error (DSHttpdClient * client)
{
  g_return_if_fail (client != NULL);

  if (client->dupin_error_msg != NULL)
    g_free (client->dupin_error_msg);

  client->dupin_error_msg = NULL;

  return;
}

gchar *
request_get_error (DSHttpdClient * client)
{
  g_return_val_if_fail (client != NULL, NULL);

  return client->dupin_error_msg;
}

void request_set_warning (DSHttpdClient * client,
			gchar * msg)
{
  g_return_if_fail (client != NULL);
  g_return_if_fail (msg != NULL);

  request_clear_warning (client);

  client->dupin_warning_msg = g_strdup ( msg );

  return;
}

void request_clear_warning (DSHttpdClient * client)
{
  g_return_if_fail (client != NULL);

  if (client->dupin_warning_msg != NULL)
    g_free (client->dupin_warning_msg);

  client->dupin_warning_msg = NULL;

  return;
}

gchar *
request_get_warning (DSHttpdClient * client)
{
  g_return_val_if_fail (client != NULL, NULL);

  return client->dupin_warning_msg;
}

/* PORTABLE LISTINGS */

static DSHttpStatusCode
request_global_get_portable_listings (DSHttpdClient * client, GList * path,
			     	      GList * arguments)
{
  DupinDB *db;

  GList *list;
  GList *results;

  gboolean descending = FALSE;
  guint count = DUPIN_DB_MAX_DOCS_COUNT;
  guint offset = 0;
  gsize total_rows = 0;

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  gsize created = 0;
  DupinCreatedType created_op = DP_CREATED_SINCE;

  gboolean declining_updated_since = FALSE;
  gboolean declining_updated_until = FALSE;
  gboolean declining_filtered = FALSE;
  gboolean declining_sorted = FALSE;

  gboolean include_relationships = FALSE;

  gchar ** types = NULL;
  DupinFilterByType types_op = DP_FILTERBY_EQUALS;

  gchar * filter_by = NULL;
  DupinFieldsFormatType filter_by_format = DP_FIELDS_FORMAT_DOTTED;
  DupinFilterByType filter_op = DP_FILTERBY_UNDEF;
  gchar * filter_values = NULL;

  gchar * fields = NULL;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      request_set_error (client, "Cannot connect to database");
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_SORT_ORDER))
	descending = (!g_strcmp0 (kv->value, "descending")) ? TRUE : FALSE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FIELDS))
        {
          fields = kv->value;
	}

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_INCLUDE_RELATIONSHIPS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              request_set_error (client, "Invalid " REQUEST_GET_PORTABLE_LISTINGS_INCLUDE_RELATIONSHIPS " parameter. Allowed values are: true, false");
              return HTTP_STATUS_400;
            }

          include_relationships = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }


      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_COUNT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_START_INDEX))
	offset = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_UPDATED_SINCE))
        {
	  if (dupin_util_iso8601_to_timestamp (kv->value, &created) == FALSE)
	    {
              request_set_error (client, "Bad " REQUEST_GET_PORTABLE_LISTINGS_UPDATED_SINCE " parameter. It must be a valie ISO8601 date expressed in UTC E.g. '2011-06-12T14:10:49.090864Z'.");
              return HTTP_STATUS_400;
	    }
          created_op = DP_CREATED_SINCE;

	  /* TODO - unimplemented */
          declining_updated_since = TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_UPDATED_UNTIL))
        {
	  if (dupin_util_iso8601_to_timestamp (kv->value, &created) == FALSE)
	    {
              request_set_error (client, "Bad " REQUEST_GET_PORTABLE_LISTINGS_UPDATED_UNTIL " parameter. It must be a valie ISO8601 date expressed in UTC E.g. '2011-06-12T14:10:49.090864Z'.");
              return HTTP_STATUS_400;
	    }
          created_op = DP_CREATED_UNTIL;

	  /* TODO - unimplemented */
          declining_updated_until = TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FILTER_OBJECT_TYPE))
        {
          types = g_strsplit (kv->value, ",", -1);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FILTER_BY))
        {
          filter_by = kv->value;
        }
 
      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP_EQUALS))
            filter_op = DP_FILTERBY_EQUALS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP_CONTAINS))
            filter_op = DP_FILTERBY_CONTAINS;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP_STARTS_WITH))
            filter_op = DP_FILTERBY_STARTS_WITH;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP_PRESENT))
            filter_op = DP_FILTERBY_PRESENT;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FILTER_VALUES))
        {
          filter_values = kv->value;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_SORT_BY))
        {
	  /* TODO - unimplemented */
          declining_sorted = TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FORMAT))
        {
          if (g_strcmp0 (kv->value,REQUEST_GET_PORTABLE_LISTINGS_FORMAT_JSON))
            {
              request_set_error (client, "Invalid " REQUEST_GET_PORTABLE_LISTINGS_FORMAT " parameter. Allowed values are: " REQUEST_GET_PORTABLE_LISTINGS_FORMAT_JSON);
              return HTTP_STATUS_400;
            }
	}
    }

  /* NOTE - hack params / arguments */

  if (fields != NULL)
    {
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE,REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_ALL_LINKS));
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS,fields));

      /* NOTE - include_links_labels=fields but fields removed prefix E.g. contributor.role -> role */

//g_message ("FIELDS: %s\n", fields);

      gchar ** fields_splitted = g_strsplit (fields, ",", -1);

      gint i,j;
      GString *str = g_string_new ("");
      GString *str1 = g_string_new ("");
      gboolean appended=FALSE;

      if (fields_splitted != NULL)
        {
          for (i = 0; fields_splitted[i]; i++)
            {
	      if (appended == TRUE)
		{
	          g_string_append (str, ",");
	          g_string_append (str1, ",");
		}

//g_message ("fields_splitted[%d] = %s\n", i, fields_splitted[i]);

	      if (!g_strcmp0 (fields_splitted[i], ""))
	        continue;

              gchar ** field_parts = g_strsplit (fields_splitted[i], ".", -1);
	      if (field_parts != NULL && field_parts[1])
	        {
	          g_string_append_printf (str1, "%s", field_parts[0]);

                  for (j = 1; field_parts[j]; j++)
                    {
//g_message ("field_parts[%d]= %s\n", j, field_parts[j]);

	              g_string_append_printf (str, "%s", field_parts[j]);
	              if (field_parts[j+1])
	                g_string_append (str, ".");
	            }
	          appended = TRUE;
	        }

	      if (field_parts != NULL)
                g_strfreev (field_parts);
            }
        }
      gchar * tmp = g_string_free (str, FALSE);
      gchar * tmp1 = g_string_free (str1, FALSE);

      if (strlen (tmp) > 0)
        arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_ANY_FILTER_LINK_FIELDS,tmp));

      if (strlen (tmp1) > 0)
        arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_ANY_FILTER_FIELDS,tmp1));

//g_message ("REL FIELDS: %s\n", tmp);
//g_message ("RELS: %s\n", tmp1);

      g_free (tmp);
      g_free (tmp1);

      if (fields_splitted != NULL)
        g_strfreev (fields_splitted);
    }

  if (include_relationships == TRUE)
    {
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS,REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_OUT));
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_LEVEL,"1"));
    }

  if (types != NULL
      || filter_by != NULL)
      total_rows = dupin_record_get_list_total (db, 0, 0, NULL, NULL, TRUE, DP_COUNT_EXIST, types, types_op, 
						filter_by, filter_by_format, filter_op, filter_values, NULL);
  else
      total_rows = dupin_database_count (db, DP_COUNT_EXIST);

  /* NOTE - bear in mind we are cheating bad (on our side) and we do a full fetch from underlying DB, always even if include_docs=false */

  if (dupin_record_get_list (db, count, offset, 0, 0, NULL, NULL, TRUE, DP_COUNT_EXIST,
				DP_ORDERBY_ID, descending, types, types_op,
				filter_by, filter_by_format, filter_op, filter_values, &results, NULL) == FALSE)
    {
      dupin_database_unref (db);
      if (types)
        g_strfreev (types);

      request_set_error (client, "Cannot list documents from database");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_record_get_list_close (results);

      dupin_database_unref (db);
      if (types)
        g_strfreev (types);
      request_set_error (client, "Cannot list documents from database");
      return HTTP_STATUS_500;
    }

  json_object_set_int_member (obj, "totalResults", total_rows);
  json_object_set_int_member (obj, "startIndex", offset);
  json_object_set_int_member (obj, "itemsPerPage", count);

  if (declining_updated_since == TRUE)
    json_object_set_boolean_member (obj, "updatedSince", FALSE);

  if (declining_updated_until == TRUE)
    json_object_set_boolean_member (obj, "updatedUntil", FALSE);

  if (declining_sorted == TRUE)
    json_object_set_boolean_member (obj, "sorted", FALSE);

  if (declining_filtered == TRUE)
    json_object_set_boolean_member (obj, "filtered", FALSE);

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_portable_listings_error;

  for (list = results; list; list = list->next)
    {
      DupinRecord *record = list->data;

      JsonNode *on;
      if (!  (on = request_record_revision_obj (client, arguments,
					record, (gchar *) dupin_record_get_id (record),
					dupin_record_get_last_revision (record), TRUE)))
        {
	  json_array_unref (array);
	  goto request_global_get_portable_listings_error;
        }

      /* NOTE - lift record to suit portable listings format */

      JsonObject * on_obj = json_node_get_object (on);

      json_object_remove_member (on_obj, REQUEST_OBJ_ATTACHMENTS);
      json_object_remove_member (on_obj, REQUEST_OBJ_TYPE);
      json_object_remove_member (on_obj, REQUEST_OBJ_REV);

      json_object_set_string_member (on_obj, RESPONSE_OBJ_ID, json_object_get_string_member (on_obj,REQUEST_OBJ_ID));
      json_object_remove_member (on_obj, REQUEST_OBJ_ID);

      if (json_object_has_member (on_obj, "updated") == FALSE)
        {
          gchar * created = dupin_util_timestamp_to_iso8601 (dupin_record_get_created (record));
          json_object_set_string_member (on_obj, "updated", created);
          g_free (created);
        }

      if (json_object_has_member (on_obj, "objectType") == FALSE)
        {
          gchar * type = (gchar *)dupin_record_get_type (record);
          if (type != NULL)
            json_object_set_string_member (on_obj, "objectType", type);
        }

      /* relationships */

      if (json_object_has_member (on_obj, REQUEST_OBJ_RELATIONSHIPS) == TRUE)
        {
	  JsonObject * relationships = json_object_get_object_member (on_obj, REQUEST_OBJ_RELATIONSHIPS);
	  GList *nodes, *n;
          nodes = json_object_get_members (relationships);
          for (n = nodes; n != NULL; n = n->next)
	    {
	      if (!g_strcmp0 ((const gchar *) n->data, "_paging"))
		continue;

	      if (json_object_has_member (on_obj, (const gchar *) n->data) == FALSE)
		{
		  JsonArray * rel_array = json_object_get_array_member (relationships, (const gchar *) n->data);

		  GList *nodes1, *n1;
      		  nodes1 = json_array_get_elements (rel_array);
      		  for (n1 = nodes1; n1 != NULL; n1 = n1->next)
        	    {
		      JsonObject * r = json_node_get_object (n1->data);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_REV);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_ID);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_LABEL);

      		      if (json_object_has_member (r, REQUEST_LINK_OBJ_REL) == TRUE)
			{
      		          json_object_set_string_member (r, RESPONSE_LINK_OBJ_REL, json_object_get_string_member (r,REQUEST_LINK_OBJ_REL));
      		          json_object_remove_member (r, REQUEST_LINK_OBJ_REL);
			}

	              if (json_object_has_member (r, RESPONSE_LINK_OBJ_DOC_OUT) == TRUE)
			{
      		          json_object_remove_member (r, REQUEST_LINK_OBJ_HREF);

		          JsonObject * d = json_object_get_object_member (r, RESPONSE_LINK_OBJ_DOC_OUT);

      			  json_object_remove_member (d, REQUEST_OBJ_ATTACHMENTS);
      			  json_object_remove_member (d, REQUEST_OBJ_REV);

      			  json_object_set_string_member (d, RESPONSE_OBJ_ID, json_object_get_string_member (d,REQUEST_OBJ_ID));
      			  json_object_remove_member (d, REQUEST_OBJ_ID);

      			  if (json_object_has_member (d, "updated") == FALSE
			      && json_object_has_member (d, "_created") == TRUE)
         		    {
      			      json_object_set_string_member (d, "updated", json_object_get_string_member (d,"_created"));
      			      json_object_remove_member (d, "_created");
        		    }

      			  if (json_object_has_member (d, "objectType") == FALSE
			      && json_object_has_member (d, REQUEST_OBJ_TYPE) == TRUE)
        		    {
      			      json_object_set_string_member (d, "objectType", json_object_get_string_member (d,REQUEST_OBJ_TYPE));
      			      json_object_remove_member (d, REQUEST_OBJ_TYPE);
        		    }

      			  if (json_object_has_member (d, REQUEST_OBJ_RELATIONSHIPS) == TRUE)
      		            json_object_remove_member (d, REQUEST_OBJ_RELATIONSHIPS);

      			  if (json_object_has_member (d, REQUEST_OBJ_LINKS) == TRUE)
      		            json_object_remove_member (d, REQUEST_OBJ_LINKS);

      		          json_object_set_member (r, "entry", json_node_copy (json_object_get_member (r,RESPONSE_LINK_OBJ_DOC_OUT)));
      		          json_object_remove_member (r, RESPONSE_LINK_OBJ_DOC_OUT);
			}
		      else
			{
      		          json_object_set_string_member (r, RESPONSE_LINK_OBJ_HREF, json_object_get_string_member (r,REQUEST_LINK_OBJ_HREF));
      		          json_object_remove_member (r, REQUEST_LINK_OBJ_HREF);
		        }
		    }
      		  g_list_free (nodes1);

                  json_object_set_member (on_obj, (const gchar *) n->data,
					json_node_copy (json_object_get_member (relationships, (const gchar *) n->data)));
		}
	    }
          g_list_free (nodes);

      	  json_object_remove_member (on_obj, REQUEST_OBJ_RELATIONSHIPS);
	}

      /* links */

      if (json_object_has_member (on_obj, REQUEST_OBJ_LINKS) == TRUE)
        {
	  JsonObject * links = json_object_get_object_member (on_obj, REQUEST_OBJ_LINKS);
	  GList *nodes, *n;
          nodes = json_object_get_members (links);
          for (n = nodes; n != NULL; n = n->next)
	    {
	      if (!g_strcmp0 ((const gchar *) n->data, "_paging"))
		continue;

	      if (json_object_has_member (on_obj, (const gchar *) n->data) == FALSE)
		{
		  JsonArray * rel_array = json_object_get_array_member (links, (const gchar *) n->data);

		  GList *nodes1, *n1;
      		  nodes1 = json_array_get_elements (rel_array);
      		  for (n1 = nodes1; n1 != NULL; n1 = n1->next)
        	    {
		      JsonObject * r = json_node_get_object (n1->data);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_REV);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_ID);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_LABEL);
      		      json_object_set_string_member (r, RESPONSE_LINK_OBJ_HREF, json_object_get_string_member (r,REQUEST_LINK_OBJ_HREF));
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_HREF);

      		      if (json_object_has_member (r, REQUEST_LINK_OBJ_REL) == TRUE)
			{
      		          json_object_set_string_member (r, RESPONSE_LINK_OBJ_REL, json_object_get_string_member (r,REQUEST_LINK_OBJ_REL));
      		          json_object_remove_member (r, REQUEST_LINK_OBJ_REL);
			}
		    }
      		  g_list_free (nodes1);

                  json_object_set_member (on_obj, (const gchar *) n->data,
					json_node_copy (json_object_get_member (links, (const gchar *) n->data)));
		}
	    }
          g_list_free (nodes);

      	  json_object_remove_member (on_obj, REQUEST_OBJ_LINKS);
	}

      json_array_add_element( array, on);
    }

  json_object_set_array_member (obj, "entry", array );

  client->output_mime = g_strdup (HTTP_MIME_PORTABLE_LISTINGS_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_portable_listings_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_portable_listings_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_portable_listings_error;

  if( results )
    dupin_record_get_list_close (results);

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (types)
    g_strfreev (types);

  return HTTP_STATUS_200;

request_global_get_portable_listings_error:

  if( results )
    dupin_record_get_list_close (results);

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (types)
    g_strfreev (types);

  request_set_error (client, "Cannot list documents from database");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_portable_listings_record (DSHttpdClient * client, GList * path,
			   		     GList * arguments)
{
  gchar * mvcc = NULL;

  GList *list=NULL;

  DupinDB *db=NULL;
  DupinRecord *record=NULL;

  gboolean declining_filtered = FALSE;

  gboolean include_relationships = FALSE;

  gchar * fields = NULL;

  gchar * doc_id=NULL;

  gchar * dbname = (gchar *) path->data;

  /* GET document_ID */
  doc_id = g_strdup_printf ("%s", (gchar *)path->next->next->data);

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, dbname, NULL)))
    {
      g_free (doc_id);
      request_set_error (client, "Cannot connect to database");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_record_read (db, doc_id, NULL)))
    {
      dupin_database_unref (db);
      g_free (doc_id);
      request_set_error (client, "Cannot read record from database");
      return HTTP_STATUS_404;
    }

  if (dupin_record_is_deleted (record, NULL) == TRUE)
    {
      dupin_record_close (record);
      dupin_database_unref (db);
      g_free (doc_id);
      request_set_error (client, "Record is deleted");
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FIELDS))
        {
          fields = kv->value;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_INCLUDE_RELATIONSHIPS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              request_set_error (client, "Invalid " REQUEST_GET_PORTABLE_LISTINGS_INCLUDE_RELATIONSHIPS " parameter. Allowed values are: true, false");
              return HTTP_STATUS_400;
            }

          include_relationships = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FORMAT))
        {
          if (g_strcmp0 (kv->value,REQUEST_GET_PORTABLE_LISTINGS_FORMAT_JSON))
            {
              request_set_error (client, "Invalid " REQUEST_GET_PORTABLE_LISTINGS_FORMAT " parameter. Allowed values are: " REQUEST_GET_PORTABLE_LISTINGS_FORMAT_JSON);
              return HTTP_STATUS_400;
            }
	}
    }

  /* NOTE - hack params / arguments */

  if (fields != NULL)
    {
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE,REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_ALL_LINKS));
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS,fields));

      /* NOTE - include_links_labels=fields but fields removed prefix E.g. contributor.role -> role */

//g_message ("FIELDS: %s\n", fields);

      gchar ** fields_splitted = g_strsplit (fields, ",", -1);

      gint i,j;
      GString *str = g_string_new ("");
      GString *str1 = g_string_new ("");
      gboolean appended=FALSE;

      if (fields_splitted != NULL)
        {
          for (i = 0; fields_splitted[i]; i++)
            {
	      if (appended == TRUE)
		{
	          g_string_append (str, ",");
	          g_string_append (str1, ",");
		}

//g_message ("fields_splitted[%d] = %s\n", i, fields_splitted[i]);

	      if (!g_strcmp0 (fields_splitted[i], ""))
	        continue;

              gchar ** field_parts = g_strsplit (fields_splitted[i], ".", -1);
	      if (field_parts != NULL && field_parts[1])
	        {
	          g_string_append_printf (str1, "%s", field_parts[0]);

                  for (j = 1; field_parts[j]; j++)
                    {
//g_message ("field_parts[%d]= %s\n", j, field_parts[j]);

	              g_string_append_printf (str, "%s", field_parts[j]);
	              if (field_parts[j+1])
	                g_string_append (str, ".");
	            }
	          appended = TRUE;
	        }

	      if (field_parts != NULL)
                g_strfreev (field_parts);
            }
        }
      gchar * tmp = g_string_free (str, FALSE);
      gchar * tmp1 = g_string_free (str1, FALSE);

      if (strlen (tmp) > 0)
        arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_ANY_FILTER_LINK_FIELDS,tmp));

      if (strlen (tmp1) > 0)
        arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_ANY_FILTER_FIELDS,tmp1));

//g_message ("REL FIELDS: %s\n", tmp);
//g_message ("RELS: %s\n", tmp1);

      g_free (tmp);
      g_free (tmp1);

      if (fields_splitted != NULL)
        g_strfreev (fields_splitted);
    }

  if (include_relationships == TRUE)
    {
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS,REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_OUT));
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_LEVEL,"1"));
    }

  /* Show a single revision: */

  JsonNode *node_temp=NULL;

  mvcc = dupin_record_get_last_revision (record);

  if (!  (node_temp = request_record_revision_obj (client, arguments,
					record, (gchar *) dupin_record_get_id (record),
					mvcc,
					TRUE)))
    {
      dupin_record_close (record);
      dupin_database_unref (db);
      g_free (doc_id);
      request_set_error (client, "Cannot get record revision");
      return HTTP_STATUS_404;
    }

  /* NOTE - lift record to suit portable listings format */

  JsonObject * on_obj = json_node_get_object (node_temp);

  json_object_remove_member (on_obj, REQUEST_OBJ_ATTACHMENTS);
  json_object_remove_member (on_obj, REQUEST_OBJ_TYPE);
  json_object_remove_member (on_obj, REQUEST_OBJ_REV);

  json_object_set_string_member (on_obj, RESPONSE_OBJ_ID, json_object_get_string_member (on_obj,REQUEST_OBJ_ID));
  json_object_remove_member (on_obj, REQUEST_OBJ_ID);

  if (json_object_has_member (on_obj, "updated") == FALSE)
    {
      gchar * created = dupin_util_timestamp_to_iso8601 (dupin_record_get_created (record));
      json_object_set_string_member (on_obj, "updated", created);
      g_free (created);
    }

  if (json_object_has_member (on_obj, "objectType") == FALSE)
    {
      gchar * type = (gchar *)dupin_record_get_type (record);
      if (type != NULL)
        json_object_set_string_member (on_obj, "objectType", type);
    }

  /* relationships */

  if (json_object_has_member (on_obj, REQUEST_OBJ_RELATIONSHIPS) == TRUE)
    {
      JsonObject * relationships = json_object_get_object_member (on_obj, REQUEST_OBJ_RELATIONSHIPS);
      GList *nodes, *n;
      nodes = json_object_get_members (relationships);
      for (n = nodes; n != NULL; n = n->next)
        {
 	  if (!g_strcmp0 ((const gchar *) n->data, "_paging"))
	    continue;

	  if (json_object_has_member (on_obj, (const gchar *) n->data) == FALSE)
	    {
	      JsonArray * rel_array = json_object_get_array_member (relationships, (const gchar *) n->data);

	      GList *nodes1, *n1;
    	      nodes1 = json_array_get_elements (rel_array);
    	      for (n1 = nodes1; n1 != NULL; n1 = n1->next)
      	        {
		  JsonObject * r = json_node_get_object (n1->data);
    		  json_object_remove_member (r, REQUEST_LINK_OBJ_REV);
    		  json_object_remove_member (r, REQUEST_LINK_OBJ_ID);
    		  json_object_remove_member (r, REQUEST_LINK_OBJ_LABEL);

    		  if (json_object_has_member (r, REQUEST_LINK_OBJ_REL) == TRUE)
		    {
    		      json_object_set_string_member (r, RESPONSE_LINK_OBJ_REL, json_object_get_string_member (r,REQUEST_LINK_OBJ_REL));
    		      json_object_remove_member (r, REQUEST_LINK_OBJ_REL);
		    }

	          if (json_object_has_member (r, RESPONSE_LINK_OBJ_DOC_OUT) == TRUE)
		    {
    		      json_object_remove_member (r, REQUEST_LINK_OBJ_HREF);

		      JsonObject * d = json_object_get_object_member (r, RESPONSE_LINK_OBJ_DOC_OUT);

    		      json_object_remove_member (d, REQUEST_OBJ_ATTACHMENTS);
    		      json_object_remove_member (d, REQUEST_OBJ_REV);

    	              json_object_set_string_member (d, RESPONSE_OBJ_ID, json_object_get_string_member (d,REQUEST_OBJ_ID));
    		      json_object_remove_member (d, REQUEST_OBJ_ID);

    	              if (json_object_has_member (d, "updated") == FALSE
			  && json_object_has_member (d, "_created") == TRUE)
       		        {
    			  json_object_set_string_member (d, "updated", json_object_get_string_member (d,"_created"));
    			  json_object_remove_member (d, "_created");
      		        }

    		      if (json_object_has_member (d, "objectType") == FALSE
			  && json_object_has_member (d, REQUEST_OBJ_TYPE) == TRUE)
      		        {
    			  json_object_set_string_member (d, "objectType", json_object_get_string_member (d,REQUEST_OBJ_TYPE));
    			  json_object_remove_member (d, REQUEST_OBJ_TYPE);
      		        }

    		     if (json_object_has_member (d, REQUEST_OBJ_RELATIONSHIPS) == TRUE)
    		         json_object_remove_member (d, REQUEST_OBJ_RELATIONSHIPS);

    		     if (json_object_has_member (d, REQUEST_OBJ_LINKS) == TRUE)
    		         json_object_remove_member (d, REQUEST_OBJ_LINKS);

    		     json_object_set_member (r, "entry", json_node_copy (json_object_get_member (r,RESPONSE_LINK_OBJ_DOC_OUT)));
    		     json_object_remove_member (r, RESPONSE_LINK_OBJ_DOC_OUT);
		   }
		 else
		   {
    		     json_object_set_string_member (r, RESPONSE_LINK_OBJ_HREF, json_object_get_string_member (r,REQUEST_LINK_OBJ_HREF));
    		     json_object_remove_member (r, REQUEST_LINK_OBJ_HREF);
		   }
	       }

    	     g_list_free (nodes1);

             json_object_set_member (on_obj, (const gchar *) n->data,
					json_node_copy (json_object_get_member (relationships, (const gchar *) n->data)));
           }
	 }

      g_list_free (nodes);

      json_object_remove_member (on_obj, REQUEST_OBJ_RELATIONSHIPS);
    }

  /* links */

  if (json_object_has_member (on_obj, REQUEST_OBJ_LINKS) == TRUE)
    {
      JsonObject * links = json_object_get_object_member (on_obj, REQUEST_OBJ_LINKS);
      GList *nodes, *n;
      nodes = json_object_get_members (links);
      for (n = nodes; n != NULL; n = n->next)
        {
	  if (!g_strcmp0 ((const gchar *) n->data, "_paging"))
	    continue;

	  if (json_object_has_member (on_obj, (const gchar *) n->data) == FALSE)
	    {
	      JsonArray * rel_array = json_object_get_array_member (links, (const gchar *) n->data);

	      GList *nodes1, *n1;
    	      nodes1 = json_array_get_elements (rel_array);
    	      for (n1 = nodes1; n1 != NULL; n1 = n1->next)
      	        {
		  JsonObject * r = json_node_get_object (n1->data);
    		  json_object_remove_member (r, REQUEST_LINK_OBJ_REV);
    		  json_object_remove_member (r, REQUEST_LINK_OBJ_ID);
    		  json_object_remove_member (r, REQUEST_LINK_OBJ_LABEL);
    		  json_object_set_string_member (r, RESPONSE_LINK_OBJ_HREF, json_object_get_string_member (r,REQUEST_LINK_OBJ_HREF));
    		  json_object_remove_member (r, REQUEST_LINK_OBJ_HREF);

    		  if (json_object_has_member (r, REQUEST_LINK_OBJ_REL) == TRUE)
		    {
    		      json_object_set_string_member (r, RESPONSE_LINK_OBJ_REL, json_object_get_string_member (r,REQUEST_LINK_OBJ_REL));
    		      json_object_remove_member (r, REQUEST_LINK_OBJ_REL);
		    }
		}
              g_list_free (nodes1);

              json_object_set_member (on_obj, (const gchar *) n->data,
					json_node_copy (json_object_get_member (links, (const gchar *) n->data)));
	    }
	}
      g_list_free (nodes);

      json_object_remove_member (on_obj, REQUEST_OBJ_LINKS);
    }

  JsonNode * entry = json_node_new (JSON_NODE_OBJECT);
  JsonObject * entry_obj = json_object_new ();
  json_node_take_object (entry, entry_obj);

  json_object_set_member (entry_obj, "entry", json_node_copy (node_temp));

  if (declining_filtered == TRUE)
    json_object_set_boolean_member (entry_obj, "filtered", FALSE);

  json_node_free (node_temp);

  /* Writing: */

  client->output.string.string = dupin_util_json_serialize (entry);
  client->output_size = strlen(client->output.string.string);

  if (client->output.string.string == NULL)
    goto request_global_get_portable_listings_record_error;

  client->output_mime = g_strdup (HTTP_MIME_PORTABLE_LISTINGS_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (entry != NULL)
    json_node_free (entry);

  dupin_record_close (record);
  dupin_database_unref (db);

  g_free (doc_id);

  return HTTP_STATUS_200;

request_global_get_portable_listings_record_error:

  dupin_record_close (record);
  dupin_database_unref (db);

  g_free (doc_id);

  request_set_error (client, "Cannot get record");

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_portable_listings_record_relationship (DSHttpdClient * client,
							  GList * path,
							  GList * arguments)
{
  DupinLinkB *linkb;

  GList *list;
  GList *results;

  gboolean descending = FALSE;
  guint count = DUPIN_LINKB_MAX_LINKS_COUNT;
  guint offset = 0;
  gsize total_rows = 0;

  gboolean inclusive_end = TRUE;

  if (!path->next->data || !path->next->next)
    {
      request_set_error (client, "Cannot get portable listings relationship");
      return HTTP_STATUS_404;
    }

  gchar * context_id = (gchar *)path->next->next->data;

  /* TODO - check if web links will be expanded as well downwards with includeRelationships=true */

  gchar ** link_rels = NULL;
  DupinFilterByType link_rels_op = DP_FILTERBY_EQUALS;
  gchar ** link_labels = NULL;
  if (path->next->next->next)
    link_labels = g_strsplit ((gchar *)path->next->next->next->data, ",", -1);
  DupinFilterByType link_labels_op = DP_FILTERBY_EQUALS;
  gchar ** link_hrefs = NULL;
  DupinFilterByType link_hrefs_op = DP_FILTERBY_EQUALS;
  gchar ** link_tags = NULL;
  DupinFilterByType link_tags_op = DP_FILTERBY_EQUALS;

  DupinLinksType link_type = DP_LINK_TYPE_RELATIONSHIP;

  gchar * filter_by = NULL;
  DupinFieldsFormatType filter_by_format = DP_FIELDS_FORMAT_DOTTED;
  DupinFilterByType filter_op = DP_FILTERBY_UNDEF;
  gchar * filter_values = NULL;

  gboolean declining_updated_since = FALSE;
  gboolean declining_updated_until = FALSE;
  gboolean declining_filtered = FALSE;
  gboolean declining_sorted = FALSE;

  gboolean include_relationships = FALSE;

  gchar * fields = NULL;

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  gchar * linkbase_name = (gchar *) path->data;

  /* hack for labels */

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, linkbase_name, NULL)))
    {
      request_set_error (client, "Cannot connect to linkabse");
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_SORT_ORDER))
	descending = (!g_strcmp0 (kv->value, "descending")) ? TRUE : FALSE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FIELDS))
        {
          fields = kv->value;
	}

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_INCLUDE_RELATIONSHIPS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              request_set_error (client, "Invalid " REQUEST_GET_PORTABLE_LISTINGS_INCLUDE_RELATIONSHIPS " parameter. Allowed values are: true, false");
              return HTTP_STATUS_400;
            }

          include_relationships = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }


      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_COUNT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_START_INDEX))
	offset = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_UPDATED_SINCE))
        {
	  /* TODO - unimplemented */
          declining_updated_since = TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_UPDATED_UNTIL))
        {
	  /* TODO - unimplemented */
          declining_updated_until = TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FILTER_OBJECT_TYPE))
        {
	  /* TODO - unimplemented */
          declining_filtered = TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FILTER_BY))
        {
	  /* TODO - unimplemented */
          declining_filtered = TRUE;
        }
 
      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP))
        {
	  /* TODO - unimplemented */
          declining_filtered = TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FILTER_VALUES))
        {
	  /* TODO - unimplemented */
          declining_filtered = TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_SORT_BY))
        {
	  /* TODO - unimplemented */
          declining_sorted = TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_PORTABLE_LISTINGS_FORMAT))
        {
          if (g_strcmp0 (kv->value,REQUEST_GET_PORTABLE_LISTINGS_FORMAT_JSON))
            {
              request_set_error (client, "Invalid " REQUEST_GET_PORTABLE_LISTINGS_FORMAT " parameter. Allowed values are: " REQUEST_GET_PORTABLE_LISTINGS_FORMAT_JSON);
              return HTTP_STATUS_400;
            }
	}
    }

  /* NOTE - hack params / arguments */

  if (fields != NULL)
    {
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE,REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_ALL_LINKS));
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS,fields));

      /* NOTE - include_links_labels=fields but fields removed prefix E.g. contributor.role -> role */

//g_message ("FIELDS: %s\n", fields);

      gchar ** fields_splitted = g_strsplit (fields, ",", -1);

      gint i,j;
      GString *str = g_string_new ("");
      GString *str1 = g_string_new ("");
      gboolean appended=FALSE;

      if (fields_splitted != NULL)
        {
          for (i = 0; fields_splitted[i]; i++)
            {
	      if (appended == TRUE)
		{
	          g_string_append (str, ",");
	          g_string_append (str1, ",");
		}

//g_message ("fields_splitted[%d] = %s\n", i, fields_splitted[i]);

	      if (!g_strcmp0 (fields_splitted[i], ""))
	        continue;

              gchar ** field_parts = g_strsplit (fields_splitted[i], ".", -1);
	      if (field_parts != NULL && field_parts[1])
	        {
	          g_string_append_printf (str1, "%s", field_parts[0]);

                  for (j = 1; field_parts[j]; j++)
                    {
//g_message ("field_parts[%d]= %s\n", j, field_parts[j]);

	              g_string_append_printf (str, "%s", field_parts[j]);
	              if (field_parts[j+1])
	                g_string_append (str, ".");
	            }
	          appended = TRUE;
	        }

	      if (field_parts != NULL)
                g_strfreev (field_parts);
            }
        }
      gchar * tmp = g_string_free (str, FALSE);
      gchar * tmp1 = g_string_free (str1, FALSE);

      if (strlen (tmp) > 0)
        arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_ANY_FILTER_LINK_FIELDS,tmp));

      if (strlen (tmp1) > 0)
        arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_ANY_FILTER_FIELDS,tmp1));

//g_message ("REL FIELDS: %s\n", tmp);
//g_message ("RELS: %s\n", tmp1);

      g_free (tmp);
      g_free (tmp1);

      if (fields_splitted != NULL)
        g_strfreev (fields_splitted);
    }

  if (include_relationships == TRUE)
    {
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS,REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_OUT));
      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_LEVEL,"1"));

      arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE,REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_ALL_LINKS));
    }

  /* hack to naviagte second level */

  arguments = g_list_append (arguments, dp_keyvalue_new (REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS,REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_OUT));

  /* NOTE - try to optimize here */
  if (context_id != NULL
      || link_labels != NULL
      || filter_by != NULL)
    total_rows = dupin_link_record_get_list_total (linkb, 0, 0, link_type, NULL, NULL, inclusive_end, DP_COUNT_EXIST, 
						   context_id, link_rels, link_rels_op,
						   link_labels, link_labels_op, link_hrefs, link_hrefs_op, link_tags, link_tags_op,
						   filter_by, filter_by_format, filter_op, filter_values);
  else
    total_rows = dupin_linkbase_count (linkb, link_type, DP_COUNT_EXIST);


  if (dupin_link_record_get_list (linkb, count, offset, 0, 0, link_type, NULL, NULL, inclusive_end, DP_COUNT_EXIST, DP_ORDERBY_ID, descending, 
				  context_id, link_rels, link_rels_op, link_labels, link_labels_op,
				  link_hrefs, link_hrefs_op, link_tags, link_tags_op,
				  filter_by, filter_by_format, filter_op, filter_values, &results, NULL) == FALSE)
    {
      if (link_labels)
        g_strfreev (link_labels);

      dupin_linkbase_unref (linkb);
      request_set_error (client, "Cannot get list of links from linkabse");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_link_record_get_list_close (results);

      if (link_labels)
        g_strfreev (link_labels);

      dupin_linkbase_unref (linkb);
      request_set_error (client, "Cannot get list of links from linkabse");
      return HTTP_STATUS_500;
    }

  json_object_set_int_member (obj, "totalResults", total_rows);
  json_object_set_int_member (obj, "startIndex", offset);
  json_object_set_int_member (obj, "itemsPerPage", count);

  if (declining_updated_since == TRUE)
    json_object_set_boolean_member (obj, "updatedSince", FALSE);

  if (declining_updated_until == TRUE)
    json_object_set_boolean_member (obj, "updatedUntil", FALSE);

  if (declining_sorted == TRUE)
    json_object_set_boolean_member (obj, "sorted", FALSE);

  if (declining_filtered == TRUE)
    json_object_set_boolean_member (obj, "filtered", FALSE);

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_portable_listings_record_relationship_error;

  for (list = results; list; list = list->next)
    {
      DupinLinkRecord *record = list->data;

      JsonNode *temp_node;
      if (!  (temp_node = request_link_record_revision_obj (client, arguments,
					     record, (gchar *) dupin_link_record_get_id (record),
			       		     dupin_link_record_get_last_revision (record),
					     TRUE)))
        {
	  json_array_unref (array);
	  goto request_global_get_portable_listings_record_relationship_error;
        }

      if (json_object_has_member (json_node_get_object (temp_node), RESPONSE_LINK_OBJ_DOC_OUT) == FALSE)
        continue;

      JsonNode *on = json_node_copy (json_object_get_member (json_node_get_object (temp_node), RESPONSE_LINK_OBJ_DOC_OUT));

      json_node_free (temp_node);

      /* NOTE - lift record to suit portable listings format */

      JsonObject * on_obj = json_node_get_object (on);

      json_object_remove_member (on_obj, REQUEST_OBJ_ATTACHMENTS);
      json_object_remove_member (on_obj, REQUEST_OBJ_REV);

      json_object_set_string_member (on_obj, RESPONSE_OBJ_ID, json_object_get_string_member (on_obj,REQUEST_OBJ_ID));
      json_object_remove_member (on_obj, REQUEST_OBJ_ID);

      if (json_object_has_member (on_obj, "updated") == FALSE
 	  && json_object_has_member (on_obj, "_created") == TRUE)
        {
          json_object_set_string_member (on_obj, "updated", json_object_get_string_member (on_obj,"_created"));
          json_object_remove_member (on_obj, "_created");
        }

      if (json_object_has_member (on_obj, "objectType") == FALSE
          && json_object_has_member (on_obj, REQUEST_OBJ_TYPE) == TRUE)
        {
          json_object_set_string_member (on_obj, "objectType", json_object_get_string_member (on_obj,REQUEST_OBJ_TYPE));
          json_object_remove_member (on_obj, REQUEST_OBJ_TYPE);
        }

      /* relationships */

      if (json_object_has_member (on_obj, REQUEST_OBJ_RELATIONSHIPS) == TRUE)
        {
	  JsonObject * relationships = json_object_get_object_member (on_obj, REQUEST_OBJ_RELATIONSHIPS);
	  GList *nodes, *n;
          nodes = json_object_get_members (relationships);
          for (n = nodes; n != NULL; n = n->next)
	    {
	      if (!g_strcmp0 ((const gchar *) n->data, "_paging"))
		continue;

	      if (json_object_has_member (on_obj, (const gchar *) n->data) == FALSE)
		{
		  JsonArray * rel_array = json_object_get_array_member (relationships, (const gchar *) n->data);

		  GList *nodes1, *n1;
      		  nodes1 = json_array_get_elements (rel_array);
      		  for (n1 = nodes1; n1 != NULL; n1 = n1->next)
        	    {
		      JsonObject * r = json_node_get_object (n1->data);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_REV);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_ID);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_LABEL);

      		      if (json_object_has_member (r, REQUEST_LINK_OBJ_REL) == TRUE)
			{
      		          json_object_set_string_member (r, RESPONSE_LINK_OBJ_REL, json_object_get_string_member (r,REQUEST_LINK_OBJ_REL));
      		          json_object_remove_member (r, REQUEST_LINK_OBJ_REL);
			}

	              if (json_object_has_member (r, RESPONSE_LINK_OBJ_DOC_OUT) == TRUE)
			{
      		          json_object_remove_member (r, REQUEST_LINK_OBJ_HREF);

		          JsonObject * d = json_object_get_object_member (r, RESPONSE_LINK_OBJ_DOC_OUT);

      			  json_object_remove_member (d, REQUEST_OBJ_ATTACHMENTS);
      			  json_object_remove_member (d, REQUEST_OBJ_REV);

      			  json_object_set_string_member (d, RESPONSE_OBJ_ID, json_object_get_string_member (d,REQUEST_OBJ_ID));
      			  json_object_remove_member (d, REQUEST_OBJ_ID);

      			  if (json_object_has_member (d, "updated") == FALSE
			      && json_object_has_member (d, "_created") == TRUE)
         		    {
      			      json_object_set_string_member (d, "updated", json_object_get_string_member (d,"_created"));
      			      json_object_remove_member (d, "_created");
        		    }

      			  if (json_object_has_member (d, "objectType") == FALSE
			      && json_object_has_member (d, REQUEST_OBJ_TYPE) == TRUE)
        		    {
      			      json_object_set_string_member (d, "objectType", json_object_get_string_member (d,REQUEST_OBJ_TYPE));
      			      json_object_remove_member (d, REQUEST_OBJ_TYPE);
        		    }

      			  if (json_object_has_member (d, REQUEST_OBJ_RELATIONSHIPS) == TRUE)
      		            json_object_remove_member (d, REQUEST_OBJ_RELATIONSHIPS);

      			  if (json_object_has_member (d, REQUEST_OBJ_LINKS) == TRUE)
      		            json_object_remove_member (d, REQUEST_OBJ_LINKS);

      		          json_object_set_member (r, "entry", json_node_copy (json_object_get_member (r,RESPONSE_LINK_OBJ_DOC_OUT)));
      		          json_object_remove_member (r, RESPONSE_LINK_OBJ_DOC_OUT);
			}
		      else
			{
      		          json_object_set_string_member (r, RESPONSE_LINK_OBJ_HREF, json_object_get_string_member (r,REQUEST_LINK_OBJ_HREF));
      		          json_object_remove_member (r, REQUEST_LINK_OBJ_HREF);
		        }
		    }
      		  g_list_free (nodes1);

                  json_object_set_member (on_obj, (const gchar *) n->data,
					json_node_copy (json_object_get_member (relationships, (const gchar *) n->data)));
		}
	    }
          g_list_free (nodes);

      	  json_object_remove_member (on_obj, REQUEST_OBJ_RELATIONSHIPS);
	}

      /* links */

      if (json_object_has_member (on_obj, REQUEST_OBJ_LINKS) == TRUE)
        {
	  JsonObject * links = json_object_get_object_member (on_obj, REQUEST_OBJ_LINKS);
	  GList *nodes, *n;
          nodes = json_object_get_members (links);
          for (n = nodes; n != NULL; n = n->next)
	    {
	      if (!g_strcmp0 ((const gchar *) n->data, "_paging"))
		continue;

	      if (json_object_has_member (on_obj, (const gchar *) n->data) == FALSE)
		{
		  JsonArray * rel_array = json_object_get_array_member (links, (const gchar *) n->data);

		  GList *nodes1, *n1;
      		  nodes1 = json_array_get_elements (rel_array);
      		  for (n1 = nodes1; n1 != NULL; n1 = n1->next)
        	    {
		      JsonObject * r = json_node_get_object (n1->data);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_REV);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_ID);
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_LABEL);
      		      json_object_set_string_member (r, RESPONSE_LINK_OBJ_HREF, json_object_get_string_member (r,REQUEST_LINK_OBJ_HREF));
      		      json_object_remove_member (r, REQUEST_LINK_OBJ_HREF);

      		      if (json_object_has_member (r, REQUEST_LINK_OBJ_REL) == TRUE)
			{
      		          json_object_set_string_member (r, RESPONSE_LINK_OBJ_REL, json_object_get_string_member (r,REQUEST_LINK_OBJ_REL));
      		          json_object_remove_member (r, REQUEST_LINK_OBJ_REL);
			}
		    }
      		  g_list_free (nodes1);

                  json_object_set_member (on_obj, (const gchar *) n->data,
					json_node_copy (json_object_get_member (links, (const gchar *) n->data)));
		}
	    }
          g_list_free (nodes);

      	  json_object_remove_member (on_obj, REQUEST_OBJ_LINKS);
	}

      json_array_add_element( array, on);
    }

  json_object_set_array_member (obj, "entry", array );

  client->output_mime = g_strdup (HTTP_MIME_PORTABLE_LISTINGS_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_portable_listings_record_relationship_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_portable_listings_record_relationship_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_portable_listings_record_relationship_error;

  if( results )
    dupin_link_record_get_list_close (results);

  dupin_linkbase_unref (linkb);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (link_labels)
    g_strfreev (link_labels);

  return HTTP_STATUS_200;

request_global_get_portable_listings_record_relationship_error:

  if( results )
    dupin_link_record_get_list_close (results);

  dupin_linkbase_unref (linkb);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  if (link_labels)
    g_strfreev (link_labels);

  request_set_error (client, "Cannot get list of links from linkabse");

  return HTTP_STATUS_500;
}

/* EOF */
