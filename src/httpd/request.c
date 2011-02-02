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

#define REQUEST_OBJ_ID			"_id"
#define REQUEST_OBJ_REV			"_rev"
#define REQUEST_OBJ_ATTACHMENTS		"_attachments"
#define REQUEST_OBJ_LINKS		"_links"
#define REQUEST_OBJ_RELATIONSHIPS	"_relationships"
#define REQUEST_OBJ_CONTENT		"_content"
#define REQUEST_OBJ_DELETED		"_deleted"

#define REQUEST_LINK_OBJ_CONTEXT_ID	"_context_id"
#define REQUEST_LINK_OBJ_HREF		"_href"
#define REQUEST_LINK_OBJ_REL		"_rel"
#define REQUEST_LINK_OBJ_TAG		"_tag"
#define REQUEST_LINK_OBJ_LABEL		"_label"

#define REQUEST_OBJ_INLINE_ATTACHMENTS_STUB	"stub"
#define REQUEST_OBJ_INLINE_ATTACHMENTS_DATA	"data"
#define REQUEST_OBJ_INLINE_ATTACHMENTS_TYPE	"content_type"

#define RESPONSE_OBJ_ID			"id"
#define RESPONSE_OBJ_REV		"rev"
#define RESPONSE_OBJ_ATTACHMENTS	REQUEST_OBJ_ATTACHMENTS
#define RESPONSE_OBJ_LINKS		REQUEST_OBJ_LINKS
#define RESPONSE_OBJ_RELATIONSHIPS	REQUEST_OBJ_RELATIONSHIPS
#define RESPONSE_OBJ_CONTENT		REQUEST_OBJ_CONTENT
#define RESPONSE_OBJ_DELETED		REQUEST_OBJ_DELETED
#define RESPONSE_OBJ_LINKS_PAGING	"_paging"

#define RESPONSE_LINK_OBJ_ID		RESPONSE_OBJ_ID
#define RESPONSE_LINK_OBJ_REV		RESPONSE_OBJ_REV
#define RESPONSE_LINK_OBJ_CONTEXT_ID	REQUEST_LINK_OBJ_CONTEXT_ID
#define RESPONSE_LINK_OBJ_HREF		"href"
#define RESPONSE_LINK_OBJ_REL		"rel"
#define RESPONSE_LINK_OBJ_TAG		"tag"
#define RESPONSE_LINK_OBJ_LABEL		REQUEST_LINK_OBJ_LABEL

#define DUPIN_DB_MAX_DOCS_COUNT     50
#define DUPIN_LINKB_MAX_LINKS_COUNT 50
#define DUPIN_VIEW_MAX_DOCS_COUNT   50
#define DUPIN_ATTACHMENTS_COUNT	    100
#define DUPIN_REVISIONS_COUNT	    100
#define DUPIN_DB_MAX_CHANGES_COUNT  100

static JsonNode *request_record_revision_obj (DSHttpdClient * client,
					      GList * arguments,
					      DupinRecord * record, gchar * id,
					      gchar * mvcc);

static JsonNode *request_link_record_revision_obj (DSHttpdClient * client,
						   GList * arguments,
						   DupinLinkRecord * record, gchar * id,
						   gchar * mvcc);

static JsonNode *request_view_record_obj (DSHttpdClient * client,
					  GList * arguments,
					  DupinViewRecord * record,
				 	  gchar * id);

static gboolean request_record_insert (DSHttpdClient * client,
				       GList * arguments,
				       JsonNode * obj_node, gchar * dbname,
				       gchar * id, DSHttpStatusCode * code,
		       		       GList ** response_list);

static gboolean request_link_record_insert (DSHttpdClient * client,
					    GList * arguments,
				            JsonNode * obj_node, gchar * dbname,
				            gchar * id, gchar * context_id, DSHttpStatusCode * code,
					    DupinLinksType link_type,
		       			    GList ** response_list);

/* TODO - check, bug ? shouldn't be DupinAttachmentRecord ? 
          at the moment it is a special case due we need to access DupinRecord revision
	  information when adding/updating the attachment - we just been lazy? */

static gboolean request_record_attachment_insert (DSHttpdClient * client,
						  GList * arguments,
				                  gchar * dbname, gchar * id,
						  GList * title_parts,
						  DSHttpStatusCode * code,
		       			          GList ** response_list);

#define	RESPONSE_STATUS_OK			"ok"
#define	RESPONSE_STATUS_ERROR			"error"
#define	RESPONSE_STATUS_REASON			"reason"

static gboolean request_record_response (DSHttpdClient * client,
					 GList * response_list);

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
          client->dupin_error_msg = g_strdup ("www path does not exist or is invalid");
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
              client->dupin_error_msg = g_strdup ("www path is not '..'");
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
              client->dupin_error_msg = g_strdup ("www path does not exist");
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
              client->dupin_error_msg = g_strdup ("www path does not exist or is invalid");
	      g_free (newpath);
	      return HTTP_STATUS_403;
	    }

	  path = newpath;
	}
    }

  if (g_stat (path, &st) != 0)
    {
      client->dupin_error_msg = g_strdup ("www path does match");
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
          client->dupin_error_msg = g_strdup ("cannot open www path for reading");
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
      client->dupin_error_msg = g_strdup ("Quit command forbidden");
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
      client->dupin_error_msg = g_strdup ("Cannot return status");
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

  g_source_get_current_time (client->channel_source, &tv);

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
  json_object_set_string_member (nobj, "interface", client->thread->data->httpd_interface);

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
  client->dupin_error_msg = g_strdup ("Cannot return status");
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

  client->dupin_error_msg = g_strdup ("Allowed methods: GET, POST, PUT or DELETE");

  return HTTP_STATUS_400;
}

/* GET *********************************************************************/
#define REQUEST_ALL_DBS		"_all_dbs"
#define REQUEST_ALL_LINKBS	"_all_linkbs"
#define REQUEST_ALL_ATTACH_DBS	"_all_attachment_dbs"
#define REQUEST_ALL_VIEWS	"_all_views"
#define REQUEST_ALL_CHANGES	"_changes"
#define REQUEST_ALL_DOCS	"_all_docs"
#define REQUEST_ATTACH_DBS	"_attach_dbs"
#define REQUEST_ALL_LINKS	"_all_links"
#define REQUEST_LINKBS		"_linkbs"
#define REQUEST_VIEWS		"_views"
#define REQUEST_SYNC		"_sync"
#define REQUEST_UUIDS		"_uuids"
#define REQUEST_UUIDS_COUNT	"count"

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

      client->dupin_error_msg = g_strdup ("Linkbases GET allowed commands: /_linkbs/linkbase, /_linkbs/linkbase/_all_links, /_linkbs/linkbase/_changes or /_linkbs/linkbase/id");

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

      client->dupin_error_msg = g_strdup ("Views GET allowed commands: /_views/view/_all_docs, /_views/view/_sync or /_views/view/id");

      return HTTP_STATUS_400;
    }

  /* GET /database/id */
  return request_global_get_record (client, path, arguments);

  client->dupin_error_msg = g_strdup ("Record GET allowed commands: /database/id");

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
      client->dupin_error_msg = g_strdup ("Cannot return UUIDs");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot return UUIDs");
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
  client->dupin_error_msg = g_strdup ("Cannot return UUIDs");
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
      client->dupin_error_msg = g_strdup ("Database /_all_dbs allows only GET method");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot return list of databases");
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
  client->dupin_error_msg = g_strdup ("Cannot return list of databases");
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
      client->dupin_error_msg = g_strdup ("Linkabse /_all_linkbs allows only GET method");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot return list of linkbases");
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
  client->dupin_error_msg = g_strdup ("Cannot return list of linkbases");
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
      client->dupin_error_msg = g_strdup ("Linkabse /_all_linkbs allows only GET method");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot return list of attachment databases");
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
  client->dupin_error_msg = g_strdup ("Cannot return list of attachment databases");
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
      client->dupin_error_msg = g_strdup ("Views /_all_views allows only GET method");
      return HTTP_STATUS_400;
    }

  array = json_array_new ();

  if (array == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot return list of views");
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
  client->dupin_error_msg = g_strdup ("Cannot return list of views");
  return HTTP_STATUS_500;
}

#define REQUEST_GET_ALL_DOCS_DESCENDING	   "descending"
#define REQUEST_GET_ALL_DOCS_COUNT	   "count"
#define REQUEST_GET_ALL_DOCS_OFFSET	   "offset"
#define REQUEST_GET_ALL_DOCS_KEY	   "key"
#define REQUEST_GET_ALL_DOCS_STARTKEY	   "startkey"
#define REQUEST_GET_ALL_DOCS_ENDKEY	   "endkey"
#define REQUEST_GET_ALL_DOCS_INCLUSIVEEND  "inclusive_end"
#define REQUEST_GET_ALL_DOCS_INCLUDE_DOCS  "include_docs"

#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE			"include_links"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TAG			"include_links_tag"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_ALL_LINKS	REQUEST_GET_ALL_LINKS_LINK_TYPE_ALL_LINKS
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_WEBLINKS	REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_RELATIONSHIPS	REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS		"include_links_labels"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_DESCENDING	"include_links_weblinks_descending"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_COUNT	"include_links_weblinks_count"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_OFFSET	"include_links_weblinks_offset"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_DESCENDING	"include_links_relationships_descending"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_COUNT		"include_links_relationships_count"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_OFFSET		"include_links_relationships_offset"

#define REQUEST_GET_ALL_LINKS_DESCENDING   	REQUEST_GET_ALL_DOCS_DESCENDING
#define REQUEST_GET_ALL_LINKS_COUNT	   	REQUEST_GET_ALL_DOCS_COUNT
#define REQUEST_GET_ALL_LINKS_OFFSET	   	REQUEST_GET_ALL_DOCS_OFFSET
#define REQUEST_GET_ALL_LINKS_KEY	   	REQUEST_GET_ALL_DOCS_KEY
#define REQUEST_GET_ALL_LINKS_STARTKEY	   	REQUEST_GET_ALL_DOCS_STARTKEY
#define REQUEST_GET_ALL_LINKS_ENDKEY	   	REQUEST_GET_ALL_DOCS_ENDKEY
#define REQUEST_GET_ALL_LINKS_INCLUSIVEEND 	REQUEST_GET_ALL_DOCS_INCLUSIVEEND

#define REQUEST_GET_ALL_LINKS_LINKBASE			"linkbase"

#define REQUEST_GET_ALL_LINKS_CONTEXT_ID 		"context_id"
#define REQUEST_GET_ALL_LINKS_TAG	 		"tag"
#define REQUEST_GET_ALL_LINKS_LABELS	 		"labels"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE			"link_type"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE_ALL_LINKS	"all_links"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS	"web_links"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS	"relationships"

#define REQUEST_GET_ALL_CHANGES_SINCE	      "since"
#define REQUEST_GET_ALL_CHANGES_STYLE	      "style"
#define REQUEST_GET_ALL_CHANGES_FEED	      "feed"
#define REQUEST_GET_ALL_CHANGES_INCLUDE_DOCS  REQUEST_GET_ALL_DOCS_INCLUDE_DOCS
#define REQUEST_GET_ALL_CHANGES_HEARTBEAT     "heartbeat"
#define REQUEST_GET_ALL_CHANGES_TIMEOUT       "timeout"
#define REQUEST_GET_ALL_CHANGES_INCLUDE_LINKS "include_links"
#define REQUEST_GET_ALL_CHANGES_CONTEXT_ID    REQUEST_GET_ALL_LINKS_CONTEXT_ID
#define REQUEST_GET_ALL_CHANGES_TAG	      REQUEST_GET_ALL_LINKS_TAG

#define REQUEST_GET_ALL_CHANGES_HEARTBEAT_DEFAULT  30000
#define REQUEST_GET_ALL_CHANGES_TIMEOUT_DEFAULT    60000

#define REQUEST_GET_ALL_CHANGES_STYLE_DEFAULT	   "main_only"

#define REQUEST_GET_ALL_CHANGES_STYLE_ALL_LINKS	   	REQUEST_GET_ALL_LINKS_LINK_TYPE_ALL_LINKS
#define REQUEST_GET_ALL_CHANGES_STYLE_WEBLINKS	   	REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS
#define REQUEST_GET_ALL_CHANGES_STYLE_RELATIONSHIPS	REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS

#define REQUEST_GET_ALL_CHANGES_FEED_POLL	"poll"
#define REQUEST_GET_ALL_CHANGES_FEED_LONGPOLL	"longpoll"
#define REQUEST_GET_ALL_CHANGES_FEED_CONTINUOUS	"continuous"

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
  gsize since = 0;
  DupinChangesType style = DP_CHANGES_MAIN_ONLY;
  DupinChangesFeedType feed = DP_CHANGES_FEED_POLL;
  gboolean include_docs = FALSE;

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

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_COUNT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_SINCE))
	since = (gsize)atof (kv->value);

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
              client->dupin_error_msg = g_strdup ("Invalid " REQUEST_GET_ALL_CHANGES_FEED " parameter. Allowed values are: " REQUEST_GET_ALL_CHANGES_FEED_LONGPOLL ", " REQUEST_GET_ALL_CHANGES_FEED_CONTINUOUS ", " REQUEST_GET_ALL_CHANGES_FEED_POLL);
              return HTTP_STATUS_400;
            }
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_INCLUDE_DOCS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              client->dupin_error_msg = g_strdup ("Invalid " REQUEST_GET_ALL_CHANGES_INCLUDE_DOCS " parameter. Allowed values are: true, false");
              return HTTP_STATUS_400;
            }

          include_docs = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }
    }

  if (feed == DP_CHANGES_FEED_LONGPOLL
      || feed == DP_CHANGES_FEED_CONTINUOUS)
    {
      if (!  (client->output.changes_comet.db =
			dupin_database_open (client->thread->data->dupin, path->data, NULL)))
        {
          client->dupin_error_msg = g_strdup ("Cannot connect to changes database");
          return HTTP_STATUS_404;
        }

      if (dupin_database_get_max_rowid (client->output.changes_comet.db, &client->output.changes_comet.change_max_rowid) == FALSE)
        {
          dupin_database_unref (client->output.changes_comet.db);
          client->dupin_error_msg = g_strdup ("Cannot get last seq number from changes database");
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
      client->output.changes_comet.param_since = since;
      client->output.changes_comet.param_feed = feed;
      client->output.changes_comet.param_include_docs = include_docs;
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
      client->dupin_error_msg = g_strdup ("Cannot connect to changes database");
      return HTTP_STATUS_404;
    }

  if (dupin_database_get_total_changes (db, &total_rows, since+1, 0, DP_COUNT_CHANGES, TRUE, NULL) == FALSE)
    {
      dupin_database_unref (db);
      client->dupin_error_msg = g_strdup ("Cannot get last seq number from changes database");
      return HTTP_STATUS_500;
    }

  if (dupin_database_get_changes_list (db, count, 0, since+1, 0, style, DP_COUNT_CHANGES, DP_ORDERBY_ROWID, descending, &results, NULL) ==
      FALSE)
    {
      dupin_database_unref (db);
      client->dupin_error_msg = g_strdup ("Cannot list of changes from database");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_database_get_changes_list_close (results);
      dupin_database_unref (db);
      client->dupin_error_msg = g_strdup ("Cannot list of changes from database");
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
						    db_record, record_id, record_mvcc)))
            {
              json_node_free (change);
              json_array_unref (array);
              goto request_global_get_changes_database_error;
            }

          dupin_record_close (db_record);

          json_object_set_member (on_obj, "doc", doc);
        }

      json_array_add_element (array, change);
    }

  json_object_set_array_member (obj, "results", array );

  if (dupin_database_get_max_rowid (db, &last_seq) == FALSE)
    {
      goto request_global_get_changes_database_error;
    }

  json_object_set_int_member (obj, "last_seq", last_seq);

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

  client->dupin_error_msg = g_strdup ("Cannot list of changes from database");

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

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      client->dupin_error_msg = g_strdup ("Cannot connect to database");
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_COUNT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_OFFSET))
	offset = atoi (kv->value);
    }

  total_rows = dupin_database_count (db, DP_COUNT_EXIST);

  if (dupin_record_get_list (db, count, offset, 0, 0, DP_COUNT_EXIST, DP_ORDERBY_ROWID, descending, &results, NULL) ==
      FALSE)
    {
      dupin_database_unref (db);
      client->dupin_error_msg = g_strdup ("Cannot list of documents from database");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_record_get_list_close (results);

      dupin_database_unref (db);
      client->dupin_error_msg = g_strdup ("Cannot list of documents from database");
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
      JsonNode *on;

      if (!
	  (on =
	   request_record_revision_obj (client, arguments,
					record, (gchar *) dupin_record_get_id (record),
					dupin_record_get_last_revision (record))))
        {
	  json_array_unref (array); /* if here, array is not under obj responsability yet */
	  goto request_global_get_all_docs_error;
        }

      json_array_add_element( array, on);
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

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  return HTTP_STATUS_200;

request_global_get_all_docs_error:

  if( results )
    dupin_record_get_list_close (results);

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  client->dupin_error_msg = g_strdup ("Cannot list of documents from database");

  return HTTP_STATUS_500;
}

#define REQUEST_QUERY		"_query"

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
      client->dupin_error_msg = g_strdup ("Cannot get server information");
      return HTTP_STATUS_500;
    }

  json_object_set_string_member (obj, "couchdb", "Welcome to Dupin");
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

  client->dupin_error_msg = g_strdup ("Cannot get server information");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to database");
      return HTTP_STATUS_404;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      dupin_database_unref (db);
      client->dupin_error_msg = g_strdup ("Cannot read database information");
      return HTTP_STATUS_500;
    }

  json_object_set_string_member (obj, "db_name", (gchar *) dupin_database_get_name (db));
  json_object_set_int_member (obj, "doc_count", dupin_database_count (db, DP_COUNT_EXIST));
  json_object_set_int_member (obj, "doc_del_count", dupin_database_count (db, DP_COUNT_DELETE));

#ifdef COUCHDB_STRICT
  /* FIXME: not really a lot of documentation about this stuff (update_seq)... - see also
     http://guide.couchdb.org/draft/replication.html and http://ayende.com/Blog/archive/2008/10/04/erlang-reading-couchdb-digging-down-to-disk.aspx
     and https://issues.apache.org/jira/browse/COUCHDB-576?page=com.atlassian.jira.plugin.system.issuetabpanels:all-tabpanel */
  json_object_set_int_member (obj, "update_seq", 0);
#endif

  json_object_set_int_member (obj, "disk_size", dupin_database_get_size (db));

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

  client->dupin_error_msg = g_strdup ("Cannot read database information");

  return HTTP_STATUS_500;
}

#define REQUEST_RECORD_ARG_REV	"rev"
#define REQUEST_RECORD_ARG_REVS	"revs"

#define REQUEST_RECORD_ARG_LINKS_TAG	"links_tag"

#define REQUEST_FIELDS		"_fields"

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
              /* GET _special_document/document_ID/_attachments/attachment  */
              if (!g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS))
                {
                  if (path->next->next->next->next)
                    title_parts = path->next->next->next->next;
                }

              /* GET _special_document/document_ID/_fields/field */
              else if (!g_strcmp0 (path->next->next->next->data, REQUEST_FIELDS))
                {
                  if (!path->next->next->next->next
                       || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ATTACHMENTS))
                    {
                      client->dupin_error_msg = g_strdup ("Cannot GET record field");
                      return HTTP_STATUS_400;
                    }

                  request_fields=(gchar *)path->next->next->next->next->data;
                }
              else
                {
                  client->dupin_error_msg = g_strdup ("Cannot GET record");
                  return HTTP_STATUS_400;
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
                  client->dupin_error_msg = g_strdup ("Cannot GET record field");
                  return HTTP_STATUS_400;
                }

              request_fields=(gchar *)path->next->next->next->data;
            }

          /* GET document_ID/_attachments/attachment */
          else if (path->next->next->next
                   && (!g_strcmp0 (path->next->next->data, REQUEST_OBJ_ATTACHMENTS)))
            {
              /* GET /document_ID/_attachments/attachment */
              title_parts = path->next->next->next;
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
          else
            {
              client->dupin_error_msg = g_strdup ("Cannot GET record");
              return HTTP_STATUS_400;
            }
        }

      /* GET document_ID */
      doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
    }

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, dbname, NULL)))
    {
      g_free (doc_id);
      client->dupin_error_msg = g_strdup ("Cannot connect to database");
      return HTTP_STATUS_404;
    }

  if (!  (attachment_db =
                dupin_attachment_db_open (client->thread->data->dupin, dbname, NULL)))
    {
      dupin_database_unref (db);
      g_free (doc_id);
      client->dupin_error_msg = g_strdup ("Cannot connect to attachments database");
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
          dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          client->dupin_error_msg = g_strdup ("Cannot get valid attachment title");
          return HTTP_STATUS_400;
        }

//g_message("request_global_get_record: title=%s\n", title);

      if ( dupin_attachment_record_exists (attachment_db, doc_id, title) == FALSE)
        {
          g_free (title);
          dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          client->dupin_error_msg = g_strdup ("Attachment does not exist");
          return HTTP_STATUS_404;
        }

      if ( (!(client->output.blob.record = dupin_attachment_record_read (attachment_db,
							       doc_id, title,
							       NULL)))
	  || (dupin_attachment_record_blob_open (client->output.blob.record) == FALSE))
        {
          g_free (title);
          dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          client->dupin_error_msg = g_strdup ("Cannot read attachment from database");
          return HTTP_STATUS_500;
        }
      
      client->output_type = DS_HTTPD_OUTPUT_BLOB;
      client->output_mime = g_strdup (dupin_attachment_record_get_type (client->output.blob.record));
      client->output_size = dupin_attachment_record_get_length (client->output.blob.record);

      g_free (title);
      dupin_database_unref (db);
      dupin_attachment_db_unref (attachment_db);

      g_free (doc_id);

      return HTTP_STATUS_200;
    }

  if (!(record = dupin_record_read (db, doc_id, NULL)))
    {
      dupin_attachment_db_unref (attachment_db);
      dupin_database_unref (db);
      g_free (doc_id);
      client->dupin_error_msg = g_strdup ("Cannot read record from database");
      return HTTP_STATUS_404;
    }

  if (dupin_record_is_deleted (record, NULL) == TRUE)
    {
      dupin_attachment_db_unref (attachment_db);
      dupin_database_unref (db);
      g_free (doc_id);
      client->dupin_error_msg = g_strdup ("Record is deleted");
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
              dupin_attachment_db_unref (attachment_db);
              dupin_database_unref (db);
              g_free (doc_id);
              client->dupin_error_msg = g_strdup ("Invalid record MVCC revision");
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

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_COUNT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_OFFSET))
	offset = atoi (kv->value);

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
          client->dupin_error_msg = g_strdup ("Cannot get list of record revisions");
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
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          client->dupin_error_msg = g_strdup ("Cannot get list of record revisions");
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
					    (gchar *) list->data)))
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
	          json_array_unref (array);
	          dupin_record_close (record);
	          dupin_database_unref (db);
                  dupin_attachment_db_unref (attachment_db);
                  g_free (doc_id);
                  client->dupin_error_msg = g_strdup ("Cannot get field for record revisions list");
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
          client->dupin_error_msg = g_strdup ("Requested record MVCC revision newer that latest revision");
	  return HTTP_STATUS_404;
	}

      if (!
	  (node_temp =
	   request_record_revision_obj (client, arguments,
					record, (gchar *) dupin_record_get_id (record),
					mvcc)))
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
          client->dupin_error_msg = g_strdup ("Cannot get record revision");
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
              client->dupin_error_msg = g_strdup ("Cannot get record revision field");
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

      if (dupin_attachment_record_get_list (attachment_db, DUPIN_ATTACHMENTS_COUNT,
					    0, 1, 0, DP_ORDERBY_TITLE, FALSE,
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
          client->dupin_error_msg = g_strdup ("Cannot get record list of attachments");
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
              client->dupin_error_msg = g_strdup ("Cannot get record attachment");
	      return HTTP_STATUS_500;
	    }

          json_object_set_member (attachments_obj, dupin_attachment_record_get_title (list->data), attachment_node);
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

  client->dupin_error_msg = g_strdup ("Cannot get record");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to linkabse");
      return HTTP_STATUS_404;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      dupin_linkbase_unref (linkb);
      client->dupin_error_msg = g_strdup ("Cannot get linkabse information");
      return HTTP_STATUS_500;
    }

  json_object_set_string_member (obj, "linkbase_name", (gchar *) dupin_linkbase_get_name (linkb));
  json_object_set_string_member (obj, "linkbase_parent", (gchar *) dupin_linkbase_get_parent (linkb));
  json_object_set_int_member (obj, "links_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_ANY, DP_COUNT_EXIST, NULL, NULL, NULL));
  json_object_set_int_member (obj, "web_links_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_WEB_LINK, DP_COUNT_EXIST, NULL, NULL, NULL));
  json_object_set_int_member (obj, "relationships_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_RELATIONSHIP, DP_COUNT_EXIST, NULL, NULL, NULL));
  json_object_set_int_member (obj, "links_del_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_ANY, DP_COUNT_DELETE, NULL, NULL, NULL));
  json_object_set_int_member (obj, "web_links_del_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_WEB_LINK, DP_COUNT_DELETE, NULL, NULL, NULL));
  json_object_set_int_member (obj, "relationships_del_count", dupin_linkbase_count (linkb, DP_LINK_TYPE_RELATIONSHIP, DP_COUNT_DELETE, NULL, NULL, NULL));

  json_object_set_int_member (obj, "disk_size", dupin_linkbase_get_size (linkb));

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

  client->dupin_error_msg = g_strdup ("Cannot get linkabse information");

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

  gchar * context_id = NULL;
  gchar ** link_labels = NULL;
  gchar * tag = NULL;
  DupinLinksType link_type = DP_LINK_TYPE_ANY;

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  gchar * linkbase_name = path->data;

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

//g_message("request_global_get_all_links_linkbase: k=%s v=%s\n", (gchar *)kv->key, (gchar *)kv->value);

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_COUNT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_OFFSET))
	offset = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_CONTEXT_ID))
        context_id = kv->value;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LABELS))
        {
          link_labels = g_strsplit (kv->value, ",", -1);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_TAG))
        tag = kv->value;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LINKBASE))
        linkbase_name = kv->value;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_LINK_TYPE))
        {
          if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS))
            link_type = DP_LINK_TYPE_WEB_LINK;
          else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS))
            link_type = DP_LINK_TYPE_RELATIONSHIP;
        }

    }

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, linkbase_name, NULL)))
    {
      if (link_labels)
        g_strfreev (link_labels);
      client->dupin_error_msg = g_strdup ("Cannot connect to linkabse");
      return HTTP_STATUS_404;
    }

  total_rows = dupin_linkbase_count (linkb, link_type, DP_COUNT_EXIST, context_id, link_labels, tag);

  if (dupin_link_record_get_list (linkb, count, offset, 0, 0, link_type, DP_COUNT_EXIST, DP_ORDERBY_ROWID, descending, 
					context_id, link_labels, tag, &results, NULL) ==
      FALSE)
    {
      if (link_labels)
        g_strfreev (link_labels);
      dupin_linkbase_unref (linkb);
      client->dupin_error_msg = g_strdup ("Cannot iget list of links from linkabse");
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
      client->dupin_error_msg = g_strdup ("Cannot get list of links from linkabse");
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
      gchar * escaped_base = g_uri_escape_string (linkbase_name, NULL, TRUE);
      GString * str = g_string_new (NULL);
      g_string_append_printf (str, "/%s/", escaped_base);
      g_free (escaped_base);
      gchar * tmp = g_string_free (str, FALSE);
      json_object_set_string_member (obj, "base", tmp);
      g_free (tmp);
    }

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_all_docs_error;

  for (list = results; list; list = list->next)
    {
      DupinLinkRecord *record = list->data;
      JsonNode *on;

      if (!
	  (on =
	   request_link_record_revision_obj (client, arguments,
					     record, (gchar *) dupin_link_record_get_id (record),
			       		     dupin_link_record_get_last_revision (record))))
        {
	  json_array_unref (array); /* if here, array is not under obj responsability yet */
	  goto request_global_get_all_docs_error;
        }

      json_array_add_element( array, on);
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

request_global_get_all_docs_error:

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

  client->dupin_error_msg = g_strdup ("Cannot get list of links from linkabse");

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
  gchar * tag = NULL;

  gsize total_rows = 0;

  gsize last_seq=0;
  gsize since = 0;
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

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_COUNT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_SINCE))
	since = (gsize)atof (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_HEARTBEAT))
	heartbeat = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_TIMEOUT))
	timeout = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_CONTEXT_ID))
	context_id = kv->value;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_TAG))
	tag = kv->value;

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
              client->dupin_error_msg = g_strdup ("Invalid " REQUEST_GET_ALL_CHANGES_FEED " parameter. Allowed values are: " REQUEST_GET_ALL_CHANGES_FEED_LONGPOLL ", " REQUEST_GET_ALL_CHANGES_FEED_CONTINUOUS ", " REQUEST_GET_ALL_CHANGES_FEED_POLL);
              return HTTP_STATUS_400;
            }
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_INCLUDE_LINKS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              client->dupin_error_msg = g_strdup ("Invalid " REQUEST_GET_ALL_CHANGES_INCLUDE_LINKS " parameter. Allowed values are: true, false");
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
          client->dupin_error_msg = g_strdup ("Cannot connect to changes linkbase");
          return HTTP_STATUS_404;
        }

      if (dupin_linkbase_get_max_rowid (client->output.changes_comet.linkb, &client->output.changes_comet.change_max_rowid) == FALSE)
        {
          dupin_linkbase_unref (client->output.changes_comet.linkb);
          client->dupin_error_msg = g_strdup ("Cannot get last seq number from changes linkbase");
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
      client->output.changes_comet.param_since = since;
      client->output.changes_comet.param_feed = feed;
      client->output.changes_comet.param_include_links = include_links;
      client->output.changes_comet.param_context_id = context_id;
      client->output.changes_comet.param_tag = tag;
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
      client->dupin_error_msg = g_strdup ("Cannot connect to changes linkbase");
      return HTTP_STATUS_404;
    }

  if (dupin_linkbase_get_total_changes (linkb, &total_rows, since+1, 0, style, DP_COUNT_CHANGES, TRUE, context_id, tag, NULL) == FALSE)
    {
      dupin_linkbase_unref (linkb);
      client->dupin_error_msg = g_strdup ("Cannot get last seq number from changes linkbase");
      return HTTP_STATUS_500;
    }

  if (dupin_linkbase_get_changes_list (linkb, count, 0, since+1, 0, style, DP_COUNT_CHANGES, DP_ORDERBY_ROWID, descending, context_id, tag, &results, NULL) ==
      FALSE)
    {
      dupin_linkbase_unref (linkb);
      client->dupin_error_msg = g_strdup ("Cannot list of changes from linkbase");
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_linkbase_get_changes_list_close (results);
      dupin_linkbase_unref (linkb);
      client->dupin_error_msg = g_strdup ("Cannot list of changes from linkbase");
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
							  link_record, record_id, record_mvcc)))
            {
              json_node_free (change);
              json_array_unref (array);
              goto request_global_get_changes_linkbase_error;
            }

          dupin_link_record_close (link_record);

          json_object_set_member (on_obj, "link", link);
        }

      json_array_add_element (array, change);
    }

  json_object_set_array_member (obj, "results", array );

  if (dupin_linkbase_get_max_rowid (linkb, &last_seq) == FALSE)
    {
      goto request_global_get_changes_linkbase_error;
    }

  json_object_set_int_member (obj, "last_seq", last_seq);

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

  dupin_linkbase_unref (linkb);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

  client->dupin_error_msg = g_strdup ("Cannot list of changes from linkbase");

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
                      client->dupin_error_msg = g_strdup ("Cannot GET link record field");
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
		  client->dupin_error_msg = g_strdup ("Cannot GET link record field");
                  return HTTP_STATUS_400;
                }

              request_fields=(gchar *)path->next->next->next->data;
            }
        }

      /* GET link_ID */
      link_id = g_strdup_printf ("%s", (gchar *)path->next->data);
    }

//g_message("request_global_get_record_linkbase: link_id=%s request_fields=%s\n", link_id, request_fields);

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
    {
      g_free (link_id);
      client->dupin_error_msg = g_strdup ("Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_link_record_read (linkb, link_id, NULL)))
    {
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      client->dupin_error_msg = g_strdup ("Cannot read link record from linkbase");
      return HTTP_STATUS_404;
    }

  if (dupin_link_record_is_deleted (record, NULL) == TRUE)
    {   
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      client->dupin_error_msg = g_strdup ("Link record is deleted");
      return HTTP_STATUS_404;
    } 

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
              dupin_linkbase_unref (linkb);
              g_free (link_id);
	      client->dupin_error_msg = g_strdup ("Invalid link record MVCC revision");
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

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_COUNT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_LINKS_OFFSET))
	offset = atoi (kv->value);
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
	  client->dupin_error_msg = g_strdup ("Cannot get list of link record revisions");
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
	  client->dupin_error_msg = g_strdup ("Cannot get list of link record revisions");
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
				   		 (gchar *) list->data)))
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
		  client->dupin_error_msg = g_strdup ("Cannot get field for link record revisions list");
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
	  client->dupin_error_msg = g_strdup ("Requested link record MVCC revision newer that latest revision");
	  return HTTP_STATUS_404;
	}

      if (!
	  (node_temp =
	   request_link_record_revision_obj (client, arguments,
					     record, (gchar *) dupin_link_record_get_id (record),
					     mvcc)))
	{
	  dupin_link_record_close (record);
	  dupin_linkbase_unref (linkb);
          g_free (link_id);
	  client->dupin_error_msg = g_strdup ("Cannot get link record revision");
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
	      client->dupin_error_msg = g_strdup ("Cannot get link record revision field");
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

  client->dupin_error_msg = g_strdup ("Cannot get link record");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to view");
      return HTTP_STATUS_404;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      dupin_view_unref (view);
      client->dupin_error_msg = g_strdup ("Cannot get view information");
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
  json_object_set_boolean_member (obj, "sync", dupin_view_is_sync (view));

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

  client->dupin_error_msg = g_strdup ("Cannot get view information");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to view");
      return HTTP_STATUS_404;
    }

  dupin_view_sync (view);

  dupin_view_unref (view);

  return HTTP_STATUS_200;
}

static DSHttpStatusCode
request_global_get_all_docs_view (DSHttpdClient * client, GList * path,
				  GList * arguments)
{
  DupinView *view=NULL;
  DupinDB *parent_db=NULL;
  DupinLinkB *parent_linkb=NULL;
  DupinView *parent_view=NULL;

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

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    {
      client->dupin_error_msg = g_strdup ("Cannot connect to view");
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      dupin_keyvalue_t *kv = list->data;

      if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_DESCENDING)
	  && !g_strcmp0 (kv->value, "true"))
	descending = TRUE;

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_COUNT))
	count = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_OFFSET))
	offset = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_DOCS))
        {
          if ( view->reduce != NULL
             || (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
                 g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE")))
            {
              if (startkey != NULL)
                g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              dupin_view_unref (view);
	      
              client->dupin_error_msg = g_strdup ("Invalid " REQUEST_GET_ALL_DOCS_INCLUDE_DOCS " parameter. Allowed values are: true, false");

              return HTTP_STATUS_400;
            }

          if (view->reduce == NULL)
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

              dupin_view_unref (view);

              client->dupin_error_msg = g_strdup ("Invalid " REQUEST_GET_ALL_DOCS_INCLUSIVEEND " parameter. Allowed values are: true, false");

              return HTTP_STATUS_400;
            }

          inclusive_end = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_KEY))
        {
	  startkey = g_strdup (kv->value);
	  endkey = g_strdup (kv->value);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_STARTKEY))
        {
	  startkey = g_strdup (kv->value);
        }

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_ENDKEY))
        {
	  endkey = g_strdup (kv->value);
        }
    }

  if (include_docs == TRUE)
    {
      if (dupin_view_get_parent_is_db (view) == TRUE)
        {
          if (!(parent_db = dupin_database_open (view->d, view->parent, NULL)))
            {
              dupin_view_unref (view);
              client->dupin_error_msg = g_strdup ("Cannot connect to parent database");
              return HTTP_STATUS_404;
            }
        }
      else if (dupin_view_get_parent_is_linkb (view) == TRUE)
        {
          if (!(parent_linkb = dupin_linkbase_open (view->d, view->parent, NULL)))
            {
              dupin_view_unref (view);
              client->dupin_error_msg = g_strdup ("Cannot connect to parent linkbase");
              return HTTP_STATUS_404;
            }
        }
      else
        {
          if (!(parent_view = dupin_view_open (view->d, view->parent, NULL)))
            {
              dupin_view_unref (view);
              client->dupin_error_msg = g_strdup ("Cannot connect to parent view");
    	      return HTTP_STATUS_404;
            }
        }
    }

  /* TODO - parse start/endkey as JSON and reserialize to make sure is matching what we stored
            E.g. string { 'foo': 1 }' is different from { "foo" : 1 } */

  /* NOTE - validate start and end keys */

  if (startkey != NULL || endkey != NULL)
    {
      JsonParser *parser = json_parser_new ();
      GError *error = NULL;
      gchar * sn = NULL;

      if (startkey != NULL)
        {
          if (!json_parser_load_from_data (parser, startkey, -1, &error)
              || (!(sn = dupin_util_json_serialize (json_parser_get_root (parser)))) )
            {
	      if (error != NULL)
                g_error_free (error);

              g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              if (parent_db != NULL)
                dupin_database_unref (parent_db);

              if (parent_linkb != NULL)
                dupin_linkbase_unref (parent_linkb);

              if (parent_view != NULL)
                dupin_view_unref (parent_view);

              dupin_view_unref (view);

              client->dupin_error_msg = g_strdup ("Invalid key or start_key parameter");

              return HTTP_STATUS_400;
            }

          g_free (startkey);

          startkey = sn;
        }

      error = NULL;
      sn = NULL;

      if (endkey != NULL)
        {
          if (!json_parser_load_from_data (parser, endkey, -1, &error)
              || (!(sn = dupin_util_json_serialize (json_parser_get_root (parser)))) )
            {
              if (error != NULL)
                g_error_free (error);

              if (startkey != NULL)
                g_free (startkey);

              g_free (endkey);

              if (parent_db != NULL)
                dupin_database_unref (parent_db);

              if (parent_linkb != NULL)
                dupin_linkbase_unref (parent_linkb);

              if (parent_view != NULL)
                dupin_view_unref (parent_view);

              dupin_view_unref (view);

              client->dupin_error_msg = g_strdup ("Invalid key or end_key parameter");

              return HTTP_STATUS_400;
            }

          g_free (endkey);

          endkey = sn;
        }

      g_object_unref (parser);
    }

  if (dupin_view_record_get_total_records (view, &total_rows, startkey, endkey, inclusive_end, NULL) == FALSE)
    {
      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      if (parent_db != NULL)
        dupin_database_unref (parent_db);

      if (parent_linkb != NULL)
        dupin_linkbase_unref (parent_linkb);

      if (parent_view != NULL)
        dupin_view_unref (parent_view);

      dupin_view_unref (view);

      client->dupin_error_msg = g_strdup ("Cannot get total of records from view");

      return HTTP_STATUS_500;
    }

  if (dupin_view_record_get_list (view, count, offset, 0, 0, DP_ORDERBY_KEY, descending,
				  startkey, endkey, inclusive_end,
				  &results, NULL) == FALSE)
    {
      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      if (parent_db != NULL)
        dupin_database_unref (parent_db);

      if (parent_linkb != NULL)
        dupin_linkbase_unref (parent_linkb);

      if (parent_view != NULL)
        dupin_view_unref (parent_view);

      dupin_view_unref (view);

      client->dupin_error_msg = g_strdup ("Cannot get list of records from view");

      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if (parent_db != NULL)
        dupin_database_unref (parent_db);

      if (parent_linkb != NULL)
        dupin_linkbase_unref (parent_linkb);

      if (parent_view != NULL)
        dupin_view_unref (parent_view);

      if( results )
        dupin_view_record_get_list_close(results);

      dupin_view_unref (view);

      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      client->dupin_error_msg = g_strdup ("Cannot get list of records from view");

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
      JsonNode *on;

      if (!
	  (on =
	   request_view_record_obj (client, arguments,
				    record,
				    (gchar *)
				    dupin_view_record_get_id (record))))
        {
          json_array_unref (array);
	  goto request_global_get_all_docs_view_error;
        }

      if (include_docs == TRUE)
        {
          gchar * record_id;
	  JsonNode * doc = NULL;
	  JsonObject * on_obj = json_node_get_object (on);

	  JsonObject * on_obj2 = NULL;
	  JsonNode * on_obj2_node = json_object_get_member (on_obj, "value");
          if (on_obj2_node != NULL
              && json_node_get_node_type (on_obj2_node) == JSON_NODE_OBJECT)
	    on_obj2 = json_object_get_object_member (on_obj, "value");

          if (on_obj2 != NULL
              && json_object_has_member (on_obj2, REQUEST_OBJ_ID))
            record_id = (gchar *) json_object_get_string_member (on_obj2, REQUEST_OBJ_ID);
          else
            record_id = (gchar *) json_object_get_string_member (on_obj, RESPONSE_OBJ_ID);

          if (dupin_view_get_parent_is_db (view) == TRUE)
            {
	      DupinRecord * db_record=NULL;
              if (!(db_record = dupin_record_read (parent_db, record_id, NULL)))
                {
                  doc = json_node_new (JSON_NODE_NULL);
                }
              else
                {
                  doc = json_node_copy (dupin_record_get_revision_node (db_record, NULL));

	          dupin_record_close (db_record);
                }
            }
          else if (dupin_view_get_parent_is_linkb (view) == TRUE)
            {
	      DupinLinkRecord * linkb_record=NULL;
              if (!(linkb_record = dupin_link_record_read (parent_linkb, record_id, NULL)))
                {
                  doc = json_node_new (JSON_NODE_NULL);
                }
              else
                {
                  doc = json_node_copy (dupin_link_record_get_revision_node (linkb_record, NULL));

	          dupin_link_record_close (linkb_record);
                }
            }
          else
            {
              DupinViewRecord * view_record=NULL;
              if (!(view_record = dupin_view_record_read (parent_view, record_id, NULL)))
                {
                  doc = json_node_new (JSON_NODE_NULL);
                }
              else
                {
                  doc = json_node_copy (dupin_view_record_get (view_record));

	          dupin_view_record_close (view_record);
                }
            }

          json_object_set_member (on_obj, "doc", doc);

        }

      json_array_add_element( array, on);
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

  if (parent_db != NULL)
    dupin_database_unref (parent_db);

  if (parent_linkb != NULL)
    dupin_linkbase_unref (parent_linkb);

  if (parent_view != NULL)
    dupin_view_unref (parent_view);

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

  if (parent_db != NULL)
    dupin_database_unref (parent_db);

  if (parent_linkb != NULL)
    dupin_linkbase_unref (parent_linkb);

  if (parent_view != NULL)
    dupin_view_unref (parent_view);

  if( results )
    dupin_view_record_get_list_close(results);

  dupin_view_unref (view);

  client->dupin_error_msg = g_strdup ("Cannot get list of records from view");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to view");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_view_record_read (view, path->next->next->data, NULL)))
    {
      dupin_view_unref (view);
      client->dupin_error_msg = g_strdup ("Cannot get view information");
      return HTTP_STATUS_404;
    }

  if (!
      (node =
       request_view_record_obj (client, arguments, record,
				(gchar *) dupin_view_record_get_id (record))))
    {
      dupin_view_record_close (record);
      dupin_view_unref (view);
      client->dupin_error_msg = g_strdup ("Cannot get view record");
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

  client->dupin_error_msg = g_strdup ("Cannot get view record");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to database");
      return HTTP_STATUS_404;
    }

  array = json_array_new ();

  while (dupin_record_get_list (db, QUERY_BLOCK, offset, 0, 0, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE, &results, NULL) == TRUE && results)
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

  client->dupin_error_msg = g_strdup ("Cannot query database");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  array = json_array_new ();

  while (dupin_link_record_get_list (linkb, QUERY_BLOCK, offset, 0, 0, DP_LINK_TYPE_ANY, DP_COUNT_EXIST, DP_ORDERBY_ROWID, FALSE,
					NULL, NULL, NULL, &results, NULL) == TRUE && results)
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

  client->dupin_error_msg = g_strdup ("Cannot query linkbase");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to view");
      return HTTP_STATUS_404;
    }

  array = json_array_new ();

  while (dupin_view_record_get_list (view, QUERY_BLOCK, offset, 0, 0, DP_ORDERBY_KEY, FALSE, NULL, NULL, TRUE, &results, NULL) == TRUE
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

  client->dupin_error_msg = g_strdup ("Cannot query view");

  return HTTP_STATUS_500;
}

/* POST ********************************************************************/
#define REQUEST_POST_BULK_DOCS		"_bulk_docs"
#define REQUEST_POST_BULK_DOCS_DOCS	"docs"
#define REQUEST_POST_COMPACT_DATABASE	"_compact"

#define REQUEST_POST_BULK_LINKS		"_bulk_links"
#define REQUEST_POST_BULK_LINKS_LINKS	"links"
#define REQUEST_POST_COMPACT_LINKBASE	REQUEST_POST_COMPACT_DATABASE
#define REQUEST_POST_CHECK_LINKBASE	"_check"

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
static DSHttpStatusCode request_global_post_compact_database (DSHttpdClient * client,
						     	      GList * path,
						     	      GList * arguments);

static DSHttpStatusCode request_global_post_compact_linkbase (DSHttpdClient * client,
						     	      GList * path,
						     	      GList * arguments);

static DSHttpStatusCode request_global_post_check_linkbase (DSHttpdClient * client,
						     	    GList * path,
						     	    GList * arguments);

/* NOTE - we use the following also for linkbase records to set/get id and rev */

static gchar *  request_record_insert_rev (JsonNode * obj_node);
static gchar *  request_record_insert_id (JsonNode * obj_node);
static gboolean request_record_insert_deleted (JsonNode * obj_node);

static gchar * request_link_record_insert_label (JsonNode * obj_node);
static gchar * request_link_record_insert_href (JsonNode * obj_node);
static gchar * request_link_record_insert_rel (JsonNode * obj_node);
static gchar * request_link_record_insert_tag (JsonNode * obj_node);

static DSHttpStatusCode
request_global_post (DSHttpdClient * client, GList * path, GList * arguments)
{
  if (!path)
    {
      client->dupin_error_msg = g_strdup ("Missing URL path");
      return HTTP_STATUS_400;
    }

  if (!g_strcmp0 (path->data, REQUEST_LINKBS))
    {
      /* POST /_linkbs/linkbase/_compact */
      if (!g_strcmp0 (path->next->next->data, REQUEST_POST_COMPACT_LINKBASE))
        return request_global_post_compact_linkbase (client, path->next, arguments);

      /* POST /_linkbs/linkbase/_check */
      if (!g_strcmp0 (path->next->next->data, REQUEST_POST_CHECK_LINKBASE))
        return request_global_post_check_linkbase (client, path->next, arguments);

      client->dupin_error_msg = g_strdup ("POST /_linkbs allowed commands are: /_linkbs/linkbase/_compact and /_linkbs/linkbase/_check");

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

  client->dupin_error_msg = g_strdup ("POST /database allowed commands are: /database, /database/_bulk_docs, /database/_compact, /database/doc_id/_links, /database/doc_id/_relationships");

  return HTTP_STATUS_400;
}

static DSHttpStatusCode
request_global_post_record (DSHttpdClient * client, GList * path,
			    GList * arguments)
{
  DSHttpStatusCode code;
  GList *response_list=NULL;
  GError *error = NULL;

  if (!client->body)
    {
      client->dupin_error_msg = g_strdup ("Missing POST body");
      return HTTP_STATUS_400;
    }

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse POST body");
      code = HTTP_STATUS_500;
      goto request_global_post_record_end;
    }

  /* TODO - add options to GET methods to pass escape/unescape Unicode if needed by client accordingly to RFC4627
            we currently read escaped JSON documents and stored them as UTF-8 - also check UTF-16 and UTF-32 encodings if needed/supported */

  /* TODO - add error checking and return any parsing error to client */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      client->dupin_error_msg = g_strdup (error->message);
      g_error_free (error);
      code = HTTP_STATUS_400;
      goto request_global_post_record_end;
    }

  JsonNode * node = json_parser_get_root (parser);

  if (node == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse POST body");
      code = HTTP_STATUS_500;
      goto request_global_post_record_end;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      client->dupin_error_msg = g_strdup ("POST body must be a JSON object");
      code = HTTP_STATUS_400;
      goto request_global_post_record_end;
    }

  if (request_record_insert
      (client, arguments, node, path->data, NULL, &code, &response_list) == TRUE)
    {
      if (request_record_response (client, response_list) == FALSE)
        {
	  code = HTTP_STATUS_500;
          client->dupin_error_msg = g_strdup ("Cannot generate JSON output response");
        }
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

request_global_post_record_end:

  if (parser != NULL)
    g_object_unref (parser);
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

  if (!client->body
      || !path->next)
    {
      client->dupin_error_msg = g_strdup ("POST body or path missing");
      return HTTP_STATUS_400;
    }

  gchar * context_id = path->next->data;

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse POST body");
      code = HTTP_STATUS_500;
      goto request_global_post_doc_links_end;
    }

  /* TODO - add options to GET methods to pass escape/unescape Unicode if needed by client accordingly to RFC4627
            we currently read escaped JSON documents and stored them as UTF-8 - also check UTF-16 and UTF-32 encodings if needed/supported */

  /* TODO - add error checking and return any parsing error to client */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      client->dupin_error_msg = g_strdup (error->message);
      g_error_free (error);
      code = HTTP_STATUS_400;
      goto request_global_post_doc_links_end;
    }

  JsonNode * node = json_parser_get_root (parser);

  if (node == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse POST body");
      code = HTTP_STATUS_500;
      goto request_global_post_doc_links_end;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      client->dupin_error_msg = g_strdup ("POST body must be a JSON object");
      code = HTTP_STATUS_400;
      goto request_global_post_doc_links_end;
    }

  if (request_link_record_insert
      (client, arguments, node, path->data, NULL, context_id, &code,
            link_type, &response_list) == TRUE)
    {
      if (request_record_response (client, response_list) == FALSE)
        {
          code = HTTP_STATUS_500;
          client->dupin_error_msg = g_strdup ("Cannot generate JSON output response");
        }
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

request_global_post_doc_links_end:

  if (parser != NULL)
    g_object_unref (parser);
  return code;
}

static DSHttpStatusCode
request_global_post_bulk_docs (DSHttpdClient * client, GList * path,
			       GList * arguments)
{
  JsonObject *obj;
  JsonNode *node;
  JsonArray *array;
  GList *nodes, *n;
  GError *error = NULL;
  GList *response_list=NULL;

  DSHttpStatusCode code;

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse POST body");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  /* TODO - check any parsing error */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      client->dupin_error_msg = g_strdup (error->message);
      g_error_free (error);
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse POST body");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      client->dupin_error_msg = g_strdup ("POST body must be a JSON object");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  obj = json_node_get_object (node); /* it is a volatile object part of parser as well as node - see docs */

  if (json_object_has_member (obj, REQUEST_POST_BULK_DOCS_DOCS) == FALSE)
    {
      client->dupin_error_msg = g_strdup ("POST body does not contain a mandatory " REQUEST_POST_BULK_DOCS_DOCS " object memeber");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  node = json_object_get_member (obj, REQUEST_POST_BULK_DOCS_DOCS);

  if (node == NULL)
    {
      client->dupin_error_msg = g_strdup ("POST body does not contain a valid " REQUEST_POST_BULK_DOCS_DOCS " object memeber");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  if (json_node_get_node_type (node) != JSON_NODE_ARRAY)
    {
      client->dupin_error_msg = g_strdup ("POST body " REQUEST_POST_BULK_DOCS_DOCS " object memeber is not an array");
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  array = json_node_get_array (node);

  if (array == NULL)
    {
      client->dupin_error_msg = g_strdup ("POST body " REQUEST_POST_BULK_DOCS_DOCS " object memeber is not a valid array");
      code = HTTP_STATUS_500;
      goto request_global_post_bulk_docs_error;
    }

  /* scan JSON array */
  nodes = json_array_get_elements (array);

  for (n = nodes; n != NULL; n = n->next)
    {
      JsonNode *element_node = (JsonNode*)n->data;

      if (json_node_get_node_type (element_node) != JSON_NODE_OBJECT)
        {
          client->dupin_error_msg = g_strdup ("POST body " REQUEST_POST_BULK_DOCS_DOCS " array memebr is not a valid JSON object");
          g_list_free (nodes);
          code = HTTP_STATUS_500;
          goto request_global_post_bulk_docs_error;
        }

      if (request_record_insert
	  (client, arguments, element_node, path->data, NULL, &code, &response_list) == FALSE)
	{
          while (response_list)
            {
              json_node_free (response_list->data);
              response_list = g_list_remove (response_list, response_list->data);
            }
          g_list_free (nodes);
          code = HTTP_STATUS_400;
          goto request_global_post_bulk_docs_error;
	}
    }
  g_list_free (nodes);

  if (request_record_response (client, response_list) == FALSE)
    {
      code = HTTP_STATUS_500;
      client->dupin_error_msg = g_strdup ("Cannot generate JSON output response");
    }
  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  if (parser != NULL)
    g_object_unref (parser);
  return code;

request_global_post_bulk_docs_error:

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
      client->dupin_error_msg = g_strdup ("Cannot connect to database");
      return HTTP_STATUS_404;
    }

  dupin_database_compact (db);

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
      client->dupin_error_msg = g_strdup ("Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  dupin_linkbase_compact (linkb);

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
      client->dupin_error_msg = g_strdup ("Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  dupin_linkbase_check (linkb);

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
      client->dupin_error_msg = g_strdup ("Path missing");
      return HTTP_STATUS_400;
    }

  if (!g_strcmp0 (path->data, REQUEST_LINKBS))
    {
      /* PUT /_linkbs/linkbase/id */
      if (path->next && path->next->next)
	return request_global_put_link_record (client, path->next, arguments);

      client->dupin_error_msg = g_strdup ("PUT /_linkbs allowed commands: /_linkbs/linkbase/id");

      return HTTP_STATUS_400;
    }

  if (!g_strcmp0 (path->data, REQUEST_VIEWS))
    {
      /* PUT /_views/view */
      if (path->next && !path->next->next)
	return request_global_put_view (client, path, arguments);

      client->dupin_error_msg = g_strdup ("PUT /_views allowed commands: /_views/view");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to database");
      return HTTP_STATUS_409;
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
  GError *error = NULL;

  if (!client->body)
    {
      client->dupin_error_msg = g_strdup ("Missing or invalid PUT body");
      return HTTP_STATUS_400;
    }

  parser = json_parser_new ();

  if (parser == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse PUT body");
      code = HTTP_STATUS_500;
      goto request_global_put_view_error;
    }

  /* TODO - check any parsing error */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      client->dupin_error_msg = g_strdup (error->message);
      g_error_free (error);
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse PUT body");
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      client->dupin_error_msg = g_strdup ("PUT body must be a JSON object");
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
      client->dupin_error_msg = g_strdup ("No map, map language or parent fields defined");
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  if (!
      (view =
       dupin_view_new (client->thread->data->dupin, path->next->data, (gchar *)parent,
		       parent_is_db, parent_is_linkb, (gchar *)map,
		       dupin_util_mr_lang_to_enum ((gchar *)map_lang), (gchar *)reduce,
		       dupin_util_mr_lang_to_enum ((gchar *)reduce_lang), NULL)))
    {
      client->dupin_error_msg = g_strdup ("Cannot create view");
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
 
  if (!client->body
      || !path->next->data)
    {
      client->dupin_error_msg = g_strdup ("Missing or invalid PUT body");
      return HTTP_STATUS_400;
    }

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
//g_message("request_global_put_record: dbname=%s id=%s\n", (gchar *) path->data, (gchar *)path->next->data);

      if (path->next->next->next)
        {
          /* PUT _special_document/document_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->next->data, REQUEST_FIELDS))
            {
	      if (!path->next->next->next->next
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ID)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_REV)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ATTACHMENTS))
	        {
      		  client->dupin_error_msg = g_strdup ("Cannot update record field");
                  return HTTP_STATUS_400;
		}

              request_fields=path->next->next->next->next->data;
            }

          /* PUT _special_document/document_ID/_attachments/attachment */
          else if (!g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS))
            {
	      return request_global_put_record_attachment (client, path, arguments);
            }
	  else
            {
      	      client->dupin_error_msg = g_strdup ("Cannot update record");
              return HTTP_STATUS_400;
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
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS))
                {
      		  client->dupin_error_msg = g_strdup ("Cannot update record field");
                  return HTTP_STATUS_400;
	        }

              request_fields=path->next->next->next->data;
            }

          /* PUT /document_ID/_attachments/attachment */
          else if (!g_strcmp0 (path->next->next->data, REQUEST_OBJ_ATTACHMENTS))
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
      client->dupin_error_msg = g_strdup ("Cannot parse PUT body");
      code = HTTP_STATUS_500;
      goto request_global_put_record_error;
    }

  /* TODO - check any parsing error */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      client->dupin_error_msg = g_strdup (error->message);
      g_error_free (error);
      code = HTTP_STATUS_400;
      goto request_global_put_record_error;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse PUT body");
      code = HTTP_STATUS_400;
      goto request_global_put_record_error;
    }

  if (request_fields != NULL)
    {
      gchar * mvcc=NULL;
      DupinDB *db;
      JsonNode * node1=NULL;
      JsonObject * node1_obj=NULL;
      DupinRecord *record;

      GList * l=NULL;
      for (l = arguments; l; l = l->next)
        {
          dupin_keyvalue_t *kv = l->data;

          if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
            {
              if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
                {
		  if (client->dupin_error_msg != NULL)
		    g_free (client->dupin_error_msg);
                  client->dupin_error_msg = g_strdup ("Invalid record MVCC revision");
                  code = HTTP_STATUS_400;
                  goto request_global_put_record_error;
                }
              mvcc = kv->value;
            }
        }

      if (mvcc == NULL)
        {
	  if (client->dupin_error_msg != NULL)
	    g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("Record MVCC revision is missing");
          code = HTTP_STATUS_400;
          goto request_global_put_record_error;
        }

      /* fetch last revision */
      if (!  (db =
       		dupin_database_open (client->thread->data->dupin, path->data, NULL)))
        {
          client->dupin_error_msg = g_strdup ("Cannot open database");
          code = HTTP_STATUS_404;
          goto request_global_put_record_error;
        }

      if (!(record = dupin_record_read (db, doc_id, NULL)))
        {
          client->dupin_error_msg = g_strdup ("Cannot read record from database");
          dupin_database_unref (db);
          code = HTTP_STATUS_404;
          goto request_global_put_record_error;
        }

      if (dupin_record_is_deleted (record, mvcc) == TRUE)
        {
          client->dupin_error_msg = g_strdup ("Record is deleted");
	  dupin_record_close (record);
          dupin_database_unref (db);
          code = HTTP_STATUS_404;
          goto request_global_put_record_error;
        }

      node1 = dupin_record_get_revision_node (record, mvcc);

      if (node1 == NULL)
        {
          client->dupin_error_msg = g_strdup ("Cannot get record revision");
          dupin_record_close (record);
          dupin_database_unref (db);
          code = HTTP_STATUS_404;
          goto request_global_put_record_error;
        }

      node1 = json_node_copy (node1);

      node1_obj = json_node_get_object (node1);

      /* Setting _id and _rev: */
      json_object_set_string_member (node1_obj, REQUEST_OBJ_ID, doc_id);
      json_object_set_string_member (node1_obj, REQUEST_OBJ_REV, mvcc);

      /* set field */
      /* NOTE - we must remove the member first to make sure the correct node type is set internally */
      json_object_remove_member (node1_obj, (const gchar *)request_fields);
      json_object_set_member (node1_obj, (const gchar *)request_fields, json_node_copy (node));

      dupin_record_close (record);
      dupin_database_unref (db);

      node = node1;
    }
  else
    {
      if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
        {
	  client->dupin_error_msg = g_strdup ("PUT body must be a JSON object");
          code = HTTP_STATUS_400;
          goto request_global_put_record_error;
        }
    }

  if (request_record_insert
      (client, arguments, node, path->data, doc_id, &code, &response_list) == TRUE)
    {
      if (request_record_response (client, response_list) == FALSE)
        {
          code = HTTP_STATUS_500;
          client->dupin_error_msg = g_strdup ("Cannot generate JSON output response");
        }
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

request_global_put_record_error:

  if (request_fields != NULL
      && node)
    json_node_free (node);

  if (parser != NULL)
    g_object_unref (parser);

  g_free (doc_id);

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
 
  if (!client->body
      || !path->next->data)
    {
      client->dupin_error_msg = g_strdup ("Missing or invalid PUT body");
      return HTTP_STATUS_400;
    }

  /* check if special link name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
//g_message("request_global_put_link_record: linkbname=%s id=%s\n", (gchar *) path->data, (gchar *)path->next->data);

      if (path->next->next->next)
        {
          /* PUT _special_link/link_ID/_fields/field */
          if (!g_strcmp0 (path->next->next->next->data, REQUEST_FIELDS))
            {
	      if (!path->next->next->next->next
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ID)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_REV)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_ATTACHMENTS)
                  || !g_strcmp0 (path->next->next->next->next->data, REQUEST_OBJ_LINKS))
                {
      		  client->dupin_error_msg = g_strdup ("Cannot update link record field");
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
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_LINKS))
                {
      		  client->dupin_error_msg = g_strdup ("Cannot update link record field");
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
      client->dupin_error_msg = g_strdup ("Cannot parse PUT body");
      code = HTTP_STATUS_500;
      goto request_global_put_link_record_error;
    }

  /* TODO - check any parsing error */
  if (!json_parser_load_from_data (parser, client->body, client->body_size, &error))
    {
      client->dupin_error_msg = g_strdup (error->message);
      g_error_free (error);
      code = HTTP_STATUS_400;
      goto request_global_put_link_record_error;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      client->dupin_error_msg = g_strdup ("Cannot parse PUT body");
      code = HTTP_STATUS_400;
      goto request_global_put_link_record_error;
    }

  if (request_fields != NULL)
    {
      gchar * mvcc=NULL;
      DupinLinkB *linkb;
      JsonNode * node1=NULL;
      JsonObject * node1_obj=NULL;
      DupinLinkRecord *record;

      GList * l=NULL;
      for (l = arguments; l; l = l->next)
        {
          dupin_keyvalue_t *kv = l->data;

          if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
            {
              if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
                {
                  client->dupin_error_msg = g_strdup ("Invalid record MVCC revision");
                  code = HTTP_STATUS_404;
                  goto request_global_put_link_record_error;
                }
              mvcc = kv->value;
            }
        }

      if (mvcc == NULL)
        {
          client->dupin_error_msg = g_strdup ("Link record MVCC revision is missing");
          code = HTTP_STATUS_404;
          goto request_global_put_link_record_error;
        }

      /* fetch last revision */
      if (!  (linkb =
       		dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
        {
          client->dupin_error_msg = g_strdup ("Cannot connect to linkbase");
          code = HTTP_STATUS_404;
          goto request_global_put_link_record_error;
        }

      if (!(record = dupin_link_record_read (linkb, link_id, NULL)))
        {
          client->dupin_error_msg = g_strdup ("Cannot read link record");
          dupin_linkbase_unref (linkb);
          code = HTTP_STATUS_404;
          goto request_global_put_link_record_error;
        }

      mvcc = dupin_link_record_get_last_revision (record);

      if (dupin_link_record_is_deleted (record, mvcc) == TRUE)
        {
          client->dupin_error_msg = g_strdup ("Link record is deleted");
          dupin_link_record_close (record);
          dupin_linkbase_unref (linkb);
          code = HTTP_STATUS_404;
          goto request_global_put_link_record_error;
        }

      node1 = dupin_link_record_get_revision_node (record, mvcc);

      if (node1 == NULL)
        {
          client->dupin_error_msg = g_strdup ("Cannot ger link record revision");
          dupin_link_record_close (record);
          dupin_linkbase_unref (linkb);
          code = HTTP_STATUS_404;
          goto request_global_put_link_record_error;
        }

      node1 = json_node_copy (node1);

      node1_obj = json_node_get_object (node1);

      /* Setting _id and _rev: */
      json_object_set_string_member (node1_obj, REQUEST_OBJ_ID, link_id);
      json_object_set_string_member (node1_obj, REQUEST_OBJ_REV, mvcc);

      /* set field */
      /* NOTE - we must remove the member first to make sure the correct node type is set internally */
      json_object_remove_member (node1_obj, (const gchar *)request_fields);
      json_object_set_member (node1_obj, (const gchar *)request_fields, json_node_copy (node));

      if (json_object_has_member (node1_obj, REQUEST_LINK_OBJ_HREF) == FALSE)
        {
          json_object_remove_member (node1_obj, REQUEST_LINK_OBJ_HREF);
          json_object_set_string_member (node1_obj, REQUEST_LINK_OBJ_HREF, dupin_link_record_get_href (record));
        }

      gchar * rel = (gchar *)dupin_link_record_get_rel (record);
      if (json_object_has_member (node1_obj, REQUEST_LINK_OBJ_REL) == FALSE
	  && rel != NULL)
        {
          json_object_remove_member (node1_obj, REQUEST_LINK_OBJ_REL);
          json_object_set_string_member (node1_obj, REQUEST_LINK_OBJ_REL, rel);
        }

      gchar * tag = (gchar *)dupin_link_record_get_tag (record);
      if (json_object_has_member (node1_obj, REQUEST_LINK_OBJ_TAG) == FALSE
	  && tag != NULL)
        {
          json_object_remove_member (node1_obj, REQUEST_LINK_OBJ_TAG);
          json_object_set_string_member (node1_obj, REQUEST_LINK_OBJ_TAG, tag);
        }

      dupin_link_record_close (record);
      dupin_linkbase_unref (linkb);

      node = node1;
    }
  else
    {
      if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
        {
	  client->dupin_error_msg = g_strdup ("PUT body must be a JSON object");
          code = HTTP_STATUS_400;
          goto request_global_put_link_record_error;
        }
    }

  if (request_link_record_insert
      (client, arguments, node, path->data, link_id, NULL, &code,
            DP_LINK_TYPE_ANY, &response_list) == TRUE)
    {
      if (request_record_response (client, response_list) == FALSE)
        {
          code = HTTP_STATUS_500;
          client->dupin_error_msg = g_strdup ("Cannot generate JSON output response");
        }
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

  if (request_fields != NULL
      && node)
    json_node_free (node);

request_global_put_link_record_error:

  if (parser != NULL)
    g_object_unref (parser);

  g_free (link_id);

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

  if (!client->body
      || !client->input_mime
      || !path->next->data
      || !path->next->next)
    {
      client->dupin_error_msg = g_strdup ("Missing or invalid PUT body attachment file, input Content-Type header or attachment name not specified");
      return HTTP_STATUS_400;
    }

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* PUT _special_document/document_ID/_attachments/attachment */
      doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);

      if (path->next->next->next
	  && (!g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS)))
        {
          if (path->next->next->next->next)
            title_parts = path->next->next->next->next;
        }
    }
  else
    {
       if (path->next->next->next
	   && (!g_strcmp0 (path->next->next->data, REQUEST_OBJ_ATTACHMENTS)))
         {
           /* PUT /document_ID/_attachments/attachment */
          doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
          title_parts = path->next->next->next;
         }
       else
         {
           client->dupin_error_msg = g_strdup ("PUT /document_ID/_attachments/attachment is the only allowed command");
           return HTTP_STATUS_400;
         }
    }

//g_message("request_global_put_record_attachment: dbname=%s doc_id=%s title_parts=%s\n", (gchar *) path->data, doc_id, (gchar *)title_parts->data);

  if (request_record_attachment_insert
      (client, arguments, path->data, doc_id,
       title_parts, &code,
       &response_list) == TRUE)
    {
      if (request_record_response (client, response_list) == FALSE)
        {
          code = HTTP_STATUS_500;
          client->dupin_error_msg = g_strdup ("Cannot generate JSON output response");
        }
    }

  while (response_list)
    {
      json_node_free (response_list->data);
      response_list = g_list_remove (response_list, response_list->data);
    }

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
      client->dupin_error_msg = g_strdup ("Path missing");
      return HTTP_STATUS_400;
    }

  if (!g_strcmp0 (path->data, REQUEST_LINKBS))
    {
      /* DELETE /_linkbs/linkbase/id */
      if (path->next && path->next->next)
	return request_global_delete_link_record (client, path->next, arguments);

      client->dupin_error_msg = g_strdup ("DELETE /_linkbs allowed commands: /_linkbs/linkbase/id");

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

      client->dupin_error_msg = g_strdup ("DELETE /_views allowed commands: /_views/view");

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
      client->dupin_error_msg = g_strdup ("Cannot connect to database");
      return HTTP_STATUS_404;
    }

  /* TODO - shoudl trigger delete any views, link bases and attachment dbs associated to the database */

  if (dupin_database_delete (db, NULL) == FALSE)
    {
      client->dupin_error_msg = g_strdup ("Cannot delete database");
      dupin_database_unref (db);
      return HTTP_STATUS_409;
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
      client->dupin_error_msg = g_strdup ("Cannot connect to view");
      return HTTP_STATUS_404;
    }

  if (dupin_view_delete (view, NULL) == FALSE)
    {
      client->dupin_error_msg = g_strdup ("Cannot delete view");
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
  DupinAttachmentDB *attachment_db;
  DupinRecord *record;
  gchar * mvcc=NULL;
  gchar * title = NULL;
  GList * title_parts=NULL;
  GList * l=NULL;
  GString *str;
  gchar * doc_id=NULL;
  gchar * request_fields=NULL;

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* TODO - shouldn't we stop/avoid user to delete /_design/something ?! */

      /* DELETE _special_document/document_ID */
      if (path->next->next)
        {
          doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);

          /* DELETE _special_document/document_ID/_attachments/attachment */
          if (path->next->next->next
              && (!g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS)))
            {
              if (path->next->next->next->next)
                title_parts = path->next->next->next->next;
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
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_OBJ_ATTACHMENTS))
	        {
      		  client->dupin_error_msg = g_strdup ("Cannot delete record field");
                  return HTTP_STATUS_400;
		}

              request_fields=path->next->next->next->data;
            }

          /* DELETE /document_ID/_attachments/attachment */
          else if (path->next->next->next
                   && (!g_strcmp0 (path->next->next->data, REQUEST_OBJ_ATTACHMENTS)))
            {
              title_parts = path->next->next->next;
            }
          else
            {
      	      client->dupin_error_msg = g_strdup ("DELETE /document_ID allowed commands: /document_ID/_fields/field /document_ID/_attachments/attachment");
              return HTTP_STATUS_400;
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
      	  client->dupin_error_msg = g_strdup ("Cannot get attachment name");
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
	      client->dupin_error_msg = g_strdup ("Invalid record MVCC revision");
              g_free (doc_id);
              return HTTP_STATUS_400;
            }
          mvcc = kv->value;
        }
    }

  if (mvcc == NULL)
    {
      client->dupin_error_msg = g_strdup ("Record MVCC revision is missing");
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
      client->dupin_error_msg = g_strdup ("Cannot connect to database");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_record_read (db, doc_id, NULL)))
    {
      if (title != NULL)
        g_free (title);
      dupin_database_unref (db);
      g_free (doc_id);
      client->dupin_error_msg = g_strdup ("Cannot read record");
      return HTTP_STATUS_404;
    }

  if (dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record)))
    {
      if (title != NULL)
        g_free (title);
      dupin_record_close (record);
      dupin_database_unref (db);
      g_free (doc_id);
      client->dupin_error_msg = g_strdup ("Record MVCC revision missing or not matching latest revision");
      return HTTP_STATUS_404;
    }

  if (request_fields != NULL
      || title_parts != NULL)
    {
      JsonNode * obj_node = NULL;

      if (title_parts != NULL
          && (!  (attachment_db =
       		dupin_attachment_db_open (client->thread->data->dupin, path->data, NULL))))
        {
          if (title != NULL)
            g_free (title);
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
	  client->dupin_error_msg = g_strdup ("Cannot open attachments database");
          return HTTP_STATUS_404;
        }

      if (!(obj_node = request_record_revision_obj (client, arguments,
						    record, doc_id, mvcc)))
        {
          if (title != NULL)
            g_free (title);
          if (title_parts != NULL)
            dupin_attachment_db_unref (attachment_db);
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
	  client->dupin_error_msg = g_strdup ("Cannot get record JSON object");
          return HTTP_STATUS_404;
        }

      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_REV);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_ID);

      if (request_fields != NULL)
        {
          if (json_object_has_member (json_node_get_object (obj_node), (const gchar *)request_fields) == FALSE)
            {
              if (title != NULL)
                g_free (title);
              if (title_parts != NULL)
                dupin_attachment_db_unref (attachment_db);
              dupin_record_close (record);
              dupin_database_unref (db);
              g_free (doc_id);
	      client->dupin_error_msg = g_strdup ("Cannot find record field");
              return HTTP_STATUS_404;
            }

          json_object_remove_member (json_node_get_object (obj_node), (const gchar *)request_fields);
        }

      if (title_parts != NULL
          && ( dupin_attachment_record_exists (attachment_db, doc_id, title) == FALSE
               || dupin_attachment_record_delete (attachment_db, doc_id, title) == FALSE))
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
	  client->dupin_error_msg = g_strdup ("Cannot find record attachment or cannot delete attachment");
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
	  client->dupin_error_msg = g_strdup ("Cannot update record");
          return HTTP_STATUS_404;
        }

      if (obj_node != NULL)
        json_node_free (obj_node);

      if (title_parts != NULL)
        dupin_attachment_db_unref (attachment_db);

      if (title != NULL)
        g_free (title);
    }
  else
    {
      if (!(dupin_record_delete (record, NULL)))
        {
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
	  client->dupin_error_msg = g_strdup ("Cannot delete record");
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

  if (request_record_response (client, response_list) == FALSE)
    {
      while (response_list)
        {
          json_node_free (response_list->data);
          response_list = g_list_remove (response_list, response_list->data);
        }
      dupin_record_close (record);
      dupin_database_unref (db);
      g_free (doc_id);
      client->dupin_error_msg = g_strdup ("Cannot generate JSON output response");
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
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_LINK_OBJ_CONTEXT_ID)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_LINK_OBJ_LABEL)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_LINK_OBJ_HREF)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_LINK_OBJ_REL)
                  || !g_strcmp0 (path->next->next->next->data, REQUEST_LINK_OBJ_TAG))
	        {
      		  client->dupin_error_msg = g_strdup ("Cannot delete link record field");
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
              client->dupin_error_msg = g_strdup ("Invalid link record MVCC revision");
              g_free (link_id);
              return HTTP_STATUS_400;
            }
          mvcc = kv->value;
        }
    }

  if (mvcc == NULL)
    {
      client->dupin_error_msg = g_strdup ("Link record MVCC revision is missing");
      g_free (link_id);
      return HTTP_STATUS_400;
    }

  if (!
      (linkb =
       dupin_linkbase_open (client->thread->data->dupin, path->data, NULL)))
    {
      g_free (link_id);
      client->dupin_error_msg = g_strdup ("Cannot connect to linkbase");
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_link_record_read (linkb, link_id, NULL)))
    {
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      client->dupin_error_msg = g_strdup ("Cannot read link record");
      return HTTP_STATUS_404;
    }

  if (dupin_util_mvcc_revision_cmp (mvcc, dupin_link_record_get_last_revision (record)))
    {
      dupin_link_record_close (record);
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      client->dupin_error_msg = g_strdup ("Link record MVCC revision missing or not matching latest revision");
      return HTTP_STATUS_404;
    }

  if (request_fields != NULL)
    {
      JsonNode * obj_node = NULL;

      if (!(obj_node = request_link_record_revision_obj (client, arguments,
							 record, link_id, mvcc)))
        {
          dupin_link_record_close (record);
          dupin_linkbase_unref (linkb);
          g_free (link_id);
	  client->dupin_error_msg = g_strdup ("Cannot get link record JSON object");
          return HTTP_STATUS_404;
        }

      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_REV);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_ID);

      if (json_object_has_member (json_node_get_object (obj_node), (const gchar *)request_fields) == FALSE)
        {
          if (obj_node != NULL)
            json_node_free (obj_node);
          dupin_link_record_close (record);
          dupin_linkbase_unref (linkb);
          g_free (link_id);
	  client->dupin_error_msg = g_strdup ("Cannot find link record field");
          return HTTP_STATUS_404;
        }

      json_object_remove_member (json_node_get_object (obj_node), (const gchar *)request_fields);

      if (dupin_link_record_update (record, obj_node, 
      				    (gchar *)dupin_link_record_get_label (record),
      				    (gchar *)dupin_link_record_get_href (record),
      				    (gchar *)dupin_link_record_get_rel (record),
      				    (gchar *)dupin_link_record_get_tag (record),
				    NULL) == FALSE)
        {
          if (obj_node != NULL)
            json_node_free (obj_node);
          dupin_link_record_close (record);
          dupin_linkbase_unref (linkb);
          g_free (link_id);
	  client->dupin_error_msg = g_strdup ("Cannot update link record");
          return HTTP_STATUS_404;
        }

      if (obj_node != NULL)
        json_node_free (obj_node);
    }
  else
    {
      if (!(dupin_link_record_delete (record, NULL)))
        {
          dupin_link_record_close (record);
          dupin_linkbase_unref (linkb);
          g_free (link_id);
	  client->dupin_error_msg = g_strdup ("Cannot delete link record");
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

  if (request_record_response (client, response_list) == FALSE)
    {
      while (response_list)
        {
          json_node_free (response_list->data);
          response_list = g_list_remove (response_list, response_list->data);
        }
      dupin_link_record_close (record);
      dupin_linkbase_unref (linkb);
      g_free (link_id);
      client->dupin_error_msg = g_strdup ("Cannot generate JSON output response");
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
#define REQUEST_WWW		"_www"
#define REQUEST_QUIT		"_quit"
#define REQUEST_STATUS		"_status"

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
request_record_insert (DSHttpdClient * client, GList * arguments, JsonNode * obj_node,
		       gchar * dbname, gchar * id, DSHttpStatusCode * code,
		       GList ** response_list)
{
  DupinDB *db;
  DupinLinkB *linkb;
  DupinAttachmentDB *attachment_db;
  DupinRecord *record;
  DSHttpStatusCode retcode;

  gchar * mvcc=NULL;
  gchar * json_record_id;

  JsonNode * links_node=NULL;
  JsonNode * relationships_node=NULL;
  JsonNode * attachments_node=NULL;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);

  gboolean to_delete = request_record_insert_deleted (obj_node);

  JsonNode * record_response_node = json_node_new (JSON_NODE_OBJECT);
  JsonObject * record_response_obj = json_object_new ();
  json_node_take_object (record_response_node, record_response_obj);

  /* fetch the _rev field in the record first, if there */
  mvcc = request_record_insert_rev (obj_node);

  /* otherwise check passed parameters */
  if (mvcc == NULL)
    {
      GList * l=NULL;
      for (l = arguments; l; l = l->next)
        {
          dupin_keyvalue_t *kv = l->data;

          if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
            {
              if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
                {
		  if (client->dupin_error_msg != NULL)
		    g_free (client->dupin_error_msg);
                  client->dupin_error_msg = g_strdup ("Invalid record MVCC revision");
                  *code = HTTP_STATUS_400;
		  if (record_response_node != NULL)
                    json_node_free (record_response_node);
                  return FALSE;
                }
              mvcc = g_strdup (kv->value);
            }
        }
    }

  if (!(db = dupin_database_open (client->thread->data->dupin, dbname, NULL)))
    {
      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);
      client->dupin_error_msg = g_strdup ("Cannot connect to database");
      *code = HTTP_STATUS_400;
      if (record_response_node != NULL)
        json_node_free (record_response_node);
      return FALSE;
    }

  if ((json_record_id = request_record_insert_id (obj_node)))
    {
      if (id && g_strcmp0 (id, json_record_id))
	{
          if (mvcc != NULL)
	    g_free (mvcc);
	  g_free (json_record_id);
          dupin_database_unref (db); /* added by AR 2010-10-05 */
	  *code = HTTP_STATUS_400;
          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("Specified record id does not match");
          if (record_response_node != NULL)
            json_node_free (record_response_node);
	  return FALSE;
	}

      id = json_record_id;
    }

  if (mvcc != NULL && !id)
    {
      if (json_record_id != NULL)
        g_free (json_record_id);
      if (mvcc != NULL)
        g_free (mvcc);
      dupin_database_unref (db);
      *code = HTTP_STATUS_400;
      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);
      client->dupin_error_msg = g_strdup ("No valid record id or MVCC specified");
      if (record_response_node != NULL)
        json_node_free (record_response_node);
      return FALSE;
    }

  /* get and remove inline _attachments element */
  attachments_node = json_object_get_member (json_node_get_object (obj_node), REQUEST_OBJ_ATTACHMENTS);
  if (attachments_node != NULL)
    {
      attachments_node = json_node_copy (attachments_node);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_ATTACHMENTS);
    }

  /* get and remove inline _links element */
  links_node = json_object_get_member (json_node_get_object (obj_node), REQUEST_OBJ_LINKS);
  if (links_node != NULL)
    {
      links_node = json_node_copy (links_node);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_LINKS);
    }

  /* get and remove inline _relationships element */
  relationships_node = json_object_get_member (json_node_get_object (obj_node), REQUEST_OBJ_RELATIONSHIPS);
  if (relationships_node != NULL)
    {
      relationships_node = json_node_copy (relationships_node);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_RELATIONSHIPS);
    }

  if (mvcc != NULL)
    {
      retcode = HTTP_STATUS_200;

      record = dupin_record_read (db, id, NULL);

      if ( to_delete == TRUE )
        {
          if (!record || dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record))
	      || dupin_record_delete (record, NULL) == FALSE)
	    {
              if (record)
	        dupin_record_close (record);
	      record = NULL;
	    }
        }
      else
        {
          if (!record || dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record))
	      || dupin_record_update (record, obj_node, NULL) == FALSE)
	    {
              if (record)
	        dupin_record_close (record);
	      record = NULL;
	    }
        }
    }

  else if (!id)
    {
      if ( to_delete == TRUE )
        {
          if (record)
	    dupin_record_close (record);
	  record = NULL;
        }
      else
        {
          retcode = HTTP_STATUS_201;
          record = dupin_record_create (db, obj_node, NULL);
        }
    }

  else
    {
      if ( to_delete == TRUE )
        {
          if (record)
	    dupin_record_close (record);
	  record = NULL;
        }
      else
        {
          retcode = HTTP_STATUS_201;

          if (dupin_record_exists (db, id) == FALSE)
	    record = dupin_record_create_with_id (db, obj_node, id, NULL);
          else
	    record = NULL;
        }
    }

  if (json_record_id)
    g_free (json_record_id);

  if (!record)
    {
      if (attachments_node != NULL)
        json_node_free (attachments_node);
      if (links_node != NULL)
        json_node_free (links_node);
      if (relationships_node != NULL)
        json_node_free (relationships_node);
      dupin_database_unref (db);

      *code = HTTP_STATUS_409;

      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);

      if (to_delete == TRUE)
        {
          if (mvcc == NULL)
            client->dupin_error_msg = g_strdup ("Deleted flag not allowed on record creation");
          else
            client->dupin_error_msg = g_strdup ("Cannot delete record");
        }
      else if (mvcc != NULL)
        client->dupin_error_msg = g_strdup ("Cannot update record");
      else
        client->dupin_error_msg = g_strdup ("Cannot insert record");

      if (mvcc != NULL)
        g_free (mvcc);

      if (record_response_node != NULL)
        json_node_free (record_response_node);

      return FALSE;
    }

  /* process _attachments object for inline attachments */

  if (attachments_node != NULL
      && json_node_get_node_type (attachments_node) == JSON_NODE_OBJECT)
    {
//g_message("process _attachments object for inline attachments\n");

      if (!  (attachment_db =
               dupin_attachment_db_open (client->thread->data->dupin, dbname, NULL)))
        {
          if (attachments_node != NULL)
            json_node_free (attachments_node);
          if (links_node != NULL)
            json_node_free (links_node);
          if (relationships_node != NULL)
            json_node_free (relationships_node);
          if (mvcc != NULL)
            g_free (mvcc);
          dupin_database_unref (db);
          *code = HTTP_STATUS_400;

          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("Cannot connect to attachments database");

          if (record_response_node != NULL)
            json_node_free (record_response_node);

          return FALSE;
        }

      JsonObject * attachments_obj = json_node_get_object (attachments_node);

      GList *n;
      GList *nodes = json_object_get_members (attachments_obj);

      for (n = nodes; n != NULL; n = n->next)
        {
          gchar *member_name = (gchar *) n->data;
          JsonNode *inline_attachment_node = json_object_get_member (attachments_obj, member_name);

          if (json_node_get_node_type (inline_attachment_node) != JSON_NODE_OBJECT)
            {
              /* TODO - should log something or fail ? */
              continue;
            }

          JsonObject *inline_attachment_obj = json_node_get_object (inline_attachment_node);

          gboolean to_delete = FALSE;
          if (json_object_has_member (inline_attachment_obj, REQUEST_OBJ_DELETED) == TRUE)
	    to_delete = json_object_get_boolean_member (inline_attachment_obj, REQUEST_OBJ_DELETED);

          gboolean stub = FALSE;
          if (json_object_has_member (inline_attachment_obj, REQUEST_OBJ_INLINE_ATTACHMENTS_STUB) == TRUE)
            stub = json_object_get_boolean_member (inline_attachment_obj, REQUEST_OBJ_INLINE_ATTACHMENTS_STUB);

	  if (stub == TRUE)
	    {
              if (to_delete == TRUE)
                {
                  // TODO - warning we used _deleted: true while we meant to ingnore attachment ? */
		  continue;
                }
	      else
                {
                  // do we need to update attachment metadata in any way ? */
	          continue;
                }
            }

          gchar * content_type = NULL;
          guchar * buff = NULL;
          gsize buff_size;
          if (to_delete == FALSE)
            {
              content_type = (gchar *) json_object_get_string_member (inline_attachment_obj, REQUEST_OBJ_INLINE_ATTACHMENTS_TYPE);
              gchar * data = (gchar *) json_object_get_string_member (inline_attachment_obj, REQUEST_OBJ_INLINE_ATTACHMENTS_DATA);

              /* decode base64 assuming data is a single (even if long) line/string */
	      if (data != NULL)
	        buff = g_base64_decode ((const gchar *)data, &buff_size);

	      if (content_type == NULL
                  || data == NULL
                  || buff == NULL)
                {
                  if (buff != NULL)
                    g_free (buff);

                  /* TODO - should log something or fail ? */
                  continue;
                }
            }

          /* NOTE - store inline attachment as normal one - correct? */
          if ( (to_delete == TRUE
                && dupin_attachment_record_exists (attachment_db, (gchar *) dupin_record_get_id (record), member_name) == FALSE)
	      || dupin_attachment_record_delete (attachment_db, (gchar *) dupin_record_get_id (record), member_name) == FALSE
              || (to_delete == FALSE
		  && dupin_attachment_record_insert (attachment_db, (gchar *) dupin_record_get_id (record), member_name,
                                          buff_size, content_type, NULL,
                                          (const void *)buff) == FALSE))
            {
              if (buff != NULL)
                g_free (buff);
              dupin_attachment_db_unref (attachment_db);

	      dupin_record_close (record);

              if (attachments_node != NULL)
                json_node_free (attachments_node);
              if (links_node != NULL)
                json_node_free (links_node);
              if (relationships_node != NULL)
                json_node_free (relationships_node);
              if (mvcc != NULL)
                g_free (mvcc);
              dupin_database_unref (db);
              *code = HTTP_STATUS_404;
              if (client->dupin_error_msg != NULL)
                g_free (client->dupin_error_msg);
              client->dupin_error_msg = g_strdup ("Cannot insert, update or delete attachment");

              if (record_response_node != NULL)
                json_node_free (record_response_node);

              return FALSE;
            }

          g_free (buff);
        }

      g_list_free (nodes);

      dupin_attachment_db_unref (attachment_db);
    }

  /* process _links object for inline links */

  if ((to_delete == FALSE)
      && (links_node != NULL || relationships_node != NULL))
    {
      GList *n, *nodes;
      gchar * context_id = (gchar *)dupin_record_get_id (record);

      if (!  (linkb =
               dupin_linkbase_open (client->thread->data->dupin, dbname, NULL)))
        {
	  dupin_record_close (record);

          if (attachments_node != NULL)
            json_node_free (attachments_node);
          if (links_node != NULL)
            json_node_free (links_node);
          if (relationships_node != NULL)
            json_node_free (relationships_node);
          if (mvcc != NULL)
            g_free (mvcc);
          dupin_database_unref (db);
          *code = HTTP_STATUS_400;
          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("Cannot connect to linkbase");
          if (record_response_node != NULL)
            json_node_free (record_response_node);

          return FALSE;
        }

      if (links_node != NULL && json_node_get_node_type (links_node) == JSON_NODE_OBJECT)
        {
  	  JsonNode * record_response_links_node = json_node_new (JSON_NODE_OBJECT);
	  JsonObject * record_response_links_object = json_object_new ();
          json_node_take_object (record_response_links_node, record_response_links_object);

          JsonObject * links_obj = json_node_get_object (links_node);
          nodes = json_object_get_members (links_obj);
          for (n = nodes; n != NULL; n = n->next)
            {
              gchar *label = (gchar *) n->data;
              JsonNode *inline_link_node = json_object_get_member (links_obj, label);

  	      JsonNode * record_response_links_label_node = json_node_new (JSON_NODE_ARRAY);
	      JsonArray * record_response_links_label_array = json_array_new ();
              json_node_take_array (record_response_links_label_node, record_response_links_label_array);

              if (json_node_get_node_type (inline_link_node) == JSON_NODE_ARRAY)
                {
                  GList *sn, *snodes;
                  snodes = json_array_get_elements (json_node_get_array (inline_link_node));
                  for (sn = snodes; sn != NULL; sn = sn->next)
                    {
		      GList * links_response_list = NULL;
                      JsonNode * lnode = (JsonNode *) sn->data;
		      gchar * lnode_context_id = NULL;
		      gchar * lnode_label = NULL;

                      if (json_node_get_node_type (lnode) != JSON_NODE_OBJECT)
                        {
                          /* TODO - should log something or fail ? */
                          continue;
                        }

                      /* add each link with context_id and label */

                      JsonObject * lobj = json_node_get_object (lnode);

		      if (json_object_has_member (lobj, REQUEST_LINK_OBJ_CONTEXT_ID) == TRUE)
                        lnode_context_id = (gchar *)json_object_get_string_member (lobj, REQUEST_LINK_OBJ_CONTEXT_ID);

		      if (json_object_has_member (lobj, REQUEST_LINK_OBJ_LABEL) == TRUE)
                        lnode_label = (gchar *)json_object_get_string_member (lobj, REQUEST_LINK_OBJ_LABEL);
                      else
  		        json_object_set_string_member (lobj, REQUEST_LINK_OBJ_LABEL, label);

//g_message("request_record_insert: context_id=%s label=%s lnode_context_id=%s lnode_label=%s\n", context_id, label, lnode_context_id, lnode_label);

		      /* TODO - rework this to report errors to poort user ! perhaps using contextual logging if useful */

		      if ( ((lnode_context_id != NULL ) && (g_strcmp0 (lnode_context_id, context_id)))
		           || ((lnode_label != NULL ) && (g_strcmp0 (lnode_label, label))))
        	        {
			  JsonObject * error_obj = json_object_new ();
			  GString * str = g_string_new("");
			  g_string_append_printf (str, "Link record (%s, %s) has an invalid context_id, not a valid label or cannot be added to document",
						lnode_context_id, lnode_label);
			  gchar * error_msg = g_string_free (str, FALSE);
			  json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, error_msg);
			  json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, DSHttpStatusList[HTTP_STATUS_400].body);
			  g_free (error_msg);
			  json_array_add_object_element (record_response_links_label_array, error_obj);

			  continue;
        		}

		      if (request_link_record_insert (client, arguments, lnode,
						      dbname, NULL, context_id, code, DP_LINK_TYPE_WEB_LINK, &links_response_list) == FALSE)
        	        {
			  JsonObject * error_obj = json_object_new ();
			  GString * str = g_string_new("");
			  if (client->dupin_error_msg != NULL)
                            {
			      g_string_append_printf (str, "%s", client->dupin_error_msg);
        		      g_free (client->dupin_error_msg);
			      client->dupin_error_msg = NULL;
			    }
			  else
                            {
			      g_string_append_printf (str, "Link record (%s, %s) cannnot be inserted", lnode_context_id, lnode_label);
                            }
			  gchar * error_msg = g_string_free (str, FALSE);
			  json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, error_msg);
			  json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, DSHttpStatusList[*code].body);
                          g_free (error_msg);
                          json_array_add_object_element (record_response_links_label_array, error_obj);

  			  while (links_response_list)
                            {
			      json_node_free (links_response_list->data);
			      links_response_list = g_list_remove (links_response_list, links_response_list->data);
			    }

		          continue;
        		}

//g_message("request_record_insert: DONE link context_id=%s label=%s lnode_context_id=%s lnode_label=%s\n", context_id, label, lnode_context_id, lnode_label);

  	              while (links_response_list)
                        {
      			  json_array_add_element (record_response_links_label_array, (JsonNode *)links_response_list->data);
			  links_response_list = g_list_remove (links_response_list, links_response_list->data);
			}
                    }
                  g_list_free (snodes);
                }

	      if (json_array_get_length (record_response_links_label_array) == 0)
                {
                  json_node_free (record_response_links_label_node);
                }
              else
                {
  		  json_object_set_member (record_response_links_object, label, record_response_links_label_node);
                }
            }
          g_list_free (nodes);

	  if (json_object_get_size (record_response_links_object) == 0)
            {
              json_node_free (record_response_links_node);
            }
          else
            {
  	      json_object_set_member (record_response_obj, RESPONSE_OBJ_LINKS, record_response_links_node);
            }
        }

      nodes = NULL;

      if (relationships_node != NULL && json_node_get_node_type (relationships_node) == JSON_NODE_OBJECT)
        {
  	  JsonNode * record_response_relationships_node = json_node_new (JSON_NODE_OBJECT);
	  JsonObject * record_response_relationships_object = json_object_new ();
          json_node_take_object (record_response_relationships_node, record_response_relationships_object);

          JsonObject * relationships_obj = json_node_get_object (relationships_node);
          nodes = json_object_get_members (relationships_obj);
          for (n = nodes; n != NULL; n = n->next)
            {
              gchar *label = (gchar *) n->data;
              JsonNode *inline_relationship_node = json_object_get_member (relationships_obj, label);

  	      JsonNode * record_response_relationships_label_node = json_node_new (JSON_NODE_ARRAY);
	      JsonArray * record_response_relationships_label_array = json_array_new ();
              json_node_take_array (record_response_relationships_label_node, record_response_relationships_label_array);

              if (json_node_get_node_type (inline_relationship_node) == JSON_NODE_ARRAY)
                {
                  GList *sn, *snodes;
                  snodes = json_array_get_elements (json_node_get_array (inline_relationship_node));
                  for (sn = snodes; sn != NULL; sn = sn->next)
                    {
		      GList * relationships_response_list = NULL;
                      JsonNode * rnode = (JsonNode *) sn->data;
		      gchar * rnode_context_id = NULL;
		      gchar * rnode_label = NULL;

                      if (json_node_get_node_type (rnode) != JSON_NODE_OBJECT)
                        {
                          /* TODO - should log something or fail ? */
                          continue;
                        }

                      /* add each relationship with context_id and label */

                      JsonObject * robj = json_node_get_object (rnode);

		      if (json_object_has_member (robj, REQUEST_LINK_OBJ_CONTEXT_ID) == TRUE)
                        rnode_context_id = (gchar *)json_object_get_string_member (robj, REQUEST_LINK_OBJ_CONTEXT_ID);

		      if (json_object_has_member (robj, REQUEST_LINK_OBJ_LABEL) == TRUE)
                        rnode_label = (gchar *)json_object_get_string_member (robj, REQUEST_LINK_OBJ_LABEL);
                      else
  		        json_object_set_string_member (robj, REQUEST_LINK_OBJ_LABEL, label);

//g_message("request_record_insert: context_id=%s label=%s rnode_context_id=%s rnode_label=%s\n", context_id, label, rnode_context_id, rnode_label);

		      /* TODO - rework this to report errors to poort user ! perhaps using contextual logging if useful */

		      if ( ((rnode_context_id != NULL ) && (g_strcmp0 (rnode_context_id, context_id)))
		           || ((rnode_label != NULL ) && (g_strcmp0 (rnode_label, label))))
        	        {
			  JsonObject * error_obj = json_object_new ();
                          GString * str = g_string_new("");
                          g_string_append_printf (str, "Relationship record (%s, %s) has an invalid context_id, not a valid label or cannot be added to document",
                                                rnode_context_id, rnode_label);
                          gchar * error_msg = g_string_free (str, FALSE);
			  json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, error_msg);
			  json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, DSHttpStatusList[HTTP_STATUS_400].body);
                          g_free (error_msg);
                          json_array_add_object_element (record_response_relationships_label_array, error_obj);

			  continue;
        		}

		      if (request_link_record_insert (client, arguments, rnode,
						      dbname, NULL, context_id, code, DP_LINK_TYPE_RELATIONSHIP, &relationships_response_list) == FALSE)
        	        {
			  JsonObject * error_obj = json_object_new ();
			  GString * str = g_string_new("");
			  if (client->dupin_error_msg != NULL)
                            {
			      g_string_append_printf (str, "%s", client->dupin_error_msg);
        		      g_free (client->dupin_error_msg);
			      client->dupin_error_msg = NULL;
			    }
			  else
                            {
			      g_string_append_printf (str, "Relationship record (%s, %s) cannnot be inserted", rnode_context_id, rnode_label);
                            }
			  gchar * error_msg = g_string_free (str, FALSE);
			  json_object_set_string_member (error_obj, RESPONSE_STATUS_REASON, error_msg);
			  json_object_set_string_member (error_obj, RESPONSE_STATUS_ERROR, DSHttpStatusList[*code].body);
                          g_free (error_msg);
                          json_array_add_object_element (record_response_relationships_label_array, error_obj);

  			  while (relationships_response_list)
                            {
			      json_node_free (relationships_response_list->data);
			      relationships_response_list = g_list_remove (relationships_response_list, relationships_response_list->data);
			    }

			  continue;
        		}

//g_message("request_record_insert: DONE relationship context_id=%s label=%s rnode_context_id=%s rnode_label=%s\n", context_id, label, rnode_context_id, rnode_label);

  	              while (relationships_response_list)
                        {
      			  json_array_add_element (record_response_relationships_label_array, (JsonNode *)relationships_response_list->data);
			  relationships_response_list = g_list_remove (relationships_response_list, relationships_response_list->data);
			}
                    }
                  g_list_free (snodes);
                }

	      if (json_array_get_length (record_response_relationships_label_array) == 0)
                {
                  json_node_free (record_response_relationships_label_node);
                }
              else
                {
                  json_object_set_member (record_response_relationships_object, label, record_response_relationships_label_node);
                }
            }
          g_list_free (nodes);

	  if (json_object_get_size (record_response_relationships_object) == 0)
            {
              json_node_free (record_response_relationships_node);
            }
          else
            {
  	      json_object_set_member (record_response_obj, RESPONSE_OBJ_RELATIONSHIPS, record_response_relationships_node);
            }
        }

      dupin_linkbase_unref (linkb);
    }

  if (attachments_node != NULL)
    json_node_free (attachments_node);

  if (links_node != NULL)
    json_node_free (links_node);

  if (relationships_node != NULL)
    json_node_free (relationships_node);

  if (mvcc != NULL)
    g_free (mvcc);

  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_ID, (gchar *) dupin_record_get_id (record));
  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_REV, dupin_record_get_last_revision (record));

  dupin_record_close (record);
  dupin_database_unref (db);

  *response_list = g_list_prepend (*response_list, record_response_node);

  *code = retcode;
  return TRUE;
}

static gchar *
request_record_insert_rev (JsonNode * obj_node)
{
  gchar * mvcc=NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_OBJ_REV) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_OBJ_REV);

  if (node == NULL
      || json_node_get_node_type  (node) != JSON_NODE_VALUE
      || json_node_get_value_type (node) != G_TYPE_STRING)
    return NULL;

  mvcc = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_OBJ_REV); 

  return mvcc;
}

static gchar *
request_record_insert_id (JsonNode * obj_node)
{
  gchar *id = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_OBJ_ID) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_OBJ_ID);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    id = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_OBJ_ID); 

  return id;
}

static gboolean
request_record_insert_deleted (JsonNode * obj_node)
{
  gboolean deleted=FALSE;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_OBJ_DELETED) == FALSE)
    return FALSE;

  node = json_object_get_member (obj, REQUEST_OBJ_DELETED);

  if (node == NULL
      || json_node_get_node_type  (node) != JSON_NODE_VALUE
      || json_node_get_value_type (node) != G_TYPE_BOOLEAN)
    return FALSE;

  deleted = json_node_get_boolean (node);

  json_object_remove_member (obj, REQUEST_OBJ_DELETED); 

  return deleted;
}

static gchar *
request_link_record_insert_label (JsonNode * obj_node)
{
  gchar *ret = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_LINK_OBJ_LABEL) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_LINK_OBJ_LABEL);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    ret = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_LINK_OBJ_LABEL); 

  return ret;
}

static gchar *
request_link_record_insert_href (JsonNode * obj_node)
{
  gchar *ret = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_LINK_OBJ_HREF) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_LINK_OBJ_HREF);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    ret = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_LINK_OBJ_HREF); 

  return ret;
}

static gchar *
request_link_record_insert_rel (JsonNode * obj_node)
{
  gchar *ret = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_LINK_OBJ_REL) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_LINK_OBJ_REL);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    ret = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_LINK_OBJ_REL); 

  return ret;
}

static gchar *
request_link_record_insert_tag (JsonNode * obj_node)
{
  gchar *ret = NULL;
  JsonNode *node;
  JsonObject *obj;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  obj = json_node_get_object (obj_node);

  if (json_object_has_member (obj, REQUEST_LINK_OBJ_TAG) == FALSE)
    return NULL;

  node = json_object_get_member (obj, REQUEST_LINK_OBJ_TAG);

  if (node == NULL)
    return NULL;

  if (json_node_get_value_type (node) == G_TYPE_STRING) /* check this is correct type */
    ret = g_strdup (json_node_get_string (node));

  json_object_remove_member (obj, REQUEST_LINK_OBJ_TAG); 

  return ret;
}

static gboolean
request_link_record_insert (DSHttpdClient * client, GList * arguments, JsonNode * obj_node,
		       	    gchar * linkbname, gchar * id, gchar * context_id,
			    DSHttpStatusCode * code,
			    DupinLinksType link_type,
			    GList ** response_list)
{
  DupinLinkB *linkb;
  DupinLinkRecord *record;
  DSHttpStatusCode retcode;

  gchar * mvcc=NULL;
  gchar * json_record_id;
  gchar * json_record_label;
  gchar * json_record_href;
  gchar * json_record_rel;
  gchar * json_record_tag;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);

  /* NOTE - context_id is purely internal and can not be set in any way by the user if not via
            the document is being linked from */

  g_return_val_if_fail (json_object_has_member (json_node_get_object (obj_node),
				REQUEST_LINK_OBJ_CONTEXT_ID) == FALSE, FALSE);

  gboolean to_delete = request_record_insert_deleted (obj_node);

  json_record_href = request_link_record_insert_href (obj_node);

  if (json_record_href != NULL)
    {
      gboolean is_weblink = dupin_util_is_valid_absolute_uri (json_record_href);

      if (link_type == DP_LINK_TYPE_WEB_LINK
	  && is_weblink == FALSE)
        {
          g_free (json_record_href);
          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("Expected a web link but a relationship was passed");
	  return FALSE;
        }
      else if (link_type == DP_LINK_TYPE_RELATIONSHIP
	       && is_weblink == TRUE)
        {
          g_free (json_record_href);
          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("Expected a relationship but a web link was passed");
	  return FALSE;
        }
      /* else is auto, picked by the system */
    }

  JsonNode * record_response_node = json_node_new (JSON_NODE_OBJECT);
  JsonObject * record_response_obj = json_object_new ();
  json_node_take_object (record_response_node, record_response_obj);

  /* fetch the _rev field in the record first, if there */
  mvcc = request_record_insert_rev (obj_node);

  /* otherwise check passed parameters */
  if (mvcc == NULL)
    {
      GList * l=NULL;
      for (l = arguments; l; l = l->next)
        {
          dupin_keyvalue_t *kv = l->data;

          if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
            {
              if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
                {
                  *code = HTTP_STATUS_400;
                  if (client->dupin_error_msg != NULL)
                    g_free (client->dupin_error_msg);
                  client->dupin_error_msg = g_strdup ("Invalid link record MVCC revision");
		  if (record_response_node != NULL)
                    json_node_free (record_response_node);
  		  if (json_record_href)
		    g_free (json_record_href);

                  return FALSE;
                }
              mvcc = g_strdup (kv->value);
            }
        }
    }

  if (!(linkb = dupin_linkbase_open (client->thread->data->dupin, linkbname, NULL)))
    {
      *code = HTTP_STATUS_400;
      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);
      client->dupin_error_msg = g_strdup ("Cannot connect to linkbase");
      if (record_response_node != NULL)
        json_node_free (record_response_node);
      if (json_record_href)
        g_free (json_record_href);
      return FALSE;
    }

  if ((json_record_id = request_record_insert_id (obj_node)))
    {
      if (id && g_strcmp0 (id, json_record_id))
	{
          if (mvcc != NULL)
	    g_free (mvcc);
	  g_free (json_record_id);
          dupin_linkbase_unref (linkb);
	  *code = HTTP_STATUS_400;
          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
	  client->dupin_error_msg = g_strdup ("Specified link record id does not match");
	  if (record_response_node != NULL)
            json_node_free (record_response_node);
          if (json_record_href)
            g_free (json_record_href);
	  return FALSE;
	}

      id = json_record_id;
    }

  if (mvcc != NULL && !id)
    {
      if (json_record_id != NULL)
        g_free (json_record_id);
      if (mvcc != NULL)
        g_free (mvcc);
      dupin_linkbase_unref (linkb);
      *code = HTTP_STATUS_400;
      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);
      client->dupin_error_msg = g_strdup ("No valid link record id or MVCC specified");
      if (record_response_node != NULL)
        json_node_free (record_response_node);
      if (json_record_href)
        g_free (json_record_href);
      return FALSE;
    }

  json_record_label = request_link_record_insert_label (obj_node);
  json_record_rel = request_link_record_insert_rel (obj_node);
  json_record_tag = request_link_record_insert_tag (obj_node);

//g_message("request_link_record_insert: context_id=%s\n", context_id);
//g_message("request_link_record_insert: json_record_label=%s\n", json_record_label);
//g_message("request_link_record_insert: json_record_href=%s\n", json_record_href);
//g_message("request_link_record_insert: json_record_rel=%s\n", json_record_rel);
//g_message("request_link_record_insert: json_record_tag=%s\n", json_record_tag);

  if (mvcc != NULL)
    {
      retcode = HTTP_STATUS_200;

      record = dupin_link_record_read (linkb, id, NULL);

      if ( to_delete == TRUE )
        {
          if (!record || dupin_util_mvcc_revision_cmp (mvcc, dupin_link_record_get_last_revision (record))
	      || dupin_link_record_delete (record, NULL) == FALSE)
	    {
              if (record)
	        dupin_link_record_close (record);
	      record = NULL;
	    }
        }
      else
        {
          if (!record || dupin_util_mvcc_revision_cmp (mvcc, dupin_link_record_get_last_revision (record))
	      || dupin_link_record_update (record, obj_node, 
				       json_record_label, json_record_href, json_record_rel, json_record_tag,
					NULL) == FALSE)
	    {
              if (record)
	        dupin_link_record_close (record);
	      record = NULL;
	    }
        }
    }

  else if (!id)
    {
      if ( to_delete == TRUE )
        {
          if (record)
	    dupin_link_record_close (record);
	  record = NULL;
        }
      else
        {
          retcode = HTTP_STATUS_201;
          record = dupin_link_record_create (linkb, obj_node,
				         context_id,
				         json_record_label, json_record_href, json_record_rel, json_record_tag,
					 NULL);
	}
    }

  else
    {
      if ( to_delete == TRUE )
        {
          if (record)
	    dupin_link_record_close (record);
	  record = NULL;
        }
      else
        {
          retcode = HTTP_STATUS_201;

          if (dupin_link_record_exists (linkb, id) == FALSE)
	    record = dupin_link_record_create_with_id (linkb, obj_node, id, 
				         	   context_id,
						   json_record_label,
				         	   json_record_href, json_record_rel, json_record_tag,
						   NULL);
          else
	    record = NULL;
        }
    }

  if (json_record_label)
    g_free (json_record_label);

  if (json_record_href)
    g_free (json_record_href);

  if (json_record_rel)
    g_free (json_record_rel);

  if (json_record_tag)
    g_free (json_record_tag);

  if (json_record_id)
    g_free (json_record_id);

  if (!record)
    {
      dupin_linkbase_unref (linkb);
      *code = HTTP_STATUS_409;
      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);

      if (to_delete == TRUE)
        {
          if (mvcc == NULL)
            client->dupin_error_msg = g_strdup ("Deleted flag not allowed on link record creation");
          else
            client->dupin_error_msg = g_strdup ("Cannot delete link record");
        }
      else if (mvcc != NULL)
        client->dupin_error_msg = g_strdup ("Cannot update link record");
      else
        client->dupin_error_msg = g_strdup ("Cannot insert link record");

      if (mvcc != NULL)
        g_free (mvcc);

      if (record_response_node != NULL)
        json_node_free (record_response_node);
      return FALSE;
    }

  if (mvcc != NULL)
    g_free (mvcc);

  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_ID, (gchar *) dupin_link_record_get_id (record));
  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_REV, dupin_link_record_get_last_revision (record));

  dupin_link_record_close (record);
  dupin_linkbase_unref (linkb);

  *response_list = g_list_prepend (*response_list, record_response_node);

  *code = retcode;
  return TRUE;
}

/* NOTE - it receives as second param a list of JsonNode so we are completly generic
          and let the caller to decide how and what to return as JSON. We just add
	  an "ok": true status and return one JSON object when response is single (I.e. one item in the list).
	  Otherwise we return a JSON Array with the info of the caller.
*/

static gboolean
request_record_response (DSHttpdClient * client,
			 GList * response_list)
{
  JsonGenerator *gen=NULL;
  JsonNode *response_node=NULL;

  if (g_list_length (response_list) == 0)
    return FALSE;

  if (g_list_length (response_list) == 1)
    {
      response_node = (JsonNode *) response_list->data;

      if (json_node_get_node_type (response_node) != JSON_NODE_OBJECT)
        return FALSE;
 
      response_node = json_node_copy (response_node);

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

static gboolean
request_record_attachment_insert (DSHttpdClient * client,
				  GList * arguments,
				  gchar * dbname, gchar * id,
				  GList * title_parts,
				  DSHttpStatusCode * code,
				  GList ** response_list)
{
  DupinDB *db;
  DupinAttachmentDB *attachment_db;
  DupinRecord *record=NULL;
  DSHttpStatusCode retcode;
  JsonNode * obj_node=NULL;

  GString *str;
  gchar * title = NULL;
  GList * l=NULL;
  gchar * mvcc=NULL;

  /* process input attachment name parameter */

  str = g_string_new (title_parts->data);

  for (l=title_parts->next ; l != NULL ; l=l->next)
    {
      g_string_append_printf (str, "/%s", (gchar *)l->data);
    }

  title = g_string_free (str, FALSE);

  if (title == NULL)
    {
      *code = HTTP_STATUS_400;
      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);
      client->dupin_error_msg = g_strdup ("Missing attachment name");
      return FALSE;
    }

//g_message("request_record_attachment_insert: title=%s\n", title);

  for (l = arguments; l; l = l->next)
    {
      dupin_keyvalue_t *kv = l->data;

      if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
        {
          if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
            {
              *code = HTTP_STATUS_400;
              if (client->dupin_error_msg != NULL)
                g_free (client->dupin_error_msg);
	      client->dupin_error_msg = g_strdup ("Invalid record MVCC revision");
              return FALSE;
            }
	  mvcc = kv->value;
        }
    }

  if (!(db = dupin_database_open (client->thread->data->dupin, dbname, NULL)))
    {
      g_free (title);
      *code = HTTP_STATUS_400;
      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);
      client->dupin_error_msg = g_strdup ("Cannot connect to record database");
      return FALSE;
    }

  if (!
      (attachment_db =
       dupin_attachment_db_open (client->thread->data->dupin, dbname, NULL)))
    {
      g_free (title);
      dupin_database_unref (db);
      *code = HTTP_STATUS_400;
      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);
      client->dupin_error_msg = g_strdup ("Cannot connect to attacchemtns database");
      return FALSE;
    }

  record = dupin_record_read (db, id, NULL);

  if (mvcc == NULL && record != NULL)
    {
      g_free (title);
      dupin_record_close (record);
      dupin_attachment_db_unref (attachment_db);
      dupin_database_unref (db);
      *code = HTTP_STATUS_400;
      if (client->dupin_error_msg != NULL)
        g_free (client->dupin_error_msg);
      client->dupin_error_msg = g_strdup ("Record found but MVCC revision number is missing");
      return FALSE;
    }

  if (record == NULL)
    {
      /* TODO - create new record instead */
     obj_node = json_node_new (JSON_NODE_OBJECT);
     JsonObject * obj = json_object_new ();
     json_node_take_object (obj_node, obj);

     if ( dupin_attachment_record_insert (attachment_db, id, title,
                                          client->body_size,
                                          client->input_mime,
                                          NULL,
                                          (const void *)client->body) == FALSE
 	 || (!( record = dupin_record_create_with_id (db, obj_node, id, NULL))))
        {
          g_free (title);
          json_node_free (obj_node);
          dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          *code = HTTP_STATUS_400;
          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("Cannot insert attachment or create record to contain attachment");
          return FALSE;
        }
    }
  else
    {
      if (mvcc == NULL
          || dupin_util_mvcc_revision_cmp (mvcc, dupin_record_get_last_revision (record)))
        {
          g_free (title);
          dupin_record_close (record);
          dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          *code = HTTP_STATUS_404;
          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("No record MVCC revision found or record revision not matching latest");
          return FALSE;
        }

      /* NOTE - need to touch/update the metadata record anyway */

      if (!(obj_node = request_record_revision_obj (client, arguments,
						    record,
						    (gchar *) dupin_record_get_id (record), mvcc)))
        {
          g_free (title);
          dupin_record_close (record);
          dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          *code = HTTP_STATUS_404;
          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("Cannot fetch record for update");
          return FALSE;
        }

      /* NOTE - remove _rev and _id from the JSON node structure */

      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_REV);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_ID);

      if ( dupin_attachment_record_delete (attachment_db, id, title) == FALSE
          || dupin_attachment_record_insert (attachment_db, id, title,
                                          client->body_size,
                                          client->input_mime,
                                          NULL,
                                          (const void *)client->body) == FALSE
          || dupin_record_update (record, obj_node, NULL) == FALSE)
        {
          g_free (title);
          dupin_record_close (record);
          dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          json_node_free (obj_node);
          *code = HTTP_STATUS_404;
          if (client->dupin_error_msg != NULL)
            g_free (client->dupin_error_msg);
          client->dupin_error_msg = g_strdup ("Cannot replace attachment");
          return FALSE;
        }
    }

  retcode = HTTP_STATUS_200;

  JsonNode * record_response_node = json_node_new (JSON_NODE_OBJECT);
  JsonObject * record_response_obj = json_object_new ();
  json_node_take_object (record_response_node, record_response_obj);

  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_ID, (gchar *) dupin_record_get_id (record));
  json_object_set_string_member (record_response_obj, RESPONSE_OBJ_REV, dupin_record_get_last_revision (record));

  dupin_record_close (record);
  dupin_attachment_db_unref (attachment_db);
  dupin_database_unref (db);
  g_free (title);
  json_node_free (obj_node);

  *response_list = g_list_prepend (*response_list, record_response_node);

  *code = retcode;

  return TRUE;
}

static JsonNode *
request_record_revision_obj (DSHttpdClient * client,
                             GList * arguments,
			     DupinRecord * record,
			     gchar * id, gchar * mvcc)
{
  g_return_val_if_fail (client != NULL, NULL);
  g_return_val_if_fail (record != NULL, NULL);

  JsonNode *obj_node;
  JsonObject *obj;

  if (dupin_record_is_deleted (record, mvcc) == TRUE)
    {
      obj_node = json_node_new (JSON_NODE_OBJECT);

      if (obj_node == NULL)
        return NULL;

      obj = json_object_new ();

      if (obj == NULL)
        {
          json_node_free (obj_node);
          return NULL;
        }

      json_node_take_object (obj_node, obj);

      json_object_set_boolean_member (obj, RESPONSE_OBJ_DELETED, TRUE);
    }

  else
    {
      obj_node = dupin_record_get_revision_node (record, mvcc);

      if (obj_node == NULL)
        return NULL;

      obj_node = json_node_copy (obj_node);

      obj = json_node_get_object (obj_node);
    }

  /* Setting _id and _rev: */
  json_object_set_string_member (obj, REQUEST_OBJ_ID, id);
  json_object_set_string_member (obj, REQUEST_OBJ_REV, mvcc);

  /* Setting links and relationships if requested */
  if (arguments != NULL)
    {
      DupinLinkB * linkb=NULL;

      DupinLinksType include_links_type = DP_LINK_TYPE_NONE;
      gchar * tag = NULL;
      gchar ** link_labels=NULL;
      gboolean include_links_weblinks_descending = FALSE;
      guint include_links_weblinks_count = DUPIN_LINKB_MAX_LINKS_COUNT;
      guint include_links_weblinks_offset = 0;
      gboolean include_links_relationships_descending = FALSE;
      guint include_links_relationships_count = DUPIN_LINKB_MAX_LINKS_COUNT;
      guint include_links_relationships_offset = 0;
      GList * list = NULL;

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

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS))
            {
              link_labels = g_strsplit (kv->value, ",", -1);
            }

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TAG)
	           && g_strcmp0 (kv->value, ""))
            tag = kv->value;

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_DESCENDING)
	           && !g_strcmp0 (kv->value, "true"))
	    include_links_weblinks_descending = TRUE;

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_COUNT))
	    include_links_weblinks_count = atoi (kv->value);

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_OFFSET))
	    include_links_weblinks_offset = atoi (kv->value);

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_DESCENDING)
	           && !g_strcmp0 (kv->value, "true"))
	    include_links_relationships_descending = TRUE;

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_COUNT))
	    include_links_relationships_count = atoi (kv->value);

          else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_OFFSET))
	    include_links_relationships_offset = atoi (kv->value);
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

              if (link_labels)
                g_strfreev (link_labels);

              client->dupin_error_msg = g_strdup ("Cannot connect to linkbase");

	      /* TODO - pass the ret code here as well ? */
              return NULL;
            }

	  if (include_links_type == DP_LINK_TYPE_ANY
	      || include_links_type == DP_LINK_TYPE_WEB_LINK)
            {
	      JsonNode * links_node = json_node_new (JSON_NODE_OBJECT);
	      JsonObject * links_obj = json_object_new ();
	      json_node_take_object (links_node, links_obj);

              gsize total_links = dupin_linkbase_count (linkb, DP_LINK_TYPE_WEB_LINK, DP_COUNT_EXIST,
					            (gchar *) dupin_record_get_id (record),
					            link_labels, tag);

              if (dupin_link_record_get_list (linkb, include_links_weblinks_count, include_links_weblinks_offset,
					      0, 0, DP_LINK_TYPE_WEB_LINK, DP_COUNT_EXIST, DP_ORDERBY_ROWID,
					      include_links_weblinks_descending,
					      (gchar *) dupin_record_get_id (record), link_labels, tag, &results, NULL) == FALSE)
                {
		  // just log the error and reason into JSON
                }

              for (list = results; list; list = list->next)
                {
                  DupinLinkRecord *link_record = list->data;
      		  JsonNode *on;

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
								 dupin_link_record_get_last_revision (link_record))))
        	    {
		      // just log the error and reason into JSON
        	    }

                  // remove context_id and label due they are implied

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

	  if (include_links_type == DP_LINK_TYPE_ANY
	      || include_links_type == DP_LINK_TYPE_RELATIONSHIP)
            {
	      JsonNode * relationships_node = json_node_new (JSON_NODE_OBJECT);
	      JsonObject * relationships_obj = json_object_new ();
	      json_node_take_object (relationships_node, relationships_obj);

              gsize total_relationships = dupin_linkbase_count (linkb, DP_LINK_TYPE_RELATIONSHIP,
						    DP_COUNT_EXIST,
					            (gchar *) dupin_record_get_id (record),
					            link_labels, tag);

              if (dupin_link_record_get_list (linkb, include_links_relationships_count, include_links_relationships_offset,
					      0, 0, DP_LINK_TYPE_RELATIONSHIP, DP_COUNT_EXIST, DP_ORDERBY_ROWID,
					      include_links_relationships_descending,
					      (gchar *) dupin_record_get_id (record), link_labels, tag, &results, NULL) == FALSE)
                {
		  // just log the error and reason into JSON
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
								 dupin_link_record_get_last_revision (link_record))))
        	    {
		      // just log the error and reason into JSON
        	    }

                  // remove context_id and label due they are implied

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

          if (linkb != NULL)
            dupin_linkbase_unref (linkb);
        }

      if (link_labels)
        g_strfreev (link_labels);
    }

  return obj_node;
}

static JsonNode *
request_link_record_revision_obj (DSHttpdClient * client, GList * arguments,
				  DupinLinkRecord * record,
				  gchar * id, gchar * mvcc)
{
  JsonNode *obj_node;
  JsonObject *obj;

  if (dupin_link_record_is_deleted (record, mvcc) == TRUE)
    {
      obj_node = json_node_new (JSON_NODE_OBJECT);

      if (obj_node == NULL)
        return NULL;

      obj = json_object_new ();

      if (obj == NULL)
        {
          json_node_free (obj_node);
          return NULL;
        }

      json_node_take_object (obj_node, obj);

      json_object_set_boolean_member (obj, RESPONSE_OBJ_DELETED, TRUE);
    }

  else
    {
      obj_node = dupin_link_record_get_revision_node (record, mvcc);

      if (obj_node == NULL)
        return NULL;

      obj_node = json_node_copy (obj_node);

      obj = json_node_get_object (obj_node);

      json_object_set_string_member (obj, RESPONSE_LINK_OBJ_CONTEXT_ID, dupin_link_record_get_context_id (record));
      json_object_set_string_member (obj, RESPONSE_LINK_OBJ_LABEL, dupin_link_record_get_label (record));
      json_object_set_string_member (obj, RESPONSE_LINK_OBJ_HREF, dupin_link_record_get_href (record));

      gchar * rel = (gchar *)dupin_link_record_get_rel (record);

      if (rel != NULL)
        json_object_set_string_member (obj, RESPONSE_LINK_OBJ_REL, rel);

      gchar * tag = (gchar *)dupin_link_record_get_tag (record);

      if (tag != NULL)
        json_object_set_string_member (obj, RESPONSE_LINK_OBJ_TAG, tag);
    }

  /* Setting _id and _rev: */
  json_object_set_string_member (obj, REQUEST_OBJ_ID, id);
  json_object_set_string_member (obj, REQUEST_OBJ_REV, mvcc);

  return obj_node;
}

static JsonNode *
request_view_record_obj (DSHttpdClient * client,
			 GList * arguments,
			 DupinViewRecord * record, gchar * id)
{
  JsonNode * node = dupin_view_record_get (record);

  if (node == NULL)
    return NULL;

  return json_node_copy (node);
}

dupin_keyvalue_t *
dupin_keyvalue_new (gchar * key, gchar * value)
{
  dupin_keyvalue_t *new;

  g_return_val_if_fail (key != NULL, NULL);
  g_return_val_if_fail (value != NULL, NULL);

  new = g_malloc0 (sizeof (dupin_keyvalue_t));
  new->key = g_strdup (key);
  new->value = g_strdup (value);

  return new;
}

void
dupin_keyvalue_destroy (dupin_keyvalue_t * data)
{
  if (!data)
    return;

  if (data->key)
    g_free (data->key);

  if (data->value)
    g_free (data->value);

  g_free (data);
}

#define DUPIN_DB_MAX_CHANGES_COMET_COUNT  1

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

//g_message("request_get_changes_comet_database: count=%d offset=%d\n", (gint)count, (gint)offset);

  if (client->output.changes_comet.change_generated == FALSE)
    {
      if (dupin_database_get_changes_list (client->output.changes_comet.db,
                                       DUPIN_DB_MAX_CHANGES_COMET_COUNT,
                                       client->output.changes_comet.change_results_offset,
                                       client->output.changes_comet.param_since+1,
                                       0,
                                       client->output.changes_comet.param_style,
                                       DP_COUNT_CHANGES, DP_ORDERBY_ROWID,
                                       client->output.changes_comet.param_descending, &results, NULL) == FALSE)
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
							    db_record, record_id, record_mvcc)))
                    {
                      json_node_free (change);
                      goto request_get_changes_comet_database_error;
                    }

                  dupin_record_close (db_record);

                  json_object_set_member (on_obj, "doc", doc);
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
                         && client->output.changes_comet.change_last_seq == (client->output.changes_comet.param_since+1))))
              g_string_append_printf (str,"], \"last_seq\": %" G_GSIZE_FORMAT " }", client->output.changes_comet.change_last_seq);

            client->output.changes_comet.change_generated = TRUE;

            client->output.changes_comet.change_results_offset += g_list_length (results);
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

#define DUPIN_LINKB_MAX_CHANGES_COMET_COUNT  1

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

//g_message("request_get_changes_comet_linkbase: count=%d offset=%d\n", (gint)count, (gint)offset);

  if (client->output.changes_comet.change_generated == FALSE)
    {
      if (dupin_linkbase_get_changes_list (client->output.changes_comet.linkb,
                                       DUPIN_DB_MAX_CHANGES_COMET_COUNT,
                                       client->output.changes_comet.change_results_offset,
                                       client->output.changes_comet.param_since+1,
                                       0,
                                       client->output.changes_comet.param_style,
                                       DP_COUNT_CHANGES, DP_ORDERBY_ROWID,
                                       client->output.changes_comet.param_descending,
                                       client->output.changes_comet.param_context_id,
                                       client->output.changes_comet.param_tag,
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
								  linkb_record, record_id, record_mvcc)))
                    {
                      json_node_free (change);
                      goto request_get_changes_comet_linkbase_error;
                    }

                  dupin_link_record_close (linkb_record);

                  json_object_set_member (on_obj, "link", link);
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
                         && client->output.changes_comet.change_last_seq == (client->output.changes_comet.param_since+1))))
              g_string_append_printf (str,"], \"last_seq\": %" G_GSIZE_FORMAT " }", client->output.changes_comet.change_last_seq);

            client->output.changes_comet.change_generated = TRUE;

            client->output.changes_comet.change_results_offset += g_list_length (results);
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

/* EOF */
