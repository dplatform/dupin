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

#define REQUEST_OBJ_ID		"_id"
#define REQUEST_OBJ_REV		"_rev"
#define REQUEST_OBJ_ATTACHMENTS	"_attachments"

#define RESPONSE_OBJ_ID		"id"
#define RESPONSE_OBJ_REV	"rev"

#define DUPIN_DB_MAX_DOCS_COUNT     50
#define DUPIN_VIEW_MAX_DOCS_COUNT   50
#define DUPIN_ATTACHMENTS_COUNT	    100
#define DUPIN_REVISIONS_COUNT	    100
#define DUPIN_DB_MAX_CHANGES_COUNT  100

static JsonNode *request_record_obj (DupinRecord * record, gchar * id,
					     gchar * mvcc);
static JsonNode *request_view_record_obj (DupinViewRecord * record,
						  gchar * id);

static gboolean request_record_insert (DSHttpdClient * client,
				       JsonNode * obj_node, gchar * dbname,
				       gchar * id, DSHttpStatusCode * code,
				       DupinRecord ** ret_record);
static gboolean request_record_attachment_insert (DSHttpdClient * client,
				                  gchar * dbname, gchar * id,
						  GList * title_parts,
						  GList * arguments,
						  DSHttpStatusCode * code,
				                  DupinRecord ** ret_record);
static gboolean request_record_response_single (DSHttpdClient * client,
						DupinRecord * record);
static gboolean request_record_response_multi (DSHttpdClient * client,
					       GList * list);

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
	      g_free (newpath);
	      return HTTP_STATUS_403;
	    }

	  path = newpath;
	}
    }

  if (g_stat (path, &st) != 0)
    {
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
  //g_warning ("_quit disabled\n");
  //return HTTP_STATUS_503;

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
    return HTTP_STATUS_500;

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

  return HTTP_STATUS_400;
}

/* GET *********************************************************************/
#define REQUEST_ALL_DBS		"_all_dbs"
#define REQUEST_ALL_VIEWS	"_all_views"
#define REQUEST_ALL_CHANGES	"_changes"
#define REQUEST_ALL_DOCS	"_all_docs"
#define REQUEST_VIEWS		"_views"
#define REQUEST_SYNC		"_sync"
#define REQUEST_UUIDS		"_uuids"
#define REQUEST_UUIDS_COUNT	"count"

static DSHttpStatusCode request_global_get_all_dbs (DSHttpdClient * client,
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
static DSHttpStatusCode request_global_get_changes (DSHttpdClient *
						    client,
						    GList * paths,
						    GList * arguments);
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
    return HTTP_STATUS_400;

  /* GET /_all_dbs */
  if (!g_strcmp0 (path->data, REQUEST_ALL_DBS))
    return request_global_get_all_dbs (client, path, arguments);

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
    return request_global_get_changes (client, path, arguments);

  /* GET /database/_all_docs */
  if (!path->next->next && !g_strcmp0 (path->next->data, REQUEST_ALL_DOCS))
    return request_global_get_all_docs (client, path, arguments);

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

      return HTTP_STATUS_400;
    }

  /* GET /database/id */
  return request_global_get_record (client, path, arguments);

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
    return HTTP_STATUS_400;

  array = json_array_new ();

  if (array == NULL)
    return HTTP_STATUS_500;

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
    return HTTP_STATUS_400;

  array = json_array_new ();

  if (array == NULL)
    return HTTP_STATUS_500;

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
    return HTTP_STATUS_400;

  array = json_array_new ();

  if (array == NULL)
    return HTTP_STATUS_500;

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

#define REQUEST_GET_ALL_CHANGES_SINCE	   "since"
#define REQUEST_GET_ALL_CHANGES_STYLE	   "style"
#define REQUEST_GET_ALL_CHANGES_FEED	   "feed"

#define REQUEST_GET_ALL_CHANGES_STYLE_DEFAULT	"main_only"
#define REQUEST_GET_ALL_CHANGES_FEED_DEFAULT	"poll"

static DSHttpStatusCode
request_global_get_changes (DSHttpdClient * client, GList * path,
			    GList * arguments)
{
  DupinDB *db;

  GList *list;
  GList *results;

  gboolean descending = FALSE;
  guint count = DUPIN_DB_MAX_CHANGES_COUNT;
  guint offset = 0;

  gsize total_rows = 0;

  gsize last_seq=0;
  gsize since = 0;
  DupinChangesType style = DP_CHANGES_MAIN_ONLY;
  gchar * feed = REQUEST_GET_ALL_CHANGES_FEED_DEFAULT;
  gboolean include_docs = FALSE;

  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_404;

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

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_SINCE))
	since = atoi (kv->value);

      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_STYLE)
	       && !g_strcmp0 (kv->value, "all_docs"))
        {
	  style = DP_CHANGES_ALL_DOCS;
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_CHANGES_FEED))
        {
	  if (!g_strcmp0 (kv->value, "longpoll"))
            {
	      feed = "longpoll";
              dupin_database_unref (db);
              return HTTP_STATUS_501;
            }
	  else if (!g_strcmp0 (kv->value, "continuous"))
            {
	      feed = "continuous";
              dupin_database_unref (db);
              return HTTP_STATUS_501;
            }
	  else if (!g_strcmp0 (kv->value, REQUEST_GET_ALL_CHANGES_FEED_DEFAULT))
            {
	      feed = REQUEST_GET_ALL_CHANGES_FEED_DEFAULT;
            }
          else
            {
              dupin_database_unref (db);
              return HTTP_STATUS_400;
            }
        }
      else if (!g_strcmp0 (kv->key, REQUEST_GET_ALL_DOCS_INCLUDE_DOCS))
        {
          if (g_strcmp0 (kv->value,"false") && g_strcmp0 (kv->value,"FALSE") &&
              g_strcmp0 (kv->value,"true") && g_strcmp0 (kv->value,"TRUE"))
            {
              dupin_database_unref (db);
              return HTTP_STATUS_400;
            }

          include_docs = (!g_strcmp0 (kv->value,"false") || !g_strcmp0 (kv->value,"FALSE")) ? FALSE : TRUE;
        }
    }

  if (dupin_database_get_total_changes (db, &total_rows, since, 0, DP_COUNT_CHANGES, TRUE, NULL) == FALSE)
    {
      dupin_database_unref (db);
      return HTTP_STATUS_500;
    }

  if (dupin_database_get_changes_list (db, count, offset, since, 0, style, DP_COUNT_CHANGES, DP_ORDERBY_ROWID, descending, &results, NULL) ==
      FALSE)
    {
      dupin_database_unref (db);
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_database_get_changes_list_close (results);
      dupin_database_unref (db);
      return HTTP_STATUS_500;
    }

  json_object_set_int_member (obj, "total_rows", total_rows);
  json_object_set_int_member (obj, "offset", offset);
  json_object_set_int_member (obj, "rows_per_page", count);

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_changes_error;

  for (list = results; list; list = list->next)
    {
      JsonNode * change = json_node_copy (list->data);

      if (include_docs == TRUE)
        {
          JsonObject * on_obj = json_node_get_object (change);

          gchar * record_id = (gchar *) json_object_get_string_member (on_obj, RESPONSE_OBJ_ID);

          JsonNode * doc = NULL;

          DupinRecord * db_record=NULL;
          if (!(db_record = dupin_record_read (db, record_id, NULL)))
            {
              json_node_free (change);
              json_array_unref (array);
              goto request_global_get_changes_error;
            }

          doc = json_node_copy (dupin_record_get_revision_node (db_record, NULL));

          dupin_record_close (db_record);

          json_object_set_member (on_obj, "doc", doc);
        }

      json_array_add_element (array, change);
    }

  json_object_set_array_member (obj, "results", array );

  if (dupin_database_get_max_rowid (db, &last_seq) == FALSE)
    {
      goto request_global_get_changes_error;
    }

  json_object_set_int_member (obj, "last_seq", last_seq);

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_global_get_changes_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_changes_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_changes_error;

  if( results )
    dupin_database_get_changes_list_close (results);

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  return HTTP_STATUS_200;

request_global_get_changes_error:

  if( results )
    dupin_database_get_changes_list_close (results);

  dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);

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
    return HTTP_STATUS_404;

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
      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if( results )
        dupin_record_get_list_close (results);

      dupin_database_unref (db);
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
	   request_record_obj (record, (gchar *) dupin_record_get_id (record),
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

  return HTTP_STATUS_500;
}

#define REQUEST_QUERY		"_query"

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
    return HTTP_STATUS_404;

  obj = json_object_new ();

  if (obj == NULL)
    {
      dupin_database_unref (db);
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
  json_object_set_boolean_member (obj, "compact_running", FALSE);

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
  return HTTP_STATUS_500;
}

#define REQUEST_RECORD_ARG_REV	"rev"
#define REQUEST_RECORD_ARG_REVS	"revs"

static DSHttpStatusCode
request_global_get_record (DSHttpdClient * client, GList * path,
			   GList * arguments)
{
  gchar * mvcc = NULL;
  gboolean allrevs = FALSE;

  GList *list, *results=NULL;

  DupinDB *db;
  DupinAttachmentDB *attachment_db;
  DupinRecord *record;

  gboolean descending = FALSE;
  guint count = DUPIN_REVISIONS_COUNT;
  guint offset = 0;

  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  gchar * doc_id=NULL;
  GList * title_parts=NULL;

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* GET /_special_document/document_ID or /_special_document/document_ID/attachment  */
      doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);
      title_parts = path->next->next->next;
    }
  else
    {
      /* GET /document_ID/attachment or /document_ID/attachment */
      doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
      title_parts = path->next->next;
    }

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      g_free (doc_id);
      return HTTP_STATUS_404;
    }

  if (!  (attachment_db =
                dupin_attachment_db_open (client->thread->data->dupin, path->data, NULL)))
    {
      dupin_database_unref (db);
      g_free (doc_id);
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
          return HTTP_STATUS_400;
        }

//g_message("request_global_get_record: title=%s\n", title);

      if ( dupin_attachment_record_exists (attachment_db, doc_id, title) == FALSE)
        {
          g_free (title);
          dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
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

      node = json_node_new (JSON_NODE_OBJECT);

      if (node == NULL)
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
	  return HTTP_STATUS_500;
	}

      obj = json_object_new ();

      if (obj == NULL)
        goto request_global_get_record_error;

      json_node_take_object (node, obj);

      array = json_array_new ();

      if (array == NULL)
        goto request_global_get_record_error;

      if (dupin_record_get_revisions_list (record,
				           count,
					   offset, 1, 0, DP_COUNT_ALL, DP_ORDERBY_REV, descending,
					   &revisions, NULL) == FALSE)
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
	  return HTTP_STATUS_500;
	}

      for (list = revisions; list; list = list->next)
        {
	  JsonNode *on;

	  if (!
	      (on =
	       request_record_obj (record,
				   (gchar *) dupin_record_get_id (record),
				   (gchar *) list->data)))
            {
              dupin_record_get_revisions_list_close (revisions);
	      json_array_unref (array);
	      goto request_global_get_record_error;
            }

          json_array_add_element( array, on);
        }
      dupin_record_get_revisions_list_close (revisions);

      json_object_set_array_member (obj, "_revs_info", array );
    }

  /* Show a single revision: */
  else
    {
      if (mvcc == NULL)
	mvcc = dupin_record_get_last_revision (record);

      else if (g_strcmp0 (mvcc, dupin_record_get_last_revision (record)) > 0)
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
	  return HTTP_STATUS_404;
	}

      if (!
	  (node =
	   request_record_obj (record, (gchar *) dupin_record_get_id (record),
			       mvcc)))
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
          dupin_attachment_db_unref (attachment_db);
          g_free (doc_id);
	  return HTTP_STATUS_404;
	}
    }

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
	  return HTTP_STATUS_500;
	}

      json_object_set_member (attachments_obj, dupin_attachment_record_get_title (list->data), attachment_node);
    }
  dupin_attachment_record_get_list_close (results);

  if (results)
    {
      if (allrevs == TRUE
          || (allrevs == FALSE && (!g_strcmp0 (mvcc, dupin_record_get_last_revision (record)))))
        json_object_set_object_member (json_node_get_object (node), REQUEST_OBJ_ATTACHMENTS, attachments_obj);
    }
  else
    {
      json_object_unref (attachments_obj);
    }

  /* Writing: */
  gen = json_generator_new();

  if (gen == NULL)
    goto request_global_get_record_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_global_get_record_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);

  dupin_record_close (record);
  dupin_database_unref (db);
  dupin_attachment_db_unref (attachment_db);

  g_free (doc_id);

  return HTTP_STATUS_200;

request_global_get_record_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);

  dupin_record_close (record);
  dupin_database_unref (db);
  dupin_attachment_db_unref (attachment_db);

  g_free (doc_id);

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
    return HTTP_STATUS_404;

  obj = json_object_new ();

  if (obj == NULL)
    {
      dupin_view_unref (view);
      return HTTP_STATUS_500;
    }

  json_object_set_string_member (obj, "view_name", (gchar *) dupin_view_get_name (view));

  subobj = json_object_new ();

  if (subobj == NULL)
    goto request_global_get_view_error;

  json_object_set_string_member (subobj, "name", (gchar *) dupin_view_get_parent (view));
  json_object_set_boolean_member (subobj, "is_db", dupin_view_get_parent_is_db (view));
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
    return HTTP_STATUS_404;

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
    return HTTP_STATUS_404;

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

              dupin_view_unref (view);
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
              return HTTP_STATUS_404;
            }
        }
      else
        {
          if (!(parent_view = dupin_view_open (view->d, view->parent, NULL)))
            {
              dupin_view_unref (view);
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
          json_parser_load_from_data (parser, startkey, -1, &error);

          if (error != NULL
              || (!(sn = dupin_util_json_serialize (json_parser_get_root (parser)))) )
            {
              g_free (startkey);

              if (endkey != NULL)
                g_free (endkey);

              if (parent_db != NULL)
                dupin_database_unref (parent_db);

              if (parent_view != NULL)
                dupin_view_unref (parent_view);

              dupin_view_unref (view);

              return HTTP_STATUS_400;
            }

          g_free (startkey);

          startkey = sn;
        }

      error = NULL;
      sn = NULL;

      if (endkey != NULL)
        {
          json_parser_load_from_data (parser, endkey, -1, &error);

          if (error != NULL
              || (!(sn = dupin_util_json_serialize (json_parser_get_root (parser)))) )
            {
              if (startkey != NULL)
                g_free (startkey);

              g_free (endkey);

              if (parent_db != NULL)
                dupin_database_unref (parent_db);

              if (parent_view != NULL)
                dupin_view_unref (parent_view);

              dupin_view_unref (view);

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

      if (parent_view != NULL)
        dupin_view_unref (parent_view);

      dupin_view_unref (view);

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

      if (parent_view != NULL)
        dupin_view_unref (parent_view);

      dupin_view_unref (view);

      return HTTP_STATUS_500;
    }

  obj = json_object_new ();

  if (obj == NULL)
    {
      if (parent_db != NULL)
        dupin_database_unref (parent_db);

      if (parent_view != NULL)
        dupin_view_unref (parent_view);

      if( results )
        dupin_view_record_get_list_close(results);

      dupin_view_unref (view);

      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

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
	   request_view_record_obj (record,
				    (gchar *)
				    dupin_view_record_get_id (record))))
        {
          json_array_unref (array);
	  goto request_global_get_all_docs_view_error;
        }

      if (include_docs == TRUE)
        {
	  JsonObject * on_obj = json_node_get_object (on);

          gchar * record_id = (gchar *) json_object_get_string_member (on_obj, RESPONSE_OBJ_ID);

	  JsonNode * doc = NULL;

          if (dupin_view_get_parent_is_db (view) == TRUE)
            {
	      DupinRecord * db_record=NULL;
              if (!(db_record = dupin_record_read (parent_db, record_id, NULL)))
                {
                  json_node_free (on);
                  json_array_unref (array);
	          goto request_global_get_all_docs_view_error;
                }

              doc = json_node_copy (dupin_record_get_revision_node (db_record, NULL));

	      dupin_record_close (db_record);
            }
          else
            {
              DupinViewRecord * view_record=NULL;
              if (!(view_record = dupin_view_record_read (parent_view, record_id, NULL)))
                {
                  json_node_free (on);
                  json_array_unref (array);
	          goto request_global_get_all_docs_view_error;
                }

              doc = json_node_copy (dupin_view_record_get (view_record));

	      dupin_view_record_close (view_record);
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

  if (parent_view != NULL)
    dupin_view_unref (parent_view);

  if( results )
    dupin_view_record_get_list_close(results);

  dupin_view_unref (view);

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
    return HTTP_STATUS_404;

  if (!(record = dupin_view_record_read (view, path->next->next->data, NULL)))
    {
      dupin_view_unref (view);
      return HTTP_STATUS_404;
    }

  if (!
      (node =
       request_view_record_obj (record,
				(gchar *) dupin_view_record_get_id (record))))
    {
      dupin_view_record_close (record);
      dupin_view_unref (view);
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
    return HTTP_STATUS_404;

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
    return HTTP_STATUS_404;

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
  return HTTP_STATUS_500;
}

/* POST ********************************************************************/
#define REQUEST_POST_BULK_DOCS		"_bulk_docs"
#define REQUEST_POST_BULK_DOCS_DOCS	"docs"

static DSHttpStatusCode request_global_post_record (DSHttpdClient * client,
						    GList * path,
						    GList * arguments);
static DSHttpStatusCode request_global_post_bulk_docs (DSHttpdClient * client,
						       GList * path,
						       GList * arguments);

static DSHttpStatusCode
request_global_post (DSHttpdClient * client, GList * path, GList * arguments)
{
  if (!path)
    return HTTP_STATUS_400;

  if (!path->next)
    return request_global_post_record (client, path, arguments);

  if (!g_strcmp0 (path->next->data, REQUEST_POST_BULK_DOCS) && !path->next->next)
    return request_global_post_bulk_docs (client, path, arguments);

  return HTTP_STATUS_400;
}

static DSHttpStatusCode
request_global_post_record (DSHttpdClient * client, GList * path,
			    GList * arguments)
{
  DupinRecord *record;
  DSHttpStatusCode code;

  if (!client->body)
    return HTTP_STATUS_400;

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      code = HTTP_STATUS_500;
      goto request_global_post_record_end;
    }

  /* TODO - add options to GET methods to pass escape/unescape Unicode if needed by client accordingly to RFC4627
            we currently read escaped JSON documents and stored them as UTF-8 - also check UTF-16 and UTF-32 encodings if needed/supported */

  /* TODO - add error checking and return any parsing error to client */
  if (json_parser_load_from_data (parser, client->body, client->body_size, NULL) == FALSE)
    {
      code = HTTP_STATUS_400;
      goto request_global_post_record_end;
    }

  JsonNode * node = json_parser_get_root (parser);

  if (node == NULL)
    {
      code = HTTP_STATUS_500;
      goto request_global_post_record_end;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      code = HTTP_STATUS_400;
      goto request_global_post_record_end;
    }

  if (request_record_insert
      (client, node, path->data, NULL, &code, &record) == TRUE)
    {
      if (request_record_response_single (client, record) == FALSE)
	code = HTTP_STATUS_500;

      dupin_record_close (record);
    }

request_global_post_record_end:

  if (parser != NULL)
    g_object_unref (parser);
  return code;
}

static DSHttpStatusCode
request_global_post_bulk_docs (DSHttpdClient * client, GList * path,
			       GList * arguments)
{
  GList *list;

  JsonObject *obj;
  JsonNode *node;
  JsonArray *array;
  GList *nodes, *n;

  DSHttpStatusCode code;

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  /* TODO - check any parsing error */
  if (json_parser_load_from_data (parser, client->body, client->body_size, NULL) == FALSE)
    {
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  obj = json_node_get_object (node); /* it is a volatile object part of parser as well as node - see docs */

  if (json_object_has_member (obj, REQUEST_POST_BULK_DOCS_DOCS) == FALSE)
    {
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  node = json_object_get_member (obj, REQUEST_POST_BULK_DOCS_DOCS);

  if (node == NULL)
    {
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  if (json_node_get_node_type (node) != JSON_NODE_ARRAY)
    {
      code = HTTP_STATUS_400;
      goto request_global_post_bulk_docs_error;
    }

  array = json_node_get_array (node);

  if (array == NULL)
    {
      code = HTTP_STATUS_500;
      goto request_global_post_bulk_docs_error;
    }

  /* scan JSON array */
  list = NULL;
 
  nodes = json_array_get_elements (array);

  for (n = nodes; n != NULL; n = n->next)
    {
      DupinRecord *record;
      JsonNode *element_node = (JsonNode*)n->data;

      if (json_node_get_node_type (element_node) != JSON_NODE_OBJECT)
        {
          g_list_free (nodes);
          code = HTTP_STATUS_500;
          goto request_global_post_bulk_docs_error;
        }

      if (request_record_insert
	  (client, element_node, path->data, NULL, &code, &record) == FALSE)
	{
          g_list_free (nodes);
          code = HTTP_STATUS_400;
          goto request_global_post_bulk_docs_error;
	}

      list = g_list_prepend (list, record);
    }
  g_list_free (nodes);

  if (request_record_response_multi (client, list) == FALSE)
    code = HTTP_STATUS_500;
  else
    code = HTTP_STATUS_200;

  while (list)
    {
      dupin_record_close (list->data);
      list = g_list_remove (list, list->data);
    }

  if (parser != NULL)
    g_object_unref (parser);
  return code;

request_global_post_bulk_docs_error:

  if (parser != NULL)
    g_object_unref (parser);
  return code;
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

static DSHttpStatusCode request_global_put_record_attachment (DSHttpdClient * client,
						   	      GList * path,
						              GList * arguments);

static DSHttpStatusCode
request_global_put (DSHttpdClient * client, GList * path, GList * arguments)
{
  if (!path)
    return HTTP_STATUS_400;

  if (!g_strcmp0 (path->data, REQUEST_VIEWS))
    {
      /* PUT /_views/view */
      if (path->next && !path->next->next)
	return request_global_put_view (client, path, arguments);

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
  DupinAttachmentDB *attachment_db;

  if (!
      (db =
       dupin_database_new (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_409;

  dupin_database_unref (db);

  /* create a default attachments DB named after the database being created */
  if (!
      (attachment_db =
       dupin_attachment_db_new (client->thread->data->dupin, path->data, path->data, NULL)))
    return HTTP_STATUS_409;

  dupin_attachment_db_unref (attachment_db);

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
  const gchar *map = NULL;
  const gchar *map_lang = "javascript";
  const gchar *reduce = NULL;
  const gchar *reduce_lang = "javascript";

  if (!client->body)
    return HTTP_STATUS_400;

  parser = json_parser_new ();

  if (parser == NULL)
    {
      code = HTTP_STATUS_500;
      goto request_global_put_view_error;
    }

  /* TODO - check any parsing error */
  if (json_parser_load_from_data (parser, client->body, client->body_size, NULL) == FALSE)
    {
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
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
      code = HTTP_STATUS_400;
      goto request_global_put_view_error;
    }

  if (!
      (view =
       dupin_view_new (client->thread->data->dupin, path->next->data, (gchar *)parent,
		       parent_is_db, (gchar *)map,
		       dupin_util_mr_lang_to_enum ((gchar *)map_lang), (gchar *)reduce,
		       dupin_util_mr_lang_to_enum ((gchar *)reduce_lang), NULL)))
    {
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
  JsonNode *node;
  JsonParser *parser;
  DupinRecord *record;
  DSHttpStatusCode code;
  gchar * doc_id=NULL; 
 
  if (!client->body
      || !path->next->data)
    return HTTP_STATUS_400;

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
//g_message("request_global_put_record: dbname=%s id=%s\n", (gchar *) path->data, (gchar *)path->next->data);

      if (path->next->next->next)
        {
          /* PUT /_special_document/document_ID/attachment */
	  return request_global_put_record_attachment (client, path, arguments);
        }

        /* PUT /_special_document/document_ID */
        doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);
    }
  else if (path->next->next)
    {
      /* PUT /document_ID/attachment */
      return request_global_put_record_attachment (client, path, arguments);
    }
  else
    {
      /* PUT /document_ID */
      doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
    }

//g_message("request_global_put_record: dbname=%s doc_id=%s\n", (gchar *) path->data, doc_id);

  parser = json_parser_new ();

  if (parser == NULL)
    {
      code = HTTP_STATUS_500;
      goto request_global_put_record_error;
    }

  /* TODO - check any parsing error */
  if (json_parser_load_from_data (parser, client->body, client->body_size, NULL) == FALSE)
    {
      code = HTTP_STATUS_400;
      goto request_global_put_record_error;
    }

  node = json_parser_get_root (parser);

  if (node == NULL)
    {
      code = HTTP_STATUS_400;
      goto request_global_put_record_error;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      code = HTTP_STATUS_400;
      goto request_global_put_record_error;
    }

  if (request_record_insert
      (client, node, path->data, doc_id, &code,
       &record) == TRUE)
    {
      if (request_record_response_single (client, record) == FALSE)
	code = HTTP_STATUS_500;
      dupin_record_close (record);
    }

  if (parser != NULL)
    g_object_unref (parser);

  g_free (doc_id);

  return code;

request_global_put_record_error:

  if (parser != NULL)
    g_object_unref (parser);

  g_free (doc_id);

  return code;
}

static DSHttpStatusCode
request_global_put_record_attachment (DSHttpdClient * client, GList * path,
				      GList * arguments)
{
  DupinRecord *record;
  DSHttpStatusCode code;
  gchar * doc_id=NULL;
  GList * title_parts=NULL;

  if (!client->body
      || !client->input_mime
      || !path->next->data
      || !path->next->next)
    return HTTP_STATUS_400;

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* PUT /_special_document/document_ID/attachment */
      doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);
      title_parts = path->next->next->next;
    }
  else
    {
      /* PUT /document_ID/attachment */
      doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
      title_parts = path->next->next;
    }

//g_message("request_global_put_record_attachment: dbname=%s doc_id=%s\n", (gchar *) path->data, doc_id);

  if (request_record_attachment_insert
      (client, path->data, doc_id,
       title_parts, arguments, &code,
       &record) == TRUE)
    {
      if (request_record_response_single (client, record) == FALSE)
        code = HTTP_STATUS_500;

      dupin_record_close (record);
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

static DSHttpStatusCode
request_global_delete (DSHttpdClient * client, GList * path,
		       GList * arguments)
{
  if (!path)
    return HTTP_STATUS_400;

  /* DELETE /database */
  if (!path->next)
    return request_global_delete_database (client, path, arguments);

  if (!g_strcmp0 (path->data, REQUEST_VIEWS))
    {
      /* DELETE /_views/view */
      if (!path->next->next)
	return request_global_delete_view (client, path, arguments);

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
  DupinAttachmentDB *attachment_db;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_404;

  if (dupin_database_delete (db, NULL) == FALSE)
    {
      dupin_database_unref (db);
      return HTTP_STATUS_409;
    }

  dupin_database_unref (db);

  /* remove also default attachment DB named after the database being deleted */

  if (!
      (attachment_db =
       dupin_attachment_db_open (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_404;

  if (dupin_attachment_db_delete (attachment_db, NULL) == FALSE)
    {
      dupin_attachment_db_unref (attachment_db);
      return HTTP_STATUS_409;
    }

  dupin_attachment_db_unref (attachment_db);

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
    return HTTP_STATUS_404;

  if (dupin_view_delete (view, NULL) == FALSE)
    {
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

  /* check if special document name/id */
  gunichar ch = g_utf8_get_char (path->next->data);

  if (ch == '_')
    {
      /* TODO - shouldn't we stop/avoid user to delete /_design/something ?! */

      /* DELETE /_special_document/document_ID and /_special_document/document_ID/attachment */
      doc_id = g_strdup_printf ("%s/%s", (gchar *)path->next->data, (gchar *)path->next->next->data);
      title_parts = path->next->next->next;
    }
  else
    {
      /* DELETE /document_ID and /document_ID/attachment */
      doc_id = g_strdup_printf ("%s", (gchar *)path->next->data);
      title_parts = path->next->next;
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
          g_free (doc_id);
          return HTTP_STATUS_400;
        }

//g_message("request_global_delete_record: title=%s\n", title);
    }

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    {
      if (title != NULL)
        g_free (title);
      g_free (doc_id);
      return HTTP_STATUS_404;
    }

  if (!(record = dupin_record_read (db, doc_id, NULL)))
    {
      if (title != NULL)
        g_free (title);
      dupin_database_unref (db);
      g_free (doc_id);
      return HTTP_STATUS_404;
    }

  if (title_parts != NULL)
    {
      JsonNode * obj_node = NULL;

      /* TODO - delete specific attachment to document */
      for (l = arguments; l; l = l->next)
        {
          dupin_keyvalue_t *kv = l->data;

          if (!g_strcmp0 (kv->key, REQUEST_RECORD_ARG_REV))
            {
              if (dupin_util_is_valid_mvcc (kv->value) == FALSE)
                {
                  if (title != NULL)
                    g_free (title);
                  dupin_database_unref (db);
                  g_free (doc_id);
                  return HTTP_STATUS_400;
                }
              mvcc = kv->value;
            }
        }

      if (mvcc == NULL
          || g_strcmp0 (mvcc, dupin_record_get_last_revision (record)))
        {
          g_free (title);
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
          return HTTP_STATUS_404;
        }

      if (!  (attachment_db =
       		dupin_attachment_db_open (client->thread->data->dupin, path->data, NULL)))
        {
          g_free (title);
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
          return HTTP_STATUS_404;
        }

      if (!(obj_node = request_record_obj (record, doc_id, mvcc)))
        {
          g_free (title);
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
          return HTTP_STATUS_404;
        }

      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_REV);
      json_object_remove_member (json_node_get_object (obj_node), REQUEST_OBJ_ID);

      if ( dupin_attachment_record_exists (attachment_db, doc_id, title) == FALSE
           || dupin_attachment_record_delete (attachment_db, doc_id, title) == FALSE
           || dupin_record_update (record, obj_node, NULL) == FALSE)
        {
          g_free (title);
          if (obj_node != NULL)
            json_node_free (obj_node);
          dupin_record_close (record);
          dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          g_free (doc_id);
          return HTTP_STATUS_404;
        }

      if (obj_node != NULL)
        json_node_free (obj_node);

      dupin_attachment_db_unref (attachment_db);

      g_free (title);
    }
  else
    {
      if (!(dupin_record_delete (record, NULL)))
        {
          dupin_record_close (record);
          dupin_database_unref (db);
          g_free (doc_id);
          return HTTP_STATUS_400;
        }

      /* NOTE - we do *NOT* delete all attachments to document - which can be individually deleted with above fetch revs=true of deleted records */
    }

  dupin_record_close (record);
  dupin_database_unref (db);

  g_free (doc_id);

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
static gchar * request_record_insert_rev (JsonNode * obj_node);
static gchar * request_record_insert_id (JsonNode * obj_node);

static gboolean
request_record_insert (DSHttpdClient * client, JsonNode * obj_node,
		       gchar * dbname, gchar * id, DSHttpStatusCode * code,
		       DupinRecord ** ret_record)
{
  DupinDB *db;
  DupinRecord *record;
  DSHttpStatusCode retcode;

  gchar * json_record_mvcc=NULL;
  gchar * json_record_id;

  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, FALSE);

  if (!(db = dupin_database_open (client->thread->data->dupin, dbname, NULL)))
    {
      *code = HTTP_STATUS_400;
      return FALSE;
    }

  json_record_mvcc = request_record_insert_rev (obj_node);

  if ((json_record_id = request_record_insert_id (obj_node)))
    {
      if (id && g_strcmp0 (id, json_record_id))
	{
          if (json_record_mvcc != NULL)
	    g_free (json_record_mvcc);
	  g_free (json_record_id);
          dupin_database_unref (db); /* added by AR 2010-10-05 */
	  *code = HTTP_STATUS_400;
	  return FALSE;
	}

      id = json_record_id;
    }

  if (json_record_mvcc != NULL && !id)
    {
      if (json_record_id != NULL)
        g_free (json_record_id);
      if (json_record_mvcc != NULL)
        g_free (json_record_mvcc);
      dupin_database_unref (db);
      *code = HTTP_STATUS_400;
      return FALSE;
    }

  if (json_record_mvcc != NULL)
    {
      retcode = HTTP_STATUS_200;

      record = dupin_record_read (db, id, NULL);

      if (!record || g_strcmp0 (json_record_mvcc, dupin_record_get_last_revision (record))
	  || dupin_record_update (record, obj_node, NULL) == FALSE)
	{
          if (record)
	    dupin_record_close (record);
	  record = NULL;
	}
    }

  else if (!id)
    {
      retcode = HTTP_STATUS_201;

      record = dupin_record_create (db, obj_node, NULL);
    }

  else
    {
      retcode = HTTP_STATUS_201;

      if (dupin_record_exists (db, id) == FALSE)
	record = dupin_record_create_with_id (db, obj_node, id, NULL);
      else
	record = NULL;
    }

  if (json_record_id)
    g_free (json_record_id);

  if (!record)
    {
      if (json_record_mvcc != NULL)
        g_free (json_record_mvcc);
      dupin_database_unref (db);
      *code = HTTP_STATUS_409;
      return FALSE;
    }

  if (json_record_mvcc != NULL)
    g_free (json_record_mvcc);
  dupin_database_unref (db);

  *ret_record = record;
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
request_record_response_single (DSHttpdClient * client, DupinRecord * record)
{
  JsonObject *obj;
  JsonNode *node=NULL;
  JsonGenerator *gen=NULL;

  obj = json_object_new ();

  if (obj == NULL)
    return FALSE;

  /* TODO - do we ever set this to false? no... ! */
  json_object_set_boolean_member (obj, "ok", TRUE);
  json_object_set_string_member (obj, RESPONSE_OBJ_ID, (gchar *) dupin_record_get_id (record));
  json_object_set_string_member (obj, RESPONSE_OBJ_REV, dupin_record_get_last_revision (record));

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_record_response_single_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_record_response_single_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_record_response_single_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  return TRUE;

request_record_response_single_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  return FALSE;
}

static gboolean
request_record_response_multi (DSHttpdClient * client, GList * list)
{
  JsonObject *obj;
  JsonNode *node=NULL;
  JsonArray *array;
  JsonGenerator *gen=NULL;

  obj = json_object_new ();

  if (obj == NULL)
    return FALSE;

  /* TODO - do we ever set this to false? no... ! */
  json_object_set_boolean_member (obj, "ok", TRUE);

  array = json_array_new ();

  if (array == NULL)
    goto request_record_response_multi_error;

  for (; list; list = list->next)
    {
      JsonObject *o;
      DupinRecord *record = list->data;

      o = json_object_new ();

      if (o == NULL)
        goto request_record_response_multi_error; 

      json_object_set_string_member (o, RESPONSE_OBJ_ID, (gchar *) dupin_record_get_id (record));
      json_object_set_string_member (o, RESPONSE_OBJ_REV, dupin_record_get_last_revision (record));

      json_array_add_object_element( array, o);
    }
  json_object_set_array_member (obj, "new_revs", array );

  node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    goto request_record_response_multi_error;

  json_node_set_object (node, obj);

  gen = json_generator_new();

  if (gen == NULL)
    goto request_record_response_multi_error;

  json_generator_set_root (gen, node );
  client->output.string.string = json_generator_to_data (gen,&client->output_size);

  if (client->output.string.string == NULL)
    goto request_record_response_multi_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  return TRUE;

request_record_response_multi_error:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    json_node_free (node);
  json_object_unref (obj);
  return FALSE;
}

static gboolean
request_record_attachment_insert (DSHttpdClient * client,
				  gchar * dbname, gchar * id,
				  GList * title_parts,
				  GList * arguments,
				  DSHttpStatusCode * code,
				  DupinRecord ** ret_record)
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
              return FALSE;
            }
	  mvcc = kv->value;
        }
    }

  if (!(db = dupin_database_open (client->thread->data->dupin, dbname, NULL)))
    {
      g_free (title);
      *code = HTTP_STATUS_400;
      return FALSE;
    }

  if (!
      (attachment_db =
       dupin_attachment_db_open (client->thread->data->dupin, dbname, NULL)))
    {
      g_free (title);
      dupin_database_unref (db);
      *code = HTTP_STATUS_400;
      return FALSE;
    }

  record = dupin_record_read (db, id, NULL);

  if (mvcc == NULL && record != NULL)
    {
      g_free (title);
      dupin_database_unref (db);
      *code = HTTP_STATUS_400;
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
          return FALSE;
        }
    }
  else
    {
      if (mvcc == NULL
          || g_strcmp0 (mvcc, dupin_record_get_last_revision (record)) > 0)
        {
          g_free (title);
          dupin_record_close (record);
          dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          *code = HTTP_STATUS_404;
          return FALSE;
        }

      if (g_strcmp0 (mvcc, dupin_record_get_last_revision (record)))
        {
          g_free (title);
          dupin_record_close (record);
          dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          *code = HTTP_STATUS_409;
          return FALSE;
        }

      /* NOTE - need to touch/update the metadata record anyway */

      if (!(obj_node = request_record_obj (record,
		(gchar *) dupin_record_get_id (record), mvcc)))
        {
          g_free (title);
          dupin_record_close (record);
          dupin_attachment_db_unref (attachment_db);
          dupin_database_unref (db);
          *code = HTTP_STATUS_404;
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
          return FALSE;
        }
    }

  dupin_attachment_db_unref (attachment_db);

  retcode = HTTP_STATUS_200;

  dupin_database_unref (db);
  g_free (title);
  json_node_free (obj_node);

  *ret_record = record;
  *code = retcode;

  return TRUE;
}

static JsonNode *
request_record_obj (DupinRecord * record, gchar * id, gchar * mvcc)
{
  JsonNode *obj_node;
  JsonObject *obj;

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

  if (dupin_record_is_deleted (record, mvcc) == TRUE)
    {
      json_object_set_boolean_member (obj, "_deleted", TRUE);
    }

  else
    {
      GList *members, *m;

      JsonNode * node = dupin_record_get_revision_node (record, mvcc);

      if (node == NULL)
        {
          json_object_unref (obj);
          return NULL;
        }

      JsonObject * nodeobject = json_node_get_object (node);

      members = json_object_get_members (nodeobject);
      for ( m = members ; m != NULL ; m = m->next )
        {
          /* TODO - check if this shouldn't be json_object_set_object_member()/json_object_set_array_member() etc instead to make sure GC works */
          json_object_set_member (obj, m->data, json_node_copy (json_object_get_member (nodeobject, m->data)));
        }
      g_list_free (members);
    }

  /* Setting _id and _rev: */
  json_object_set_string_member (obj, REQUEST_OBJ_ID, id);
  json_object_set_string_member (obj, REQUEST_OBJ_REV, mvcc);

  return obj_node;
}

static JsonNode *
request_view_record_obj (DupinViewRecord * record, gchar * id)
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

/* EOF */
