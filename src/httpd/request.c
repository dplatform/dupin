#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin.h"
#include "map.h"
#include "request.h"

#include "../tbjson/tb_json.h"
#include "../tbjson/tb_keyvalue.h"
#include "../tbjsonpath/tb_jsonpath.h"

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include <string.h>
#include <stdlib.h>

#define REQUEST_OBJ_ID	"_id"
#define REQUEST_OBJ_REV	"_rev"
#define REQUEST_OBJ_PID	"_pid"

static JsonObject *request_record_obj (DupinRecord * record, gchar * id,
					     guint rev);
static tb_json_object_t *request_view_record_obj (DupinViewRecord * record,
						  gchar * id);

static gboolean request_record_insert (DSHttpdClient * client,
				       tb_json_object_t * obj, gchar * db,
				       gchar * id, DSHttpStatusCode * code,
				       DupinRecord ** record);
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
	  if (!strcmp (paths->data, ".."))
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
  g_main_loop_quit (client->thread->data->loop);
  return HTTP_STATUS_200;
}

/* STATUS FUNCTION *********************************************************/
static DSHttpStatusCode
request_status (DSHttpdClient * client, GList * paths, GList * arguments)
{
  JsonObject *obj;
  JsonObject *nobj;
  JsonNode *node;
  JsonGenerator *gen;

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
    g_object_unref (node);
  g_object_unref (obj);
  return HTTP_STATUS_200;

request_status_quit:
  g_mutex_unlock (client->thread->data->httpd_mutex);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    g_object_unref (node);
  g_object_unref (obj);
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
#define REQUEST_ALL_DOCS	"_all_docs"
#define REQUEST_VIEWS		"_views"

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

static DSHttpStatusCode
request_global_get (DSHttpdClient * client, GList * path, GList * arguments)
{
  if (!path)
    return HTTP_STATUS_400;

  /* GET /_all_dbs */
  if (!strcmp (path->data, REQUEST_ALL_DBS))
    return request_global_get_all_dbs (client, path, arguments);

  /* GET /_all_views */
  if (!strcmp (path->data, REQUEST_ALL_VIEWS))
    return request_global_get_all_views (client, path, arguments);

  /* GET /database */
  if (!path->next)
    return request_global_get_database (client, path, arguments);

  /* GET /database/_all_docs */
  if (!path->next->next && !strcmp (path->next->data, REQUEST_ALL_DOCS))
    return request_global_get_all_docs (client, path, arguments);

  if (!strcmp (path->data, REQUEST_VIEWS))
    {
      /* GET /_views/view */
      if (!path->next->next)
	return request_global_get_view (client, path, arguments);

      else if (!path->next->next->next)
	{
	  /* GET /_views/view/_all_docs */
	  if (!strcmp (path->next->next->data, REQUEST_ALL_DOCS))
	    return request_global_get_all_docs_view (client, path, arguments);

	  /* GET /_views/view/id */
	  return request_global_get_record_view (client, path, arguments);
	}

      return HTTP_STATUS_400;
    }

  /* GET /database/id */
  if (!path->next->next)
    return request_global_get_record (client, path, arguments);

  return HTTP_STATUS_400;
}

static DSHttpStatusCode
request_global_get_all_dbs (DSHttpdClient * client, GList * paths,
			    GList * arguments)
{
  guint i;
  gchar **dbs;
  JsonArray *array;
  JsonNode *node;
  JsonGenerator *gen;

  if (client->request != DS_HTTPD_REQUEST_GET)
    return HTTP_STATUS_400;

  array = json_array_new ();

  if (array == NULL)
    return HTTP_STATUS_500;

  dbs = dupin_get_databases (client->thread->data->dupin);

  for (i = 0; dbs && dbs[i]; i++)
    {
      json_array_add_string_element  (array, dbs[i]);
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
    g_object_unref (node);
  g_object_unref (array);
  g_strfreev (dbs);
  return HTTP_STATUS_200;

request_global_get_all_dbs:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    g_object_unref (node);
  g_object_unref (array);
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
  JsonNode *node;
  JsonGenerator *gen;

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
    g_object_unref (node);
  g_object_unref (array);
  g_strfreev (views);
  return HTTP_STATUS_200;

request_global_get_all_views:

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    g_object_unref (node);
  g_object_unref (array);
  g_strfreev (views);
  return HTTP_STATUS_500;
}

#define REQUEST_GET_ALL_DOCS_DESCENDING	"descending"
#define REQUEST_GET_ALL_DOCS_COUNT	"count"
#define REQUEST_GET_ALL_DOCS_OFFSET	"offset"

static DSHttpStatusCode
request_global_get_all_docs (DSHttpdClient * client, GList * path,
			     GList * arguments)
{
  DupinDB *db;

  GList *list;
  GList *results;

  gboolean descending = FALSE;
  guint count = 0;
  guint offset = 0;

  JsonObject *obj;
  JsonNode *node;
  JsonArray *array;
  JsonGenerator *gen;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_404;

  for (list = arguments; list; list = list->next)
    {
      tb_keyvalue_t *kv = list->data;

      if (!strcmp (kv->key, REQUEST_GET_ALL_DOCS_DESCENDING)
	  && !strcmp (kv->value, "true"))
	descending = TRUE;

      else if (!strcmp (kv->key, REQUEST_GET_ALL_DOCS_COUNT))
	count = atoi (kv->value);

      else if (!strcmp (kv->key, REQUEST_GET_ALL_DOCS_OFFSET))
	offset = atoi (kv->value);
    }

  if (dupin_record_get_list (db, count, offset, descending, &results, NULL) ==
      FALSE)
    return HTTP_STATUS_500;

  obj = json_object_new ();

  if (obj == NULL)
    return HTTP_STATUS_500;

  json_object_set_int_member (obj, "total_rows", g_list_length (results));
  json_object_set_int_member (obj, "offset", offset);

  array = json_array_new ();

  if (array == NULL)
    goto request_global_get_all_docs_error;

  for (list = results; list; list = list->next)
    {
      DupinRecord *record = list->data;
      JsonObject *o;

      if (!
	  (o =
	   request_record_obj (record, (gchar *) dupin_record_get_id (record),
			       dupin_record_get_last_revision (record))))
        {
	  g_object_unref (array); /* if here, array is not under obj responsability yet */
	  goto request_global_get_all_docs_error;
        }

      json_array_add_object_element( array, o);
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
  else
     dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    g_object_unref (node);
  g_object_unref (obj);
  return HTTP_STATUS_200;

request_global_get_all_docs_error:

  if( results )
     dupin_record_get_list_close (results);
  else
     dupin_database_unref (db);

  if (gen != NULL)
    g_object_unref (gen);
  if (node != NULL)
    g_object_unref (node);
  g_object_unref (obj);
  return HTTP_STATUS_500;
}

#define REQUEST_QUERY		"_query"

static DSHttpStatusCode
request_global_get_database (DSHttpdClient * client, GList * path,
			     GList * arguments)
{
  DupinDB *db;
  GList *list;
  tb_json_object_t *obj;
  tb_json_node_t *node;
  tb_json_value_t *value;

  for (list = arguments; list; list = list->next)
    {
      tb_keyvalue_t *kv = list->data;

      if (!strcmp (kv->key, REQUEST_QUERY))
	return request_global_get_database_query (client, path, kv->value);
    }

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_404;

  if (tb_json_object_new (&obj) == FALSE)
    {
      dupin_database_unref (db);
      return HTTP_STATUS_404;
    }

  if (tb_json_object_add_node (obj, "db_name", &node) == FALSE)
    goto request_global_get_database_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string (value, (gchar *) dupin_database_get_name (db))
      == FALSE)
    goto request_global_get_database_error;

  if (tb_json_object_add_node (obj, "doc_count", &node) == FALSE)
    goto request_global_get_database_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_number
      (value, dupin_database_count (db, DP_COUNT_EXIST)) == FALSE)
    goto request_global_get_database_error;

  if (tb_json_object_add_node (obj, "doc_del_count", &node) == FALSE)
    goto request_global_get_database_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_number
      (value, dupin_database_count (db, DP_COUNT_DELETE)) == FALSE)
    goto request_global_get_database_error;

#ifdef COUCHDB_STRICT
  if (tb_json_object_add_node (obj, "update_seq", &node) == FALSE)
    goto request_global_get_database_error;

  value = tb_json_node_get_value (node);
  /* FIXME: no a really documentation about this stuff (update_seq)... */
  if (tb_json_value_set_number (value, 0) == FALSE)
    goto request_global_get_database_error;
#endif

  if (tb_json_object_add_node (obj, "disk_size", &node) == FALSE)
    goto request_global_get_database_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_number (value, dupin_database_get_size (db)) == FALSE)
    goto request_global_get_database_error;

  if (tb_json_object_add_node (obj, "compact_running", &node) == FALSE)
    goto request_global_get_database_error;

  value = tb_json_node_get_value (node);
  /* FIXME: this is no sense for dupin: */
  if (tb_json_value_set_boolean (value, FALSE) == FALSE)
    goto request_global_get_database_error;

  if (tb_json_object_write_to_buffer
      (obj, &client->output.string.string, &client->output_size,
       NULL) == FALSE)
    goto request_global_get_database_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  tb_json_object_destroy (obj);
  dupin_database_unref (db);
  return HTTP_STATUS_200;

request_global_get_database_error:
  tb_json_object_destroy (obj);
  dupin_database_unref (db);
  return HTTP_STATUS_500;
}

#define REQUEST_RECORD_ARG_REV	"rev"
#define REQUEST_RECORD_ARG_REVS	"revs"

static DSHttpStatusCode
request_global_get_record (DSHttpdClient * client, GList * path,
			   GList * arguments)
{
  guint rev = 0;
  gboolean allrevs = FALSE;

  GList *list;

  DupinDB *db;
  DupinRecord *record;

  JsonObject *obj;
  tb_json_node_t *node;
  tb_json_value_t *value;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_404;

  if (!(record = dupin_record_read (db, path->next->data, NULL)))
    {
      dupin_database_unref (db);
      return HTTP_STATUS_404;
    }

  for (list = arguments; list; list = list->next)
    {
      tb_keyvalue_t *kv = list->data;

      if (!strcmp (kv->key, REQUEST_RECORD_ARG_REV))
	rev = atoi (kv->value);

      else if (!strcmp (kv->key, REQUEST_RECORD_ARG_REVS)
	       && !strcmp (kv->value, "true"))
	allrevs = TRUE;
    }

  /* Show all revisions: */
  if (allrevs == TRUE)
    {
      guint i;
      tb_json_array_t *array;

      if (tb_json_object_new (&obj) == FALSE)
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
	  return HTTP_STATUS_404;
	}

      if (tb_json_object_add_node (obj, "_revs_info", &node) == FALSE)
	goto request_global_get_record_error;

      value = tb_json_node_get_value (node);
      if (tb_json_value_set_array_new (value, &array) == FALSE)
	goto request_global_get_record_error;

      for (i = 1; i <= dupin_record_get_last_revision (record); i++)
	{
	  JsonObject *o;

	  tb_json_array_add (array, NULL, &value);

	  if (!
	      (o =
	       request_record_obj (record,
				   (gchar *) dupin_record_get_id (record),
				   i)))
	    goto request_global_get_record_error;

	  tb_json_value_set_object (value, o);
	}
    }

  /* Show a single revision: */
  else
    {
      if (rev == 0)
	rev = dupin_record_get_last_revision (record);

      else if (rev > dupin_record_get_last_revision (record))
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
	  return HTTP_STATUS_404;
	}

      if (!
	  (obj =
	   request_record_obj (record, (gchar *) dupin_record_get_id (record),
			       rev)))
	{
	  dupin_record_close (record);
	  dupin_database_unref (db);
	  return HTTP_STATUS_404;
	}
    }

  /* Writing: */
  if (tb_json_object_write_to_buffer
      (obj, &client->output.string.string, &client->output_size,
       NULL) == FALSE)
    goto request_global_get_record_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  tb_json_object_destroy (obj);
  dupin_record_close (record);
  dupin_database_unref (db);
  return HTTP_STATUS_200;

request_global_get_record_error:
  tb_json_object_destroy (obj);
  dupin_record_close (record);
  dupin_database_unref (db);
  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_view (DSHttpdClient * client, GList * path,
			 GList * arguments)
{
  GList *list;
  DupinView *view;
  tb_json_object_t *obj;
  tb_json_object_t *subobj;
  tb_json_node_t *node;
  tb_json_value_t *value;

  for (list = arguments; list; list = list->next)
    {
      tb_keyvalue_t *kv = list->data;

      if (!strcmp (kv->key, REQUEST_QUERY))
	return request_global_get_view_query (client, path, kv->value);
    }

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    return HTTP_STATUS_404;

  if (tb_json_object_new (&obj) == FALSE)
    {
      dupin_view_unref (view);
      return HTTP_STATUS_404;
    }

  if (tb_json_object_add_node (obj, "view_name", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string (value, (gchar *) dupin_view_get_name (view))
      == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (obj, "parent", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_object_new (value, &subobj) == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (subobj, "name", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string (value, (gchar *) dupin_view_get_parent (view))
      == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (subobj, "is_db", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_boolean (value, dupin_view_get_parent_is_db (view)) ==
      FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (obj, "map", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_object_new (value, &subobj) == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (subobj, "code", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string (value, (gchar *) dupin_view_get_map (view)) ==
      FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (subobj, "language", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string
      (value,
       (gchar *)
       dupin_util_mr_lang_to_string (dupin_view_get_map_language (view))) ==
      FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (obj, "reduce", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_object_new (value, &subobj) == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (subobj, "code", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string (value, (gchar *) dupin_view_get_reduce (view))
      == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (subobj, "language", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string
      (value,
       (gchar *)
       dupin_util_mr_lang_to_string (dupin_view_get_reduce_language (view)))
      == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (obj, "doc_count", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_number (value, dupin_view_count (view)) == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (obj, "disk_size", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_number (value, dupin_view_get_size (view)) == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_add_node (obj, "sync", &node) == FALSE)
    goto request_global_get_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_boolean (value, dupin_view_is_sync (view)) == FALSE)
    goto request_global_get_view_error;

  if (tb_json_object_write_to_buffer
      (obj, &client->output.string.string, &client->output_size,
       NULL) == FALSE)
    goto request_global_get_view_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  tb_json_object_destroy (obj);
  dupin_view_unref (view);
  return HTTP_STATUS_200;

request_global_get_view_error:
  tb_json_object_destroy (obj);
  dupin_view_unref (view);
  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_all_docs_view (DSHttpdClient * client, GList * path,
				  GList * arguments)
{
  DupinView *view;

  GList *list;
  GList *results;

  gboolean descending = FALSE;
  guint count = 0;
  guint offset = 0;

  tb_json_object_t *obj;
  tb_json_node_t *node;
  tb_json_array_t *array;
  tb_json_value_t *value;

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    return HTTP_STATUS_404;

  for (list = arguments; list; list = list->next)
    {
      tb_keyvalue_t *kv = list->data;

      if (!strcmp (kv->key, REQUEST_GET_ALL_DOCS_DESCENDING)
	  && !strcmp (kv->value, "true"))
	descending = TRUE;

      else if (!strcmp (kv->key, REQUEST_GET_ALL_DOCS_COUNT))
	count = atoi (kv->value);

      else if (!strcmp (kv->key, REQUEST_GET_ALL_DOCS_OFFSET))
	offset = atoi (kv->value);
    }

  if (dupin_view_record_get_list
      (view, count, offset, descending, &results, NULL) == FALSE)
    return HTTP_STATUS_500;

  if (tb_json_object_new (&obj) == FALSE)
    return HTTP_STATUS_500;

  if (tb_json_object_add_node (obj, "total_rows", &node) == FALSE)
    goto request_global_get_all_docs_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_number (value, g_list_length (results)) == FALSE)
    goto request_global_get_all_docs_view_error;

  if (tb_json_object_add_node (obj, "offset", &node) == FALSE)
    goto request_global_get_all_docs_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_number (value, offset) == FALSE)
    goto request_global_get_all_docs_view_error;

  if (tb_json_object_add_node (obj, "rows", &node) == FALSE)
    goto request_global_get_all_docs_view_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_array_new (value, &array) == FALSE)
    goto request_global_get_all_docs_view_error;

  for (list = results; list; list = list->next)
    {
      DupinViewRecord *record = list->data;
      tb_json_object_t *o;

      if (!
	  (o =
	   request_view_record_obj (record,
				    (gchar *)
				    dupin_view_record_get_id (record))))
	goto request_global_get_all_docs_view_error;

      if (tb_json_array_add (array, NULL, &value) == FALSE)
	goto request_global_get_all_docs_view_error;

      if (tb_json_value_set_object (value, o) == FALSE)
	goto request_global_get_all_docs_view_error;
    }

  if (tb_json_object_write_to_buffer
      (obj, &client->output.string.string, &client->output_size,
       NULL) == FALSE)
    goto request_global_get_all_docs_view_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  if( results )
     dupin_view_record_get_list_close(results);
  else
     dupin_view_unref (view);

  tb_json_object_destroy (obj);
  return HTTP_STATUS_200;

request_global_get_all_docs_view_error:
  /* by AR 2010-05-24 - CHECK corrected/changed the below to dupin_view_record_get_list_close() - it was dupin_record_get_list_close() - see above for matching statement ! */
  if( results )
     dupin_view_record_get_list_close(results);
  else
     dupin_view_unref (view);
  tb_json_object_destroy (obj);
  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_get_record_view (DSHttpdClient * client, GList * path,
				GList * arguments)
{
  DupinView *view;
  DupinViewRecord *record;

  tb_json_object_t *obj;

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
      (obj =
       request_view_record_obj (record,
				(gchar *) dupin_view_record_get_id (record))))
    {
      dupin_view_record_close (record);
      dupin_view_unref (view);
      return HTTP_STATUS_404;
    }

  /* Writing: */
  if (tb_json_object_write_to_buffer
      (obj, &client->output.string.string, &client->output_size,
       NULL) == FALSE)
    goto request_global_get_view_record_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  tb_json_object_destroy (obj);
  dupin_view_record_close (record);
  dupin_view_unref (view);
  return HTTP_STATUS_200;

request_global_get_view_record_error:
  tb_json_object_destroy (obj);
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

  tb_json_array_t *array;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_404;

  tb_json_array_new (&array);

  while (dupin_record_get_list
	 (db, QUERY_BLOCK, offset, FALSE, &results, NULL) == TRUE && results)
    {
      GList *list;

      for (list = results; list; list = list->next)
	{
	  DupinRecord *record = list->data;
	  tb_jsonpath_result_t *ret = NULL;

	  if (tb_jsonpath_exec
	      (query, -1, dupin_record_get_revision (record, -1), &ret, NULL,
	       NULL) == TRUE && ret)
	    {
	      tb_json_value_t *value, *v;

	      while (tb_jsonpath_result_next (ret, &value) == TRUE)
		{
		  tb_json_array_add (array, NULL, &v);
		  tb_json_value_duplicate_exists (value, v);
		}

	      tb_jsonpath_result_free (ret);
	    }
	}

      offset += g_list_length (results);
      dupin_record_get_list_close (results);
    }

  tb_json_array_write_to_buffer (array, &client->output.string.string,
				 &client->output_size, NULL);

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;
  tb_json_array_destroy (array);

  dupin_database_unref (db);
  return HTTP_STATUS_200;
}

static DSHttpStatusCode
request_global_get_view_query (DSHttpdClient * client, GList * path,
			       gchar * query)
{
  DupinView *view;
  GList *results;
  gsize offset = 0;

  tb_json_array_t *array;

  if (!
      (view =
       dupin_view_open (client->thread->data->dupin, path->next->data, NULL)))
    return HTTP_STATUS_404;

  tb_json_array_new (&array);

  while (dupin_view_record_get_list
	 (view, QUERY_BLOCK, offset, FALSE, &results, NULL) == TRUE
	 && results)
    {
      GList *list;

      for (list = results; list; list = list->next)
	{
	  DupinViewRecord *record = list->data;
	  tb_jsonpath_result_t *ret;

	  if (tb_jsonpath_exec
	      (query, -1, dupin_view_record_get (record), &ret, NULL,
	       NULL) == TRUE)
	    {
	      tb_json_value_t *value, *v;

	      while (tb_jsonpath_result_next (ret, &value) == TRUE)
		{
		  tb_json_array_add (array, NULL, &v);
		  tb_json_value_duplicate_exists (value, v);
		}
	    }

	  tb_jsonpath_result_free (ret);
	}

      offset += g_list_length (results);
      dupin_view_record_get_list_close (results);
    }

  tb_json_array_write_to_buffer (array, &client->output.string.string,
				 &client->output_size, NULL);

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;
  tb_json_array_destroy (array);

  dupin_view_unref (view);
  return HTTP_STATUS_200;
}

/* POST ********************************************************************/
#define REQUEST_POST_UUIDS		"_uuids"
#define REQUEST_POST_UUIDS_COUNT	"count"
#define REQUEST_POST_BULK_DOCS		"_bulk_docs"
#define REQUEST_POST_BULK_DOCS_DOCS	"docs"

static DSHttpStatusCode request_global_post_record (DSHttpdClient * client,
						    GList * path,
						    GList * arguments);
static DSHttpStatusCode request_global_post_uuids (DSHttpdClient * client,
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

  if (!strcmp (path->data, REQUEST_POST_UUIDS) && !path->next)
    return request_global_post_uuids (client, path, arguments);

  if (!path->next)
    return request_global_post_record (client, path, arguments);

  if (!strcmp (path->next->data, REQUEST_POST_BULK_DOCS) && !path->next->next)
    return request_global_post_bulk_docs (client, path, arguments);

  return HTTP_STATUS_400;
}

static DSHttpStatusCode
request_global_post_record (DSHttpdClient * client, GList * path,
			    GList * arguments)
{
  tb_json_t *json;
  DupinRecord *record;
  DSHttpStatusCode code;

  if (!client->body)
    return HTTP_STATUS_400;

  if (!(json = tb_json_new ()))
    return HTTP_STATUS_500;

  if (tb_json_load_from_buffer (json, client->body, client->body_size, NULL)
      == FALSE || tb_json_is_object (json) == FALSE)
    {
      tb_json_destroy (json);
      return HTTP_STATUS_400;
    }

  if (request_record_insert
      (client, tb_json_object (json), path->data, NULL, &code,
       &record) == TRUE)
    {
      if (request_record_response_single (client, record) == FALSE)
	code = HTTP_STATUS_500;
      dupin_record_close (record);
    }

  tb_json_destroy (json);
  return code;
}

static DSHttpStatusCode
request_global_post_uuids (DSHttpdClient * client, GList * path,
			   GList * arguments)
{
  GList *list;
  gboolean ret;

  guint count = 0;
  guint i;

  tb_json_array_t *array;

  for (list = arguments; list; list = list->next)
    {
      tb_keyvalue_t *kv = list->data;

      if (!strcmp (kv->key, REQUEST_POST_UUIDS_COUNT))
	count = atoi (kv->value);
    }

  if (!count)
    return HTTP_STATUS_400;

  if (tb_json_array_new (&array) == FALSE)
    return HTTP_STATUS_500;

  for (i = 0; i < count; i++)
    {
      gchar id[255];

      tb_json_value_t *value;

      dupin_util_generate_id (id);

      if (tb_json_array_add (array, NULL, &value) == FALSE
	  || tb_json_value_set_string (value, id) == FALSE)
	{
	  tb_json_array_destroy (array);
	  return HTTP_STATUS_500;
	}
    }

  ret =
    tb_json_array_write_to_buffer (array, &client->output.string.string,
				   &client->output_size, NULL);

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;
  tb_json_array_destroy (array);

  if (ret == TRUE)
    return HTTP_STATUS_200;

  return HTTP_STATUS_500;
}

static DSHttpStatusCode
request_global_post_bulk_docs (DSHttpdClient * client, GList * path,
			       GList * arguments)
{
  GList *list;
  guint i, len;

  tb_json_t *json;
  tb_json_object_t *obj;
  tb_json_node_t *node;
  tb_json_value_t *value;
  tb_json_array_t *array;

  DSHttpStatusCode code;

  if (!(json = tb_json_new ()))
    return HTTP_STATUS_500;

  if (tb_json_load_from_buffer (json, client->body, client->body_size, NULL)
      == FALSE || tb_json_is_object (json) == FALSE)
    {
      tb_json_destroy (json);
      return HTTP_STATUS_400;
    }

  obj = tb_json_object (json);

  if (tb_json_object_has_node (obj, REQUEST_POST_BULK_DOCS_DOCS) == FALSE
      || !(node = tb_json_object_get_node (obj, REQUEST_POST_BULK_DOCS_DOCS)))
    {
      tb_json_destroy (json);
      return HTTP_STATUS_400;
    }

  value = tb_json_node_get_value (node);
  if (tb_json_value_get_type (value) != TB_JSON_VALUE_ARRAY)
    {
      tb_json_destroy (json);
      return HTTP_STATUS_400;
    }

  array = tb_json_value_get_array (value);
  len = tb_json_array_length (array);
  list = NULL;

  for (i = 0; i < len; i++)
    {
      DupinRecord *record;
      DSHttpStatusCode code;

      value = tb_json_array_get (array, i);
      if (tb_json_value_get_type (value) != TB_JSON_VALUE_OBJECT)
	continue;

      obj = tb_json_value_get_object (value);

      if (request_record_insert
	  (client, obj, path->data, NULL, &code, &record) == FALSE)
	{
	  tb_json_destroy (json);
	  return HTTP_STATUS_400;
	}

      list = g_list_prepend (list, record);
    }

  if (request_record_response_multi (client, list) == FALSE)
    code = HTTP_STATUS_500;
  else
    code = HTTP_STATUS_200;

  while (list)
    {
      dupin_record_close (list->data);
      list = g_list_remove (list, list->data);
    }

  tb_json_destroy (json);
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

static DSHttpStatusCode
request_global_put (DSHttpdClient * client, GList * path, GList * arguments)
{
  if (!path)
    return HTTP_STATUS_400;

  if (!strcmp (path->data, REQUEST_VIEWS))
    {
      /* PUT /_views/view */
      if (path->next && !path->next->next)
	return request_global_put_view (client, path, arguments);

      return HTTP_STATUS_400;
    }

  /* PUT /database */
  if (!path->next)
    return request_global_put_database (client, path, arguments);

  if (!path->next->next)
    return request_global_put_record (client, path, arguments);

  return HTTP_STATUS_400;
}

static DSHttpStatusCode
request_global_put_database (DSHttpdClient * client, GList * path,
			     GList * arguments)
{
  DupinDB *db;

  if (!
      (db =
       dupin_database_new (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_409;

  dupin_database_unref (db);
  return HTTP_STATUS_201;
}

static DSHttpStatusCode
request_global_put_view (DSHttpdClient * client, GList * path,
			 GList * arguments)
{
  tb_json_t *json;
  tb_json_object_t *obj;

  DupinView *view;

  GList *nodes;

  gchar *parent = NULL;
  gboolean parent_is_db = FALSE;
  gchar *map = NULL;
  gchar *map_lang = NULL;
  gchar *reduce = NULL;
  gchar *reduce_lang = NULL;

  if (!client->body)
    return HTTP_STATUS_400;

  if (!(json = tb_json_new ()))
    return HTTP_STATUS_500;

  if (tb_json_load_from_buffer (json, client->body, client->body_size, NULL)
      == FALSE || tb_json_is_object (json) == FALSE)
    {
      tb_json_destroy (json);
      return HTTP_STATUS_400;
    }

  obj = tb_json_object (json);
  for (nodes = tb_json_object_get_nodes (obj); nodes; nodes = nodes->next)
    {
      tb_json_node_t *node = nodes->data;
      tb_json_value_t *value = tb_json_node_get_value (node);

      if (!strcmp (tb_json_node_get_string (node), "parent")
	  && tb_json_value_get_type (value) == TB_JSON_VALUE_OBJECT)
	{
	  tb_json_object_t *subobj = tb_json_value_get_object (value);
	  GList *subnodes;

	  for (subnodes = tb_json_object_get_nodes (subobj); subnodes;
	       subnodes = subnodes->next)
	    {
	      tb_json_node_t *node = subnodes->data;
	      tb_json_value_t *value = tb_json_node_get_value (node);

	      if (!strcmp (tb_json_node_get_string (node), "name")
		  && tb_json_value_get_type (value) == TB_JSON_VALUE_STRING)
		parent = tb_json_value_get_string (value);

	      else if (!strcmp (tb_json_node_get_string (node), "is_db")
		       && tb_json_value_get_type (value) ==
		       TB_JSON_VALUE_BOOLEAN)
		parent_is_db = tb_json_value_get_boolean (value);
	    }
	}

      else if (!strcmp (tb_json_node_get_string (node), "map")
	       && tb_json_value_get_type (value) == TB_JSON_VALUE_OBJECT)
	{
	  tb_json_object_t *subobj = tb_json_value_get_object (value);
	  GList *subnodes;

	  for (subnodes = tb_json_object_get_nodes (subobj); subnodes;
	       subnodes = subnodes->next)
	    {
	      tb_json_node_t *node = subnodes->data;
	      tb_json_value_t *value = tb_json_node_get_value (node);

	      if (!strcmp (tb_json_node_get_string (node), "code")
		  && tb_json_value_get_type (value) == TB_JSON_VALUE_STRING)
		map = tb_json_value_get_string (value);

	      else if (!strcmp (tb_json_node_get_string (node), "language")
		       && tb_json_value_get_type (value) ==
		       TB_JSON_VALUE_STRING)
		map_lang = tb_json_value_get_string (value);
	    }
	}

      else if (!strcmp (tb_json_node_get_string (node), "reduce")
	       && tb_json_value_get_type (value) == TB_JSON_VALUE_OBJECT)
	{
	  tb_json_object_t *subobj = tb_json_value_get_object (value);
	  GList *subnodes;

	  for (subnodes = tb_json_object_get_nodes (subobj); subnodes;
	       subnodes = subnodes->next)
	    {
	      tb_json_node_t *node = subnodes->data;
	      tb_json_value_t *value = tb_json_node_get_value (node);

	      if (!strcmp (tb_json_node_get_string (node), "code")
		  && tb_json_value_get_type (value) == TB_JSON_VALUE_STRING)
		reduce = tb_json_value_get_string (value);

	      else if (!strcmp (tb_json_node_get_string (node), "language")
		       && tb_json_value_get_type (value) ==
		       TB_JSON_VALUE_STRING)
		reduce_lang = tb_json_value_get_string (value);
	    }
	}
    }

  if (!map || !map_lang || !reduce || !reduce_lang || !parent)
    {
      tb_json_destroy (json);
      return HTTP_STATUS_400;
    }

  if (!
      (view =
       dupin_view_new (client->thread->data->dupin, path->next->data, parent,
		       parent_is_db, map,
		       dupin_util_mr_lang_to_enum (map_lang), reduce,
		       dupin_util_mr_lang_to_enum (reduce_lang), NULL)))
    {
      tb_json_destroy (json);
      return HTTP_STATUS_409;
    }

  dupin_view_unref (view);
  tb_json_destroy (json);
  return HTTP_STATUS_201;
}

static DSHttpStatusCode
request_global_put_record (DSHttpdClient * client, GList * path,
			   GList * arguments)
{
  tb_json_t *json;
  DupinRecord *record;
  DSHttpStatusCode code;

  if (!client->body)
    return HTTP_STATUS_400;

  if (!(json = tb_json_new ()))
    return HTTP_STATUS_500;

  if (tb_json_load_from_buffer (json, client->body, client->body_size, NULL)
      == FALSE || tb_json_is_object (json) == FALSE)
    {
      tb_json_destroy (json);
      return HTTP_STATUS_400;
    }

  if (request_record_insert
      (client, tb_json_object (json), path->data, path->next->data, &code,
       &record) == TRUE)
    {
      if (request_record_response_single (client, record) == FALSE)
	code = HTTP_STATUS_500;
      dupin_record_close (record);
    }

  tb_json_destroy (json);
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

  if (!strcmp (path->data, REQUEST_VIEWS))
    {
      /* DELETE /_views/view */
      if (!path->next->next)
	return request_global_delete_view (client, path, arguments);

      return HTTP_STATUS_400;
    }

  /* DELETE /database/id */
  if (!path->next->next)
    return request_global_delete_record (client, path, arguments);

  return HTTP_STATUS_400;
}

static DSHttpStatusCode
request_global_delete_database (DSHttpdClient * client, GList * path,
				GList * arguments)
{
  DupinDB *db;

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
  DupinRecord *record;

  if (!
      (db =
       dupin_database_open (client->thread->data->dupin, path->data, NULL)))
    return HTTP_STATUS_404;

  if (!(record = dupin_record_read (db, path->next->data, NULL)))
    {
      dupin_database_unref (db);
      return HTTP_STATUS_404;
    }

  if (!(dupin_record_delete (record, NULL)))
    {
      dupin_record_close (record);
      dupin_database_unref (db);
      return HTTP_STATUS_400;
    }

  dupin_record_close (record);
  dupin_database_unref (db);
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
static guint request_record_insert_rev (tb_json_object_t * obj);
static gchar *request_record_insert_id (tb_json_object_t * obj);

static gboolean
request_record_insert (DSHttpdClient * client, tb_json_object_t * obj,
		       gchar * dbname, gchar * id, DSHttpStatusCode * code,
		       DupinRecord ** ret_record)
{
  DupinDB *db;
  DupinRecord *record;
  DSHttpStatusCode retcode;

  guint rev;
  gchar *iid;

  if (!(db = dupin_database_open (client->thread->data->dupin, dbname, NULL)))
    {
      *code = HTTP_STATUS_400;
      return FALSE;
    }

  rev = request_record_insert_rev (obj);

  if ((iid = request_record_insert_id (obj)))
    {
      if (id && strcmp (id, iid))
	{
	  g_free (iid);
	  *code = HTTP_STATUS_400;
	  return FALSE;
	}

      id = iid;
    }

  if (rev && !id)
    {
      *code = HTTP_STATUS_400;
      return FALSE;
    }

  if (rev)
    {
      retcode = HTTP_STATUS_200;

      record = dupin_record_read (db, id, NULL);

      if (!record || rev != dupin_record_get_last_revision (record)
	  || dupin_record_update (record, obj, NULL) == FALSE)
	{
	  dupin_record_close (record);
	  record = NULL;
	}
    }

  else if (!id)
    {
      retcode = HTTP_STATUS_201;
      record = dupin_record_create (db, obj, NULL);
    }

  else
    {
      retcode = HTTP_STATUS_201;

      if (dupin_record_exists (db, id) == FALSE)
	record = dupin_record_create_with_id (db, obj, id, NULL);
      else
	record = NULL;
    }

  if (iid)
    g_free (iid);

  if (!record)
    {
      dupin_database_unref (db);
      *code = HTTP_STATUS_409;
      return FALSE;
    }

  dupin_database_unref (db);

  *ret_record = record;
  *code = retcode;
  return TRUE;
}

static guint
request_record_insert_rev (tb_json_object_t * obj)
{
  guint rev = 0;
  tb_json_node_t *node;
  tb_json_value_t *value;

  if (tb_json_object_has_node (obj, REQUEST_OBJ_REV) == FALSE
      || !(node = tb_json_object_get_node (obj, REQUEST_OBJ_REV)))
    return 0;

  if ((value = tb_json_node_get_value (node))
      && tb_json_value_get_type (value) == TB_JSON_VALUE_NUMBER)
    rev = (guint) tb_json_value_get_number (value);

  tb_json_object_remove_node (obj, node);
  return rev;
}

static gchar *
request_record_insert_id (tb_json_object_t * obj)
{
  gchar *id = NULL;
  tb_json_node_t *node;
  tb_json_value_t *value;

  if (tb_json_object_has_node (obj, REQUEST_OBJ_ID) == FALSE
      || !(node = tb_json_object_get_node (obj, REQUEST_OBJ_ID)))
    return 0;

  if ((value = tb_json_node_get_value (node))
      && tb_json_value_get_type (value) == TB_JSON_VALUE_STRING)
    id = g_strdup (tb_json_value_get_string (value));

  tb_json_object_remove_node (obj, node);
  return id;
}

static gboolean
request_record_response_single (DSHttpdClient * client, DupinRecord * record)
{
  tb_json_object_t *obj;
  tb_json_node_t *node;
  tb_json_value_t *value;

  if (tb_json_object_new (&obj) == FALSE)
    return FALSE;

  if (tb_json_object_add_node (obj, "ok", &node) == FALSE)
    goto request_record_response_single_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_boolean (value, TRUE) == FALSE)
    goto request_record_response_single_error;

  if (tb_json_object_add_node (obj, REQUEST_OBJ_ID, &node) == FALSE)
    goto request_record_response_single_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string (value, (gchar *) dupin_record_get_id (record))
      == FALSE)
    goto request_record_response_single_error;

  if (tb_json_object_add_node (obj, REQUEST_OBJ_REV, &node) == FALSE)
    goto request_record_response_single_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_number
      (value, dupin_record_get_last_revision (record)) == FALSE)
    goto request_record_response_single_error;

  if (tb_json_object_write_to_buffer
      (obj, &client->output.string.string, &client->output_size,
       NULL) == FALSE)
    goto request_record_response_single_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  tb_json_object_destroy (obj);
  return TRUE;

request_record_response_single_error:
  tb_json_object_destroy (obj);
  return FALSE;
}

static gboolean
request_record_response_multi (DSHttpdClient * client, GList * list)
{
  tb_json_object_t *obj;
  tb_json_node_t *node;
  tb_json_value_t *value;
  tb_json_array_t *array;

  if (tb_json_object_new (&obj) == FALSE)
    return FALSE;

  if (tb_json_object_add_node (obj, "ok", &node) == FALSE)
    goto request_record_response_multi_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_boolean (value, TRUE) == FALSE)
    goto request_record_response_multi_error;

  if (tb_json_object_add_node (obj, "new_revs", &node) == FALSE)
    goto request_record_response_multi_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_array_new (value, &array) == FALSE)
    goto request_record_response_multi_error;

  for (; list; list = list->next)
    {
      tb_json_object_t *o;
      DupinRecord *record = list->data;

      if (tb_json_array_add (array, NULL, &value) == FALSE)
	goto request_record_response_multi_error;

      if (tb_json_value_set_object_new (value, &o) == FALSE)
	goto request_record_response_multi_error;

      if (tb_json_object_add_node (o, REQUEST_OBJ_ID, &node) == FALSE)
	goto request_record_response_multi_error;

      value = tb_json_node_get_value (node);
      if (tb_json_value_set_string
	  (value, (gchar *) dupin_record_get_id (record)) == FALSE)
	goto request_record_response_multi_error;

      if (tb_json_object_add_node (o, REQUEST_OBJ_REV, &node) == FALSE)
	goto request_record_response_multi_error;

      value = tb_json_node_get_value (node);
      if (tb_json_value_set_number
	  (value, dupin_record_get_last_revision (record)) == FALSE)
	goto request_record_response_multi_error;
    }

  if (tb_json_object_write_to_buffer
      (obj, &client->output.string.string, &client->output_size,
       NULL) == FALSE)
    goto request_record_response_multi_error;

  client->output_mime = g_strdup (HTTP_MIME_JSON);
  client->output_type = DS_HTTPD_OUTPUT_STRING;

  tb_json_object_destroy (obj);
  return TRUE;

request_record_response_multi_error:
  tb_json_object_destroy (obj);
  return FALSE;
}

static JsonObject *
request_record_obj (DupinRecord * record, gchar * id, guint rev)
{
  JsonObject *obj;

  if (dupin_record_is_deleted (record, rev) == TRUE)
    {
      obj = json_object_new ();

      if (obj == NULL)
        return NULL;

      json_object_set_boolean_member (obj, "_deleted", TRUE);
    }

  else
    {
      obj = dupin_record_get_revision (record, rev);

      if (obj == NULL)
        return NULL;
    }

  /* Setting _id and _rev: */
  json_object_set_string_member (obj, REQUEST_OBJ_ID, id);
  json_object_set_double_member (obj, REQUEST_OBJ_REV, (gdouble) rev);

  return obj;
}

static tb_json_object_t *
request_view_record_obj (DupinViewRecord * record, gchar * id)
{
  tb_json_object_t *obj;
  tb_json_node_t *node;
  tb_json_value_t *value;

  tb_json_object_t *o;

  o = dupin_view_record_get (record);
  tb_json_object_duplicate (o, &obj);

  /* Setting _id and _rev: */
  if (tb_json_object_add_node (obj, REQUEST_OBJ_ID, &node) == FALSE)
    goto request_view_record_obj_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string (value, id) == FALSE)
    goto request_view_record_obj_error;

  if (tb_json_object_add_node (obj, REQUEST_OBJ_PID, &node) == FALSE)
    goto request_view_record_obj_error;

  value = tb_json_node_get_value (node);
  if (tb_json_value_set_string
      (value, (gchar *) dupin_view_record_get_pid (record)) == FALSE)
    goto request_view_record_obj_error;

  return obj;

request_view_record_obj_error:
  tb_json_object_destroy (obj);
  return NULL;
}


/* EOF */
