#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_mr.h"
#include "dupin_internal.h"

static gsize dupin_mr_map (gchar * map, DupinMRLang language,
			   JsonObject * obj, JsonArray * array);
static JsonNode *dupin_mr_reduce (gchar * reduce,
					  DupinMRLang language,
					  JsonArray * array);

JsonNode *
dupin_mr_record (DupinView * view, JsonObject * obj)
{
  JsonArray *array=NULL;
  JsonNode *object_node=NULL;

  array = json_array_new ();

  if (array == NULL)
    return NULL;

  if (!dupin_mr_map (view->map, view->map_lang, obj, array))
    {
      json_array_unref (array);
      return NULL;
    }

  /* TODO - we can have just map and no reduce step */

  object_node = dupin_mr_reduce (view->reduce, view->reduce_lang, array);

  json_array_unref (array);

  /* TODO - make sure called frees this node properly due dupin_mr_reduce() here allocates it */

  return object_node;
}

static gsize
dupin_mr_map (gchar * map, DupinMRLang language, JsonObject * obj,
	      JsonArray * ret_array)
{
  switch (language)
    {
    case DP_MR_LANG_JAVASCRIPT:
      {
	DupinJs *js;
	JsonArray *array;
	gchar *buffer;
	gint len;
	gsize size;
	JsonGenerator *gen;
	JsonNode *node;

        node = json_node_new (JSON_NODE_OBJECT);

	if (node == NULL)
	  return 0;

        json_node_set_object (node, obj);

  	gen = json_generator_new();

	if (gen == NULL)
          {
            json_node_free (node);
            return 0;
          }

	json_generator_set_root (gen, node );
	buffer = json_generator_to_data (gen,&size);

	if (buffer == NULL)
          {
            g_object_unref (gen);
            json_node_free (node);
            return 0;
          }

	/* TODO - we should really make sure escaping from JSON to Javascript structures is transferred right - and returned clean JSON too */

        //g_message("to call MAP code %s for length=%d of JSON: %s\n", map, (gint)size, buffer);

	if (!(js = dupin_js_new (buffer,map,"map", NULL)))
	  {
	    g_free (buffer);
            g_object_unref (gen);
            json_node_free (node);
	    return 0;
	  }

	g_free (buffer);

	if (!(array = (JsonArray *) dupin_js_get_emitIntermediate (js)))
	  {
            g_object_unref (gen);
            json_node_free (node);
	    return 0;
	  }

        GList *nodes, *n;

	nodes = json_array_get_elements (array);

 	len = g_list_length (nodes);

	for (n = nodes; n != NULL; n = n->next)
	  {
	    JsonNode *copy = json_node_copy ((JsonNode*)n->data);

	    if (copy == NULL)
              {
                g_free (nodes);
                g_object_unref (gen);
                json_node_free (node);
	        return 0;
              }

            json_array_add_element (ret_array, copy);
	  }
        g_list_free (nodes);

        g_object_unref (gen);
        json_node_free (node);
	return len;
      }
    }

  return 0;
}

static JsonNode *
dupin_mr_reduce (gchar * reduce, DupinMRLang language,
		 JsonArray * array)
{
  switch (language)
    {
    case DP_MR_LANG_JAVASCRIPT:
      {
	DupinJs *js;
	JsonNode *ret;
	JsonNode *object_node;
	gchar *buffer;
	gsize size;
	JsonGenerator *gen;
	JsonNode *node;

        node = json_node_new (JSON_NODE_ARRAY);

	if (node == NULL)
	  return NULL;

        json_node_set_array (node, array);

  	gen = json_generator_new();

	if (gen == NULL)
          {
            json_node_free (node);
            return NULL;
          }

	json_generator_set_root (gen, node );
	buffer = json_generator_to_data (gen,&size);

	if (buffer == NULL)
          {
            g_object_unref (gen);
            json_node_free (node);
            return NULL;
          }

	/* TODO - check that mapObjects is correct plural here */

	//g_message("to call REDUCE code %s for length=%d of JSON: %s\n", reduce, (gint)size, buffer);

	if (!(js = dupin_js_new (buffer,reduce,"reduce", NULL)))
	  {
	    g_free (buffer);
            g_object_unref (gen);
            json_node_free (node);
	    return NULL;
	  }

	g_free (buffer);

	if (!(object_node = (JsonNode *) dupin_js_get_emit (js)))
	  {
            g_object_unref (gen);
            json_node_free (node);
	    return NULL;
	  }

        /* TODO - check if json-glib has a simpler and safe way to copy objects ! */
        ret = json_node_copy ((JsonNode*)object_node);

        g_object_unref (gen);
        json_node_free (node);

	return ret;
      }
    }

  return NULL;
}

/* EOF */
