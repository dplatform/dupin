#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_mr.h"
#include "dupin_internal.h"

static gsize dupin_mr_map (gchar * map,
		           DupinMRLang language,
			   JsonObject * obj,
                           JsonArray * array);

static JsonNode * dupin_mr_reduce (gchar * reduce,
		                   DupinMRLang language,
		 		   JsonArray * keys,
				   JsonArray * values,
				   gboolean rereduce);

JsonArray *
dupin_mr_record_map (DupinView * view, JsonObject * obj)
{
  JsonArray *array=NULL;

  array = json_array_new ();

  if (array == NULL)
    return NULL;

  if (!dupin_mr_map (view->map, view->map_lang, obj, array))
    {
      json_array_unref (array);
      return NULL;
    }

  return array;
}

JsonNode *
dupin_mr_record_reduce  (DupinView * view, JsonArray * keys, JsonArray * values, gboolean rereduce)
{
  g_return_val_if_fail (values != NULL, NULL);

  return dupin_mr_reduce (view->reduce, view->reduce_lang, keys, values, rereduce);
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
	if (!(js = dupin_js_new_map (buffer,map, NULL)))
	  {
	    g_free (buffer);
            g_object_unref (gen);
            json_node_free (node);
	    return 0;
	  }

	g_free (buffer);

	if (!(array = (JsonArray *) dupin_js_get_mapResults (js)))
	  {
            g_object_unref (gen);
            json_node_free (node);
            dupin_js_destroy (js);
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
                dupin_js_destroy (js);
	        return 0;
              }

            json_array_add_element (ret_array, copy);
	  }
        g_list_free (nodes);

        g_object_unref (gen);
        json_node_free (node);
        dupin_js_destroy (js);
	return len;
      }
    }

  return 0;
}

static JsonNode *
dupin_mr_reduce (gchar * reduce,
		 DupinMRLang language,
		 JsonArray  * keys,
	         JsonArray  * values,
		 gboolean rereduce)
{
  switch (language)
    {
    case DP_MR_LANG_JAVASCRIPT:
      {
	DupinJs *js=NULL;
	JsonNode *ret=NULL;
	JsonNode *object_node=NULL;
	gchar *buffer_keys=NULL;
	gchar *buffer_values=NULL;
	gsize size_values,size_keys;
	JsonGenerator *gen=NULL;
	JsonNode *node_keys=NULL;
	JsonNode *node_values=NULL;

        /* serialise keys */
        if (keys != NULL) /* re-reduce requires this null */
          {
            node_keys = json_node_new (JSON_NODE_ARRAY);

	    if (node_keys == NULL)
              {
	        return NULL;
              }

            json_node_set_array (node_keys, keys);

  	    gen = json_generator_new();

	    if (gen == NULL)
              {
                return NULL;
              }

	    json_generator_set_root (gen, node_keys );
	    buffer_keys = json_generator_to_data (gen,&size_keys);
            g_object_unref (gen);

	    if (buffer_keys == NULL)
              {
                if (node_keys != NULL)
                  json_node_free (node_keys);
                return NULL;
              }
          }

        /* serialise values */
        node_values = json_node_new (JSON_NODE_ARRAY);

	if (node_values == NULL)
          {
            if (node_keys != NULL)
              json_node_free (node_keys);
	    return NULL;
          }

        json_node_set_array (node_values, values);

  	gen = json_generator_new();

	if (gen == NULL)
          {
            if (node_keys != NULL)
              json_node_free (node_keys);
            json_node_free (node_values);
            return NULL;
          }

	json_generator_set_root (gen, node_values );
	buffer_values = json_generator_to_data (gen,&size_values);
        g_object_unref (gen);

	if (buffer_values == NULL)
          {
            if (node_keys != NULL)
              json_node_free (node_keys);
            json_node_free (node_values);
            return NULL;
          }

	/* TODO - check that keys and values are plural here */

	/* TODO - we should really make sure escaping from JSON to Javascript structures is transferred right - and returned clean JSON too */

	if (!(js = dupin_js_new_reduce (buffer_keys, buffer_values, rereduce, reduce, NULL)))
	  {
            if (node_keys != NULL)
              json_node_free (node_keys);
            if (buffer_keys != NULL)
	      g_free (buffer_keys);
	    g_free (buffer_values);
            json_node_free (node_values);
	    return NULL;
	  }

        if (node_keys != NULL)
          json_node_free (node_keys);
        if (buffer_keys != NULL)
	  g_free (buffer_keys);
	g_free (buffer_values);
        json_node_free (node_values);

	if (!(object_node = (JsonNode *) dupin_js_get_reduceResult (js)))
	  {
            dupin_js_destroy (js);
	    return NULL;
	  }

        /* TODO - check if json-glib has a simpler and safe way to copy objects ! */
        ret = json_node_copy ((JsonNode*)object_node);

        dupin_js_destroy (js);

	return ret;
      }
    }

  return NULL;
}

/* EOF */
