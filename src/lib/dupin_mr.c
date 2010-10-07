#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_mr.h"
#include "dupin_internal.h"

static gsize dupin_mr_map (gchar * map, DupinMRLang language,
			   JsonObject * obj, JsonArray * array);
static JsonObject *dupin_mr_reduce (gchar * reduce,
					  DupinMRLang language,
					  JsonArray * array);

JsonObject *
dupin_mr_record (DupinView * view, JsonObject * obj)
{
  JsonArray *array;
  JsonObject *object;

  array = json_array_new ();

  if (array == NULL)
    return NULL;

  if (!dupin_mr_map (view->map, view->map_lang, obj, array))
    {
      g_object_unref (array);
      return NULL;
    }

  object = dupin_mr_reduce (view->reduce, view->reduce_lang, array);
  g_object_unref (array);

  return object;
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
            g_object_unref (node);
            return 0;
          }

	json_generator_set_root (gen, node );
	buffer = json_generator_to_data (gen,&size);

	if (buffer == NULL)
          {
            g_object_unref (gen);
            g_object_unref (node);
            return 0;
          }

	GString *str = g_string_new ("var mapObject = ");

	/* TODO - we should really make sure escaping from JSON to Javascript structures is transferred right */

	str = g_string_append_len (str, buffer, size);
	g_free (buffer);

	str = g_string_append (str, ";\n");
	str = g_string_append (str, map);

	buffer = g_string_free (str, FALSE);

	if (!(js = dupin_js_new (buffer)))
	  {
	    g_free (buffer);
            g_object_unref (gen);
            g_object_unref (node);
	    return 0;
	  }

	g_free (buffer);

	if (!(array = (JsonArray *) dupin_js_get_emitIntermediate (js)))
	  {
            g_object_unref (gen);
            g_object_unref (node);
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
                g_object_unref (node);
	        return 0;
              }

            json_array_add_element (ret_array, copy);
	  }
        g_free (nodes);

        g_object_unref (gen);
        g_object_unref (node);
	return len;
      }
    }

  return 0;
}

static JsonObject *
dupin_mr_reduce (gchar * reduce, DupinMRLang language,
		 JsonArray * array)
{
  switch (language)
    {
    case DP_MR_LANG_JAVASCRIPT:
      {
	DupinJs *js;
	JsonObject *object, *ret;
	gchar *buffer;
	gsize size;
	JsonGenerator *gen;
	JsonNode *node, *nodecopy, *nodecopy_real;

        node = json_node_new (JSON_NODE_ARRAY);

	if (node == NULL)
	  return NULL;

        json_node_set_array (node, array);

  	gen = json_generator_new();

	if (gen == NULL)
          {
            g_object_unref (node);
            return NULL;
          }

	json_generator_set_root (gen, node );
	buffer = json_generator_to_data (gen,&size);

	if (buffer == NULL)
          {
            g_object_unref (gen);
            g_object_unref (node);
            return NULL;
          }

	/* TODO - check that mapObjects is correct plural here */
	GString *str = g_string_new ("var mapObjects = ");

	/* TODO - we should really make sure escaping from JSON to Javascript structures is transferred right */

	str = g_string_append_len (str, buffer, size);
	g_free (buffer);

	str = g_string_append (str, ";\n");
	str = g_string_append (str, reduce);

	buffer = g_string_free (str, FALSE);

	if (!(js = dupin_js_new (buffer)))
	  {
	    g_free (buffer);
            g_object_unref (gen);
            g_object_unref (node);
	    return NULL;
	  }

	g_free (buffer);

	if (!(object = (JsonObject *) dupin_js_get_emit (js)))
	  {
            g_object_unref (gen);
            g_object_unref (node);
	    return NULL;
	  }

        /* TODO - check if json-glib has a simpler and safe way to copy objects ! */
        nodecopy = json_node_new (JSON_NODE_OBJECT);

	if (nodecopy == NULL)
	  {
            g_object_unref (gen);
            g_object_unref (node);
	    return NULL;
	  }

        json_node_set_object (nodecopy, object);

	nodecopy_real = json_node_copy (nodecopy);

	if (nodecopy_real == NULL)
	  {
            g_object_unref (nodecopy);
            g_object_unref (gen);
            g_object_unref (node);
	    return NULL;
	  }

	ret = json_node_get_object (nodecopy_real);

        g_object_unref (nodecopy);
        g_object_unref (gen);
        g_object_unref (node);

	return ret;
      }
    }

  return NULL;
}

/* EOF */
