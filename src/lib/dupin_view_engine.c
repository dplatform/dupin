#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"
#include "dupin_view_engine.h"

DupinViewEngine *
dupin_view_engine_new (Dupin * d,
	               DupinViewEngineLang language,
	               gchar * map_code,
	               gchar * reduce_code,
	               GError ** error)
{
  g_return_val_if_fail (d != NULL, NULL);
  g_return_val_if_fail (map_code != NULL, NULL);

  /* reduce is optional i.e. map only view */

  DupinViewEngine * engine = NULL;

  engine = g_malloc0 (sizeof (DupinViewEngine));

  if (language == DP_VIEW_ENGINE_LANG_JAVASCRIPT)
    {
      engine->runtime.javascript.webkit = dupin_webkit_new (d, error);

      if (engine->runtime.javascript.webkit == NULL)
        {
          g_free (engine);

          return NULL;
        }

      engine->language = language;

      /* TODO - validation of map_code and reduce_code */

      engine->map_code = g_strdup (map_code);

      engine->reduce_code = NULL;
      if (reduce_code != NULL)
        engine->reduce_code = g_strdup (reduce_code);

      return engine;
    }
  else
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_error_quark (), DUPIN_ERROR_INIT,
                   "View Engine runtime undefined.");

      g_free (engine);

      return NULL;
    }
}

void
dupin_view_engine_free (DupinViewEngine * engine)
{
  g_return_if_fail (engine != NULL);

  if (engine->map_code != NULL)
    g_free (engine->map_code);

  if (engine->reduce_code != NULL)
    g_free (engine->reduce_code);

  switch (engine->language)
    {
      case DP_VIEW_ENGINE_LANG_JAVASCRIPT:
	if (engine->runtime.javascript.webkit != NULL)
          {
	    dupin_webkit_free (engine->runtime.javascript.webkit);
	  }
        break;

      case DP_VIEW_ENGINE_LANG_DUPIN_GI:
        break;
    }

  g_free (engine);
}

DupinViewEngineLang
dupin_view_engine_get_language (DupinViewEngine * engine)
{
  return engine->language;
}

gboolean
dupin_view_engine_set_map_code (DupinViewEngine * engine,
		       	        gchar * map_code)
{
  g_return_val_if_fail (engine != NULL, FALSE);

  if (engine->map_code != NULL)
    g_free (engine->map_code);

  /* TODO - validation of map_code */

  engine->map_code = g_strdup (map_code);

  return TRUE;
}

gchar *
dupin_view_engine_get_map_code (DupinViewEngine * engine)
{
  g_return_val_if_fail (engine != NULL, NULL);

  return engine->map_code;
}

gboolean
dupin_view_engine_set_reduce_code (DupinViewEngine * engine,
			           gchar * reduce_code)
{
  g_return_val_if_fail (engine != NULL, FALSE);

  if (engine->reduce_code != NULL)
    g_free (engine->reduce_code);

  /* TODO - validation of reduce_code */

  engine->reduce_code = g_strdup (reduce_code);

  return TRUE;
}

gchar *
dupin_view_engine_get_reduce_code (DupinViewEngine * engine)
{
  g_return_val_if_fail (engine != NULL, NULL);

  return engine->reduce_code;
}

JsonNode *
dupin_view_engine_record_map (DupinViewEngine * engine,
		              JsonNode * obj)
{
  g_return_val_if_fail (engine != NULL, NULL);
  g_return_val_if_fail (obj != NULL, NULL);

  switch (engine->language)
    {
    case DP_VIEW_ENGINE_LANG_JAVASCRIPT:
      {
	gchar *buffer = dupin_util_json_serialize (obj);
	JsonNode * result;

	/* TODO - we should really make sure escaping from JSON to Javascript structures is transferred right - and returned clean JSON too */

#if DUPIN_VIEW_DEBUG
	DUPIN_UTIL_DUMP_JSON ("dupin_view_engine_record_map(): OBJ", obj);
        g_message ("dupin_view_engine_record_map(): buffer=%s\n", buffer);
#endif

	result = dupin_webkit_map (engine->runtime.javascript.webkit,
				   buffer,
			           dupin_view_engine_get_map_code (engine),
				   NULL);
	g_free (buffer);

#if DUPIN_VIEW_DEBUG
	DUPIN_UTIL_DUMP_JSON ("dupin_view_engine_record_map(): result", result);
#endif

	return result;
      }
    case DP_VIEW_ENGINE_LANG_DUPIN_GI:
      {
        /* 
	   - index all fields, one level, two levels, all levels - see portable-listings filterby and filterop
		-> foo, foo.bar.baz etc. etc...	
	   - free-text index of set of of fileds (E.g. title and summary)
	   - date-search
	   - spatial-search (geo-json / geo-couch and using r-tree module in sqlite)
	   - similary matching of records
         */
	   
      }
    }

  return NULL;
}

JsonNode *
dupin_view_engine_record_reduce (DupinViewEngine * engine,
			         JsonNode * keys,
			         JsonNode * values,
			         gboolean rereduce)
{
  g_return_val_if_fail (engine != NULL, NULL);
  g_return_val_if_fail (values != NULL, NULL);

  switch (engine->language)
    {
    case DP_VIEW_ENGINE_LANG_JAVASCRIPT:
      {
        JsonNode * result = NULL;
	gchar * buffer_keys = NULL;
	gchar * buffer_values = NULL;

        if (keys != NULL)
	  {
	    buffer_keys = dupin_util_json_serialize (keys);

	    if (buffer_keys == NULL)
	      return NULL;
	  }

	buffer_values = dupin_util_json_serialize (values);

	if (buffer_values == NULL)
          {
            if (buffer_keys != NULL)
              g_free (buffer_keys);

            return NULL;
          }

#if DUPIN_VIEW_DEBUG
	DUPIN_UTIL_DUMP_JSON ("dupin_view_engine_record_reduce(): keys", keys);
        g_message ("dupin_view_engine_record_reduce(): buffer_keys=%s\n", buffer_keys);

	DUPIN_UTIL_DUMP_JSON ("dupin_view_engine_record_reduce(): values", values);
        g_message ("dupin_view_engine_record_reduce(): buffer_values=%s\n", buffer_values);
#endif

	/* TODO - check that keys and values are plural here */

	/* TODO - we should really make sure escaping from JSON to Javascript structures is transferred right - and returned clean JSON too */

	result = dupin_webkit_reduce (engine->runtime.javascript.webkit,
				          buffer_keys,
				          buffer_values,
				          rereduce,
				          dupin_view_engine_get_reduce_code (engine),
				          NULL);

        if (buffer_keys != NULL)
          g_free (buffer_keys);

        g_free (buffer_values);

#if DUPIN_VIEW_DEBUG
	DUPIN_UTIL_DUMP_JSON ("dupin_view_engine_record_reduce(): result", result);
#endif

	return result;
      }
    case DP_VIEW_ENGINE_LANG_DUPIN_GI:
      {
      }
    }

  return NULL;
}

/* EOF */
