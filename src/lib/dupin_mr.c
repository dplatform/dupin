#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_mr.h"
#include "dupin_internal.h"

static gsize dupin_mr_map (gchar * map, DupinMRLang language,
			   tb_json_object_t * obj, tb_json_array_t * array);
static tb_json_object_t *dupin_mr_reduce (gchar * reduce,
					  DupinMRLang language,
					  tb_json_array_t * array);

tb_json_object_t *
dupin_mr_record (DupinView * view, tb_json_object_t * obj)
{
  tb_json_array_t *array;
  tb_json_object_t *object;

  if (tb_json_array_new (&array) == FALSE)
    return NULL;

  if (!dupin_mr_map (view->map, view->map_lang, obj, array))
    {
      tb_json_array_destroy (array);
      return NULL;
    }

  object = dupin_mr_reduce (view->reduce, view->reduce_lang, array);
  tb_json_array_destroy (array);

  return object;
}

static gsize
dupin_mr_map (gchar * map, DupinMRLang language, tb_json_object_t * obj,
	      tb_json_array_t * ret_array)
{
  switch (language)
    {
    case DP_MR_LANG_JAVASCRIPT:
      {
	DupinJs *js;
	tb_json_array_t *array;
	gchar *buffer;
	gint i, len;
	gsize size;

	if (tb_json_object_write_to_buffer (obj, &buffer, &size, NULL) ==
	    FALSE)
	  return 0;

	GString *str = g_string_new ("var mapObject = ");

	str = g_string_append_len (str, buffer, size);
	g_free (buffer);

	str = g_string_append (str, ";\n");
	str = g_string_append (str, map);

	buffer = g_string_free (str, FALSE);

	if (!(js = dupin_js_new (buffer)))
	  {
	    g_free (buffer);
	    return 0;
	  }

	g_free (buffer);

	if (!(array = (tb_json_array_t *) dupin_js_get_emitIntermediate (js)))
	  {
	    dupin_js_destroy (js);
	    return 0;
	  }

	for (i = 0, len = tb_json_array_length (array); i < len; i++)
	  {
	    tb_json_value_t *v_s = tb_json_array_get (array, i);
	    tb_json_value_t *v_d;

	    tb_json_array_add (ret_array, NULL, &v_d);
	    tb_json_value_duplicate_exists (v_s, v_d);
	  }

	dupin_js_destroy (js);
	return len;
      }
    }

  return 0;
}

static tb_json_object_t *
dupin_mr_reduce (gchar * reduce, DupinMRLang language,
		 tb_json_array_t * array)
{
  switch (language)
    {
    case DP_MR_LANG_JAVASCRIPT:
      {
	DupinJs *js;
	tb_json_object_t *object, *ret;
	gchar *buffer;
	gsize size;

	if (tb_json_array_write_to_buffer (array, &buffer, &size, NULL) ==
	    FALSE)
	  return NULL;

	GString *str = g_string_new ("var mapObjects = ");

	str = g_string_append_len (str, buffer, size);
	g_free (buffer);

	str = g_string_append (str, ";\n");
	str = g_string_append (str, reduce);

	buffer = g_string_free (str, FALSE);

	if (!(js = dupin_js_new (buffer)))
	  {
	    g_free (buffer);
	    return NULL;
	  }

	g_free (buffer);

	if (!(object = (tb_json_object_t *) dupin_js_get_emit (js)))
	  {
	    dupin_js_destroy (js);
	    return NULL;
	  }

	tb_json_object_duplicate (object, &ret);
	dupin_js_destroy (js);
	return ret;
      }
    }

  return NULL;
}

/* EOF */
