#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"

#include <string.h>

gboolean
dupin_util_is_valid_db_name (gchar * db)
{
  g_return_val_if_fail (db != NULL, FALSE);

  if (*db == '_')
    return FALSE;

  /* FIXME: something else? */

  return TRUE;
}

gboolean
dupin_util_is_valid_linkb_name (gchar * linkb)
{
  g_return_val_if_fail (linkb != NULL, FALSE);

  if (*linkb == '_')
    return FALSE;

  /* FIXME: something else? */

  return TRUE;
}

gboolean
dupin_util_is_valid_view_name (gchar * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (*view == '_')
    return FALSE;

  /* FIXME: something else? */

  return TRUE;
}

gboolean
dupin_util_is_valid_attachment_db_name (gchar * attachment_db)
{
  g_return_val_if_fail (attachment_db != NULL, FALSE);

  if (*attachment_db == '_')
    return FALSE;

  /* FIXME: something else? */

  return TRUE;
}

gint
dupin_util_dupin_mode_to_sqlite_mode (DupinSQLiteOpenType dupin_mode)
{
  gint sqlite3_open_v2_mode;

  if (dupin_mode == DP_SQLITE_OPEN_READONLY)
    sqlite3_open_v2_mode = SQLITE_OPEN_READONLY;

  else if (dupin_mode == DP_SQLITE_OPEN_READWRITE)
    sqlite3_open_v2_mode = SQLITE_OPEN_READWRITE;

  else if (dupin_mode == DP_SQLITE_OPEN_CREATE)
    sqlite3_open_v2_mode = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

  return sqlite3_open_v2_mode;
}

gboolean
dupin_util_is_valid_record_id (gchar * id)
{
  g_return_val_if_fail (id != NULL, FALSE);

  /* FIXME: something else? */

 /* TODO - consider '/' as valid document id sub-parts I.e. _design/foo/bar-attachment */

  return (strlen(id)<=DUPIN_ID_MAX_LEN) ? TRUE : FALSE;
}

gboolean
dupin_util_is_valid_record_type (gchar * type)
{
  g_return_val_if_fail (type != NULL, FALSE);

  return g_strcmp0 (type, "") ? TRUE : FALSE;
}

gboolean
dupin_util_is_valid_absolute_uri (gchar * uri)
{
  g_return_val_if_fail (uri != NULL, FALSE);

  gchar *check = (gchar *) uri;

  if (g_ascii_isalpha (*check))
    {
      check++;
      while (g_ascii_isalnum (*check) || *check == '+'
             || *check == '-' || *check == '.')
        check++;
    }

  return *check == ':';
}

/* see also http://engineering.twitter.com/2010/06/announcing-snowflake.html */

/* TODO - rework this function to be network portable (indep. of NTP) and sequential etc */
/* roughly we want an ID which is unique per thread, machine/server and sequential, and sortable */

gchar *
dupin_util_generate_id (GError **  error)
{
  gchar guid[32];

  static const unsigned char rchars[] =
	"abcdefghijklmnopqrstuvwxyz"
	"0123456789";
  gint i;

  sqlite3_randomness(sizeof(guid), guid);

  for (i=0; i<sizeof(guid); i++)
    {
      guid[i] = rchars[ guid[i] % (sizeof(rchars)-1) ];
    }
  guid[sizeof(guid)] = '\0';

  return g_compute_checksum_for_string (DUPIN_ID_HASH_ALGO, guid, -1);
}

gboolean
dupin_util_is_valid_view_engine_lang (gchar * lang)
{
  g_return_val_if_fail (lang != NULL, FALSE);

  if (!g_strcmp0 (lang, "javascript"))
    return TRUE;

  return FALSE;
}

DupinViewEngineLang
dupin_util_view_engine_lang_to_enum (gchar * lang)
{
  g_return_val_if_fail (lang != NULL, 0);

  if (!g_strcmp0 (lang, "javascript"))
    return DP_VIEW_ENGINE_LANG_JAVASCRIPT;

  if (!g_strcmp0 (lang, "dupin_gi"))
    return DP_VIEW_ENGINE_LANG_DUPIN_GI;

  g_return_val_if_fail (dupin_util_is_valid_view_engine_lang (lang) == TRUE, 0);
  return 0;
}

const gchar *
dupin_util_view_engine_lang_to_string (DupinViewEngineLang lang)
{
  switch (lang)
    {
    case DP_VIEW_ENGINE_LANG_JAVASCRIPT:
      return "javascript";

    default:
      return NULL;
    }
}

/* borrowed from json-glib json-generator.c library */

/* non-ASCII characters can't be escaped, otherwise UTF-8
 * chars will break, so we just pregenerate this table of
 * high characters and then we feed it to g_strescape()
 */
static const char dupin_util_json_exceptions[] = {
  0x7f,  0x80,  0x81,  0x82,  0x83,  0x84,  0x85,  0x86,
  0x87,  0x88,  0x89,  0x8a,  0x8b,  0x8c,  0x8d,  0x8e,
  0x8f,  0x90,  0x91,  0x92,  0x93,  0x94,  0x95,  0x96,
  0x97,  0x98,  0x99,  0x9a,  0x9b,  0x9c,  0x9d,  0x9e,
  0x9f,  0xa0,  0xa1,  0xa2,  0xa3,  0xa4,  0xa5,  0xa6,
  0xa7,  0xa8,  0xa9,  0xaa,  0xab,  0xac,  0xad,  0xae,
  0xaf,  0xb0,  0xb1,  0xb2,  0xb3,  0xb4,  0xb5,  0xb6,
  0xb7,  0xb8,  0xb9,  0xba,  0xbb,  0xbc,  0xbd,  0xbe,
  0xbf,  0xc0,  0xc1,  0xc2,  0xc3,  0xc4,  0xc5,  0xc6,
  0xc7,  0xc8,  0xc9,  0xca,  0xcb,  0xcc,  0xcd,  0xce,
  0xcf,  0xd0,  0xd1,  0xd2,  0xd3,  0xd4,  0xd5,  0xd6,
  0xd7,  0xd8,  0xd9,  0xda,  0xdb,  0xdc,  0xdd,  0xde,
  0xdf,  0xe0,  0xe1,  0xe2,  0xe3,  0xe4,  0xe5,  0xe6,
  0xe7,  0xe8,  0xe9,  0xea,  0xeb,  0xec,  0xed,  0xee,
  0xef,  0xf0,  0xf1,  0xf2,  0xf3,  0xf4,  0xf5,  0xf6,
  0xf7,  0xf8,  0xf9,  0xfa,  0xfb,  0xfc,  0xfd,  0xfe,
  0xff,
  '\0'   /* g_strescape() expects a NUL-terminated string */
};

gchar *
dupin_util_json_strescape (const gchar *str)
{
  return g_strescape (str, dupin_util_json_exceptions);
}

/* NOTE - JsonGenerator does not work for simple JSON value or null, too bad */

gchar *
dupin_util_json_serialize (JsonNode * node)
{
  g_return_val_if_fail (node != NULL, NULL);

  gchar * node_serialized = NULL;

  if (json_node_get_node_type (node) == JSON_NODE_VALUE)
    {
      if (json_node_get_value_type (node) == G_TYPE_STRING)
        {
          gchar *tmp = dupin_util_json_strescape (json_node_get_string (node)); 
	  node_serialized = g_strdup_printf ("\"%s\"", tmp); 
	  g_free (tmp);
        }

      if (json_node_get_value_type (node) == G_TYPE_DOUBLE
          || json_node_get_value_type (node) == G_TYPE_FLOAT)
        {
          gdouble numb = json_node_get_double (node);
          node_serialized = g_strdup_printf ("%f", numb);
        }

      if (json_node_get_value_type (node) == G_TYPE_INT
          || json_node_get_value_type (node) == G_TYPE_INT64
          || json_node_get_value_type (node) == G_TYPE_UINT)
        {
          gint numb = (gint) json_node_get_int (node);
          node_serialized = g_strdup_printf ("%" G_GUINT64_FORMAT, (long unsigned int)numb);
        }

      if (json_node_get_value_type (node) == G_TYPE_BOOLEAN)
        {
          node_serialized = g_strdup_printf (json_node_get_boolean (node) == TRUE ? "true" : "false");
        }
    }
  else if (json_node_get_node_type (node) == JSON_NODE_NULL)
    {
      node_serialized = g_strdup ("null");
    }
  else
    {
      JsonGenerator * gen = json_generator_new();

      if (gen == NULL)
        return NULL;

      //g_object_set (gen, "pretty", TRUE, NULL);
      //g_object_set (gen, "indent", 2, NULL);

      json_generator_set_root (gen, node);

      node_serialized = json_generator_to_data (gen,NULL);

      g_object_unref (gen);
    }

  return node_serialized;
}

gchar *
dupin_util_json_value_to_string (JsonNode * node)
{
  g_return_val_if_fail (node != NULL, NULL);
  g_return_val_if_fail (json_node_get_node_type (node) == JSON_NODE_VALUE
			|| json_node_get_node_type (node) == JSON_NODE_NULL, NULL);

  gchar * node_serialized = NULL;

  if (json_node_get_node_type (node) == JSON_NODE_VALUE)
    {
      if (json_node_get_value_type (node) == G_TYPE_STRING)
        {
          gchar *tmp = dupin_util_json_strescape (json_node_get_string (node));

	  /* NOTE - the only difference with dupin_util_json_serialize() above ! */
          node_serialized = g_strdup_printf ("%s", tmp);

          g_free (tmp);
        }

      if (json_node_get_value_type (node) == G_TYPE_DOUBLE
          || json_node_get_value_type (node) == G_TYPE_FLOAT)
        {
          gdouble numb = json_node_get_double (node);
          node_serialized = g_strdup_printf ("%f", numb);
        }

      if (json_node_get_value_type (node) == G_TYPE_INT
          || json_node_get_value_type (node) == G_TYPE_INT64
          || json_node_get_value_type (node) == G_TYPE_UINT)
        {
          gint numb = (gint) json_node_get_int (node);
          node_serialized = g_strdup_printf ("%" G_GUINT64_FORMAT, (long unsigned int)numb);
        }

      if (json_node_get_value_type (node) == G_TYPE_BOOLEAN)
        {
          node_serialized = g_strdup_printf (json_node_get_boolean (node) == TRUE ? "true" : "false");
        }
    }
  else if (json_node_get_node_type (node) == JSON_NODE_NULL)
    {
      node_serialized = g_strdup ("null");
    }

  return node_serialized;
}

/* NOTE - proper deep copy/clone of given JsonNode */

JsonNode *
dupin_util_json_node_clone (JsonNode * node, GError **error)
{
  g_return_val_if_fail (node != NULL, NULL);

  JsonParser * parser = json_parser_new ();

  if (parser == NULL)
    return NULL;

  gchar * node_serialized = dupin_util_json_serialize (node);

  if (node_serialized == NULL)
    {
      g_object_unref (parser);
      return NULL;
    }

  if (node_serialized == NULL
      || (!json_parser_load_from_data (parser, node_serialized, -1, error)))
    {
      if (error && *error)
        g_error_free (*error);

      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_CRUD, "Cannot parser JSON");

      g_object_unref (parser);

      if (node_serialized != NULL)
        g_free (node_serialized);

      return NULL;
    }

  g_free (node_serialized);

  JsonNode * clone = json_node_copy (json_parser_get_root (parser));

  g_object_unref (parser);

  return clone;
}


/* WARNING - the following code even if correct, might have issues with
             buggy json-glib and treating ints vs. double, with others - see the above version instead */

JsonNode *
dupin_util_json_node_clone_v1 (JsonNode * node)
{
  g_return_val_if_fail (node != NULL, NULL);

  JsonNode * clone = NULL;

  if (json_node_get_node_type (node) == JSON_NODE_OBJECT)
    {
      clone = json_node_new (JSON_NODE_OBJECT);
      JsonObject *obj = json_object_new();
      GList *nodes, *n;
      nodes = json_object_get_members (json_node_get_object (node));
      for (n = nodes; n != NULL; n = n->next)
        json_object_set_member (obj,
				(const gchar *) n->data,
				dupin_util_json_node_clone_v1 (
					json_object_get_member (json_node_get_object (node), (const gchar *)n->data)));
      g_list_free (nodes);
      json_node_take_object(clone, obj);
    }
  else if (json_node_get_node_type (node) == JSON_NODE_ARRAY)
    {
      clone = json_node_new (JSON_NODE_ARRAY);
      JsonArray *array = json_array_new();
      GList *nodes, *n;
      nodes = json_array_get_elements (json_node_get_array (node));
      for (n = nodes; n != NULL; n = n->next)
        json_array_add_element(array, dupin_util_json_node_clone_v1 (n->data));
      g_list_free (nodes);
      json_node_take_array(clone, array);
    }
  else if (json_node_get_node_type (node) == JSON_NODE_VALUE)
    {
      clone = json_node_new (JSON_NODE_VALUE); 

      if (json_node_get_value_type (node) == G_TYPE_STRING)
        {
          json_node_set_string (clone, json_node_get_string (node));
        }

      if (json_node_get_value_type (node) == G_TYPE_DOUBLE
          || json_node_get_value_type (node) == G_TYPE_FLOAT)
        {
          json_node_set_double (clone, json_node_get_double (node));
        }

      if (json_node_get_value_type (node) == G_TYPE_INT
          || json_node_get_value_type (node) == G_TYPE_INT64
          || json_node_get_value_type (node) == G_TYPE_UINT)
        {
          json_node_set_int (clone, json_node_get_int (node));
        }

      if (json_node_get_value_type (node) == G_TYPE_BOOLEAN)
        {
          json_node_set_boolean (clone, json_node_get_boolean (node));
        }
    }
  else if (json_node_get_node_type (node) == JSON_NODE_NULL)
    {
      clone = json_node_new (JSON_NODE_NULL); 
    }

  return clone;
}

gboolean
dupin_util_json_node_object_filter_fields_real (JsonNode * node,
                                                DupinFieldsFormatType format,
					 	gchar ** iesim_field_splitted,
				 	 	gint level,
					 	JsonNode * result_node,
					   	gboolean not,
                                 	 	GError **  error)
{
//g_message("dupin_util_json_node_object_filter_fields_real: level %d\n", level);

  if (format == DP_FIELDS_FORMAT_DOTTED)
    {
//g_message("dupin_util_json_node_object_filter_fields_real: check member %s\n", iesim_field_splitted[level]);

      if (json_object_has_member (json_node_get_object (node), iesim_field_splitted[level]) == TRUE)
        {
	  JsonNode * member = json_object_get_member (json_node_get_object (node), iesim_field_splitted[level]);

//g_message("dupin_util_json_node_object_filter_fields_real: has member %s\n", iesim_field_splitted[level]);
//DUPIN_UTIL_DUMP_JSON ("member", member);

	  /* NOTE - deep first visit the children */
          if (json_node_get_node_type (member) == JSON_NODE_OBJECT)
 	    {
//g_message("dupin_util_json_node_object_filter_fields_real: member %s is object\n", iesim_field_splitted[level]);

              JsonNode * sub_result_node = NULL;
              if (json_object_has_member (json_node_get_object (result_node), iesim_field_splitted[level]) == FALSE)
                {
		  if (not == FALSE)
		    {
	              if (iesim_field_splitted[level+1])
		        {
                          sub_result_node = json_node_new (JSON_NODE_OBJECT);
                          JsonObject * sub_result_node_obj = json_object_new ();
                          json_node_take_object (sub_result_node, sub_result_node_obj);
		        }
		      else
		        {
                          sub_result_node = dupin_util_json_node_clone (member, error);
		        }
		    }
		}
	      else
	        {
		  if (not == TRUE)
	            json_object_remove_member (json_node_get_object (result_node), iesim_field_splitted[level]);
                  else
	            sub_result_node = json_object_get_member (json_node_get_object (result_node), iesim_field_splitted[level]);
		}

	      if (iesim_field_splitted[level+1])
	        {
                  if (dupin_util_json_node_object_filter_fields_real (member, format, iesim_field_splitted, level+1, sub_result_node, not, error) == FALSE)
	            {
                      if ((json_object_has_member (json_node_get_object (result_node), iesim_field_splitted[level]) == FALSE)
		          && not == FALSE)
                        json_node_free (sub_result_node);

		      return FALSE;
 		    }
 		}

              if ((json_object_has_member (json_node_get_object (result_node), iesim_field_splitted[level]) == FALSE)
		  && not == FALSE)
	        json_object_set_member (json_node_get_object (result_node),
					iesim_field_splitted[level],
					sub_result_node);
	      return TRUE;
            }

          else
            {
	      if (not == TRUE)
                {
//g_message("dupin_util_json_node_object_filter_fields_real: removed member %s from result\n", iesim_field_splitted[level]);

	          json_object_remove_member (json_node_get_object (result_node), iesim_field_splitted[level]);
 	        }
	      else
                {
//g_message("dupin_util_json_node_object_filter_fields_real: added member %s to result\n", iesim_field_splitted[level]);

	          json_object_set_member (json_node_get_object (result_node),
				          iesim_field_splitted[level],
				          dupin_util_json_node_clone (member, error));
                }
              return TRUE;
            }
        }
      else
        {
          return FALSE;
        }
    }

  return FALSE;
}

JsonNode *
dupin_util_json_node_object_filter_fields (JsonNode * node,
                               	    	   DupinFieldsFormatType format,
                               	    	   gchar **   fields,
					   gboolean not,
                               	    	   GError **  error)
{
  g_return_val_if_fail (node != NULL, NULL);
  g_return_val_if_fail (json_node_get_node_type (node) == JSON_NODE_OBJECT, NULL);

  JsonNode * filtered_node = NULL;
  JsonObject * filtered_node_obj = NULL;

  if (format == DP_FIELDS_FORMAT_NONE
      || !fields[0])
    { 
      if (not == TRUE)
        {
          filtered_node = dupin_util_json_node_clone (node, error);
        }
      else
        {
          filtered_node = json_node_new (JSON_NODE_OBJECT);
          filtered_node_obj = json_object_new ();
          json_node_take_object (filtered_node, filtered_node_obj);
        }

      return filtered_node;
    }

  /* NOTE - parse the field names */

  GList * parsed_fields = NULL;

  /* TODO - with JSONPath we might have problems with commas, due
	    URI-unescape already happened in httpd.c */

  gint i;
  gboolean any = FALSE;
  for (i = 0; fields[i]; i++)
    {
      if (fields[i] == NULL || (!strlen (fields[i]))) 
        {
	  continue;
	}

      if (!g_strcmp0 (fields[i], REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL)
	  || !g_strcmp0 (fields[i], REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL_FIELDS))
        {
	  any = TRUE;
	  break;
        }

      if (format == DP_FIELDS_FORMAT_DOTTED)
        {
	  gchar ** iesim_field_splitted = g_strsplit (fields[i], ".", -1);
          parsed_fields = g_list_prepend (parsed_fields, iesim_field_splitted);
        }
      else if (format == DP_FIELDS_FORMAT_JSONPATH)
        {
        }
    }

  if (any == FALSE)
    {
      if (not == TRUE)
        {
          filtered_node = dupin_util_json_node_clone (node, error);
        }
      else
        {
          filtered_node = json_node_new (JSON_NODE_OBJECT);
          filtered_node_obj = json_object_new ();
          json_node_take_object (filtered_node, filtered_node_obj);
	}

      GList *f;
      for (f = parsed_fields; f != NULL; f = f->next)
        {
          gchar ** iesim_field_splitted = f->data;

          dupin_util_json_node_object_filter_fields_real (node, format, iesim_field_splitted, 0, filtered_node, not, error);
	}
    }
  else
    {
      if (not == TRUE)
        {
          filtered_node = json_node_new (JSON_NODE_OBJECT);
          filtered_node_obj = json_object_new ();
          json_node_take_object (filtered_node, filtered_node_obj);
        }
      else
        {
          filtered_node = dupin_util_json_node_clone (node, error);
        }
    }

  while (parsed_fields)
    {
      if (format == DP_FIELDS_FORMAT_DOTTED)
        {
          if (parsed_fields->data != NULL)
            g_strfreev (parsed_fields->data);  
        }
      else if (format == DP_FIELDS_FORMAT_JSONPATH)
        {
        }
      parsed_fields = g_list_remove (parsed_fields, parsed_fields->data);
    }

  return filtered_node;
}

gboolean
dupin_util_json_node_object_grep_nodes_real (JsonNode * node,
                                             DupinFieldsFormatType format,
					     gchar ** iesim_field_splitted,
				 	     gint level,
					     JsonNode * result_node,
                                 	     GError **  error)
{
//g_message("dupin_util_json_node_object_grep_nodes_real: level %d\n", level);
//DUPIN_UTIL_DUMP_JSON ("result_node: ", result_node);

  if (format == DP_FIELDS_FORMAT_DOTTED)
    {
//g_message("dupin_util_json_node_object_grep_nodes_real: check member %s\n", iesim_field_splitted[level]);

      if (json_object_has_member (json_node_get_object (node), iesim_field_splitted[level]) == TRUE)
        {
	  JsonNode * member = json_object_get_member (json_node_get_object (node), iesim_field_splitted[level]);

//g_message("dupin_util_json_node_object_grep_nodes_real: has member %s\n", iesim_field_splitted[level]);
//DUPIN_UTIL_DUMP_JSON ("member", member);

	  /* NOTE - deep first visit the children */
          if (json_node_get_node_type (member) == JSON_NODE_OBJECT)
 	    {
//g_message("dupin_util_json_node_object_grep_nodes_real: member %s is object\n", iesim_field_splitted[level]);

	      if (iesim_field_splitted[level+1])
	        {
                  if (dupin_util_json_node_object_grep_nodes_real (member, format, iesim_field_splitted, level+1, result_node, error) == FALSE)
	            {
		      return FALSE;
 		    }
 		}
              else
                {
	          json_array_add_element (json_node_get_array (result_node), dupin_util_json_node_clone (member, error));
		}

	      return TRUE;
            }

          else
            {
	      /* NOTE - leaf node (json value, null or array) */

//g_message("dupin_util_json_node_object_grep_nodes_real: add member %s to result\n", iesim_field_splitted[level]);

	      json_array_add_element (json_node_get_array (result_node), dupin_util_json_node_clone (member, error));
              return TRUE;
            }
        }
      else
        {
          return FALSE;
        }
    }

  return FALSE;
}

JsonNode *
dupin_util_json_node_object_grep_nodes (JsonNode * node,
                               	    	DupinFieldsFormatType format,
                               	    	gchar **   fields,
				  	DupinFieldsFormatType filter_op,
                               	    	gchar **   filter_values,
                               	    	GError **  error)
{
  g_return_val_if_fail (node != NULL, NULL);
  g_return_val_if_fail (json_node_get_node_type (node) == JSON_NODE_OBJECT, NULL);

  JsonNode * grep_nodes = json_node_new (JSON_NODE_ARRAY);
  JsonArray * grep_nodes_array = json_array_new ();
  json_node_take_array (grep_nodes, grep_nodes_array);

  if (format == DP_FIELDS_FORMAT_NONE
      || !fields[0]
      || (filter_op != DP_FILTERBY_PRESENT && !filter_values[0]))
    {
      json_array_add_element (grep_nodes_array, dupin_util_json_node_clone (node, error));
      return grep_nodes;
    }

  /* NOTE - parse the field names */

  GList * parsed_fields = NULL;

  /* TODO - with JSONPath we might have problems with commas, due
	    URI-unescape already happened in httpd.c */

  gint i;
  for (i = 0; fields[i]; i++)
    {
      if (format == DP_FIELDS_FORMAT_DOTTED)
        {
	  gchar ** iesim_field_splitted = g_strsplit (fields[i], ".", -1);
          parsed_fields = g_list_prepend (parsed_fields, iesim_field_splitted);
        }
      else if (format == DP_FIELDS_FORMAT_JSONPATH)
        {
        }
    }

  /* TODO - add JsonPath logic */

  JsonNode * matched_nodes = json_node_new (JSON_NODE_ARRAY);
  JsonArray * matched_nodes_array = json_array_new ();
  json_node_take_array (matched_nodes, matched_nodes_array);

  GList *f;
  for (f = parsed_fields; f != NULL; f = f->next)
    {
      gchar ** iesim_field_splitted = f->data;

      /* NOTE - dotted selector */
      dupin_util_json_node_object_grep_nodes_real (node, format, iesim_field_splitted, 0, matched_nodes, error);
    }

  /* NOTE - does the Portable Listings filterBy logic here - see http://www.portablelistings.net/spec/portable-listings-spec.html#filtering */

  /* NOTE - we consider the comma a light 'OR' operator, return on first match
            I.e. for each selected node (matched) check the operator on each of the values */

#if 0
g_message("dupin_util_json_node_object_grep_nodes: done selectors \n");
g_message("dupin_util_json_node_object_grep_nodes: fields:\n");
for (i = 0; fields[i]; i++)
  {
g_message("\t\tfields[%d]=%s\n", i, fields[i]);
  }
g_message("dupin_util_json_node_object_grep_nodes: op:\n");
if (filter_op == DP_FILTERBY_PRESENT)
  g_message("\t\tpresent\n");
else if (filter_op == DP_FILTERBY_STARTS_WITH)
  g_message("\t\tstarts with\n");
else if (filter_op == DP_FILTERBY_EQUALS)
  g_message("\t\tequals\n");
else if (filter_op == DP_FILTERBY_CONTAINS)
  g_message("\t\tcontains\n");
g_message("dupin_util_json_node_object_grep_nodes: filter_values:\n");
if (filter_values != NULL
    && filter_values[0])
{
for (i = 0; filter_values[i]; i++)
  {
g_message("\t\tfilter_values[%d]=%s\n", i, filter_values[i]);
  }
}
else
{
g_message("\t\t(EMPTY)\n");
}
DUPIN_UTIL_DUMP_JSON ("dupin_util_json_node_object_grep_nodes: matched nodes:", matched_nodes);
#endif

  GList *n, *nodes;
  JsonNode * field_value_to_match=NULL;
  nodes = json_array_get_elements (matched_nodes_array);
  for (n = nodes; n != NULL ; n = n->next)
    {
      JsonNode * n_node = (JsonNode *)n->data;

      gboolean matched = FALSE;

//DUPIN_UTIL_DUMP_JSON ("dupin_util_json_node_object_grep_nodes: comparing node value:", n_node);

      /* plural field */
      if (json_node_get_node_type (n_node) == JSON_NODE_ARRAY)
        {
//g_message("dupin_util_json_node_object_grep_nodes: plural field\n");

	  /* plural fields, stop at first match */
          if (filter_op == DP_FILTERBY_PRESENT
	      && json_array_get_length (json_node_get_array (n_node)) > 0)
            {
              matched = TRUE;

//g_message("dupin_util_json_node_object_grep_nodes: OK it is PRESENT\n");
	    }
	  else
	    {
//g_message("dupin_util_json_node_object_grep_nodes: checking array elements\n");

              GList *p, *pnodes;
              pnodes = json_array_get_elements (json_node_get_array (n_node));
	      for (p = pnodes; p != NULL ; p = p->next)
                {
                  JsonNode * p_node = (JsonNode *)p->data;

	          field_value_to_match=p_node;

//DUPIN_UTIL_DUMP_JSON ("dupin_util_json_node_object_grep_nodes: checking sub-field value:", p_node);

	          /* complex */
                  if (json_node_get_node_type (p_node) == JSON_NODE_OBJECT)
                    {
//g_message("dupin_util_json_node_object_grep_nodes: complex field\n");

		      /* get primary sub-field */
		      GList * primary_fields = dupin_util_poli_get_primary_fields (NULL, NULL,
									       p_node, error);
		      gchar * main_primary_field_name = (gchar *) primary_fields->data;

//g_message("dupin_util_json_node_object_grep_nodes: got main primary field name = '%s'\n", main_primary_field_name);

		      field_value_to_match = json_object_get_member (json_node_get_object (p_node), main_primary_field_name);
 
//DUPIN_UTIL_DUMP_JSON ("dupin_util_json_node_object_grep_nodes: got main primary field value:", field_value_to_match);

		      dupin_util_poli_get_primary_fields_list_close (primary_fields);
	            }
                  else if (json_node_get_node_type (p_node) == JSON_NODE_ARRAY)
		    {
//g_message("dupin_util_json_node_object_grep_nodes: plural inside complex field, ingnored\n");

		      continue;
                    }

	          gchar * field_value_to_match_string=dupin_util_json_value_to_string (field_value_to_match);

//g_message("dupin_util_json_node_object_grep_nodes: value to check is %s\n", field_value_to_match_string);

  	          /* NOTE - per PoLi spec passed values are "strings" */
  	          for (i = 0; filter_values[i]; i++)
                    {
                      if (filter_op == DP_FILTERBY_EQUALS )
		        {
			  matched = (g_utf8_collate (field_value_to_match_string, filter_values[i]) == 0) ? TRUE : FALSE;

//if (matched)
//g_message("dupin_util_json_node_object_grep_nodes: OK it is EQUALS to %s\n", filter_values[i]);
			}

                      else if (filter_op == DP_FILTERBY_STARTS_WITH)
                        {
			  matched = g_str_has_prefix (field_value_to_match_string, filter_values[i]);

//if (matched)
//g_message("dupin_util_json_node_object_grep_nodes: OK it STARTS-WITH %s\n", filter_values[i]);
                        }

                      else if (filter_op == DP_FILTERBY_CONTAINS)
                        {
			  matched = g_strrstr (field_value_to_match_string, filter_values[i]) ? TRUE : FALSE;

//if (matched)
//g_message("dupin_util_json_node_object_grep_nodes: OK it CONTAINS %s\n", filter_values[i]);
                        }

	              if (matched == TRUE)
	                break;
                    }

                 g_free (field_value_to_match_string);
	       }
             g_list_free (pnodes);
           }
        }
      else if (filter_op == DP_FILTERBY_PRESENT)
        {
          matched = TRUE;

//g_message("dupin_util_json_node_object_grep_nodes: OK it is PRESENT\n");
        }
      else
        {
//g_message("dupin_util_json_node_object_grep_nodes: singular field\n");

	  field_value_to_match=n_node;

	  /* complex */
          if (json_node_get_node_type (n_node) == JSON_NODE_OBJECT)
            {
//g_message("dupin_util_json_node_object_grep_nodes: complex field\n");

              /* get primary sub-field */
              GList * primary_fields = dupin_util_poli_get_primary_fields (NULL, NULL,
                                                                               n_node, error);
              gchar * main_primary_field_name = (gchar *) primary_fields->data;

//g_message("dupin_util_json_node_object_grep_nodes: got main primary field name = '%s'\n", main_primary_field_name);

              field_value_to_match = json_object_get_member (json_node_get_object (n_node), main_primary_field_name);

//DUPIN_UTIL_DUMP_JSON ("dupin_util_json_node_object_grep_nodes: got main primary field value:", field_value_to_match);

              dupin_util_poli_get_primary_fields_list_close (primary_fields);
            }
          else
            {
//g_message("dupin_util_json_node_object_grep_nodes: simple field\n");

              gchar * field_value_to_match_string=dupin_util_json_value_to_string (field_value_to_match);

//g_message("dupin_util_json_node_object_grep_nodes: value to check is %s\n", field_value_to_match_string);

              /* NOTE - per PoLi spec passed values are "strings" */
              for (i = 0; filter_values[i]; i++)
                {
                  if (filter_op == DP_FILTERBY_EQUALS )
                    {
                      matched = (g_utf8_collate (field_value_to_match_string, filter_values[i]) == 0) ? TRUE : FALSE;

//if (matched)
//g_message("dupin_util_json_node_object_grep_nodes: OK it is EQUALS to %s\n", filter_values[i]);
                    }

                  else if (filter_op == DP_FILTERBY_STARTS_WITH)
                    {
                      matched = g_str_has_prefix (field_value_to_match_string, filter_values[i]);

//if (matched)
//g_message("dupin_util_json_node_object_grep_nodes: OK it STARTS-WITH %s\n", filter_values[i]);
                    }

                  else if (filter_op == DP_FILTERBY_CONTAINS)
                    {
                      matched = g_strrstr (field_value_to_match_string, filter_values[i]) ? TRUE : FALSE;

//if (matched)
//g_message("dupin_util_json_node_object_grep_nodes: OK it CONTAINS %s\n", filter_values[i]);
                    }  

	          if (matched == TRUE)
	            break;
	        }

              g_free (field_value_to_match_string);
            }
        }

      if (matched == TRUE)
        json_array_add_element (grep_nodes_array, json_node_copy (n_node));
    }
  g_list_free (nodes);

  json_node_free (matched_nodes);

  while (parsed_fields)
    {
      if (format == DP_FIELDS_FORMAT_DOTTED)
        {
          if (parsed_fields->data != NULL)
            g_strfreev (parsed_fields->data);  
        }
      else if (format == DP_FIELDS_FORMAT_JSONPATH)
        {
        }
      parsed_fields = g_list_remove (parsed_fields, parsed_fields->data);
    }

//DUPIN_UTIL_DUMP_JSON ("dupin_util_json_node_object_grep_nodes: returned nodes:", grep_nodes);

  return grep_nodes;
}

static void
dupin_sqlite_json_filterby_json_node_free (void *p)
{
  JsonNode *node = (JsonNode *)p;
  json_node_free (node);
}

static void
dupin_sqlite_json_filterby_strfreev (void *p)
{
  gchar ** c= (gchar **)p;
  g_strfreev (c);
}

/* filterBy (fields, fields_format, filter_op, obj, filter_values) */

void
dupin_sqlite_json_filterby (sqlite3_context *ctx, int argc, sqlite3_value **argv)
{
  //Dupin * d = (Dupin *)sqlite3_user_data (ctx);

  gchar *fields = NULL;
  gchar *op;
  gchar *filter_values;
  gchar *obj;
  DupinFieldsFormatType fields_format = DP_FIELDS_FORMAT_DOTTED; 
  DupinFilterByType filter_op = DP_FILTERBY_EQUALS;

  gchar **fields_splitted=NULL;
  gchar **fields_values_splitted=NULL;
  JsonNode * obj_node=NULL;
  int ret;

  if (argc != 5
      || (fields = (gchar *)sqlite3_value_text(argv[0])) == NULL
      || (!g_strcmp0 (fields, ""))
      || (op = (gchar *)sqlite3_value_text(argv[2])) == NULL
      || (!g_strcmp0 (op, ""))
      || (obj = (gchar *)sqlite3_value_text(argv[3])) == NULL
      || (!g_strcmp0 (obj, "")))
    {
      sqlite3_result_error(ctx, "SQL function filterBy() called with invalid arguments.\n", -1);
      return;
    }

  /* cache param 1 - fields */

  fields_splitted = (gchar **)sqlite3_get_auxdata(ctx, 0);
  if (fields_splitted == NULL)
    {
      fields_splitted = g_strsplit (fields, ",", -1);
      sqlite3_set_auxdata(ctx, 0, fields_splitted, dupin_sqlite_json_filterby_strfreev);
    }

  /* param 2 - fields_format */

  gchar * fields_format_label = (gchar *) sqlite3_value_text(argv[1]);
  if (!g_strcmp0 (fields_format_label, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED))
    {
      fields_format = DP_FIELDS_FORMAT_DOTTED; 
    }
  //else if (!g_strcmp0 (fields_format_label, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_JSONPATH))
  else
    {
      //fields_format = DP_FIELDS_FORMAT_JSONPATH; 
      sqlite3_result_error(ctx, "SQL function filterBy() fields format not supported.\n", -1);
      return;
    }

  /* param 3 - filter_op */

  if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
    filter_op = DP_FILTERBY_EQUALS;
  else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
    filter_op = DP_FILTERBY_CONTAINS;
  else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
    filter_op = DP_FILTERBY_STARTS_WITH;
  else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
    filter_op = DP_FILTERBY_PRESENT;

  /* cache param 4 - obj */

  /* NOTE - value might be null e.g. op is 'present' */
  obj = (gchar *)sqlite3_value_text(argv[3]);

  obj_node = (JsonNode *)sqlite3_get_auxdata(ctx, 3);
  if (obj_node == NULL)
    {
      JsonParser *parser = json_parser_new ();

      if (parser == NULL)
        {
          //sqlite3_result_error(ctx, "Cannot create parser to parse obj body.\n", -1);
          return;
        }

      if (!json_parser_load_from_data (parser, obj, strlen(obj), NULL))
        {
          //sqlite3_result_error(ctx, "Cannot parse obj body.\n", -1);
          return;
        }

      obj_node = json_parser_get_root (parser);

      if (obj_node == NULL)
        {
          //sqlite3_result_error(ctx, "Cannot parse obj body.\n", -1);
          return;
        }

      obj_node = json_node_copy (obj_node);

      g_object_unref (parser);

      sqlite3_set_auxdata(ctx, 3, obj_node, dupin_sqlite_json_filterby_json_node_free);
    }

  /* cache param 5 - filter_values */

  filter_values = (gchar *)sqlite3_value_text(argv[4]);

  fields_values_splitted = (gchar **)sqlite3_get_auxdata(ctx, 4);
  if (filter_values != NULL
      && g_strcmp0 (filter_values, "")
      && fields_values_splitted == NULL)
    {
      fields_values_splitted = g_strsplit (filter_values, ",", -1);
      sqlite3_set_auxdata(ctx, 4, fields_values_splitted, dupin_sqlite_json_filterby_strfreev);
    }

  if (filter_op != DP_FILTERBY_PRESENT
      && (fields_values_splitted == NULL || !fields_values_splitted[0]))
    {
      ret = 0;
    }
  else
    {
      /* NOTE - the problem is, we can not cache any of the following?! ;( */

      JsonNode * matched_nodes = dupin_util_json_node_object_grep_nodes (obj_node,
								         fields_format,
								         fields_splitted,
								         filter_op,
								         fields_values_splitted, NULL);

      ret = 1;
      if (matched_nodes == NULL
          || json_array_get_length (json_node_get_array (matched_nodes)) == 0)
        ret = 0;

      json_node_free (matched_nodes);
    }

//g_message ("dupin_sqlite_json_filterby: matched=%d\n", ret);
  
  sqlite3_result_int(ctx, ret);
}

/* NOTE - try to make SQLite operations more robust */

gint
dupin_sqlite_subs_mgr_busy_handler (sqlite3* dbconn,
				    gchar *sql_stmt,
                                    gint (*callback_func)(void *, gint, char **, gchar **),
                                    void *args,
                                    gchar **error_msg,
                                    gint rc)
{
  gint counter = 0;

g_message("sqlite in busy handler BEGIN\n");

  while(rc == SQLITE_BUSY && counter < 512)
    {
g_message("sqlite in busy handler\n");

      if (*error_msg)
        {
          g_error ("dupin_sqlite_subs_mgr_busy_handler: %s", *error_msg);

          sqlite3_free (*error_msg);
        }

      counter++;

      /* TODO - check glib if there is a better way to do this ? */

      usleep(100000);

      rc = sqlite3_exec(dbconn, sql_stmt, callback_func, args, error_msg);
    }

g_message("sqlite in busy handler END\n");

  return rc; 
}

/* NOTE - Portable Listings related utilities - some of the above too should
          be included into separated library/file */

gboolean
dupin_util_poli_is_primary_field (gchar **   profiles,
                                  gchar *    type,
                                  gchar *    field_name,
                                  GError **  error)
{
  /* TODO - field_name may by dotted or jsonpath selection on a complex field/sub-field
	    which needs to be looked up into profiles DB / schema */

  gint ret = 0;

  /*
  ret = g_utf8_collate (field_name, LOOKUPSOMETHING(profiles, type, field_name)....);
  */

  ret = g_utf8_collate (field_name, "value");

  return (ret == 0) ? TRUE : FALSE;
}

/* NOTE - it may return list of matches in decreasing order of relevance - first is the main */

GList *
dupin_util_poli_get_primary_fields (gchar **   profiles,
                                    gchar *    type,
				    JsonNode * obj_node,
                                    GError **  error)
{
  g_return_val_if_fail (obj_node != NULL, NULL);
  g_return_val_if_fail (json_node_get_node_type (obj_node) == JSON_NODE_OBJECT, NULL);

  /* NOTE - profiles NULL, it is core profile - type NULL it is an 'entry' */

  GList * primary_fields = NULL;

  /* TODO */

  GList * nodes = json_object_get_members (json_node_get_object (obj_node));
  GList * l=NULL;

  for (l = nodes; l != NULL ; l = l->next)
    {
      gint ret=0;

      gchar * member_name = (gchar *)l->data;

      /*
      ret = g_utf8_collate (member_name, LOOKUPSOMETHING(profiles, type, member_name)....);
      */

      ret = g_utf8_collate (member_name, "value");

      if (ret == 0)
        {
          primary_fields = g_list_prepend (primary_fields, g_strdup (member_name));
          break;
        }
    }
  g_list_free (nodes);

  return primary_fields;
}

void
dupin_util_poli_get_primary_fields_list_close (GList * primary_fields)
{
  while (primary_fields)
    {
      if (primary_fields->data != NULL) 
        g_free (primary_fields->data);  
      primary_fields = g_list_remove (primary_fields, primary_fields->data);
    }
}

/* UTF-8 utility functions from http://midnight-commander.org/
   updated to include glib gint/gchar and dupin_util namespace */

gchar *
dupin_util_utf8_normalize (const gchar *text)
{
  GString *fixed = g_string_new ("");
  gchar *tmp;
  gchar *result=NULL;
  const gchar *start;
  const gchar *end;

  start = text;
  while (!g_utf8_validate (start, -1, &end) && start[0] != '\0')
  {
    if (start != end)
    {
      tmp = g_utf8_normalize (start, end - start, G_NORMALIZE_ALL);
      if (tmp != NULL)
        {
          g_string_append (fixed, tmp);
          g_free (tmp);
        }
    }
    g_string_append_c (fixed, end[0]);
    start = end + 1;
  }

  if (start == text)
  {
    result = g_utf8_normalize (text, -1, G_NORMALIZE_ALL);
  }
  else
  {
    if (start[0] != '\0' && start != end)
    {
      tmp = g_utf8_normalize (start, end - start, G_NORMALIZE_ALL);
      if (tmp != NULL)
        {
          g_string_append (fixed, tmp);
          g_free (tmp);
        }
    }
    result = g_strdup (fixed->str);
  }
  g_string_free (fixed, TRUE);

  return result;
}

gchar *
dupin_util_utf8_casefold_normalize (const gchar *text)
{
  GString *fixed = g_string_new ("");
  gchar *tmp, *fold;
  gchar *result=NULL;
  const gchar *start;
  const gchar *end;

  start = text;
  while (!g_utf8_validate (start, -1, &end) && start[0] != '\0')
  {
    if (start != end)
    {
      fold = g_utf8_casefold (start, end - start);
      if (fold != NULL)
        {
          tmp = g_utf8_normalize (fold, -1, G_NORMALIZE_ALL);
          if (tmp != NULL)
            {
              g_string_append (fixed, tmp);
              g_free (tmp);
            }
          g_free (fold);
        }
    }
    g_string_append_c (fixed, end[0]);
    start = end + 1;
  }

  if (start == text)
  {
    fold = g_utf8_casefold (text, -1);
    if (fold != NULL)
      {
        result = g_utf8_normalize (fold, -1, G_NORMALIZE_ALL);
        g_free (fold);
      }
  }
  else
  {
    if (start[0] != '\0' && start != end)
    {
      fold = g_utf8_casefold (start, end - start);
      if (fold != NULL)
        {
          tmp = g_utf8_normalize (fold, -1, G_NORMALIZE_ALL);
          if (tmp != NULL)
            {
              g_string_append (fixed, tmp);
              g_free (tmp);
            }
          g_free (fold);
        }
    }
    result = g_strdup (fixed->str);
  }
  g_string_free (fixed, TRUE);

  return result;
}

gint
dupin_util_utf8_compare (const gchar *t1, const gchar *t2)
{
  gchar *n1, *n2;
  gint result;

  n1 = dupin_util_utf8_normalize (t1);
  n2 = dupin_util_utf8_normalize (t2);

  result = g_strcmp0 (n1, n2);

  if (n1 != NULL)
    g_free (n1);
  if (n2 != NULL)
    g_free (n2);

  return result;
}

gint
dupin_util_utf8_ncompare (const gchar *t1, const gchar *t2)
{
  gchar *n1, *n2;
  gint result;
  gint min_len=0;

  n1 = dupin_util_utf8_normalize (t1);
  n2 = dupin_util_utf8_normalize (t2);

  if (n1 != NULL && n2 != NULL)
    min_len = MIN (strlen (n1), strlen (n2));

  result = strncmp (n1, n2, min_len);

  if (n1 != NULL)
    g_free (n1);
  if (n2 != NULL)
    g_free (n2);

  return result;
}

gint
dupin_util_utf8_casecmp (const gchar *t1, const gchar *t2)
{
  gchar *n1, *n2;
  gint result;

  n1 = dupin_util_utf8_casefold_normalize (t1);
  n2 = dupin_util_utf8_casefold_normalize (t2);

  result = g_strcmp0 (n1, n2);

  if (n1 != NULL)
    g_free (n1);
  if (n2 != NULL)
    g_free (n2);

  return result;
}

gint
dupin_util_utf8_ncasecmp (const gchar *t1, const gchar *t2)
{
  gchar *n1, *n2;
  gint result;
  gint min_len=0;

  n1 = dupin_util_utf8_casefold_normalize (t1);
  n2 = dupin_util_utf8_casefold_normalize (t2);

  if (n1 != NULL && n2 != NULL)
    min_len = MIN (strlen (n1), strlen (n2));

  result = strncmp (n1, n2, min_len);

  if (n1 != NULL)
    g_free (n1);
  if (n2 != NULL)
    g_free (n2);

  return result;
}

gchar *
dupin_util_utf8_create_key_gen (const gchar *text, gint case_sen,
             gchar * (*keygen) (const gchar * text, gssize size))
{
  gchar *result=NULL;

  if (case_sen)
  {
    result = dupin_util_utf8_normalize (text);
  }
  else
  {
    gboolean dot;
    GString *fixed;
    const gchar *start, *end;
    gchar *fold, *key;

    dot = text[0] == '.';
    fixed = g_string_sized_new (16);

    if (!dot)
      start = text;
    else
    {
      start = text + 1;
      g_string_append_c (fixed, '.');
    }

    while (!g_utf8_validate (start, -1, &end) && start[0] != '\0')
    {
      if (start != end)
      {
        fold = g_utf8_casefold (start, end - start);
        if (fold != NULL)
          {
            key = keygen (fold, -1);
            if (key != NULL)
              {
                g_string_append (fixed, key);
                g_free (key);
              }
            g_free (fold);
          }
      }
      g_string_append_c (fixed, end[0]);
      start = end + 1;
    }

    if (start == text)
    {
      fold = g_utf8_casefold (start, -1);
      if (fold != NULL)
        {
          result = keygen (fold, -1);
          if (result != NULL)
            g_free (fold);
        }
      g_string_free (fixed, TRUE);
    }
    else if (dot && (start == text + 1))
    {
      fold = g_utf8_casefold (start, -1);
      if (fold != NULL)
        {
          key = keygen (fold, -1);
          if (key != NULL)
            {
              g_string_append (fixed, key);
              g_free (key);
            }
          g_free (fold);
        }
      result = g_string_free (fixed, FALSE);
    }
    else
    {
      if (start[0] != '\0' && start != end)
      {
        fold = g_utf8_casefold (start, end - start);
        if (fold != NULL)
          {
            key = keygen (fold, -1);
            if (key != NULL)
              {
                g_string_append (fixed, key);
                g_free (key);
              }
            g_free (fold);
          }
      }
      result = g_string_free (fixed, FALSE);
    }
  }
  return result;
}

gchar *
dupin_util_utf8_create_key (const gchar *text, gint case_sen)
{
  return dupin_util_utf8_create_key_gen (text, case_sen, g_utf8_collate_key);
}

gchar *
dupin_util_utf8_create_key_for_filename (const gchar *text, gint case_sen)
{
  return dupin_util_utf8_create_key_gen (text, case_sen, g_utf8_collate_key_for_filename);
}

/* MVCC stuff */

gboolean
dupin_util_mvcc_new (guint revision,
                     gchar * hash,
                     gchar mvcc[DUPIN_ID_MAX_LEN])
{
  g_return_val_if_fail (revision > 0, FALSE);
  g_return_val_if_fail (hash != NULL, FALSE);

  snprintf (mvcc, DUPIN_ID_MAX_LEN, "%d-%s", revision, hash);

//g_message("dupin_util_mvcc_new: revision=%d hash=%s -> mvcc=%s\n", (gint)revision, hash, mvcc);

  return TRUE;
}

gboolean
dupin_util_is_valid_mvcc (gchar * mvcc)
{
  g_return_val_if_fail (mvcc != NULL, FALSE);

  gchar **parts;

  parts = g_strsplit (mvcc, "-", -1);

//g_message("dupin_util_is_valid_mvcc: parts[0]='%s'\n", parts[0]);
//g_message("dupin_util_is_valid_mvcc: parts[1]='%s'\n", parts[1]);
//g_message("dupin_util_is_valid_mvcc: parts[2]='%s'\n", parts[2]);

  if (!parts || !parts[0] || !parts[1] || parts[2])
    {
      if (parts)
        g_strfreev (parts);

      return FALSE;
    }

//g_message("dupin_util_is_valid_mvcc: checking ID syntax for '%s'\n", parts[0]);

  /* basic validation of id part - see also http://stackoverflow.com/posts/1640804/revisions */
  errno=0;
  gchar *end;
  strtol(parts[0], &end, 10); 
  if (end == parts[0] || *end != '\0' || errno == ERANGE)
    {
      if (parts)
        g_strfreev (parts);

      return FALSE;
    }

//g_message("dupin_util_is_valid_mvcc: ID syntax for '%s' is OK\n", parts[0]);

  if (strlen (parts[1]) != DUPIN_ID_HASH_ALGO_LEN)
    {
      if (parts)
        g_strfreev (parts);

      return FALSE;
    }

//g_message("dupin_util_is_valid_mvcc: HASH syntax for '%s' is OK\n", parts[1]);

  g_strfreev (parts);

  return TRUE;
}

gint
dupin_util_mvcc_revision_cmp (gchar * mvcc_a,
		              gchar * mvcc_b)
{
  g_return_val_if_fail (mvcc_a != NULL, -1);
  g_return_val_if_fail (mvcc_b != NULL, -1);

  gint status=-1;

  guint rev_a, rev_b;
  gchar hash_a[DUPIN_ID_MAX_LEN];
  gchar hash_b[DUPIN_ID_MAX_LEN];

  if ((dupin_util_mvcc_get_hash( mvcc_a, hash_a) == FALSE)
      || (dupin_util_mvcc_get_hash( mvcc_b, hash_b) == FALSE))
    return status;

  if (g_strcmp0 (hash_a, hash_b))
    return status;

  if ((dupin_util_mvcc_get_revision( mvcc_a, &rev_a) == FALSE)
      || (dupin_util_mvcc_get_revision( mvcc_b, &rev_b) == FALSE))
    return status;

  if (rev_a == rev_b)
    status = 0;
  else if (rev_a < rev_b)
    status = -1 ;
  else if (rev_a > rev_b)
    status = 1;

  return status;
}

gboolean
dupin_util_mvcc_get_revision (gchar * mvcc,
                              guint * revision)
{
  g_return_val_if_fail (mvcc != NULL, FALSE);

  gchar **parts;
  *revision = 0;

  parts = g_strsplit (mvcc, "-", -1);

  if (!parts || !parts[0] || !parts[1] || parts[2])
    {
      if (parts)
        g_strfreev (parts);

      return FALSE;
    }

  *revision = atoi (parts[0]);

//g_message("dupin_util_mvcc_get_rev: mvcc=%s -> revision=%d\n", mvcc, (gint)*revision);

  g_strfreev (parts);

  return TRUE;
}

gboolean
dupin_util_mvcc_get_hash (gchar * mvcc,
                          gchar hash[DUPIN_ID_HASH_ALGO_LEN])
{
  g_return_val_if_fail (mvcc != NULL, FALSE);

  gchar **parts;

  parts = g_strsplit (mvcc, "-", -1);

  if (!parts || !parts[0] || !parts[1] || parts[2])
    {
      if (parts)
        g_strfreev (parts);

      return FALSE;
    }

  g_stpcpy (hash, parts[1]);

//g_message("dupin_util_mvcc_get_hash: mvcc=%s -> hash=%s\n", mvcc, hash);

  g_strfreev (parts);

  return TRUE;
}

/* JSON Collation related functions - see also http://wiki.apache.org/couchdb/View_collation */

/* map JSON node type to our collation sorted type */

DupinCollateType
dupin_util_get_collate_type (JsonNode * node)
{
  JsonNodeType node_type = json_node_get_node_type (node);

  if (node_type == JSON_NODE_NULL)
    return DP_COLLATE_TYPE_NULL;

  else if (node_type == JSON_NODE_VALUE)
    {
      if (json_node_get_value_type (node) == G_TYPE_STRING)
        {
	  if (json_node_get_string (node) == NULL)
            return DP_COLLATE_TYPE_EMPTY;

	  else
            return DP_COLLATE_TYPE_STRING;
        }

      if (json_node_get_value_type (node) == G_TYPE_DOUBLE
          || json_node_get_value_type (node) == G_TYPE_FLOAT)
        {
          return DP_COLLATE_TYPE_DOUBLE;
        }

      if (json_node_get_value_type (node) == G_TYPE_INT
          || json_node_get_value_type (node) == G_TYPE_INT64
          || json_node_get_value_type (node) == G_TYPE_UINT)
        {
          return DP_COLLATE_TYPE_INTEGER;
        }

      if (json_node_get_value_type (node) == G_TYPE_BOOLEAN)
        {
          return DP_COLLATE_TYPE_BOOLEAN;
        }
    }

  else if (node_type == JSON_NODE_ARRAY)
    return DP_COLLATE_TYPE_ARRAY;

  else if (node_type == JSON_NODE_OBJECT)
    return DP_COLLATE_TYPE_OBJECT;

  return DP_COLLATE_TYPE_ANY;
}

int
dupin_util_collation (void        * ref,
                      int         left_len,
                      const void  *left_void,
                      int         right_len,
                      const void  *right_void)
{
  int ret = 0;

  gchar * left  = g_string_free (g_string_new_len ((gchar*)left_void, (gint) left_len), FALSE);
  gchar * right = g_string_free (g_string_new_len ((gchar*)right_void, (gint) right_len), FALSE);

  int min_len = MIN(left_len, right_len);

  if (min_len == 0)
    {
      // empty string sorts at end of list
      if (left_len == right_len)
        {
          ret = 0;
        }
      else if (left_len == 0)
        {
          ret = 1;
        }
      else
        {
          ret = -1;
        }
    }
  else
    {
      JsonParser * parser = (JsonParser *) ref;

      json_parser_load_from_data (parser, left, -1, NULL);
      JsonNode * left_node = json_node_copy (json_parser_get_root (parser));

      json_parser_load_from_data (parser, right, -1, NULL);
      JsonNode * right_node = json_node_copy (json_parser_get_root (parser));

      ret = dupin_util_collation_compare_pair (left_node, right_node);

      json_node_free (left_node);
      json_node_free (right_node);
    }

  g_free (left);
  g_free (right);

  return ret;
}

int
dupin_util_collation_compare_pair (JsonNode * left_node,
                                   JsonNode * right_node)
{
  int ret = 0;

/*
g_message("dupin_util_collation_compare_pair: BEGIN");
DUPIN_UTIL_DUMP_JSON ("left", left_node);
DUPIN_UTIL_DUMP_JSON ("right", right_node);
g_message("dupin_util_collation_compare_pair: BEGIN");
*/

  DupinCollateType left_type = dupin_util_get_collate_type (left_node);
  DupinCollateType right_type = dupin_util_get_collate_type (right_node);

  if (left_type == right_type)
    {
      if (left_type == DP_COLLATE_TYPE_EMPTY
          || left_type == DP_COLLATE_TYPE_NULL)
        ret = 0;

      else if (left_type == DP_COLLATE_TYPE_STRING)
        {
          /* TODO - study how to get g_utf8_collate_key() to work - if we use it on left/right
                    strings glib returns random values and sort order on strcmp() ?! */

          gchar * left_val = (gchar *)json_node_get_string (left_node);
          gchar * right_val = (gchar *)json_node_get_string (right_node);

          ret = g_utf8_collate (left_val, right_val);
        }

      else if (left_type == DP_COLLATE_TYPE_DOUBLE)
        {
          gdouble left_val = json_node_get_double (left_node);
          gdouble right_val = json_node_get_double (right_node);

          if (left_val == right_val)
            ret = 0;
          else if (left_val < right_val)
            ret = -1;
          else if (left_val > right_val)
            ret = 1;
        }

      else if (left_type == DP_COLLATE_TYPE_INTEGER)
        {
          gint left_val = json_node_get_int (left_node);
          gint right_val = json_node_get_int (right_node);

          if (left_val == right_val)
            ret = 0;
          else if (left_val < right_val)
            ret = -1;
          else if (left_val > right_val)
            ret = 1;
        }

      else if (left_type == DP_COLLATE_TYPE_BOOLEAN)
        {
          gboolean left_val = json_node_get_boolean (left_node);
          gboolean right_val = json_node_get_boolean (right_node);

          if (left_val == right_val)
            ret = 0;
          else if (left_val == FALSE)
            ret = -1;
          else if (left_val == TRUE)
            ret = 1;
        }

      else if (left_type == DP_COLLATE_TYPE_ARRAY)
        {
          gint left_length = json_array_get_length (json_node_get_array (left_node));
          gint right_length = json_array_get_length (json_node_get_array (right_node));

          if (left_length == right_length)
            {
              /* loop on the above, at first difference return -1 or 1 - otherwise 0 */
              GList *ln, *lnodes;
              GList *rn, *rnodes;
              lnodes = json_array_get_elements (json_node_get_array (left_node));
              rnodes = json_array_get_elements (json_node_get_array (right_node));

              for (ln = lnodes, rn = rnodes; ln != NULL && rn != NULL ; ln = ln->next, rn = rn->next)
                {
                  JsonNode * ln_node = (JsonNode *)ln->data;
                  JsonNode * rn_node = (JsonNode *)rn->data;

                  ret = dupin_util_collation_compare_pair (ln_node, rn_node);

/*
g_message("dupin_util_collation_compare_pair: checking array element ret=%d", (gint)ret);
DUPIN_UTIL_DUMP_JSON ("left array element", ln_node);
DUPIN_UTIL_DUMP_JSON ("right array element", rn_node);
g_message("dupin_util_collation_compare_pair: checking array element");
*/

                  if (ret != 0)
                    break;
                }
              g_list_free (lnodes);
              g_list_free (rnodes);
            }
          else if (left_length < right_length)
            ret = -1;
          else if (left_length > right_length)
            ret = 1;
        }

      else if (left_type == DP_COLLATE_TYPE_OBJECT)
        {
          gint left_length = json_object_get_size (json_node_get_object (left_node));
          gint right_length = json_object_get_size (json_node_get_object (right_node));

          if (left_length == right_length)
            {
              /* loop on the above, at first difference return -1 or 1 - otherwise 0 */

              GList *ln, *lnodes;
              GList *rn, *rnodes;

              /* WARNING - bear in mind that there might be a bug in json-glib which
                           makes interger numbers to be returned as double in a json node value
                           see for example the work around in dupin_view_sync_thread_real_map()
                           we do not reparse here due to efficency reasons, and we will consider
                           and object member of value 1 to be the same as 1.0 */

              lnodes = json_object_get_members (json_node_get_object (left_node));
              rnodes = json_object_get_members (json_node_get_object (right_node));

              /* NOTE - we assume json-glib will return object members returned somehow - see library source code */

              for (ln = lnodes, rn = rnodes; ln != NULL && rn != NULL ; ln = ln->next, rn = rn->next)
                {
                  gchar * ln_member_name = (gchar *)ln->data;
                  gchar * rn_member_name = (gchar *)rn->data;

                  ret = g_utf8_collate (ln_member_name, rn_member_name);

                  if (ret != 0)
                    break;

                  JsonNode * ln_node = json_object_get_member (json_node_get_object (left_node), ln_member_name);
                  JsonNode * rn_node = json_object_get_member (json_node_get_object (right_node), rn_member_name);

                  ret = dupin_util_collation_compare_pair (ln_node, rn_node);

/*
g_message("dupin_util_collation_compare_pair: checking object member ret=%d", (gint)ret);
DUPIN_UTIL_DUMP_JSON ("left array element", ln_node);
DUPIN_UTIL_DUMP_JSON ("right array element", rn_node);
g_message("dupin_util_collation_compare_pair: checking object member ");
*/

                  if (ret != 0)
                    break;
                }
              g_list_free (lnodes);
              g_list_free (rnodes);
            }
          else if (left_length < right_length)
            ret = -1;
          else if (left_length > right_length)
            ret = 1;
        }
    }
  else if (left_type < right_type)
    {
      ret = -1;
    }
  else if (left_type > right_type)
    {
      ret = 1;
    }

/*
g_message("dupin_util_collation_compare_pair: END ret=%d", (gint)ret);
DUPIN_UTIL_DUMP_JSON ("left", left_node);
DUPIN_UTIL_DUMP_JSON ("right", right_node);
g_message("dupin_util_collation_compare_pair: END\n");
*/

  return ret;
}

gchar *
dupin_util_json_string_normalize (gchar * input_string)
{
  g_return_val_if_fail (input_string != NULL, NULL);

  JsonParser *parser = json_parser_new ();
  GError *error = NULL;
  gchar * output_string = NULL;

  if (!json_parser_load_from_data (parser, input_string, -1, &error)
      || (!(output_string = dupin_util_json_serialize (json_parser_get_root (parser)))) )
    {
      if (error != NULL)
        g_error_free (error);

      g_object_unref (parser);

      return NULL;
    }

  g_object_unref (parser);

  return output_string; 
}

gchar *
dupin_util_json_string_normalize_docid (gchar * input_string_docid)
{
  g_return_val_if_fail (input_string_docid != NULL, NULL);

  JsonParser *parser = json_parser_new ();
  GError *error = NULL;
  gchar * output_string = NULL;

  if (!json_parser_load_from_data (parser, input_string_docid, -1, &error)
      || (json_node_get_node_type (json_parser_get_root (parser)) != JSON_NODE_VALUE)
      || (json_node_get_value_type (json_parser_get_root (parser)) != G_TYPE_STRING)
      || (!(output_string = g_strdup (json_node_get_string (json_parser_get_root (parser))))))
    {
      if (error != NULL)
        g_error_free (error);

      g_object_unref (parser);

      return NULL;
    }

  g_object_unref (parser);

  return output_string; 
}

gchar *
dupin_util_json_string_normalize_rev (gchar * input_string_rev)
{
  g_return_val_if_fail (input_string_rev != NULL, NULL);

  JsonParser *parser = json_parser_new ();
  GError *error = NULL;
  gchar * output_string = NULL;

  if (!json_parser_load_from_data (parser, input_string_rev, -1, &error)
      || (json_node_get_node_type (json_parser_get_root (parser)) != JSON_NODE_VALUE)
      || (json_node_get_value_type (json_parser_get_root (parser)) != G_TYPE_STRING)
      || (!(output_string = g_strdup (json_node_get_string (json_parser_get_root (parser))))))
    {
      if (error != NULL)
        g_error_free (error);

      g_object_unref (parser);

      return NULL;
    }

  g_object_unref (parser);

  return output_string; 
}

/*
   partial update request

   see http://code.google.com/apis/gdata/docs/2.0/reference.html#PartialUpdate
   and http://code.google.com/apis/youtube/2.0/developers_guide_protocol_partial_updates.html
*/

JsonNode *
dupin_util_json_node_object_patch_real (JsonNode * input,
                                        JsonNode * changes)
{
  JsonNode * patched_node = NULL;

  JsonNodeType input_type = json_node_get_node_type (input);
  JsonNodeType changes_type = json_node_get_node_type (changes);

  if (input_type != changes_type)
    return patched_node;

//g_message("dupin_util_json_node_object_patch_real: going to process the following\n");
//DUPIN_UTIL_DUMP_JSON ("Input_node", input);
//DUPIN_UTIL_DUMP_JSON ("Changes_node", changes);

  /* 1 - process deletion of fields (delete of whole record is done with dupin_record_delete() )

	 there are two ways to delete input fields:

		-> using the patch { "_patched_fields": { "fieldsFormat": "dotted", "fields": "field_to_delete" } ... }

		or

		-> using the patch { .... "field_to_delete": { "_deleted": true } ... }

   */

  /* 2 - process changes (new fileds or old with new values) of fields and their structure using JSON collation and heuristics */

  /* 2.1 if new field, add it - even if structured */

  /* 2.2 if existing field
        -> sort field JSON current field value node with new node value
        -> if equal do not ad it
	-> if array - add using A <=> B
   */

  GList *n, *nodes;

  if (changes_type == JSON_NODE_OBJECT)
    {
      JsonObject * changes_object = json_node_get_object (changes);

      /* 1 - process deletion of fields */

      if (json_object_has_member (changes_object, REQUEST_OBJ_PATCHED_FIELDS) == TRUE)
        {
          JsonNode * patched_fields_node = json_object_get_member (changes_object, REQUEST_OBJ_PATCHED_FIELDS);
          JsonNodeType patched_fields_node_type = json_node_get_node_type (patched_fields_node);
	  if (patched_fields_node_type == JSON_NODE_OBJECT)
	    {
	      /* { "_patched_fields": { "fieldsFormat": "dotted", "fields": "a,b,c" } ... } */

	      DupinFieldsFormatType format = DP_FIELDS_FORMAT_DOTTED;
	      gchar * fields = NULL;
	      gchar ** fields_splitted = NULL;

	      JsonObject * patched_fields_node_obj = json_node_get_object (patched_fields_node);

      	      if (json_object_has_member (patched_fields_node_obj, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT) == TRUE)
	        {
		  JsonNode * format_node = json_object_get_member (patched_fields_node_obj, REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT);
		  if (json_node_get_node_type (format_node) == JSON_NODE_VALUE 
		      && json_node_get_value_type (format_node) == G_TYPE_STRING)
                    {
		      gchar * format_value = (gchar *)json_node_get_string (format_node);
		      if (!g_strcmp0 (format_value, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_DOTTED))
		        format = DP_FIELDS_FORMAT_DOTTED;
          	      else if (!g_strcmp0 (format_value, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_JSONPATH))
            	        format = DP_FIELDS_FORMAT_JSONPATH;
		    }
		}

      	      if (json_object_has_member (patched_fields_node_obj, REQUEST_GET_ALL_ANY_FILTER_FIELDS) == TRUE)
	        {
		  JsonNode * fields_node = json_object_get_member (patched_fields_node_obj, REQUEST_GET_ALL_ANY_FILTER_FIELDS);
		  if (json_node_get_node_type (fields_node) == JSON_NODE_VALUE 
		      && json_node_get_value_type (fields_node) == G_TYPE_STRING)
                    {
		      fields = (gchar *)json_node_get_string (fields_node);

		      if (fields != NULL)
		        {
			  fields_splitted = g_strsplit (fields, ",", -1);

			  patched_node = dupin_util_json_node_object_filter_fields (input, format, fields_splitted, TRUE, NULL);

			  /* TODO - check and log any error if could not delete fields */

			  if (fields_splitted != NULL)
			    g_strfreev (fields_splitted);
			}
		    }
		}
	    }

          json_object_remove_member (changes_object, REQUEST_OBJ_PATCHED_FIELDS);
        }

      if (patched_node == NULL)
        patched_node = dupin_util_json_node_clone (input, NULL);

      JsonObject * patched_node_object = json_node_get_object (patched_node);

      /* 2 - process changes */

      nodes = json_object_get_members (changes_object);

      for (n = nodes; n != NULL ; n = n->next)
        {
          gchar * member_name = (gchar *)n->data;

          JsonNode * changes_node = json_object_get_member (changes_object, member_name);
          JsonNodeType changes_node_type = json_node_get_node_type (changes_node);

          JsonNode * input_node = NULL;
          JsonNodeType input_node_type;
          if (json_object_has_member (patched_node_object, member_name) == TRUE)
            {
              input_node = json_object_get_member (patched_node_object, member_name);
              input_node_type = json_node_get_node_type (input_node);
            }

          if (changes_node_type == JSON_NODE_NULL
              || changes_node_type == JSON_NODE_VALUE)
            {
	      /* If a non-repeating element in the PATCH request already appears in the resource,
		 and the element does not have child elements, then the new element in the request replaces the existing element in the resource. */

              /* patch single simple field */

	      if (input_node != NULL)
                json_object_remove_member (patched_node_object, member_name);
              json_object_set_member (patched_node_object, member_name, json_node_copy (changes_node));
            }

          else if (changes_node_type == JSON_NODE_OBJECT)
            {
              /* If a non-repeating element in the PATCH request already appears in the resource, and the element has child elements, then 
		 the child elements included in the request will replace the corresponding child elements in the resource. However, child elements
		 that are not included in the PATCH request will not be affected by the update. */

              JsonObject * sub_obj = json_node_get_object (changes_node);
	      JsonNode * deleted = NULL;

	      if (json_object_has_member (sub_obj, REQUEST_OBJ_DELETED) == TRUE)
	        deleted = json_object_get_member (sub_obj, REQUEST_OBJ_DELETED);

	      if (deleted != NULL
		  && json_node_get_node_type (deleted) == JSON_NODE_VALUE
		  && json_node_get_value_type (deleted) == G_TYPE_BOOLEAN
		  && json_node_get_boolean (deleted) == TRUE)
                {
		  /* remove single structured field */

	          if (input_node != NULL)
                    json_object_remove_member (patched_node_object, member_name);
                }
              else
                {
                  if (input_node == NULL
		      || input_node_type != JSON_NODE_OBJECT)
                    {
		      /* replace array, value or null with object */

		      if (input_node != NULL)
                        json_object_remove_member (patched_node_object, member_name);
                      input_node = json_node_new (JSON_NODE_OBJECT);
		      JsonObject * input_node_obj = json_object_new ();
		      json_node_take_object (input_node, input_node_obj);
                    }
                  
  		  JsonNode * patched_node = dupin_util_json_node_object_patch_real (input_node, changes_node); 

	          if (patched_node != NULL)
		    json_object_set_member (patched_node_object, member_name, patched_node);
 		  else
		    json_object_set_member (patched_node_object, member_name, input_node);
		}
            }

          else if (changes_node_type == JSON_NODE_ARRAY)
            {
              if (input_node == NULL
		  || input_node_type != JSON_NODE_ARRAY)
                {
		  /* replace object, value or null with array */

		  if (input_node != NULL)
                    json_object_remove_member (patched_node_object, member_name);
                  input_node = json_node_new (JSON_NODE_ARRAY);
		  JsonArray * input_node_array = json_array_new ();
		  json_node_take_array (input_node, input_node_array);

//g_message("dupin_util_json_node_object_patch_real: replaced member %s object, value or null with empty array \n", member_name);
                }

//g_message("dupin_util_json_node_object_patch_real: got array for member %s\n", member_name);
//DUPIN_UTIL_DUMP_JSON ("Input_node", input_node);
//DUPIN_UTIL_DUMP_JSON ("Changes_node", changes_node);
                  
  	      JsonNode * patched_node = dupin_util_json_node_object_patch_real (input_node, changes_node); 

	      if (patched_node != NULL)
	        json_object_set_member (patched_node_object, member_name, patched_node);
 	      else
		json_object_set_member (patched_node_object, member_name, input_node);
            }
        }
      g_list_free (nodes);
    }

  else if (changes_type == JSON_NODE_ARRAY)
    {
      /* NOTE - the input node type is guaranteed to be the same as changes type when recursing - see above */

      patched_node = dupin_util_json_node_clone (input, NULL);

      JsonArray * patched_node_array = json_node_get_array (patched_node);

      JsonArray * changes_array = json_node_get_array (changes);

      nodes = json_array_get_elements (changes_array);
      for (n = nodes; n != NULL ; n = n->next)
        {
          JsonNode * changes_node = (JsonNode *)n->data;

	  /* NOTE - if changes_node does not already exists into 
		    input it is added to the end */

	  GList * n1=NULL;
          GList * input_nodes = json_array_get_elements (patched_node_array);
	  gboolean matched = FALSE;
          for (n1 = input_nodes; n1 != NULL ; n1 = n1->next)
            {
	      JsonNode * input_node = (JsonNode *)n1->data;

	      gint ret = dupin_util_collation_compare_pair (input_node, changes_node);

	      if (ret == 0)
                {
		  matched = TRUE;
		  break;
		}
	    } 
          g_list_free (input_nodes);

	  if (matched == FALSE)
            json_array_add_element (patched_node_array, json_node_copy (changes_node));
        }
      g_list_free (nodes);
    }

  return patched_node;
}

JsonNode *
dupin_util_json_node_object_patch (JsonNode * input,
                                   JsonNode * changes)
{
  g_return_val_if_fail (input != NULL, NULL);
  g_return_val_if_fail (changes != NULL, NULL);
  g_return_val_if_fail (json_node_get_node_type (input) == JSON_NODE_OBJECT, NULL);
  g_return_val_if_fail (json_node_get_node_type (changes) == JSON_NODE_OBJECT, NULL);

/*
g_message ("dupin_util_json_node_object_patch: to patch\n");
DUPIN_UTIL_DUMP_JSON ("Input", input);
DUPIN_UTIL_DUMP_JSON ("Changes", changes);
*/

  JsonNode * patched_node = dupin_util_json_node_object_patch_real (input, changes);

/*
g_message ("dupin_util_json_node_object_patch: patched\n");
DUPIN_UTIL_DUMP_JSON ("Output", patched_node);
*/

  return patched_node;
}

gboolean
dupin_util_http_if_none_match (gchar * header_if_none_match,
                               gchar * etag)
{
  gboolean changed = TRUE;

  /* Check If-None-Match */
  if (header_if_none_match != NULL)
    {
      if (g_strstr_len (header_if_none_match, strlen (header_if_none_match), "*") ||
          g_strstr_len (header_if_none_match, strlen (header_if_none_match), etag))
        changed = FALSE;
    }

  return changed;
}

gboolean
dupin_util_http_if_modified_since (gchar * header_if_modified_since,
                                   gsize last_modified)
{
  gboolean changed = TRUE;

  /* Check If-Modified-Since */
  if (header_if_modified_since != NULL)
    {
      gsize modified = 0;
      dupin_date_string_to_timestamp (header_if_modified_since, &modified);

      /* NOTE - See assumptions and recommentations about client if-modified-since date value
                at http://tools.ietf.org/html/rfc2616#section-14.25 */

      if (dupin_date_timestamp_cmp (modified, last_modified) >= 0)
        changed = FALSE;
    }

  return changed;
}





/* k/v pairs management functions for arguments list */

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
