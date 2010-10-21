/******************************************************************************
* 	Copyright (c) 2007 All rights reserved
*		Asemantics S.r.l
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions
* are met:
*
* 1. Redistributions of source code must retain the above copyright
*    notice, this list of conditions and the following disclaimer.
*
* 2. Redistributions in binary form must reproduce the above copyright
*    notice, this list of conditions and the following disclaimer in
*    the documentation and/or other materials provided with the
*    distribution.
*
* 3. The end-user documentation included with the redistribution,
*    if any, must include the following acknowledgment:
*       "This product includes software developed by
*	 Asemantics S.r.l."
*    Alternately, this acknowledgment may appear in the software itself,
*    if and wherever such third-party acknowledgments normally appear.
*
* 4. All advertising materials mentioning features or use of this software
*    must display the following acknowledgement:
*    This product includes software developed by Asemantics S.r.l.
*    the Semantic Web company, Rome, London, Leiden and its contributors.
*
* 5. Neither the name of the company nor the names of its contributors
*    may be used to endorse or promote products derived from this software
*    without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
* ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
* PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE
* LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
* CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
* SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
* INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
* CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
* ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
******************************************************************************/

#include "tb_jsonpath.h"
#include "tb_jsonpath_function.h"
#include "tb_jsonpath_function_internal.h"

#include <stdlib.h>
#include <string.h>

static gboolean tb_jsonpath_function_value_string (JsonNode * value,
						   gchar ** string,
						   GError ** error);
static gboolean tb_jsonpath_function_value_number (JsonNode * value,
						   gdouble * number,
						   GError ** error);

static gboolean tb_jsonpath_function_last (GList * args,
					   JsonNode * retvalue,
					   GError ** error);
static gboolean tb_jsonpath_function_count (GList * args,
					    JsonNode * retvalue,
					    GError ** error);
static gboolean tb_jsonpath_function_string (GList * args,
					     JsonNode * retvalue,
					     GError ** error);
static gboolean tb_jsonpath_function_concat (GList * args,
					     JsonNode * retvalue,
					     GError ** error);
static gboolean tb_jsonpath_function_starts_with (GList * args,
						  JsonNode * retvalue,
						  GError ** error);
static gboolean tb_jsonpath_function_contains (GList * args,
					       JsonNode * retvalue,
					       GError ** error);
static gboolean tb_jsonpath_function_substring_before (GList * args,
						       JsonNode * retvalue,
						       GError ** error);
static gboolean tb_jsonpath_function_substring_after (GList * args,
						      JsonNode * retvalue,
						      GError ** error);
static gboolean tb_jsonpath_function_substring (GList * args,
						JsonNode * retvalue,
						GError ** error);
static gboolean tb_jsonpath_function_string_length (GList * args,
						    JsonNode * retvalue,
						    GError ** error);
static gboolean tb_jsonpath_function_normalize_space (GList * args,
						      JsonNode * retvalue,
						      GError ** error);
static gboolean tb_jsonpath_function_translate (GList * args,
						JsonNode * retvalue,
						GError ** error);
static gboolean tb_jsonpath_function_boolean (GList * args,
					      JsonNode * retvalue,
					      GError ** error);
static gboolean tb_jsonpath_function_not (GList * args,
					  JsonNode * retvalue,
					  GError ** error);
static gboolean tb_jsonpath_function_true (GList * args,
					   JsonNode * retvalue,
					   GError ** error);
static gboolean tb_jsonpath_function_false (GList * args,
					    JsonNode * retvalue,
					    GError ** error);
static gboolean tb_jsonpath_function_number (GList * args,
					     JsonNode * retvalue,
					     GError ** error);
static gboolean tb_jsonpath_function_sum (GList * args,
					  JsonNode * retvalue,
					  GError ** error);
static gboolean tb_jsonpath_function_floor (GList * args,
					    JsonNode * retvalue,
					    GError ** error);
static gboolean tb_jsonpath_function_ceiling (GList * args,
					      JsonNode * retvalue,
					      GError ** error);
static gboolean tb_jsonpath_function_round (GList * args,
					    JsonNode * retvalue,
					    GError ** error);

static tb_jsonpath_function_t tb_jsonpath_default_functions[] = {
  {"last", 1, tb_jsonpath_function_last},
  {"count", 1, tb_jsonpath_function_count},
  {"string", 1, tb_jsonpath_function_string},
  {"concat", -1, tb_jsonpath_function_concat},
  {"starts-with", 2, tb_jsonpath_function_starts_with},
  {"contains", 2, tb_jsonpath_function_contains},
  {"substring-before", 2, tb_jsonpath_function_substring_before},
  {"substring-after", 2, tb_jsonpath_function_substring_after},
  {"substring", -1, tb_jsonpath_function_substring},
  {"string-length", 1, tb_jsonpath_function_string_length},
  {"normalize-space", 1, tb_jsonpath_function_normalize_space},
  {"translate", 3, tb_jsonpath_function_translate},
  {"boolean", 1, tb_jsonpath_function_boolean},
  {"not", 1, tb_jsonpath_function_not},
  {"true", 0, tb_jsonpath_function_true},
  {"false", 0, tb_jsonpath_function_false},
  {"number", 1, tb_jsonpath_function_number},
  {"sum", -1, tb_jsonpath_function_sum},
  {"floor", -1, tb_jsonpath_function_floor},
  {"ceiling", -1, tb_jsonpath_function_ceiling},
  {"round", 1, tb_jsonpath_function_round},
  {NULL, 0, NULL}
};

/**
 * tb_jsonpath_function_new:
 * @returns: a new tb_jsonpath_function_t data struct
 *
 * Creates a data struct for the functions
 */
tb_jsonpath_functions_t *
tb_jsonpath_function_new (void)
{
  tb_jsonpath_functions_t *new;

  new = g_malloc0 (sizeof (tb_jsonpath_functions_t));
  return new;
}

/**
 * tb_jsonpath_function_add:
 * @data: a jsonpath function data struct
 * @name: the name of the new function
 * @numb_args: number of argument of the new function
 * @func: pointer to the new function
 * @error: the location for a GError, or NULL
 * @returns: TRUE or FALSE
 *
 * Adds a function to a jsonpath function data struct
 */
gboolean
tb_jsonpath_function_add (tb_jsonpath_functions_t * data,
			  gchar * name, gint numb_args,
			  tb_jsonpath_function_cb func, GError ** error)
{
  tb_jsonpath_function_t *new;
  gint i;

  g_return_val_if_fail (data != NULL, FALSE);
  g_return_val_if_fail (name != NULL, FALSE);
  g_return_val_if_fail (func != NULL, FALSE);

  for (i = 0; tb_jsonpath_default_functions[i].name; i++)
    {
      if (!g_utf8_collate (tb_jsonpath_default_functions[i].name, name))
	{
	  g_set_error (error, tb_jsonpath_error_quark (),
		       TB_ERROR_JSONPATH, "The name '%s' is reserved", name);
	  return FALSE;
	}
    }

  new = g_malloc (sizeof (tb_jsonpath_function_t));

  new->name = g_strdup (name);
  new->numb_args = numb_args;
  new->func = func;

  data->functions = g_list_append (data->functions, new);
  return TRUE;
}

static void
tb_jsonpath_function_destroy_single (tb_jsonpath_function_t * func);

/**
 * tb_jsonpath_function_destroy:
 * @func: a jsonpath function data struct
 * 
 * Destroies the data struct and free the memory
 */
void
tb_jsonpath_function_destroy (tb_jsonpath_functions_t * func)
{
  if (!func)
    return;

  if (func->functions)
    {
      g_list_foreach (func->functions,
		      (GFunc) tb_jsonpath_function_destroy_single, NULL);
      g_list_free (func->functions);
    }

  g_free (func);
}

static void
tb_jsonpath_function_destroy_single (tb_jsonpath_function_t * func)
{
  if (!func)
    return;

  if (func->name)
    g_free (func->name);

  g_free (func);
}

static gboolean
tb_jsonpath_function_value_string (JsonNode * value,
				   gchar ** string, GError ** error)
{
  switch (json_node_get_node_type (value))
    {   
      case JSON_NODE_VALUE:
        {   
          if (json_node_get_value_type (value) == G_TYPE_STRING)
          { 
            *string = g_strdup (json_node_get_string (value));
            break;
          } 

          if (json_node_get_value_type (value) == G_TYPE_DOUBLE
                || json_node_get_value_type (value) == G_TYPE_FLOAT)
          {
	    gdouble numb = json_node_get_double (value);
	    *string = g_strdup_printf ("%f", numb);
	    break;
          }

          if (json_node_get_value_type (value) == G_TYPE_INT
                || json_node_get_value_type (value) == G_TYPE_INT64
                || json_node_get_value_type (value) == G_TYPE_UINT)
          { 
	    gint numb = (gint) json_node_get_int (value);
	    *string = g_strdup_printf ("%d", numb);
	    break;
          } 
          if (json_node_get_value_type (value) == G_TYPE_BOOLEAN)
          { 
            *string = g_strdup_printf (json_node_get_boolean (value) == TRUE ? "true" : "false");
            break;
          } 
        } 

      case JSON_NODE_OBJECT:
        {
          JsonGenerator *gen;

          gen = json_generator_new();

          if (gen == NULL)
            {
              return FALSE;
            }

          json_generator_set_root (gen, value );
          *string = json_generator_to_data (gen,NULL);

          if (string == NULL)
            {
	      g_object_unref (gen);
              return FALSE;
            }

	  g_object_unref (gen);

          break;
        }

      case JSON_NODE_ARRAY:
        {
          JsonGenerator *gen;

          gen = json_generator_new();

          if (gen == NULL)
            {
              return FALSE;
            }

          json_generator_set_root (gen, value );
          *string = json_generator_to_data (gen,NULL);

          if (string == NULL)
            {
	      g_object_unref (gen);
              return FALSE;
            }

	  g_object_unref (gen);

          break;
        }

      case JSON_NODE_NULL:
        {
          *string = g_strdup ("null");
          break;
        }

      default:
        {
          *string = g_strdup ("");
          break;
        }
    }

  return TRUE;
}

static gboolean
tb_jsonpath_function_value_number (JsonNode * value,
				   gdouble * number, GError ** error)
{
  switch (json_node_get_node_type (value))
    {   
      case JSON_NODE_VALUE:
        {   
          if (json_node_get_value_type (value) == G_TYPE_STRING)
          { 
            gchar *string = (gchar *) json_node_get_string (value);
	    *number = strtod (string, NULL);
            break;
          } 

          if (json_node_get_value_type (value) == G_TYPE_DOUBLE
                || json_node_get_value_type (value) == G_TYPE_FLOAT)
          {
	    *number = json_node_get_double (value);
	    break;
          }

          if (json_node_get_value_type (value) == G_TYPE_INT
                || json_node_get_value_type (value) == G_TYPE_INT64
                || json_node_get_value_type (value) == G_TYPE_UINT)
          { 
	    *number = (gdouble) json_node_get_int (value);
	    break;
          } 
          if (json_node_get_value_type (value) == G_TYPE_BOOLEAN)
          { 
            *number = json_node_get_boolean (value) == TRUE ? 1 : 0;
            break;
          } 
        } 

      case JSON_NODE_OBJECT:
      case JSON_NODE_ARRAY:
        {
          *number = 0;
          break;
        }

      case JSON_NODE_NULL:
        {
          *number = 0;
          break;
        }

      default:
        {
          *number = 0;
          break;
        }
    }

  return TRUE;
}

/* Functions */
static gboolean
tb_jsonpath_function_last (GList * args,
			   JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *value;
  JsonArray *array;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &value) == FALSE
      || json_node_get_node_type (value) != JSON_NODE_ARRAY) /* see tb_jsonpath.c we never have arrays or objects returned?!? */
    {
      json_node_set_double (retvalue, 0);
      return TRUE;
    }

  array = json_node_get_array (value);
  json_node_set_double (retvalue, json_array_get_length (array) - 1); /* -1 correct ? */
  return TRUE;
}

static gboolean
tb_jsonpath_function_count (GList * args,
			    JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *value;
  JsonArray *array;

  result = args->data;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &value) == FALSE
      || json_node_get_node_type (value) != JSON_NODE_ARRAY) /* see tb_jsonpath.c we never have arrays or objects returned?!? */
    {
      json_node_set_double (retvalue, 1);
      return TRUE;
    }

  array = json_node_get_array (value);
  json_node_set_double (retvalue, json_array_get_length (array));
  return TRUE;
}

static gboolean
tb_jsonpath_function_string (GList * args,
			     JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *value;
  gchar *string;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &value) == FALSE)
    {
      json_node_set_string (retvalue, "");
      return TRUE;
    }

  if (tb_jsonpath_function_value_string (value, &string, error) == FALSE)
    return FALSE;

  json_node_set_string (retvalue, string);
  g_free (string);
  return TRUE;
}

static gboolean
tb_jsonpath_function_concat (GList * args,
			     JsonNode * retvalue, GError ** error)
{
  GList *list;
  GString *str;
  gchar *string;

  str = g_string_new (NULL);

  for (list = args; list; list = list->next)
    {
      tb_jsonpath_result_t *result = list->data;
      JsonNode *value;

      if (tb_jsonpath_result_next (result, &value) == FALSE)
	continue;

      if (tb_jsonpath_function_value_string (value, &string, error) == FALSE)
	{
	  g_string_free (str, TRUE);
	  return FALSE;
	}

      if (string)
	{
	  g_string_append_printf (str, "%s", string);
	  g_free (string);
	}
    }

  string = g_string_free (str, FALSE);
  json_node_set_string (retvalue, string);
  g_free (string);

  return TRUE;
}

static gboolean
tb_jsonpath_function_starts_with (GList * args,
				  JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *v1, *v2;
  gchar *string;
  gchar *prefix;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v1) == FALSE)
    {
      json_node_set_boolean (retvalue, FALSE);
      return TRUE;
    }

  args = args->next;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v2) == FALSE)
    {
      json_node_set_boolean (retvalue, FALSE);
      return TRUE;
    }

  if (tb_jsonpath_function_value_string (v1, &string, error) == FALSE)
    return FALSE;

  if (tb_jsonpath_function_value_string (v2, &prefix, error) == FALSE)
    {
      g_free (string);
      return FALSE;
    }

  json_node_set_boolean (retvalue, g_str_has_prefix (string, prefix));

  g_free (prefix);
  g_free (string);

  return TRUE;
}

static gboolean
tb_jsonpath_function_contains (GList * args,
			       JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *v1, *v2;
  gchar *string;
  gchar *contained;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v1) == FALSE)
    {
      json_node_set_boolean (retvalue, FALSE);
      return TRUE;
    }

  args = args->next;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v2) == FALSE)
    {
      json_node_set_boolean (retvalue, FALSE);
      return TRUE;
    }

  if (tb_jsonpath_function_value_string (v1, &string, error) == FALSE)
    return FALSE;

  if (tb_jsonpath_function_value_string (v2, &contained, error) == FALSE)
    {
      g_free (string);
      return FALSE;
    }

  json_node_set_boolean (retvalue,
			     g_strrstr (string, contained) ? TRUE : FALSE);

  g_free (contained);
  g_free (string);

  return TRUE;
}

static gboolean
tb_jsonpath_function_substring_before (GList * args,
				       JsonNode * retvalue,
				       GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *v1, *v2;
  gchar *string, *sub, *ret;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v1) == FALSE)
    {
      json_node_set_string (retvalue, "");
      return TRUE;
    }

  args = args->next;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v2) == FALSE)
    {
      json_node_set_string (retvalue, string);
      return TRUE;
    }

  if (tb_jsonpath_function_value_string (v1, &string, error) == FALSE)
    return FALSE;

  if (tb_jsonpath_function_value_string (v2, &sub, error) == FALSE)
    {
      g_free (string);
      return FALSE;
    }

  if (!(ret = g_strstr_len (string, strlen (string), sub)))
    {
      json_node_set_string (retvalue, string);

      g_free (string);
      g_free (sub);
      return TRUE;
    }

  ret[0] = 0;
  json_node_set_string (retvalue, string);

  g_free (string);
  g_free (sub);
  return TRUE;
}

static gboolean
tb_jsonpath_function_substring_after (GList * args,
				      JsonNode * retvalue,
				      GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *v1, *v2;
  gchar *string, *sub, *ret;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v1) == FALSE)
    {
      json_node_set_string (retvalue, "");
      return TRUE;
    }

  args = args->next;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v2) == FALSE)
    {
      json_node_set_string (retvalue, "");
      return TRUE;
    }

  if (tb_jsonpath_function_value_string (v1, &string, error) == FALSE)
    return FALSE;

  if (tb_jsonpath_function_value_string (v2, &sub, error) == FALSE)
    return FALSE;

  if (!(ret = g_strstr_len (string, strlen (string), sub)))
    {
      json_node_set_string (retvalue, "");

      g_free (string);
      g_free (sub);
      return TRUE;
    }

  ret++;
  json_node_set_string (retvalue, ret);

  g_free (string);
  g_free (sub);
  return TRUE;
}

static gboolean
tb_jsonpath_function_substring (GList * args,
				JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *v1, *v2, *v3;
  gdouble numb1, numb2, i;
  gchar *string, *ptr;
  GString *ret;
  gint len;

  len = g_list_length (args);

  if (len < 2 || len > 3)
    {
      g_set_error (error, tb_jsonpath_error_quark (),
		   TB_ERROR_JSONPATH,
		   "The function 'substring' needs 2 or 3 arguments");
      return FALSE;
    }

  if (!(result = args->data)
      || tb_jsonpath_result_next (result, &v1) == FALSE)
    {
      json_node_set_string (retvalue, "");
      return TRUE;
    }

  if (tb_jsonpath_function_value_string (v1, &string, error) == FALSE)
    return FALSE;

  args = args->next;

  if (!(result = args->data)
      || tb_jsonpath_result_next (result, &v2) == FALSE)
    {
      json_node_set_string (retvalue, string);
      g_free (string);
      return TRUE;
    }

  if (tb_jsonpath_function_value_number (v2, &numb1, error) == FALSE)
    {
      g_free (string);
      return FALSE;
    }

  args = args->next;

  if (args && (result = args->data)
      && tb_jsonpath_result_next (result, &v3) == TRUE)
    {
      if (tb_jsonpath_function_value_number (v3, &numb2, error) == FALSE)
	{
	  g_free (string);
	  return FALSE;
	}
    }
  else
    numb2 = -1;

  if ((len = g_utf8_strlen (string, -1)) < numb1)
    {
      json_node_set_string (retvalue, "");

      g_free (string);
      return TRUE;
    }

  for (i = 0, ptr = string; i < numb1 && i < len && ptr; i++)
    ptr = g_utf8_next_char (ptr);

  ret = g_string_new (NULL);

  if (numb2 < 0)
    numb2 = len;

  for (; i < len && ptr && numb2 > 0; i++, numb2--)
    {
      g_string_append_unichar (ret, g_utf8_get_char (ptr));
      ptr = g_utf8_next_char (ptr);
    }

  g_free (string);

  string = g_string_free (ret, FALSE);
  json_node_set_string (retvalue, string);
  g_free (string);

  return TRUE;
}

static gboolean
tb_jsonpath_function_string_length (GList * args,
				    JsonNode * retvalue,
				    GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *v;
  gchar *string;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v) == FALSE)
    {
      json_node_set_double (retvalue, 0);
      return TRUE;
    }

  if (tb_jsonpath_function_value_string (v, &string, error) == FALSE)
    return FALSE;

  json_node_set_double (retvalue, g_utf8_strlen (string, -1));
  g_free (string);

  return TRUE;
}

static gboolean
tb_jsonpath_function_normalize_space (GList * args,
				      JsonNode * retvalue,
				      GError ** error)
{
  gint len;
  gunichar ch;
  GString *str;
  gboolean w = TRUE;
  gchar *string, *ptr;
  JsonNode *v;
  tb_jsonpath_result_t *result;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v) == FALSE)
    {
      json_node_set_string (retvalue, "");
      return TRUE;
    }

  if (tb_jsonpath_function_value_string (v, &string, error) == FALSE)
    return FALSE;

  str = g_string_new (NULL);
  ptr = string;

  while (ptr && *ptr)
    {
      ch = g_utf8_get_char (ptr);

      if (g_unichar_isspace (ch) == TRUE)
	{
	  if (w == FALSE)
	    {
	      g_string_append_unichar (str, ch);
	      w = TRUE;
	    }
	}
      else
	{
	  if (ch == '\\')
	    {
	      ptr = g_utf8_next_char (ptr);
	      ch = g_utf8_get_char (ptr);
	    }

	  g_string_append_unichar (str, ch);
	  w = FALSE;
	}

      ptr = g_utf8_next_char (ptr);
    }

  string = g_string_free (str, FALSE);

  if ((len = strlen (string)))
    {
      ch = string[len - 1];

      if (g_unichar_isspace (ch) == TRUE)
	string[len - 1] = 0;
    }

  json_node_set_string (retvalue, string);
  g_free (string);

  return TRUE;
}

static gboolean
tb_jsonpath_function_translate (GList * args,
				JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  gchar *string, *a, *b, *ptr;
  JsonNode *v1, *v2, *v3;
  GString *str;
  gint len;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v1) == FALSE)
    {
      json_node_set_string (retvalue, "");
      return TRUE;
    }

  args = args->next;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v2) == FALSE)
    {
      json_node_set_string (retvalue, "");
      return TRUE;
    }

  args = args->next;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &v3) == FALSE)
    {
      json_node_set_string (retvalue, "");
      return TRUE;
    }

  if (tb_jsonpath_function_value_string (v1, &string, error) == FALSE)
    return FALSE;

  if (tb_jsonpath_function_value_string (v2, &a, error) == FALSE)
    {
      g_free (string);
      return FALSE;
    }

  if (tb_jsonpath_function_value_string (v3, &b, error) == FALSE)
    {
      g_free (string);
      g_free (a);
      return FALSE;
    }

  len = g_utf8_strlen (a, -1);

  if (g_utf8_strlen (b, -1) != len)
    {
      g_set_error (error, tb_jsonpath_error_quark (),
		   TB_ERROR_JSONPATH,
		   "The second and third strings must have the same length for the function 'translate'");

      g_free (string);
      g_free (a);
      g_free (b);
      return FALSE;
    }

  str = g_string_new (NULL);

  for (ptr = string; ptr && *ptr; ptr = g_utf8_next_char (ptr))
    {
      gunichar ch;
      gchar *pos;

      ch = g_utf8_get_char (ptr);

      if ((pos = g_utf8_strchr (a, -1, ch)))
	{
	  glong pointer;

	  if ((pointer = g_utf8_pointer_to_offset (a, pos)) >= 0)
	    {
	      pos = g_utf8_offset_to_pointer (b, pointer);
	      ch = g_utf8_get_char (b);
	    }
	}

      g_string_append_unichar (str, ch);
    }

  g_free (string);
  g_free (a);
  g_free (b);

  string = g_string_free (str, FALSE);
  json_node_set_string (retvalue, string);
  g_free (string);

  return TRUE;
}

static gboolean
tb_jsonpath_function_boolean (GList * args,
			      JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *value;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &value) == FALSE)
    {
      json_node_set_boolean (retvalue, FALSE);
      return TRUE;
    }

  switch (json_node_get_node_type (value))
    {   
      case JSON_NODE_VALUE:
        {   
          if (json_node_get_value_type (value) == G_TYPE_STRING)
          {
	    gchar *string = (gchar *) json_node_get_string (value);
	    json_node_set_boolean (retvalue, *string ? TRUE : FALSE);
	    break;
          }

          if (json_node_get_value_type (value) == G_TYPE_DOUBLE
                || json_node_get_value_type (value) == G_TYPE_FLOAT)
          {
	    gdouble numb = json_node_get_double (value);
	    json_node_set_boolean (retvalue, numb ? TRUE : FALSE);
	    break;
          }

          if (json_node_get_value_type (value) == G_TYPE_INT
                || json_node_get_value_type (value) == G_TYPE_INT64
                || json_node_get_value_type (value) == G_TYPE_UINT)
          { 
	    gint numb = (gint) json_node_get_int (value);
	    json_node_set_boolean (retvalue, numb ? TRUE : FALSE);
	    break;
          } 
          if (json_node_get_value_type (value) == G_TYPE_BOOLEAN)
          { 
            json_node_set_boolean (retvalue, json_node_get_boolean (value));
            break;
          } 
        } 

      case JSON_NODE_OBJECT:
        {
          json_node_set_boolean (retvalue, TRUE);
        }

      case JSON_NODE_ARRAY:
        {
          json_node_set_boolean (retvalue, TRUE);
        }

      case JSON_NODE_NULL:
        {
          json_node_set_boolean (retvalue, FALSE);
          break;
        }
    }

  return TRUE;
}

static gboolean
tb_jsonpath_function_not (GList * args,
			  JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *value;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &value) == FALSE)
    {
      json_node_set_boolean (retvalue, TRUE);
      return TRUE;
    }

  if (json_node_get_value_type (value) == G_TYPE_BOOLEAN
      && json_node_get_boolean (value) == FALSE)
    json_node_set_boolean (retvalue, TRUE);
  else
    json_node_set_boolean (retvalue, FALSE);

  return TRUE;
}

static gboolean
tb_jsonpath_function_true (GList * args,
			   JsonNode * retvalue, GError ** error)
{
  json_node_set_boolean (retvalue, TRUE);
  return TRUE;
}

static gboolean
tb_jsonpath_function_false (GList * args,
			    JsonNode * retvalue, GError ** error)
{
  json_node_set_boolean (retvalue, FALSE);
  return TRUE;
}

static gboolean
tb_jsonpath_function_number (GList * args,
			     JsonNode * retvalue, GError ** error)
{
  tb_jsonpath_result_t *result;
  JsonNode *value;
  gdouble number;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &value) == FALSE)
    {
      json_node_set_double (retvalue, 0);
      return TRUE;
    }

  if (tb_jsonpath_function_value_number (value, &number, error) == FALSE)
    return FALSE;

  json_node_set_double (retvalue, number);
  return TRUE;
}

static gboolean
tb_jsonpath_function_sum (GList * args,
			  JsonNode * retvalue, GError ** error)
{
  GList *list;
  gdouble numb, total;

  for (total = 0, list = args; list; list = list->next)
    {
      tb_jsonpath_result_t *result = list->data;
      JsonNode *value;

      if (tb_jsonpath_result_next (result, &value) == FALSE)
	continue;

      if (tb_jsonpath_function_value_number (value, &numb, error) == FALSE)
	return FALSE;

      total += numb;
    }

  json_node_set_double (retvalue, total);
  return TRUE;
}

static gboolean
tb_jsonpath_function_floor (GList * args,
			    JsonNode * retvalue, GError ** error)
{
  GList *list;
  gdouble max, numb;
  JsonNode *value;
  tb_jsonpath_result_t *result;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &value) == FALSE)
    {
      json_node_set_double (retvalue, 0);
      return TRUE;
    }

  if (tb_jsonpath_function_value_number (value, &max, error) == FALSE)
    return FALSE;

  for (list = args->next; list; list = list->next)
    {
      result = list->data;

      if (tb_jsonpath_result_next (result, &value) == FALSE)
	continue;

      if (tb_jsonpath_function_value_number (value, &numb, error) == FALSE)
	return FALSE;

      if (numb > max)
	max = numb;
    }

  json_node_set_double (retvalue, max);
  return TRUE;
}

static gboolean
tb_jsonpath_function_ceiling (GList * args,
			      JsonNode * retvalue, GError ** error)
{
  GList *list;
  gdouble min, numb;
  JsonNode *value;
  tb_jsonpath_result_t *result;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &value) == FALSE)
    {
      json_node_set_double (retvalue, 0);
      return TRUE;
    }

  if (tb_jsonpath_function_value_number (value, &min, error) == FALSE)
    return FALSE;


  for (list = args->next; list; list = list->next)
    {
      result = list->data;

      if (tb_jsonpath_result_next (result, &value) == FALSE)
	continue;

      if (tb_jsonpath_function_value_number (value, &numb, error) == FALSE)
	return FALSE;

      if (numb < min)
	min = numb;
    }

  json_node_set_double (retvalue, min);
  return TRUE;
}

static gboolean
tb_jsonpath_function_round (GList * args,
			    JsonNode * retvalue, GError ** error)
{
  gchar *string, **split;
  gdouble numb, mantissa;
  JsonNode *value;
  tb_jsonpath_result_t *result;

  if (!args || !(result = args->data)
      || tb_jsonpath_result_next (result, &value) == FALSE)
    {
      json_node_set_double (retvalue, 0);
      return TRUE;
    }

  if (tb_jsonpath_function_value_number (value, &numb, error) == FALSE)
    return FALSE;

  string = g_strdup_printf ("%f", numb);
  split = g_strsplit (string, ".", 2);

  numb = strtod (split[0], NULL);

  if (split[1])
    {
      split[1][1] = 0;
      mantissa = strtod (split[1], NULL);
    }
  else
    mantissa = 0;

  g_strfreev (split);
  g_free (string);

  if (mantissa >= 5)
    numb = numb >= 0 ? numb + 1 : numb - 1;

  json_node_set_double (retvalue, numb);
  return TRUE;
}

/* FIND */
tb_jsonpath_function_t *
tb_jsonpath_function_find (tb_jsonpath_item_t * item, gchar * string)
{
  gint i;
  GList *list;

  for (i = 0; tb_jsonpath_default_functions[i].name; i++)
    {
      if (!g_utf8_collate (tb_jsonpath_default_functions[i].name, string))
	return &tb_jsonpath_default_functions[i];
    }

  if (!item->functions)
    return NULL;

  for (list = item->functions->functions; list; list = list->next)
    {
      tb_jsonpath_function_t *func = list->data;

      if (!g_utf8_collate (func->name, string))
	return func;
    }

  return NULL;
}

/* EOF */
