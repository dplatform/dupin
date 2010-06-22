/******************************************************************************
* 	Copyright (c) 2007-8 All rights reserved
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
* Author: Andrea Marchesini - baku@asemantics.com
*
* $Id$
******************************************************************************/

#include "tb_json.h"

#include "tb_scanner.h"
#include "tb_keyvalue.h"

#include "tb_json_internal.h"
#include "tb_json_markup_internal.h"

#include <string.h>

static void tb_json_node_set_and_destroy_value (tb_json_node_t * node,
						tb_json_value_t * value);

static gboolean tb_json_duplicate_object (tb_json_object_t * src,
					  tb_json_object_t * dest);
static gboolean tb_json_duplicate_array (tb_json_array_t * src,
					 tb_json_array_t * dest);

/**
 * tb_json_new:
 * @returns: a generic json data struct
 *
 * Creates a generic json data struct
 */
tb_json_t *
tb_json_new (void)
{
  tb_json_t *new;
  new = g_malloc0 (sizeof (tb_json_t));
  return new;
}

/**
 * tb_json_load_from_file:
 * @data: the tb_json_t
 * @filename: the name of the file
 * @error: the location for a GError or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * parses a file and returns a tb_json_t struct.
 **/
gboolean
tb_json_load_from_file (tb_json_t * data, gchar * filename, GError ** error)
{
  gchar *content;
  gboolean ret;
  gsize size;

  g_return_val_if_fail (filename != NULL, FALSE);
  g_return_val_if_fail (data != NULL, FALSE);

  if (g_file_get_contents (filename, &content, &size, error) == FALSE)
    return FALSE;

  ret = tb_json_load_from_buffer (data, content, size, error);
  g_free (content);
  return ret;
}

static tb_json_object_t *tb_json_parser_object (tb_scanner_t * scanner,
						tb_json_t * data);
static tb_json_array_t *tb_json_parser_array (tb_scanner_t * scanner,
					      tb_json_t * data);
static tb_json_value_t *tb_json_parser_value (tb_scanner_t * scanner,
					      tb_json_t * data);

/**
 * tb_json_load_from_buffer:
 * @data: the tb_json_t
 * @buffer: the buffer for the json
 * @size: size of the buffer or -1
 * @error: the location for a GError or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * parses a buffer in memory and returns a tb_json_t struct.
 **/
gboolean
tb_json_load_from_buffer (tb_json_t * data, gchar * buffer, gssize size,
			  GError ** error)
{
  tb_scanner_t *scanner;

  g_return_val_if_fail (buffer != NULL, FALSE);
  g_return_val_if_fail (data != NULL, FALSE);

  scanner = tb_scanner_new (error);
  tb_scanner_input_text (scanner, buffer, size);

  tb_scanner_get_next_token (scanner);

  if (tb_scanner_cur_token (scanner) == TB_SCANNER_LEFT_CURLY)
    {
      tb_json_object_t *object;

      if (!(object = tb_json_parser_object (scanner, data)))
	{
	  tb_scanner_destroy (scanner);
	  return FALSE;
	}

      data->object = object;
    }

  else if (tb_scanner_cur_token (scanner) == TB_SCANNER_LEFT_BRACE)
    {
      tb_json_array_t *array;

      if (!(array = tb_json_parser_array (scanner, data)))
	{
	  tb_scanner_destroy (scanner);
	  return FALSE;
	}

      data->array = array;
    }

  else
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER, "{ or [");
      tb_scanner_destroy (scanner);
      return FALSE;
    }

  if (tb_scanner_get_next_token (scanner) != TB_SCANNER_EOF)
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_EOF, NULL);
      tb_scanner_destroy (scanner);
      return FALSE;
    }

  tb_scanner_destroy (scanner);
  return TRUE;
}

static tb_json_object_t *
tb_json_parser_object (tb_scanner_t * scanner, tb_json_t * data)
{
  tb_json_object_t *object;

  if (tb_json_object_new (&object) == FALSE)
    {
      g_set_error (tb_scanner_error (scanner), tb_json_error_quark (),
		   TB_ERROR_JSON,
		   "Error creating a new object (Line: %d, Position: %d).",
		   tb_scanner_get_cur_line (scanner),
		   tb_scanner_get_cur_position (scanner));
      return NULL;
    }

  while (tb_scanner_get_next_token (scanner) == TB_SCANNER_STRING
	 || tb_scanner_cur_token (scanner) == TB_SCANNER_IDENTIFIER)
    {
      tb_json_node_t *node;
      tb_json_value_t *value;
      gchar *tmp;

      tmp = g_strdup (tb_scanner_cur_value_string (scanner));

      if (tb_json_object_has_node (object, tmp) == TRUE)
	{
	  g_set_error (tb_scanner_error (scanner), tb_json_error_quark (),
		       TB_ERROR_JSON,
		       "Duplicated the element '%s' (Line: %d, Position: %d).",
		       tmp, tb_scanner_get_cur_line (scanner),
		       tb_scanner_get_cur_position (scanner));
	  tb_json_object_destroy (object);
	  g_free (tmp);
	  return NULL;
	}

      if (tb_json_object_add_node (object, tmp, &node) == FALSE)
	{
	  g_set_error (tb_scanner_error (scanner), tb_json_error_quark (),
		       TB_ERROR_JSON,
		       "Error creating a new node (Line: %d, Position: %d).",
		       tb_scanner_get_cur_line (scanner),
		       tb_scanner_get_cur_position (scanner));
	  tb_json_object_destroy (object);
	  g_free (tmp);
	  return NULL;
	}

      g_free (tmp);

      if (tb_scanner_get_next_token (scanner) != TB_SCANNER_COLON)
	{
	  tb_scanner_unexp_token (scanner, TB_SCANNER_COLON, NULL);
	  tb_json_object_destroy (object);
	  return NULL;
	}

      tb_scanner_get_next_token (scanner);

      if (!(value = tb_json_parser_value (scanner, data)))
	{
	  tb_json_object_destroy (object);
	  return NULL;
	}

      tb_json_node_set_and_destroy_value (node, value);

      if (tb_scanner_peek_next_token (scanner) == TB_SCANNER_COMMA)
	{
	  tb_scanner_get_next_token (scanner);

	  if (tb_scanner_peek_next_token (scanner) !=
	      TB_SCANNER_STRING
	      && tb_scanner_peek_next_token (scanner) !=
	      TB_SCANNER_IDENTIFIER)
	    {
	      tb_scanner_unexp_token (scanner, TB_SCANNER_STRING, NULL);
	      tb_json_object_destroy (object);
	      return NULL;
	    }
	}

      else if (tb_scanner_peek_next_token (scanner) != TB_SCANNER_RIGHT_CURLY)
	{
	  tb_scanner_get_next_token (scanner);
	  tb_scanner_unexp_token (scanner, TB_SCANNER_RIGHT_CURLY, NULL);
	  tb_json_object_destroy (object);
	  return NULL;
	}
    }

  if (tb_scanner_cur_token (scanner) != TB_SCANNER_RIGHT_CURLY)
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_RIGHT_CURLY, NULL);
      tb_json_object_destroy (object);
      return NULL;
    }

  return object;
}

static tb_json_array_t *
tb_json_parser_array (tb_scanner_t * scanner, tb_json_t * data)
{
  tb_json_array_t *array;

  if (tb_json_array_new (&array) == FALSE)
    {
      g_set_error (tb_scanner_error (scanner), tb_json_error_quark (),
		   TB_ERROR_JSON,
		   "Error creating a new array (Line: %d, Position: %d).",
		   tb_scanner_get_cur_line (scanner),
		   tb_scanner_get_cur_position (scanner));
      return NULL;
    }

  while (tb_scanner_get_next_token (scanner) != TB_SCANNER_RIGHT_BRACE
	 && tb_scanner_cur_token (scanner) != TB_SCANNER_EOF)
    {
      tb_json_value_t *value;

      if (!(value = tb_json_parser_value (scanner, data)))
	{
	  tb_json_array_destroy (array);
	  return NULL;
	}

      value->parent_array = array;
      array->values = g_list_append (array->values, value);

      if (tb_scanner_peek_next_token (scanner) == TB_SCANNER_COMMA)
	{
	  tb_scanner_get_next_token (scanner);

	  if (tb_scanner_peek_next_token (scanner) == TB_SCANNER_RIGHT_BRACE)
	    {
	      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER, NULL);
	      tb_json_array_destroy (array);
	      return NULL;
	    }
	}

      else if (tb_scanner_peek_next_token (scanner) != TB_SCANNER_RIGHT_BRACE)
	{
	  tb_scanner_get_next_token (scanner);
	  tb_scanner_unexp_token (scanner, TB_SCANNER_RIGHT_BRACE, NULL);
	  tb_json_array_destroy (array);
	  return NULL;
	}
    }

  if (tb_scanner_cur_token (scanner) != TB_SCANNER_RIGHT_BRACE)
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_RIGHT_BRACE, NULL);
      tb_json_array_destroy (array);
      return NULL;
    }

  return array;
}

static tb_json_value_t *
tb_json_parser_value (tb_scanner_t * scanner, tb_json_t * data)
{
  tb_json_value_t *value;
  tb_json_object_t *object;
  tb_json_array_t *array;

  switch (tb_scanner_cur_token (scanner))
    {
    case TB_SCANNER_STRING:
      value = g_malloc0 (sizeof (tb_json_value_t));
      value->type = TB_JSON_VALUE_STRING;
      value->string = g_strdup (tb_scanner_cur_value_string (scanner));
      return value;

    case TB_SCANNER_IDENTIFIER:
      if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "true"))
	{
	  value = g_malloc0 (sizeof (tb_json_value_t));
	  value->type = TB_JSON_VALUE_BOOLEAN;
	  value->boolean = TRUE;
	  return value;
	}

      if (!g_utf8_collate
	  (tb_scanner_cur_value_identifier (scanner), "false"))
	{
	  value = g_malloc0 (sizeof (tb_json_value_t));
	  value->type = TB_JSON_VALUE_BOOLEAN;
	  value->boolean = FALSE;
	  return value;
	}

      if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "null"))
	{
	  value = g_malloc0 (sizeof (tb_json_value_t));
	  value->type = TB_JSON_VALUE_NULL;
	  return value;
	}

      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER, "value");
      return NULL;

    case TB_SCANNER_NUMBER:
      value = g_malloc0 (sizeof (tb_json_value_t));
      value->type = TB_JSON_VALUE_NUMBER;
      value->number = tb_scanner_cur_value_number (scanner);
      return value;

    case TB_SCANNER_LEFT_CURLY:
      if ((object = tb_json_parser_object (scanner, data)))
	{
	  value = g_malloc0 (sizeof (tb_json_value_t));
	  value->type = TB_JSON_VALUE_OBJECT;
	  value->object = object;
	  return value;
	}

      return NULL;

    case TB_SCANNER_LEFT_BRACE:
      if ((array = tb_json_parser_array (scanner, data)))
	{
	  value = g_malloc0 (sizeof (tb_json_value_t));
	  value->type = TB_JSON_VALUE_ARRAY;
	  value->array = array;
	  return value;
	}

      return NULL;

    default:
      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER, "value");
      return NULL;
    }
}

/**
 * tb_json_destroy:
 * @data: the tb_json_t data struct
 *
 * frees the json data.
 **/
void
tb_json_destroy (tb_json_t * data)
{
  if (!data)
    return;

  if (data->array)
    tb_json_array_destroy (data->array);

  if (data->object)
    tb_json_object_destroy (data->object);

  g_free (data);
}

/**
 * tb_json_is_object
 * @data: the tb_json_t data struct
 * @returns: TRUE if the json data struct contains a object
 *
 * Returns TRUE is the json data struct contains a object
 **/
gboolean
tb_json_is_object (tb_json_t * data)
{
  g_return_val_if_fail (data != NULL, FALSE);

  return data->object ? TRUE : FALSE;
}

/**
 * tb_json_is_array
 * @data: the tb_json_t data struct
 * @returns: TRUE if the json data struct contains an array
 *
 * Returns TRUE is the json data struct contains a array
 **/
gboolean
tb_json_is_array (tb_json_t * data)
{
  g_return_val_if_fail (data != NULL, FALSE);

  return data->array ? TRUE : FALSE;
}

/**
 * tb_json_object
 * @data: the tb_json_t data struct
 * @returns: the object
 *
 * Returns the object into the json data struct. the object is not to free because
 * it is a internal pointer.
 **/
tb_json_object_t *
tb_json_object (tb_json_t * data)
{
  g_return_val_if_fail (data != NULL, FALSE);
  g_return_val_if_fail (tb_json_is_object (data) == TRUE, FALSE);

  return data->object;
}

tb_json_object_t *
tb_json_object_and_detach (tb_json_t * data)
{
  tb_json_object_t *obj;

  g_return_val_if_fail (data != NULL, FALSE);
  g_return_val_if_fail (tb_json_is_object (data) == TRUE, FALSE);

  obj=data->object;
  data->object = NULL;
  return obj;
}

/**
 * tb_json_array
 * @data: the tb_json_t data struct
 * @returns: the array
 *
 * Returns the array into the json data struct. the array is not to free because
 * it is a internal pointer.
 **/
tb_json_array_t *
tb_json_array (tb_json_t * data)
{
  g_return_val_if_fail (data != NULL, FALSE);
  g_return_val_if_fail (tb_json_is_array (data) == TRUE, FALSE);

  return data->array;
}

tb_json_array_t *
tb_json_array_and_detach (tb_json_t * data)
{
  tb_json_array_t *obj;

  g_return_val_if_fail (data != NULL, FALSE);
  g_return_val_if_fail (tb_json_is_array (data) == TRUE, FALSE);

  obj=data->array;
  data->array = NULL;
  return obj;
}

/* Write */

/**
 * tb_json_write_to_file
 * @data: the tb_json_t data struct
 * @filename: the filename
 * @error: the location for a GError, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Writes a json data struct into a file.
 **/
gboolean
tb_json_write_to_file (tb_json_t * data, gchar * filename, GError ** error)
{
  gboolean ret;
  gchar *buffer;
  gsize size;

  g_return_val_if_fail (data != NULL, FALSE);
  g_return_val_if_fail (filename != NULL, FALSE);

  if (tb_json_write_to_buffer (data, &buffer, &size, error) == FALSE)
    return FALSE;

  ret = g_file_set_contents (filename, buffer, size, error);
  g_free (buffer);

  return ret;
}

/**
 * tb_json_write_to_buffer
 * @data: the tb_json_t data struct
 * @buffer: the location for the memory buffer
 * @size: the location for the size of the buffer, or NULL
 * @error: the location for a GError, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Writes a json data struct into a memory buffer. This buffer must be freed with
 * a g_free.
 **/
gboolean
tb_json_write_to_buffer (tb_json_t * data, gchar ** buffer, gsize * size,
			 GError ** error)
{
  g_return_val_if_fail (data != NULL, FALSE);
  g_return_val_if_fail (buffer != NULL, FALSE);

  if (tb_json_is_object (data) == TRUE)
    return tb_json_object_write_to_buffer (tb_json_object (data), buffer,
					   size, error);

  return tb_json_array_write_to_buffer (tb_json_array (data), buffer, size,
					error);
}

static gboolean
tb_json_write_to_buffer_value (tb_json_value_t * value, GString * str,
			       GError ** error);

static gboolean
tb_json_write_to_buffer_node (tb_json_node_t * node, GString * str,
			      GError ** error)
{
  gchar *tmp;

  tmp = tb_json_markup (node->name);
  g_string_append_printf (str, "\"%s\":", tmp);
  g_free (tmp);

  tb_json_write_to_buffer_value (&node->value, str, error);
  return TRUE;
}

static gboolean
tb_json_write_to_buffer_value (tb_json_value_t * value, GString * str,
			       GError ** error)
{
  GList *list;
  gchar *tmp;

  switch (value->type)
    {
    case TB_JSON_VALUE_STRING:
      tmp = tb_json_markup (value->string);
      g_string_append_printf (str, "\"%s\"", tmp);
      g_free (tmp);
      break;

    case TB_JSON_VALUE_NUMBER:
      g_string_append_printf (str, "%lf", value->number);
      break;

    case TB_JSON_VALUE_OBJECT:
      str = g_string_append (str, "{");

      for (list = value->object->nodes; list; list = list->next)
	{
	  if (tb_json_write_to_buffer_node (list->data, str, error) == FALSE)
	    return FALSE;

	  if (list->next)
	    str = g_string_append_c (str, ',');
	}

      str = g_string_append (str, "}");
      break;

    case TB_JSON_VALUE_ARRAY:
      str = g_string_append (str, "[");

      for (list = value->array->values; list; list = list->next)
	{
	  if (tb_json_write_to_buffer_value (list->data, str, error) == FALSE)
	    return FALSE;

	  if (list->next)
	    str = g_string_append_c (str, ',');
	}

      str = g_string_append (str, "]");
      break;

    case TB_JSON_VALUE_BOOLEAN:
      g_string_append_printf (str, "%s",
			      value->boolean == TRUE ? "true" : "false");
      break;

    case TB_JSON_VALUE_NULL:
      str = g_string_append (str, "null");
      break;
    }

  return TRUE;
}

/* Object */

/**
 * tb_json_object_write_to_file
 * @object: the JSON object
 * @filename: the filename
 * @error: the location for a GError, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Writes a json object into a file.
 */
gboolean
tb_json_object_write_to_file (tb_json_object_t * object, gchar * filename,
			      GError ** error)
{
  gboolean ret;
  gchar *buffer;
  gsize size;

  g_return_val_if_fail (object != NULL, FALSE);
  g_return_val_if_fail (filename != NULL, FALSE);

  if (tb_json_object_write_to_buffer (object, &buffer, &size, error) == FALSE)
    return FALSE;

  ret = g_file_set_contents (filename, buffer, size, error);
  g_free (buffer);

  return ret;
}

/**
 * tb_json_object_write_to_buffer
 * @object: the JSON object
 * @buffer: the location for the memory buffer
 * @size: the location for the size of the buffer, or NULL
 * @error: the location for a GError, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Writes a json object into a memory buffer. This buffer must be freed with a g_free.
 **/
gboolean
tb_json_object_write_to_buffer (tb_json_object_t * object, gchar ** buffer,
				gsize * size, GError ** error)
{
  GList *list;
  GString *str;

  g_return_val_if_fail (object != NULL, FALSE);
  g_return_val_if_fail (buffer != NULL, FALSE);

  str = g_string_new ("{");

  for (list = object->nodes; list; list = list->next)
    {
      if (tb_json_write_to_buffer_node (list->data, str, error) == FALSE)
	{
	  g_string_free (str, FALSE);
	  return FALSE;
	}
      if (list->next)
	str = g_string_append_c (str, ',');
    }

  str = g_string_append (str, "}");
  *buffer = g_string_free (str, FALSE);

  if (size)
    *size = g_utf8_strlen (*buffer, -1);

  return TRUE;
}

/**
 * tb_json_object_new
 * @object: the location for a new json object
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Creates a new json object.
 */
gboolean
tb_json_object_new (tb_json_object_t ** object)
{
  tb_json_object_t *new;

  g_return_val_if_fail (object != NULL, FALSE);

  new = g_malloc0 (sizeof (tb_json_object_t));
  *object = new;
  return TRUE;
}

/**
 * tb_json_object_destroy
 * @object: the json object
 *
 * Destroies a json object.
 */
void
tb_json_object_destroy (tb_json_object_t * object)
{
  if (!object)
    return;

  if (object->nodes)
    {
      g_list_foreach (object->nodes, (GFunc) tb_json_node_destroy, NULL);
      g_list_free (object->nodes);
    }

  g_free (object);
}

/**
 * tb_json_object_duplicate
 * @src: the json object template
 * @dest: the location for the new json object
 * 
 * Duplicates a json object
 */
gboolean
tb_json_object_duplicate (tb_json_object_t * src, tb_json_object_t ** dest)
{
  g_return_val_if_fail (src != NULL, FALSE);
  g_return_val_if_fail (dest != NULL, FALSE);

  tb_json_object_new (dest);
  return tb_json_duplicate_object (src, *dest);
}

/**
 * tb_json_object_to_data:
 * @object: the json object
 * @returns: a generic tb_json_t
 *
 * Creates a tb_json_t from a tb_json_object
 */
tb_json_t *
tb_json_object_to_data (tb_json_object_t * object)
{
  tb_json_t *data;

  g_return_val_if_fail (object != NULL, NULL);

  data = g_malloc0 (sizeof (tb_json_t));
  data->object = object;
  return data;
}

/**
 * tb_json_object_get_nodes
 * @object: the json object
 * @returns: a list of nodes.
 *
 * Returns a list of nodes. This list is a internal pointer and is not to free.
 */
GList *
tb_json_object_get_nodes (tb_json_object_t * object)
{
  g_return_val_if_fail (object != NULL, NULL);
  return object->nodes;
}

/**
 * tb_json_object_get_node
 * @object: the  json object
 * @node: the name of the node
 * @returns: a json node
 *
 * Returns a json node. It is a internal pointer.
 */
tb_json_node_t *
tb_json_object_get_node (tb_json_object_t * object, gchar * node)
{
  GList *list;

  g_return_val_if_fail (object != NULL, NULL);
  g_return_val_if_fail (node != NULL, NULL);

  for (list = object->nodes; list; list = list->next)
    {
      tb_json_node_t *data = list->data;

      if (!g_utf8_collate (node, data->name))
	return data;
    }

  return NULL;
}

/**
 * tb_json_object_has_node
 * @object: the  json object
 * @node: the name of the node
 * @returns: if the json node exists or not
 *
 * Returns a TRUE if the json node is contained into the json object.
 */
gboolean
tb_json_object_has_node (tb_json_object_t * object, gchar * node)
{
  if (tb_json_object_get_node (object, node))
    return TRUE;

  return FALSE;
}

/**
 * tb_json_object_add_node
 * @object: the  json object
 * @string: the name of the new node
 * @node: the location for the new json node, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Creates a new json node and puts it into the json object
 */
gboolean
tb_json_object_add_node (tb_json_object_t * object, gchar * string,
			 tb_json_node_t ** node)
{
  tb_json_node_t *data;

  g_return_val_if_fail (object != NULL, FALSE);
  g_return_val_if_fail (string != NULL, FALSE);
  g_return_val_if_fail (tb_json_object_has_node (object, string) == FALSE,
			FALSE);

  data = g_malloc0 (sizeof (tb_json_node_t));
  data->name = g_strdup (string);
  data->parent = object;

  data->value.type = TB_JSON_VALUE_NULL;
  data->value.parent_node = data;

  object->nodes = g_list_append (object->nodes, data);

  if (node)
    *node = data;

  return TRUE;
}

/**
 * tb_json_object_remove_node:
 * @object: the  json object
 * @node: the json node
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Removes the json node from a json object. The json node is not freed.
 */
gboolean
tb_json_object_remove_node (tb_json_object_t * object, tb_json_node_t * node)
{
  g_return_val_if_fail (object != NULL, FALSE);
  g_return_val_if_fail (node != NULL, FALSE);

  object->nodes = g_list_remove (object->nodes, node);
  tb_json_node_destroy (node);

  return TRUE;
}

/**
 * tb_json_object_parent:
 * @object: the json object
 * @returns: the json value parent of this object, or NULL
 *
 * returns the json value parent of this object, or NULL
 **/
tb_json_value_t *
tb_json_object_parent (tb_json_object_t * object)
{
  g_return_val_if_fail (object != NULL, NULL);

  return object->parent;
}

/* Node */

/**
 * tb_json_node_destroy:
 * @node: the json node
 *
 * Destroies the json node.
 */
void
tb_json_node_destroy (tb_json_node_t * node)
{
  if (!node)
    return;

  if (node->name)
    g_free (node->name);

  tb_json_value_destroy_internal (&node->value);
  g_free (node);
}

/**
 * tb_json_node_get_value:
 * @node: the json node
 * @returns: the json value of the node
 *
 * Returns the json value of the node
 */
tb_json_value_t *
tb_json_node_get_value (tb_json_node_t * node)
{
  g_return_val_if_fail (node != NULL, NULL);

  return &node->value;
}

/**
 * tb_json_node_get_string:
 * @node: the json node
 * @returns: the string of the node
 *
 * Returns the string of the node. Free it with g_free
 */
gchar *
tb_json_node_get_string (tb_json_node_t * node)
{
  g_return_val_if_fail (node != NULL, NULL);

  return node->name;
}

/**
 * tb_json_node_set_string:
 * @node: the json node
 * @string: a string
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Sets a string to a json node
 */
gboolean
tb_json_node_set_string (tb_json_node_t * node, gchar * string)
{
  g_return_val_if_fail (node != NULL, FALSE);
  g_return_val_if_fail (string != NULL, FALSE);

  if (tb_json_node_parent (node))
    g_return_val_if_fail (tb_json_object_has_node
			  (tb_json_node_parent (node), string) == FALSE,
			  FALSE);

  if (node->name)
    g_free (node->name);

  node->name = g_strdup (string);
  return TRUE;
}

/**
 * tb_json_node_parent:
 * @node: the json node
 * @returns: the parent of this node, or NULL
 *
 * returns the parent of this node or NULL
 **/
tb_json_object_t *
tb_json_node_parent (tb_json_node_t * node)
{
  g_return_val_if_fail (node != NULL, FALSE);

  return node->parent;
}

/* Value */

/**
 * tb_json_value_new:
 * @value: a pointer for a value
 * @returns: the status
 *
 * Creates a new value data struct
 */
gboolean
tb_json_value_new (tb_json_value_t ** value)
{
  tb_json_value_t *new;

  g_return_val_if_fail (value != NULL, FALSE);

  new = g_malloc0 (sizeof (tb_json_value_t));
  tb_json_value_set_null (new);

  *value = new;
  return TRUE;
}

/**
 * tb_json_value_destroy:
 * @value: a value
 * 
 * Destroies a value
 */
void
tb_json_value_destroy (tb_json_value_t * value)
{
  if (!value)
    return;

  tb_json_value_destroy_internal (value);
  g_free (value);
}

/**
 * tb_json_value_duplicate
 * @src: the json value template
 * @dest: the location for the new json value
 * 
 * Duplicates a json value
 */
gboolean
tb_json_value_duplicate (tb_json_value_t * src, tb_json_value_t ** dest)
{
  g_return_val_if_fail (src != NULL, FALSE);
  g_return_val_if_fail (dest != NULL, FALSE);

  tb_json_value_new (dest);
  return tb_json_value_duplicate_exists (src, *dest);
}

/**
 * tb_json_value_get_type:
 * @value: the json value
 * @returns: the type of the json value
 *
 * Returns the type of the json value.
 */
tb_json_value_type_t
tb_json_value_get_type (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, TB_JSON_VALUE_NULL);
  return value->type;
}

/**
 * tb_json_value_set_string
 * @value: the json value
 * @string: the string of the value
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Set a string into a value
 */
gboolean
tb_json_value_set_string (tb_json_value_t * value, gchar * string)
{
  g_return_val_if_fail (value != NULL, FALSE);
  g_return_val_if_fail (string != NULL, FALSE);

  tb_json_value_destroy_internal (value);
  value->type = TB_JSON_VALUE_STRING;
  value->string = g_strdup (string);
  return TRUE;
}

/**
 * tb_json_value_get_string
 * @value: the json value
 * @returns: a new allocated string
 *
 * Get the string into a value
 */
gchar *
tb_json_value_get_string (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, NULL);
  g_return_val_if_fail (tb_json_value_get_type (value) ==
			TB_JSON_VALUE_STRING, NULL);

  return value->string;
}

/**
 * tb_json_value_set_number
 * @value: the json value
 * @number: the number of the value
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Set a number into a value
 */
gboolean
tb_json_value_set_number (tb_json_value_t * value, gdouble number)
{
  g_return_val_if_fail (value != NULL, FALSE);

  tb_json_value_destroy_internal (value);
  value->type = TB_JSON_VALUE_NUMBER;
  value->number = number;
  return TRUE;
}

/**
 * tb_json_value_get_number
 * @value: the json value
 * @returns: a number
 *
 * Get the number into a value
 */
gdouble
tb_json_value_get_number (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, 0);
  g_return_val_if_fail (tb_json_value_get_type (value) ==
			TB_JSON_VALUE_NUMBER, 0);

  return value->number;
}

/**
 * tb_json_value_set_object
 * @value: the json value
 * @object: an object already existing
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Set an object into a value
 */
gboolean
tb_json_value_set_object (tb_json_value_t * value, tb_json_object_t * object)
{
  g_return_val_if_fail (value != NULL, FALSE);
  g_return_val_if_fail (object != NULL, FALSE);

  tb_json_value_destroy_internal (value);
  value->type = TB_JSON_VALUE_OBJECT;
  value->object = object;

  value->object->parent = value;

  return TRUE;
}

/**
 * tb_json_value_set_object_new
 * @value: the json value
 * @object: location for the new object, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Set an object into a value
 */
gboolean
tb_json_value_set_object_new (tb_json_value_t * value,
			      tb_json_object_t ** object)
{
  g_return_val_if_fail (value != NULL, FALSE);

  tb_json_value_destroy_internal (value);
  value->type = TB_JSON_VALUE_OBJECT;

  if (tb_json_object_new (&value->object) == FALSE)
    return FALSE;

  value->object->parent = value;

  if (object)
    *object = tb_json_value_get_object (value);

  return TRUE;
}

/**
 * tb_json_value_get_object
 * @value: the json value
 * @returns: a object
 *
 * Get the object into a value
 */
tb_json_object_t *
tb_json_value_get_object (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, NULL);
  g_return_val_if_fail (tb_json_value_get_type (value) ==
			TB_JSON_VALUE_OBJECT, NULL);

  return value->object;
}

/**
 * tb_json_value_set_boolean
 * @value: the json value
 * @boolean: the boolean of the value
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Set a boolean into a value
 */
gboolean
tb_json_value_set_boolean (tb_json_value_t * value, gboolean boolean)
{
  g_return_val_if_fail (value != NULL, FALSE);

  tb_json_value_destroy_internal (value);
  value->type = TB_JSON_VALUE_BOOLEAN;
  value->boolean = boolean;
  return TRUE;
}

/**
 * tb_json_value_get_boolean
 * @value: the json value
 * @returns: a boolean
 *
 * Get the boolean into a value
 */
gboolean
tb_json_value_get_boolean (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, FALSE);
  g_return_val_if_fail (tb_json_value_get_type (value) ==
			TB_JSON_VALUE_BOOLEAN, FALSE);

  return value->boolean;
}

/**
 * tb_json_value_set_null
 * @value: the json value
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Set a null into a value
 */
gboolean
tb_json_value_set_null (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, FALSE);

  tb_json_value_destroy_internal (value);
  value->type = TB_JSON_VALUE_NULL;
  return TRUE;
}

/**
 * tb_json_value_set_array
 * @value: the json value
 * @array: an array already existing
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Set a array into a value
 */
gboolean
tb_json_value_set_array (tb_json_value_t * value, tb_json_array_t * array)
{
  g_return_val_if_fail (value != NULL, FALSE);
  g_return_val_if_fail (array != NULL, FALSE);

  tb_json_value_destroy_internal (value);
  value->type = TB_JSON_VALUE_ARRAY;
  value->array = array;

  value->array->parent = value;

  return TRUE;
}

/**
 * tb_json_value_set_array_new
 * @value: the json value
 * @array: location for the new array, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Set a array into a value
 */
gboolean
tb_json_value_set_array_new (tb_json_value_t * value,
			     tb_json_array_t ** array)
{
  g_return_val_if_fail (value != NULL, FALSE);

  tb_json_value_destroy_internal (value);
  value->type = TB_JSON_VALUE_ARRAY;

  if (tb_json_array_new (&value->array) == FALSE)
    return FALSE;

  value->array->parent = value;

  if (array)
    *array = tb_json_value_get_array (value);

  return TRUE;
}

/**
 * tb_json_value_get_array
 * @value: the json value
 * @returns: the array
 *
 * Get the array into a value
 */
tb_json_array_t *
tb_json_value_get_array (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, FALSE);
  g_return_val_if_fail (tb_json_value_get_type (value) == TB_JSON_VALUE_ARRAY,
			FALSE);

  return value->array;
}

/**
 * tb_json_value_parent_is_node:
 * @value: the json value
 * @returns: TRUE if the parent is a json node
 * 
 * returns TRUE if the parent of this json value is a node
 **/
gboolean
tb_json_value_parent_is_node (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, FALSE);

  return value->parent_node ? TRUE : FALSE;
}

/**
 * tb_json_value_parent_is_array:
 * @value: the json value
 * @returns: TRUE if the parent is a json value
 * 
 * returns TRUE if the parent of this json value is a value
 **/
gboolean
tb_json_value_parent_is_array (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, FALSE);

  return value->parent_array ? TRUE : FALSE;
}

/**
 * tb_json_value_parent_node:
 * @value: the json value
 * @returns: the parent JSON node
 * 
 * returns the json node parent this json value
 **/
tb_json_node_t *
tb_json_value_parent_node (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, FALSE);
  g_return_val_if_fail (tb_json_value_parent_is_node (value) == TRUE, FALSE);

  return value->parent_node;
}

/**
 * tb_json_value_parent_array:
 * @value: the json value
 * @returns: the parent JSON array
 * 
 * returns the json array parent this json value
 **/
tb_json_array_t *
tb_json_value_parent_array (tb_json_value_t * value)
{
  g_return_val_if_fail (value != NULL, FALSE);
  g_return_val_if_fail (tb_json_value_parent_is_array (value) == TRUE, FALSE);

  return value->parent_array;
}

void
tb_json_value_destroy_internal (tb_json_value_t * value)
{
  switch (value->type)
    {
    case TB_JSON_VALUE_STRING:
      if (value->string)
	{
	  g_free (value->string);
	  value->string = NULL;
	}
      break;

    case TB_JSON_VALUE_NUMBER:
      value->number = 0;
      break;

    case TB_JSON_VALUE_OBJECT:
      if (value->object)
	{
	  tb_json_object_destroy (value->object);
	  value->object = NULL;
	}
      break;

    case TB_JSON_VALUE_ARRAY:
      if (value->array)
	{
	  tb_json_array_destroy (value->array);
	  value->array = NULL;
	}
      break;

    case TB_JSON_VALUE_BOOLEAN:
      value->boolean = FALSE;
      break;

    case TB_JSON_VALUE_NULL:
      break;
    }
}

static void
tb_json_node_set_and_destroy_value (tb_json_node_t * node,
				    tb_json_value_t * value)
{
  tb_json_value_destroy_internal (&node->value);
  memcpy (&node->value, value, sizeof (tb_json_value_t));
  node->value.parent_node = node;
  g_free (value);
}

/* Array */

/**
 * tb_json_array_write_to_file
 * @array: the JSON array
 * @filename: the filename
 * @error: the location for a GError, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Writes a json array into a file.
 */
gboolean
tb_json_array_write_to_file (tb_json_array_t * array, gchar * filename,
			     GError ** error)
{
  gboolean ret;
  gchar *buffer;
  gsize size;

  g_return_val_if_fail (array != NULL, FALSE);
  g_return_val_if_fail (filename != NULL, FALSE);

  if (tb_json_array_write_to_buffer (array, &buffer, &size, error) == FALSE)
    return FALSE;

  ret = g_file_set_contents (filename, buffer, size, error);
  g_free (buffer);

  return ret;
}

/**
 * tb_json_array_write_to_buffer
 * @array: the JSON array
 * @buffer: the location for the memory buffer
 * @size: the location for the size of the buffer, or NULL
 * @error: the location for a GError, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Writes a json array into a memory buffer. This buffer must be freed with a g_free.
 **/
gboolean
tb_json_array_write_to_buffer (tb_json_array_t * array, gchar ** buffer,
			       gsize * size, GError ** error)
{
  GList *list;
  GString *str;

  g_return_val_if_fail (array != NULL, FALSE);
  g_return_val_if_fail (buffer != NULL, FALSE);

  str = g_string_new ("[");

  for (list = array->values; list; list = list->next)
    {
      if (tb_json_write_to_buffer_value (list->data, str, error) == FALSE)
	{
	  g_string_free (str, FALSE);
	  return FALSE;
	}
      if (list->next)
	str = g_string_append_c (str, ',');
    }

  str = g_string_append (str, "]");
  *buffer = g_string_free (str, FALSE);

  if (size)
    *size = g_utf8_strlen (*buffer, -1);

  return TRUE;
}

/**
 * tb_json_array_new
 * @array: the location for a new json array
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Creates a new json array
 */
gboolean
tb_json_array_new (tb_json_array_t ** array)
{
  tb_json_array_t *new;

  g_return_val_if_fail (array != NULL, FALSE);

  new = g_malloc0 (sizeof (tb_json_array_t));
  *array = new;
  return TRUE;
}

/**
 * tb_json_array_destroy
 * @array: the json array
 *
 * Destroies a json array
 */
void
tb_json_array_destroy (tb_json_array_t * array)
{
  GList *list;

  if (!array)
    return;

  if (array->values)
    {
      for (list = array->values; list; list = list->next)
	{
	  tb_json_value_destroy_internal (list->data);
	  g_free (list->data);
	}

      g_list_free (array->values);
    }

  g_free (array);
}

/**
 * tb_json_array_duplicate
 * @src: the json array template
 * @dest: the location for the new json array
 * 
 * Duplicates a json array
 */
gboolean
tb_json_array_duplicate (tb_json_array_t * src, tb_json_array_t ** dest)
{
  g_return_val_if_fail (src != NULL, FALSE);
  g_return_val_if_fail (dest != NULL, FALSE);

  tb_json_array_new (dest);
  return tb_json_duplicate_array (src, *dest);
}

/**
 * tb_json_array_to_data:
 * @array: the json array
 * @returns: a generic tb_json_t
 *
 * Creates a tb_json_t from a tb_json_array
 */
tb_json_t *
tb_json_array_to_data (tb_json_array_t * array)
{
  tb_json_t *data;

  g_return_val_if_fail (array != NULL, NULL);

  data = g_malloc0 (sizeof (tb_json_t));
  data->array = array;
  return data;
}

/**
 * tb_json_array_length
 * @array: the json array
 * @returns: the length of the array
 *
 * returns the length of the array
 */
guint
tb_json_array_length (tb_json_array_t * array)
{
  g_return_val_if_fail (array != NULL, 0);
  return g_list_length (array->values);
}

/**
 * tb_json_array_remove
 * @array: the json array
 * @id: the position of the array
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Destroies the element id of the array
 */
gboolean
tb_json_array_remove (tb_json_array_t * array, guint id)
{
  tb_json_value_t *value;

  g_return_val_if_fail (array != NULL, FALSE);

  if (!(value = g_list_nth_data (array->values, id)))
    return FALSE;

  array->values = g_list_remove (array->values, value);
  tb_json_value_destroy_internal (value);
  g_free (value);
  return TRUE;
}

/**
 * tb_json_array_add
 * @array: the json array
 * @id: the location for the id, or NULL
 * @value: location for the new value, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Adds a new element of the array
 */
gboolean
tb_json_array_add (tb_json_array_t * array, guint * id,
		   tb_json_value_t ** value)
{
  tb_json_value_t *v;

  g_return_val_if_fail (array != NULL, FALSE);

  v = g_malloc0 (sizeof (tb_json_value_t));
  v->type = TB_JSON_VALUE_NULL;
  v->parent_array = array;

  array->values = g_list_append (array->values, v);

  if (id)
    *id = g_list_index (array->values, v);

  if (value)
    *value = v;

  return TRUE;
}

/**
 * tb_json_array_get:
 * @array: the json array
 * @id: the id of the array
 * @returns: the value of the array
 *
 * Gets the value of the array by id.
 */
tb_json_value_t *
tb_json_array_get (tb_json_array_t * array, guint id)
{
  g_return_val_if_fail (array != NULL, FALSE);

  return g_list_nth_data (array->values, id);
}

/**
 * tb_json_array_parent:
 * @array: the json array
 * @returns: the json value parent of this json array, or NULL
 *
 * returns the json value parent of this json array, or NULL
 **/
tb_json_value_t *
tb_json_array_parent (tb_json_array_t * array)
{
  g_return_val_if_fail (array != NULL, FALSE);

  return array->parent;
}

/* DUPLICATE *****************************************************************/

static gboolean tb_json_duplicate_node (tb_json_node_t * src,
					tb_json_node_t * dest);

static gboolean
tb_json_duplicate_object (tb_json_object_t * src, tb_json_object_t * dest)
{
  GList *nodes;

  for (nodes = tb_json_object_get_nodes (src); nodes; nodes = nodes->next)
    {
      tb_json_node_t *node_s, *node_d;
      gchar *string;

      node_s = nodes->data;
      string = tb_json_node_get_string (node_s);
      tb_json_object_add_node (dest, string, &node_d);

      if (tb_json_duplicate_node (node_s, node_d) == FALSE)
	return FALSE;
    }

  return TRUE;
}

static gboolean
tb_json_duplicate_array (tb_json_array_t * src, tb_json_array_t * dest)
{
  gint i, len;

  len = tb_json_array_length (src);

  for (i = 0; i < len; i++)
    {
      tb_json_value_t *v_s = tb_json_array_get (src, i);
      tb_json_value_t *v_d;

      tb_json_array_add (dest, NULL, &v_d);
      if (tb_json_value_duplicate_exists (v_s, v_d) == FALSE)
	return FALSE;
    }

  return TRUE;
}

static gboolean
tb_json_duplicate_node (tb_json_node_t * src, tb_json_node_t * dest)
{
  tb_json_value_t *v_s, *v_d;

  v_s = tb_json_node_get_value (src);
  v_d = tb_json_node_get_value (dest);

  return tb_json_value_duplicate_exists (v_s, v_d);
}

gboolean
tb_json_value_duplicate_exists (tb_json_value_t * src, tb_json_value_t * dest)
{
  switch (tb_json_value_get_type (src))
    {
    case TB_JSON_VALUE_STRING:
      {
	gchar *string;
	string = tb_json_value_get_string (src);
	tb_json_value_set_string (dest, string);
	break;
      }

    case TB_JSON_VALUE_NUMBER:
      tb_json_value_set_number (dest, tb_json_value_get_number (src));
      break;

    case TB_JSON_VALUE_OBJECT:
      {
	tb_json_object_t *o_s, *o_d;
	o_s = tb_json_value_get_object (src);
	tb_json_value_set_object_new (dest, &o_d);
	return tb_json_duplicate_object (o_s, o_d);
      }

    case TB_JSON_VALUE_ARRAY:
      {
	tb_json_array_t *a_s, *a_d;
	a_s = tb_json_value_get_array (src);
	tb_json_value_set_array_new (dest, &a_d);
	return tb_json_duplicate_array (a_s, a_d);
      }

    case TB_JSON_VALUE_BOOLEAN:
      tb_json_value_set_boolean (dest, tb_json_value_get_boolean (src));
      break;

    case TB_JSON_VALUE_NULL:
      tb_json_value_set_null (dest);
      break;
    }

  return TRUE;
}

/* EVALUATE *****************************************************************/

/**
 * tb_json_value_evaluate:
 * @value: the json value already allocated
 * @buffer: the json "string" to evaluate
 * @size: the size of the buffer or -1
 * @error: the location for the GError, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Evaluates a string and the value will be putted into the json value
 */
gboolean
tb_json_value_evaluate (tb_json_value_t * value, gchar * buffer, gssize size,
			GError ** error)
{
  gchar *tmp;
  GString *str;
  tb_json_t *data;
  tb_json_node_t *node;

  str = g_string_new ("{ \"value\" : ");

  if (size >= 0)
    g_string_append_len (str, buffer, size);
  else
    g_string_append (str, buffer);

  g_string_append (str, "}");
  tmp = g_string_free (str, FALSE);

  data = tb_json_new ();
  if (tb_json_load_from_buffer (data, tmp, -1, error) == FALSE)
    {
      g_free (tmp);
      return FALSE;
    }

  g_free (tmp);

  node = tb_json_object_get_node (tb_json_object (data), "value");

  if (tb_json_value_duplicate_exists (tb_json_node_get_value (node), value) == FALSE)
    {
      g_set_error (error, tb_json_error_quark (), TB_ERROR_JSON,
		   "Malformat string.");
      tb_json_destroy (data);
      return FALSE;
    }

  tb_json_destroy (data);
  return TRUE;
}

/* ERRORS **********************************************************/
/**
 * tb_json_error_quark:
 * @returns: The error domain of for the tb json
 *
 * The error domain for the tb json
 **/
GQuark
tb_json_error_quark (void)
{
  return g_quark_from_static_string ("thebox-json-error-quark");
}

/* EOF */
