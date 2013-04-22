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
#include "tb_jsonpath_internal.h"
#include "tb_jsonpath_function_internal.h"

#include "tb_jsonpath_scanner.h"

static void tb_jsonpath_free_match (tb_jsonpath_match_t * match);
static void tb_jsonpath_free_query (tb_jsonpath_query_t * query);
static void tb_jsonpath_free_script (tb_jsonpath_script_t * script);
static void tb_jsonpath_free_filter (tb_jsonpath_filter_t * filter);

/* VALIDATE ********************************************************************/

/**
 * tb_jsonpath_validate:
 * @jsonpath: a jsonpath query
 * @size: size of the query or -1
 * @error: the location for a GError, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Validates a JSONPath query
 **/
gboolean
tb_jsonpath_validate (gchar * jsonpath, gssize size, GError ** error)
{
  tb_jsonpath_item_t *jp;

  g_return_val_if_fail (jsonpath != NULL, FALSE);

  if (tb_jsonpath_parser (jsonpath, size, &jp, error) == FALSE)
    return FALSE;

  tb_jsonpath_free (jp);
  return TRUE;
}

/* FREE *******************************************************************/
void
tb_jsonpath_free (tb_jsonpath_item_t * item)
{
  if (!item)
    return;

  if (item->filter)
    tb_jsonpath_free_filter (item->filter);

  if (item->script)
    tb_jsonpath_free_script (item->script);

  if (item->query)
    tb_jsonpath_free_query (item->query);

  if (item->value)
    json_node_free (item->value);

  g_free (item);
}

static void
tb_jsonpath_free_match (tb_jsonpath_match_t * match)
{
  if (!match)
    return;

  switch (match->type)
    {
    case TB_JSONPATH_MATCH_TYPE_NODE:
      if (match->node.node)
	g_free (match->node.node);
      break;

    case TB_JSONPATH_MATCH_TYPE_CONDITION:
      if (match->condition.start)
	tb_jsonpath_free (match->condition.start);

      if (match->condition.end)
	tb_jsonpath_free (match->condition.end);

      if (match->condition.step)
	tb_jsonpath_free (match->condition.step);

      break;
    }

  g_free (match);
}

static void
tb_jsonpath_free_filter (tb_jsonpath_filter_t * filter)
{
  if (!filter)
    return;

  if (filter->first)
    tb_jsonpath_free (filter->first);

  if (filter->second)
    tb_jsonpath_free (filter->second);

  g_free (filter);
}

static void
tb_jsonpath_free_query (tb_jsonpath_query_t * query)
{
  if (!query)
    return;

  if (query->matches)
    {
      g_list_foreach (query->matches, (GFunc) tb_jsonpath_free_match, NULL);
      g_list_free (query->matches);
    }

  g_free (query);
}

static void
tb_jsonpath_free_script (tb_jsonpath_script_t * script)
{
  if (!script)
    return;

  if (script->name)
    g_free (script->name);

  if (script->items)
    {
      g_list_foreach (script->items, (GFunc) tb_jsonpath_free, NULL);
      g_list_free (script->items);
    }

  g_free (script);
}

/* PARSER ********************************************************************/

static gboolean tb_jsonpath_parser_item (tb_jsonpath_scanner_t * scanner,
					 tb_jsonpath_item_t ** item,
					 gboolean operation, GError ** error);
static gboolean tb_jsonpath_parser_query (tb_jsonpath_scanner_t * scanner,
					  tb_jsonpath_query_t ** jp,
					  GError ** error);
static gboolean tb_jsonpath_parser_condition (tb_jsonpath_scanner_t * scanner,
					      tb_jsonpath_match_t **
					      match_ret, GError ** error);
static gboolean tb_jsonpath_parser_filter (tb_jsonpath_scanner_t * scanner,
					   tb_jsonpath_filter_t ** filter,
					   GError ** error);
static gboolean tb_jsonpath_parser_script (tb_jsonpath_scanner_t * scanner,
					   tb_jsonpath_script_t ** script,
					   GError ** error);

/* This function parses a string: */
gboolean
tb_jsonpath_parser (gchar * jsonpath, gssize size,
		    tb_jsonpath_item_t ** item_ret, GError ** error)
{
  tb_jsonpath_item_t *item;
  tb_jsonpath_scanner_t *scanner;

  /* It uses our scanner parser: */
  scanner = tb_jsonpath_scanner_new (error);
  tb_jsonpath_scanner_input_text (scanner, jsonpath, size);
  tb_jsonpath_scanner_set_qname (scanner, FALSE);

  tb_jsonpath_scanner_get_next_token (scanner);

  /* Parse the item. An item is something like @.aa[conditions/filters/...] */
  if (tb_jsonpath_parser_item (scanner, &item, FALSE, error) == FALSE)
    {
      tb_jsonpath_scanner_destroy (scanner);
      return FALSE;
    }

  /* End of the string: */
  if (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_EOF)
    {
      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_EOF, NULL);
      tb_jsonpath_scanner_destroy (scanner);
      tb_jsonpath_free (item);
      return FALSE;
    }

  tb_jsonpath_scanner_destroy (scanner);
  *item_ret = item;
  return TRUE;
}

/* This function parse an item: */
static gboolean
tb_jsonpath_parser_item (tb_jsonpath_scanner_t * scanner,
			 tb_jsonpath_item_t ** item_ret, gboolean operation,
			 GError ** error)
{
  tb_jsonpath_item_t *item;

  item = g_malloc0 (sizeof (tb_jsonpath_item_t));

  /* A @ or $ stuff: */
  if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_DOLLAR
      || tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_AT)
    {
      if (tb_jsonpath_parser_query (scanner, &item->query, error) == FALSE)
	{
	  tb_jsonpath_free (item);
	  return FALSE;
	}
    }

  /* Filter: */
  else if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_LEFT_PAREN)
    {
      if (tb_jsonpath_parser_filter (scanner, &item->filter, error) == FALSE)
	{
	  tb_jsonpath_free (item);
	  return FALSE;
	}
    }

  /* Script: */
  else if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_IDENTIFIER
	   && g_str_has_prefix (tb_jsonpath_scanner_cur_value_identifier (scanner),
				"?") == TRUE)
    {
      if (tb_jsonpath_parser_script (scanner, &item->script, error) == FALSE)
	{
	  tb_jsonpath_free (item);
	  return FALSE;
	}
    }

  /* Value: */
  else if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_STRING)
    {
      item->value = json_node_new (JSON_NODE_VALUE);

      json_node_set_string (item->value,
				tb_jsonpath_scanner_cur_value_string (scanner));
      tb_jsonpath_scanner_get_next_token (scanner);
    }

  else if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_NUMBER)
    {
      item->value = json_node_new (JSON_NODE_VALUE);

      json_node_set_double (item->value,
				tb_jsonpath_scanner_cur_value_number (scanner));
      tb_jsonpath_scanner_get_next_token (scanner);
    }

  /* Operation: */
  else if (operation == TRUE
	   && tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_IDENTIFIER)
    {
      if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "+"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_ADD;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "-"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_SUB;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "="))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_EQ;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "*"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_MUL;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "/"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_DIV;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), ">"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_GT;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), ">="))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_GE;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "<"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_LT;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "<="))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_LE;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "!="))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_NE;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "&&"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_AND;
	}

      else
	if (!g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "||"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_OR;
	}
      else
	{
	  tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_IDENTIFIER,
				  "Valid jsonpath item or operation");
	  tb_jsonpath_free (item);
	  return FALSE;
	}

      tb_jsonpath_scanner_get_next_token (scanner);
    }
  else
    {
      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_IDENTIFIER,
			      "Valid jsonpath item");
      tb_jsonpath_free (item);
      return FALSE;
    }

  *item_ret = item;
  return TRUE;
}

/* This function parse an item: */
static gboolean
tb_jsonpath_parser_query (tb_jsonpath_scanner_t * scanner,
			  tb_jsonpath_query_t ** query_ret, GError ** error)
{
  tb_jsonpath_query_t *query;
  gboolean dot = FALSE;

  query = g_malloc0 (sizeof (tb_jsonpath_query_t));

  /* How does it start: */
  if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_DOLLAR)
    query->root = TRUE;
  else if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_AT)
    query->root = FALSE;

  else
    {
      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_IDENTIFIER, "$ or @");
      tb_jsonpath_free_query (query);
      return FALSE;
    }

  tb_jsonpath_scanner_get_next_token (scanner);

  /* List of nodes/conditions: */
  while (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_EOF)
    {
      gboolean recursive = FALSE;

      /* Node: */
      if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_IDENTIFIER)
	{
	  gchar **split;
	  gint i;

	  split =
	    g_strsplit (tb_jsonpath_scanner_cur_value_identifier (scanner), ".", -1);

	  /* If the first is not '.': */
	  if (*split[0])
	    {
	      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_IDENTIFIER,
				      ".node name");
	      g_strfreev (split);
	      return FALSE;
	    }

	  /* List of the nodes: */
	  for (i = 1; split[i]; i++)
	    {
	      tb_jsonpath_match_t *match;

	      /* It was a '.' : */
	      if (!*split[i])
		{
		  if (recursive == TRUE)
		    {
		      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_IDENTIFIER,
					      ".node name");
		      return FALSE;
		    }

		  recursive = TRUE;
		  continue;
		}

	      /* Alloc: */
	      match = g_malloc0 (sizeof (tb_jsonpath_match_t));
	      match->type = TB_JSONPATH_MATCH_TYPE_NODE;

	      match->node.recursive = recursive;
	      recursive = FALSE;

	      if (g_utf8_collate (split[i], "*"))
		match->node.node = g_strdup (split[i]);

	      /* Add to the jsonpath data struct: */
	      query->matches = g_list_append (query->matches, match);
	    }

	  /* The last is a '.' or not? */
	  if (!*split[i - 1])
	    dot = TRUE;

	  g_strfreev (split);
	}

      /* Node into braces: $.["test"] or Condition: */
      else if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_LEFT_BRACE)
	{
	  /* Node in braces: */
	  if (dot == TRUE)
	    {
	      tb_jsonpath_match_t *match;

	      if (tb_jsonpath_scanner_get_next_token (scanner) != TB_JSONPATH_SCANNER_STRING)
		{
		  tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_STRING, NULL);
		  tb_jsonpath_free_query (query);
		  return FALSE;
		}

	      match = g_malloc0 (sizeof (tb_jsonpath_match_t));
	      match->type = TB_JSONPATH_MATCH_TYPE_NODE;

	      match->node.recursive = recursive;
	      recursive = FALSE;

	      match->node.node =
		g_strdup (tb_jsonpath_scanner_cur_value_string (scanner));

	      query->matches = g_list_append (query->matches, match);

	      /* After the "string" I look for a ']': */
	      if (tb_jsonpath_scanner_get_next_token (scanner) !=
		  TB_JSONPATH_SCANNER_RIGHT_BRACE)
		{
		  tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_RIGHT_BRACE,
					  NULL);
		  tb_jsonpath_free_query (query);
		  return FALSE;
		}
	    }

	  /* Condition: */
	  else
	    {
	      tb_jsonpath_match_t *match;

	      if (tb_jsonpath_parser_condition (scanner, &match, error) ==
		  FALSE)
		{
		  tb_jsonpath_free_query (query);
		  return FALSE;
		}

	      if (match)
		query->matches = g_list_append (query->matches, match);
	    }
	}

      /* UnExp.. returns */
      else
	break;

      tb_jsonpath_scanner_get_next_token (scanner);
    }

  *query_ret = query;
  return TRUE;
}

/* Condition parser: */
static gboolean
tb_jsonpath_parser_condition (tb_jsonpath_scanner_t * scanner,
			      tb_jsonpath_match_t ** match_ret,
			      GError ** error)
{
  tb_jsonpath_match_t *match;
  gint i;

  tb_jsonpath_scanner_get_next_token (scanner);

  /* * */
  if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_IDENTIFIER
      && !g_utf8_collate (tb_jsonpath_scanner_cur_value_identifier (scanner), "*"))
    {
      if (tb_jsonpath_scanner_get_next_token (scanner) != TB_JSONPATH_SCANNER_RIGHT_BRACE)
	{
	  tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_RIGHT_BRACE, NULL);
	  return FALSE;
	}

      match = g_malloc0 (sizeof (tb_jsonpath_match_t));
      match->type = TB_JSONPATH_MATCH_TYPE_CONDITION;

      match->condition.start = g_malloc0 (sizeof (tb_jsonpath_item_t));
      match->condition.start->value = json_node_new (JSON_NODE_VALUE);
      json_node_set_double (match->condition.start->value, 0);

      match->condition.end = g_malloc0 (sizeof (tb_jsonpath_item_t));
      match->condition.end->value = json_node_new (JSON_NODE_VALUE);
      json_node_set_double (match->condition.end->value, -1);

      *match_ret = match;
      return TRUE;
    }

  /* Normal slice: */
  match = g_malloc0 (sizeof (tb_jsonpath_match_t));
  match->type = TB_JSONPATH_MATCH_TYPE_CONDITION;

  /* The 3 elements of the slice (start:stop:step) */
  for (i = 0; i < 3; i++)
    {
      tb_jsonpath_item_t *item = NULL;

      if (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_COLON
	  && tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_RIGHT_BRACE)
	{
	  if (tb_jsonpath_parser_item (scanner, &item, FALSE, error) == FALSE)
	    {
	      tb_jsonpath_free_match (match);
	      return FALSE;
	    }
	}

      switch (i)
	{
	case 0:
	  match->condition.start = item;
	  break;

	case 1:
	  match->condition.end = item;
	  break;

	case 2:
	  match->condition.step = item;
	  break;
	}

      if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_RIGHT_BRACE)
	{
	  *match_ret = match;
	  return TRUE;
	}

      if (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_COLON)
	{
	  tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_IDENTIFIER, "] or :");
	  tb_jsonpath_free_match (match);
	  return FALSE;
	}

      tb_jsonpath_scanner_get_next_token (scanner);
    }

  if (tb_jsonpath_scanner_get_next_token (scanner) == TB_JSONPATH_SCANNER_RIGHT_BRACE)
    {
      *match_ret = match;
      return TRUE;
    }

  tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_RIGHT_BRACE, NULL);
  tb_jsonpath_free_match (match);
  return FALSE;
}

/* This function parse a script really: */
static gboolean
tb_jsonpath_parser_script (tb_jsonpath_scanner_t * scanner,
			   tb_jsonpath_script_t ** script_ret,
			   GError ** error)
{
  tb_jsonpath_script_t *script;

  script = g_malloc0 (sizeof (tb_jsonpath_script_t));

  /* the name: */
  if (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_IDENTIFIER)
    {
      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_IDENTIFIER, NULL);
      tb_jsonpath_free_script (script);
      return FALSE;
    }

  script->name = g_strdup (tb_jsonpath_scanner_cur_value_identifier (scanner) + 1);

  /* '(' */
  if (tb_jsonpath_scanner_get_next_token (scanner) != TB_JSONPATH_SCANNER_LEFT_PAREN)
    {
      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_LEFT_PAREN, NULL);
      tb_jsonpath_free_script (script);
      return FALSE;
    }

  tb_jsonpath_scanner_get_next_token (scanner);

  /* List of arguments: */
  while (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_RIGHT_PAREN
	 && tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_EOF)
    {
      tb_jsonpath_item_t *item;

      if (tb_jsonpath_parser_item (scanner, &item, FALSE, error) == FALSE)
	{
	  tb_jsonpath_free_script (script);
	  return FALSE;
	}

      script->items = g_list_append (script->items, item);

      if (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_RIGHT_PAREN)
	break;

      /* Comma between the arguments: */
      if (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_COMMA)
	{
	  tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_COMMA, NULL);
	  tb_jsonpath_free_script (script);
	  return FALSE;
	}

      tb_jsonpath_scanner_get_next_token (scanner);
    }

  /* ')' */
  if (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_RIGHT_PAREN)
    {
      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_RIGHT_PAREN, NULL);
      tb_jsonpath_free_script (script);
      return FALSE;
    }

  tb_jsonpath_scanner_get_next_token (scanner);
  *script_ret = script;
  return TRUE;
}

static gboolean tb_jsonpath_parser_filter_sort (tb_jsonpath_scanner_t * scanner,
						GList ** list,
						tb_jsonpath_filter_t **
						filter, GError ** error);

/* This function parses a filter: */
static gboolean
tb_jsonpath_parser_filter (tb_jsonpath_scanner_t * scanner,
			   tb_jsonpath_filter_t ** filter, GError ** error)
{
  GList *list = NULL;
  gboolean ret;

  tb_jsonpath_scanner_get_next_token (scanner);

  /* Untill the filter exists: */
  while (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_EOF
	 && tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_RIGHT_PAREN)
    {
      tb_jsonpath_item_t *item;

      if (tb_jsonpath_parser_item (scanner, &item, TRUE, error) == FALSE)
	{
	  if (list)
	    {
	      g_list_foreach (list, (GFunc) tb_jsonpath_free, NULL);
	      g_list_free (list);
	    }

	  return FALSE;
	}

      list = g_list_append (list, item);
    }

  /* The ')' of the filter: */
  if (tb_jsonpath_scanner_cur_token (scanner) != TB_JSONPATH_SCANNER_RIGHT_PAREN)
    {
      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_RIGHT_PAREN, NULL);

      if (list)
	{
	  g_list_foreach (list, (GFunc) tb_jsonpath_free, NULL);
	  g_list_free (list);
	}
      return FALSE;
    }

  tb_jsonpath_scanner_get_next_token (scanner);

  /* No elements inside? */
  if (!list)
    {
      tb_jsonpath_scanner_unexp_token (scanner, TB_JSONPATH_SCANNER_IDENTIFIER,
			      "a filter element");
      return FALSE;
    }

  /* Create a single filter: */
  ret = tb_jsonpath_parser_filter_sort (scanner, &list, filter, error);

  if (list)
    {
      g_list_foreach (list, (GFunc) tb_jsonpath_free, NULL);
      g_list_free (list);
    }

  return ret;
}

static gint
tb_jsonpath_parser_filter_compare (tb_jsonpath_operation_type_t a,
				   tb_jsonpath_operation_type_t b);

static gboolean
tb_jsonpath_parser_filter_sort (tb_jsonpath_scanner_t * scanner, GList ** items,
				tb_jsonpath_filter_t ** filter,
				GError ** error)
{
  tb_jsonpath_item_t *item;
  gboolean operation = FALSE;
  GList *list;

  /* Checking the order: item operator item operator... */
  for (list = *items; list; list = list->next)
    {
      item = list->data;

      if (operation == FALSE && item->is_operation)
	{
          if (tb_jsonpath_scanner_error (scanner) != NULL && *tb_jsonpath_scanner_error (scanner) == NULL)
	    g_set_error (tb_jsonpath_scanner_error (scanner), tb_jsonpath_error_quark (),
		       TB_ERROR_JSONPATH,
		       "Error in the filter (Line: %d, Position: %d).",
		       tb_jsonpath_scanner_get_cur_line (scanner),
		       tb_jsonpath_scanner_get_cur_position (scanner));
	  return FALSE;
	}

      else if (operation == TRUE && (!item->is_operation || !list->next))
	{
          if (tb_jsonpath_scanner_error (scanner) != NULL && *tb_jsonpath_scanner_error (scanner) == NULL)
	    g_set_error (tb_jsonpath_scanner_error (scanner), tb_jsonpath_error_quark (),
		       TB_ERROR_JSONPATH,
		       "Error in the filter (Line: %d, Position: %d).",
		       tb_jsonpath_scanner_get_cur_line (scanner),
		       tb_jsonpath_scanner_get_cur_position (scanner));
	  return FALSE;
	}

      operation = !operation;
    }

  /* Only an element: */
  if (g_list_length (*items) == 1)
    {
      *filter = g_malloc0 (sizeof (tb_jsonpath_filter_t));
      (*filter)->first = (*items)->data;
      (*items)->data = NULL;
      return TRUE;
    }

  /* Until there is only 1 filter: */
  while ((*items)->next)
    {
      GList *primary = NULL;
      tb_jsonpath_item_t *first, *second;

      /* Check major important operation (the operations are between the
       * operator: X - X + X > X): */
      for (list = (*items)->next; list; list = list->next->next)
	{
	  item = list->data;

	  if (!primary
	      || tb_jsonpath_parser_filter_compare (item->operation,
						    ((tb_jsonpath_item_t *)
						     primary->
						     data)->operation) == -1)
	    primary = list;
	}

      /* A * B - C 
       *   ^
       * now I want: FILTER(A *B) - C:
       */
      first = primary->prev->data;
      *items = g_list_remove (*items, first);

      second = primary->next->data;
      *items = g_list_remove (*items, second);

      item = primary->data;

      /* I create the new filter: */
      item->filter = g_malloc0 (sizeof (tb_jsonpath_filter_t));

      item->filter->first = first;
      item->filter->second = second;
      item->filter->operation = item->operation;
    }

  /* Return the new filter: */
  item = (*items)->data;
  *filter = item->filter;
  item->filter = NULL;

  return TRUE;
}

/* This function resolves the operation major important: */
static gint
tb_jsonpath_parser_filter_compare (tb_jsonpath_operation_type_t a,
				   tb_jsonpath_operation_type_t b)
{
  if (a < b)
    return -1;

  return 1;
}

/* EXEC ******************************************************************/
static gboolean tb_jsonpath_exec_query (tb_jsonpath_item_t * item,
					JsonObject * parent,
					JsonObject * object,
					tb_jsonpath_query_t * query,
					tb_jsonpath_result_t ** result,
					GError ** error);
static gboolean tb_jsonpath_exec_filter (tb_jsonpath_item_t * item,
					 JsonObject * parent,
					 JsonObject * object,
					 tb_jsonpath_filter_t * filter,
					 tb_jsonpath_result_t ** result,
					 GError ** error);
static gboolean tb_jsonpath_exec_script (tb_jsonpath_item_t * item,
					 JsonObject * parent,
					 JsonObject * object,
					 tb_jsonpath_script_t * script,
					 tb_jsonpath_result_t ** result,
					 GError ** error);

static gboolean tb_jsonpath_exec_query_real (tb_jsonpath_item_t * item,
					     JsonObject * parent,
					     JsonObject * object,
					     tb_jsonpath_query_t * query,
					     GList ** result,
					     GError ** error);
static gboolean tb_jsonpath_exec_query_node_rec (tb_jsonpath_item_t * item,
						 JsonObject * parent,
						 JsonObject * object,
						 tb_jsonpath_query_t * query,
						 gchar * node,
						 GList ** results,
						 GError ** error);
static gboolean tb_jsonpath_exec_query_node_rec_array (tb_jsonpath_item_t *
						       item,
						       JsonObject *
						       parent,
						       JsonArray *
						       array,
						       tb_jsonpath_query_t *
						       query, gchar * node,
						       GList ** results,
						       GError ** error);
static gboolean tb_jsonpath_exec_query_node (tb_jsonpath_item_t * item,
					     JsonObject * parent,
					     JsonObject * object,
					     tb_jsonpath_query_t * query,
					     JsonNode * node,
					     GList ** results,
					     GError ** error);
static gboolean tb_jsonpath_exec_query_value (tb_jsonpath_item_t * item,
					      JsonObject * parent,
					      JsonObject * object,
					      tb_jsonpath_query_t * query,
					      JsonNode * value,
					      GList ** result,
					      GError ** error);
static gboolean tb_jsonpath_exec_query_value_node (tb_jsonpath_item_t * item,
						   JsonObject * parent,
						   JsonObject * object,
						   tb_jsonpath_query_t *
						   query,
						   JsonNode * value,
						   GList ** result,
						   GError ** error);
static gboolean tb_jsonpath_exec_query_condition (tb_jsonpath_item_t * item,
						  JsonObject * parent,
						  JsonObject * object,
						  tb_jsonpath_query_t * query,
						  JsonNode * value,
						  GList ** result,
						  GError ** error);
static gboolean tb_jsonpath_exec_query_condition_slice (tb_jsonpath_item_t *
							item,
							JsonObject * parent,
							JsonObject * object,
							tb_jsonpath_query_t *
							query,
							JsonArray *
							array,
							tb_jsonpath_result_t *
							start,
							tb_jsonpath_result_t *
							end,
							tb_jsonpath_result_t *
							step, GList ** result,
							GError ** error);
static gboolean tb_jsonpath_exec_query_condition_generic (tb_jsonpath_item_t *
							  item,
							  JsonObject *
							  parent,
							  JsonObject *
							  object,
							  tb_jsonpath_query_t
							  * query,
							  JsonNode *
							  value,
							  tb_jsonpath_result_t
							  * start,
							  tb_jsonpath_result_t
							  * end,
							  tb_jsonpath_result_t
							  * step,
							  GList ** result,
							  GError ** error);

/**
 * tb_jsonpath_exec:
 * @jsonpath: a jsonpath query
 * @size: size of the query or -1
 * @object: the JSON Object
 * @result: the location for a JSONPath result
 * @functions: the functions, or NULL
 * @error: the location for a GError, or NULL
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * Execs a query to a JSON Object and returns an array of results
 **/
gboolean
tb_jsonpath_exec (gchar * jsonpath, gssize size, JsonObject * object,
		  tb_jsonpath_result_t ** result,
		  tb_jsonpath_functions_t * functions, GError ** error)
{
  tb_jsonpath_item_t *item;
  gboolean ret;

  g_return_val_if_fail (jsonpath != NULL, FALSE);
  g_return_val_if_fail (object != NULL, FALSE);
  g_return_val_if_fail (result != NULL, FALSE);

  if (tb_jsonpath_parser (jsonpath, size, &item, error) == FALSE)
    return FALSE;

  if (functions
      && tb_jsonpath_set_functions (item, functions, error) == FALSE)
    {
      tb_jsonpath_free (item);
      return FALSE;
    }

  ret = tb_jsonpath_exec_real (item, object, object, result, error);
  tb_jsonpath_free (item);
  return ret;
}

gboolean
tb_jsonpath_exec_real (tb_jsonpath_item_t * item, JsonObject * parent,
		       JsonObject * object,
		       tb_jsonpath_result_t ** result, GError ** error)
{
  /* Exec a filter: */
  if (item->filter)
    {
      if (tb_jsonpath_exec_filter
	  (item, object, object, item->filter, result, error) == FALSE)
	return FALSE;
    }

  /* Exec a script: */
  else if (item->script)
    {
      if (tb_jsonpath_exec_script
	  (item, object, object, item->script, result, error) == FALSE)
	return FALSE;
    }

  /* A query: */
  else if (item->query)
    {
      if (tb_jsonpath_exec_query
	  (item, object, object, item->query, result, error) == FALSE)
	return FALSE;
    }

  /* A single value: */
  else if (item->value)
    {
      JsonNode *value = json_node_copy (item->value);

      *result = g_malloc0 (sizeof (tb_jsonpath_result_t));
      (*result)->values = g_list_append (NULL, value);
    }

  return TRUE;
}

/* This function exec a normal query and return a result data struct: */
static gboolean
tb_jsonpath_exec_query (tb_jsonpath_item_t * item, JsonObject * parent,
			JsonObject * object,
			tb_jsonpath_query_t * query,
			tb_jsonpath_result_t ** result, GError ** error)
{
  *result = g_malloc0 (sizeof (tb_jsonpath_result_t));

  if (tb_jsonpath_exec_query_real
      (item, parent, object, query, &(*result)->values, error) == FALSE)
    {
      g_free (*result);
      return FALSE;
    }

  if ((*result)->values == NULL)
    {
      g_free (*result);
      *result = NULL;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_query_real (tb_jsonpath_item_t * item,
			     JsonObject * parent,
			     JsonObject * object,
			     tb_jsonpath_query_t * query, GList ** result,
			     GError ** error)
{
  tb_jsonpath_match_t *match;

  if (!query->matches)
    return TRUE;

  match = query->matches->data;

  switch (match->type)
    {
    case TB_JSONPATH_MATCH_TYPE_NODE:
      /* If the match is recursive: */
      if (match->node.recursive == TRUE && match->node.node)
	return tb_jsonpath_exec_query_node_rec (item, parent, object, query,
						match->node.node, result,
						error);

      /* If the match is normal node: */
      if (match->node.node
	  && json_object_has_member (object, match->node.node) == TRUE
	  && json_node_get_node_type (json_object_get_member (object, match->node.node)) != JSON_NODE_NULL) /* need to check more on array values ? */
	return tb_jsonpath_exec_query_node (item, parent, object, query,
					    json_object_get_member (object,
								     match->node.node),
					    result, error);

      /* If the match is a '*': */
      if (!match->node.node)
	{
	  GList *list, *l;
	  GList *matches;

	  /* A copy of the list: */
	  matches = g_list_copy (query->matches);
	  query->matches = g_list_remove (query->matches, match);

          list = json_object_get_values (object);

	  for (l = list; l; l = l->next)
	    {
	      if (tb_jsonpath_exec_query_value
		  (item, parent, object, query,
		   l->data, result,
		   error) == FALSE)
                {
                  g_list_free (list);
		  return FALSE;
                }
	    }
          g_list_free (list);

	  g_list_free (query->matches);
	  query->matches = matches;

	  return TRUE;
	}

      /* No other options: */
      return TRUE;

    default:
      {
	GList *list, *l;

	/* A copy of the list: */
        list = json_object_get_values (object);

	for (l = list; l; l = l->next)
	  {
	    if (tb_jsonpath_exec_query_value
		(item, parent, object, query,
		 l->data, result, error) == FALSE)
              {
                g_list_free (list);
	        return FALSE;
              }
	  }
        g_list_free (list);

	return TRUE;
      }
      break;
    }

  return TRUE;
}

/* Recurive node search: */
static gboolean
tb_jsonpath_exec_query_node_rec (tb_jsonpath_item_t * item,
				 JsonObject * parent,
				 JsonObject * object,
				 tb_jsonpath_query_t * query, gchar * node,
				 GList ** result, GError ** error)
{
  GList *list, *l;

  if (json_object_has_member (object, node) == TRUE
      && json_node_get_node_type (json_object_get_member (object, node)) != JSON_NODE_NULL
      && tb_jsonpath_exec_query_node (item, parent, object, query,
				      json_object_get_member (object, node),
				      result, error) == FALSE)
    return FALSE;

  list = json_object_get_values (object);

  for (l = list; l; l = l->next)
    {
      JsonNode *value = l->data;

      if (json_node_get_node_type (value) == JSON_NODE_OBJECT
	  && tb_jsonpath_exec_query_node_rec (item, parent,
					      json_node_get_object (value),
					      query, node, result,
					      error) == FALSE)
        {
          g_list_free (list);
	  return FALSE;
        }

      if (json_node_get_node_type (value) == JSON_NODE_ARRAY
	  && tb_jsonpath_exec_query_node_rec_array (item, parent,
						    json_node_get_array (value),
						    query, node,
						    result, error) == FALSE)
        {
          g_list_free (list);
	  return FALSE;
        }
    }
  g_list_free (list);

  return TRUE;
}

/* Recursive node search into array: */
static gboolean
tb_jsonpath_exec_query_node_rec_array (tb_jsonpath_item_t * item,
				       JsonObject * parent,
				       JsonArray * array,
				       tb_jsonpath_query_t * query,
				       gchar * node, GList ** result,
				       GError ** error)
{
  GList *list, *l;

  list = json_array_get_elements (array);

  for (l = list; l; l = l->next)
    {
      JsonNode *value = l->data;

      if (json_node_get_node_type (value) == JSON_NODE_OBJECT
	  && tb_jsonpath_exec_query_node_rec (item, parent,
					      json_node_get_object (value),
					      query, node, result,
					      error) == FALSE)
        {
          g_list_free (list);
	  return FALSE;
        }

      if (json_node_get_node_type (value) == JSON_NODE_ARRAY
	  && tb_jsonpath_exec_query_node_rec_array (item, parent,
						    json_node_get_array (value),
						    query, node,
						    result, error) == FALSE)
        {
          g_list_free (list);
	  return FALSE;
        }
    }
  g_list_free (list);

  return TRUE;
}

/* Match a single node: */
static gboolean
tb_jsonpath_exec_query_node (tb_jsonpath_item_t * item,
			     JsonObject * parent,
			     JsonObject * object,
			     tb_jsonpath_query_t * query,
			     JsonNode * node, GList ** result,
			     GError ** error)
{
  tb_jsonpath_match_t *match;
  GList *matches;
  gchar *string;
  gboolean ret;

  string = (gchar *) json_node_get_string (node);

  matches = g_list_copy (query->matches);
  match = query->matches->data;

  if (match->type == TB_JSONPATH_MATCH_TYPE_NODE
      && !g_utf8_collate (match->node.node, string))
    query->matches = g_list_remove (query->matches, match);

  ret =
    tb_jsonpath_exec_query_value (item, parent, object, query,
				  node, result,
				  error);

  g_list_free (query->matches);
  query->matches = matches;

  return ret;
}

/* Check a single value: */
static gboolean
tb_jsonpath_exec_query_value (tb_jsonpath_item_t * item,
			      JsonObject * parent,
			      JsonObject * object,
			      tb_jsonpath_query_t * query,
			      JsonNode * value, GList ** result,
			      GError ** error)
{
  tb_jsonpath_match_t *match;

  if (!query->matches)
    {
      JsonNode *new = json_node_copy (value);

      *result = g_list_append (*result, new);
      return TRUE;
    }

  match = query->matches->data;

  switch (match->type)
    {
    case TB_JSONPATH_MATCH_TYPE_NODE:
      return tb_jsonpath_exec_query_value_node (item, parent, object, query,
						value, result, error);

    case TB_JSONPATH_MATCH_TYPE_CONDITION:
      return tb_jsonpath_exec_query_condition (item, parent, object, query,
					       value, result, error);
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_query_value_node (tb_jsonpath_item_t * item,
				   JsonObject * parent,
				   JsonObject * object,
				   tb_jsonpath_query_t * query,
				   JsonNode * value, GList ** result,
				   GError ** error)
{
  if (json_node_get_node_type (value) == JSON_NODE_OBJECT)
    return tb_jsonpath_exec_query_real (item, parent,
					json_node_get_object (value),
					query, result, error);

  if (json_node_get_node_type (value) == JSON_NODE_ARRAY)
    {
      JsonArray *array = json_node_get_array (value);

      GList *list, *l;

      list = json_array_get_elements (array);

      for (l = list; l; l = l->next)
        {
          JsonNode *value = l->data;

	  if (tb_jsonpath_exec_query_value
	      (item, parent, object, query, value, result, error) == FALSE)
            {
              g_list_free (list);
	      return FALSE;
            }
	}
      g_list_free (list);
    }

  return TRUE;
}

/* Exec the condition: */
static gboolean
tb_jsonpath_exec_query_condition (tb_jsonpath_item_t * item,
				  JsonObject * parent,
				  JsonObject * object,
				  tb_jsonpath_query_t * query,
				  JsonNode * value, GList ** result,
				  GError ** error)
{
  GList *matches;
  gboolean ret = TRUE;
  tb_jsonpath_match_t *match;
  tb_jsonpath_result_t *start, *end, *step;

  matches = g_list_copy (query->matches);

  match = query->matches->data;
  query->matches = g_list_remove (query->matches, match);

  start = end = step = NULL;

  if (match->condition.start
      && tb_jsonpath_exec_real (match->condition.start, parent, object,
				&start, error) == FALSE)
    return FALSE;

  if (match->condition.end
      && tb_jsonpath_exec_real (match->condition.end, parent, object, &end,
				error) == FALSE)
    {
      tb_jsonpath_result_free (start);
      return FALSE;
    }

  if (match->condition.step
      && tb_jsonpath_exec_real (match->condition.step, parent, object, &step,
				error) == FALSE)
    {
      tb_jsonpath_result_free (start);
      tb_jsonpath_result_free (end);
      return FALSE;
    }

  if (json_node_get_node_type (value) == JSON_NODE_ARRAY)
    ret =
      tb_jsonpath_exec_query_condition_slice (item, parent, object, query,
					      json_node_get_array (value),
					      start, end, step, result,
					      error);

  else
    ret =
      tb_jsonpath_exec_query_condition_generic (item, parent, object, query,
						value, start, end, step,
						result, error);


  tb_jsonpath_result_free (start);
  tb_jsonpath_result_free (end);
  tb_jsonpath_result_free (step);

  g_list_free (query->matches);
  query->matches = matches;
  return ret;
}

/* Condition as slice into array: */
static gboolean
tb_jsonpath_exec_query_condition_slice (tb_jsonpath_item_t * item,
					JsonObject * parent,
					JsonObject * object,
					tb_jsonpath_query_t * query,
					JsonArray * array,
					tb_jsonpath_result_t * rstart,
					tb_jsonpath_result_t * rend,
					tb_jsonpath_result_t * rstep,
					GList ** result, GError ** error)
{
  JsonNode *vstart, *vend, *vstep;
  gint start, end, step;
  gint len;

  if (!rstart || tb_jsonpath_result_next (rstart, &vstart) == FALSE)
    return TRUE;

  /* TODO - we allow array indexes to be double and integers - should this be just G_TYPE_DOUBLE ? */

  GType vstart_type = json_node_get_value_type (vstart);
  if (vstart_type != G_TYPE_UINT && vstart_type != G_TYPE_INT64 &&
      vstart_type != G_TYPE_INT && vstart_type != G_TYPE_FLOAT &&
      vstart_type != G_TYPE_DOUBLE)
    return TRUE;

  len = (gint) json_array_get_length (array);
  start = (gint) (json_node_get_value_type (rstart->values->data) == G_TYPE_DOUBLE || json_node_get_value_type (rstart->values->data) == G_TYPE_FLOAT) ? 
		json_node_get_double (rstart->values->data) : 
		json_node_get_int (rstart->values->data) ;

  if (start < 0)
    start = len + start;

  if (rend && tb_jsonpath_result_next (rend, &vend) == TRUE
      && (json_node_get_value_type (vend) == G_TYPE_UINT ||
	  json_node_get_value_type (vend) == G_TYPE_INT64 ||
	  json_node_get_value_type (vend) == G_TYPE_INT ||
	  json_node_get_value_type (vend) == G_TYPE_FLOAT ||
	  json_node_get_value_type (vend) == G_TYPE_DOUBLE ))
    {
      end = (gint) (json_node_get_value_type (vend) == G_TYPE_DOUBLE || json_node_get_value_type (vend) == G_TYPE_FLOAT) ? 
                json_node_get_double (vend) : 
                json_node_get_int (vend) ;

      if (end < 0)
	end = len + end;

      end++;

      if (len < end || end >= start)
	end = len;
    }
  else
    end = start + 1;


  if (rstep && tb_jsonpath_result_next (rstep, &vstep) == TRUE
      && (json_node_get_value_type (vstep) == G_TYPE_UINT ||
	  json_node_get_value_type (vstep) == G_TYPE_INT64 ||
	  json_node_get_value_type (vstep) == G_TYPE_INT ||
	  json_node_get_value_type (vstep) == G_TYPE_FLOAT ||
	  json_node_get_value_type (vstep) == G_TYPE_DOUBLE ))
    {
      step = (gint) (json_node_get_value_type (vstep) == G_TYPE_DOUBLE || json_node_get_value_type (vstep) == G_TYPE_FLOAT) ? 
                json_node_get_double (vstep) : 
                json_node_get_int (vstep) ;

      if (step <= 0)
	step = 1;
    }
  else
    step = 1;

  for (; start < end; start += step)
    {
      JsonNode *value = json_array_get_element (array, (guint)start);

      if (!value)
	continue;

      switch (json_node_get_node_type (value))
	{
	case JSON_NODE_OBJECT:
	  {
	    JsonObject *obj = json_node_get_object (value);

	    if (tb_jsonpath_exec_query_value
		(item, parent, obj, query, value, result, error) == FALSE)
	      return FALSE;
	  }

	  break;

	case JSON_NODE_ARRAY:
	  {
	    JsonArray *array1 = json_node_get_array (value);

	    GList *list, *l;

            list = json_array_get_elements (array1);

            for (l = list; l; l = l->next)
              {
                JsonNode *value = l->data;

                if (tb_jsonpath_exec_query_value
                    (item, parent, object, query, value, result, error) == FALSE)
                  {
                    g_list_free (list);
                    return FALSE;
                  }
              }
            g_list_free (list);
	  }

	  break;

	default:
	  if (tb_jsonpath_exec_query_value
	      (item, parent, object, query, value, result, error) == FALSE)
	    return FALSE;
	}
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_query_condition_generic (tb_jsonpath_item_t * item,
					  JsonObject * parent,
					  JsonObject * object,
					  tb_jsonpath_query_t *
					  query,
					  JsonNode * value,
					  tb_jsonpath_result_t *
					  rstart,
					  tb_jsonpath_result_t *
					  rend,
					  tb_jsonpath_result_t *
					  rstep, GList ** result,
					  GError ** error)
{
  JsonNode *tmp;

  /* It is not an array: */
  if (rend || rstep)
    return TRUE;

  while (tb_jsonpath_result_next (rstart, &tmp) == TRUE)
    {
      gboolean todo = FALSE;

      /* TODO - make sure json-glib actually retuns this list of GType */

      switch (json_node_get_node_type (tmp))
	{
	case JSON_NODE_VALUE:
          {
            if (json_node_get_value_type (tmp) == G_TYPE_STRING)
	    {
	      gchar *string = (gchar *) json_node_get_string (tmp);
	      todo = *string ? TRUE : FALSE;
	      break;
	    }

            if (json_node_get_value_type (tmp) == G_TYPE_DOUBLE
	        || json_node_get_value_type (tmp) == G_TYPE_FLOAT)
	    {
	      gdouble numb = json_node_get_double (tmp);
	      todo = numb ? TRUE : FALSE;
	      break;
	    }

            if (json_node_get_value_type (tmp) == G_TYPE_INT
                || json_node_get_value_type (tmp) == G_TYPE_INT64
		|| json_node_get_value_type (tmp) == G_TYPE_UINT)
	    {
	      gint numb = (gint) json_node_get_int (tmp);
	      todo = numb ? TRUE : FALSE;
	      break;
	    }

            if (json_node_get_value_type (tmp) == G_TYPE_BOOLEAN)
            {
	      todo = json_node_get_boolean (tmp);
	      break;
            }
          }

	case JSON_NODE_OBJECT:
	case JSON_NODE_ARRAY:
	  todo = TRUE;
	  break;

        case JSON_NODE_NULL:
	default:
	  todo = FALSE;
	  break;
	}

      if (todo
	  && tb_jsonpath_exec_query_value (item, parent, object, query,
					   value, result, error) == FALSE)
	return FALSE;
    }

  return TRUE;
}

/* Script exec */
static gboolean
tb_jsonpath_exec_script (tb_jsonpath_item_t * item,
			 JsonObject * parent,
			 JsonObject * object,
			 tb_jsonpath_script_t * script,
			 tb_jsonpath_result_t ** result, GError ** error)
{
  gint len;
  GList *args = NULL;
  gboolean ret = TRUE;
  JsonNode *value;

  tb_jsonpath_function_t *func;

  if (!(func = tb_jsonpath_function_find (item, script->name)))
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, tb_jsonpath_error_quark (),
		   TB_ERROR_JSONPATH, "The function '%s' doesn't exist",
		   script->name);
      return FALSE;
    }

  len = g_list_length (script->items);

  if (func->numb_args != -1 && func->numb_args != len)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, tb_jsonpath_error_quark (),
		   TB_ERROR_JSONPATH,
		   "The function '%s' needs %d arguments and not %d",
		   script->name, func->numb_args, len);
      return FALSE;
    }

  if (len)
    {
      GList *list;

      for (list = script->items; list; list = list->next)
	{
	  tb_jsonpath_result_t *tmp;

	  if (tb_jsonpath_exec_real (list->data, parent, object, &tmp, error)
	      == FALSE)
	    {
	      g_list_foreach (args, (GFunc) tb_jsonpath_result_free, NULL);
	      g_list_free (args);
	      return FALSE;
	    }

	  if (!tmp)
	    {
	      tmp = g_malloc0 (sizeof (tb_jsonpath_result_t));

              value = json_node_new (JSON_NODE_VALUE);

	      tmp->values = g_list_append (NULL, value);
	    }

	  args = g_list_append (args, tmp);
	}
    }

  value = json_node_new (JSON_NODE_VALUE);

  *result = g_malloc0 (sizeof (tb_jsonpath_result_t));
  (*result)->values = g_list_append (NULL, value);

  ret = func->func (args, value, error);

  g_list_foreach (args, (GFunc) tb_jsonpath_result_free, NULL);
  g_list_free (args);

  return ret;
}

/* Filter exec */
static gboolean tb_jsonpath_exec_filter_value (JsonNode * value);

static gboolean
tb_jsonpath_exec_filter_value (JsonNode * value)
{
  /* TODO - make sure json-glib actually retuns this list of GType */

  switch (json_node_get_node_type (value))
    {
      case JSON_NODE_VALUE:
        {
          if (json_node_get_value_type (value) == G_TYPE_STRING)
          {
            gchar *string = (gchar *) json_node_get_string (value);
	    if (*string)
              return TRUE;

            return FALSE;
          }

          if (json_node_get_value_type (value) == G_TYPE_DOUBLE
	      || json_node_get_value_type (value) == G_TYPE_FLOAT)
          {
            if (json_node_get_double (value))
	      return TRUE;

	    return FALSE;
          }

          if (json_node_get_value_type (value) == G_TYPE_INT
                || json_node_get_value_type (value) == G_TYPE_INT64
                || json_node_get_value_type (value) == G_TYPE_UINT)
          {
            if (json_node_get_int (value))
	      return TRUE;

	    return FALSE;
          }

          if (json_node_get_value_type (value) == G_TYPE_BOOLEAN)
          {
            return json_node_get_boolean (value);
          }
        }

      case JSON_NODE_OBJECT:
      case JSON_NODE_ARRAY:
        return TRUE;

      case JSON_NODE_NULL:
        return FALSE;
    }

  return TRUE;
}

static gboolean tb_jsonpath_exec_filter_operation (JsonNode * first,
						   JsonNode * second,
						   tb_jsonpath_operation_type_t operation,
						   JsonNode ** status,
						   GError ** error);

static gboolean
tb_jsonpath_exec_filter (tb_jsonpath_item_t * item,
			 JsonObject * parent,
			 JsonObject * object,
			 tb_jsonpath_filter_t * filter,
			 tb_jsonpath_result_t ** result, GError ** error)
{
  tb_jsonpath_result_t *first, *second;
  JsonNode *value;
  gboolean ret;

  if (tb_jsonpath_exec_real (filter->first, parent, object, &first, error) ==
      FALSE)
    return FALSE;

  if (!filter->second)
    {
      *result = first;
      return TRUE;
    }

  if (tb_jsonpath_exec_real (filter->second, parent, object, &second, error)
      == FALSE)
    return FALSE;

  ret =
    tb_jsonpath_exec_filter_operation (first->values->data,
				       second->values->data,
				       filter->operation, &value, error);

  *result = g_malloc0 (sizeof (tb_jsonpath_result_t));
  (*result)->values = g_list_append (NULL, value);

  tb_jsonpath_result_free (first);
  tb_jsonpath_result_free (second);

  return ret;
}

static gboolean
tb_jsonpath_exec_filter_operation_and (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_or (JsonNode * first,
						      JsonNode * second,
						      JsonNode ** status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_gt (JsonNode * first,
						      JsonNode * second,
						      JsonNode ** status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_ge (JsonNode * first,
						      JsonNode * second,
						      JsonNode ** status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_lt (JsonNode * first,
						      JsonNode * second,
						      JsonNode ** status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_le (JsonNode * first,
						      JsonNode * second,
						      JsonNode ** status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_ne (JsonNode * first,
						      JsonNode * second,
						      JsonNode ** status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_eq (JsonNode * first,
						      JsonNode * second,
						      JsonNode ** status,
						      GError ** error);
static gboolean
tb_jsonpath_exec_filter_operation_mul (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error);
static gboolean
tb_jsonpath_exec_filter_operation_div (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error);
static gboolean
tb_jsonpath_exec_filter_operation_add (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error);
static gboolean
tb_jsonpath_exec_filter_operation_sub (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error);

static gboolean
tb_jsonpath_exec_filter_operation (JsonNode * first,
				   JsonNode * second,
				   tb_jsonpath_operation_type_t operation,
				   JsonNode ** status,
				   GError ** error)
{
  switch (operation)
    {
    case TB_JSONPATH_FILTER_OPERATION_AND:
      return tb_jsonpath_exec_filter_operation_and (first, second,
						    status, error);

    case TB_JSONPATH_FILTER_OPERATION_OR:
      return tb_jsonpath_exec_filter_operation_or (first, second, status,
						   error);

    case TB_JSONPATH_FILTER_OPERATION_GT:
      return tb_jsonpath_exec_filter_operation_gt (first, second, status,
						   error);

    case TB_JSONPATH_FILTER_OPERATION_GE:
      return tb_jsonpath_exec_filter_operation_ge (first, second, status,
						   error);

    case TB_JSONPATH_FILTER_OPERATION_LT:
      return tb_jsonpath_exec_filter_operation_lt (first, second, status,
						   error);

    case TB_JSONPATH_FILTER_OPERATION_LE:
      return tb_jsonpath_exec_filter_operation_le (first, second, status,
						   error);

    case TB_JSONPATH_FILTER_OPERATION_NE:
      return tb_jsonpath_exec_filter_operation_ne (first, second, status,
						   error);

    case TB_JSONPATH_FILTER_OPERATION_EQ:
      return tb_jsonpath_exec_filter_operation_eq (first, second, status,
						   error);

    case TB_JSONPATH_FILTER_OPERATION_MUL:
      return tb_jsonpath_exec_filter_operation_mul (first, second,
						    status, error);

    case TB_JSONPATH_FILTER_OPERATION_DIV:
      return tb_jsonpath_exec_filter_operation_div (first, second,
						    status, error);

    case TB_JSONPATH_FILTER_OPERATION_ADD:
      return tb_jsonpath_exec_filter_operation_add (first, second,
						    status, error);

    case TB_JSONPATH_FILTER_OPERATION_SUB:
      return tb_jsonpath_exec_filter_operation_sub (first, second,
						    status, error);

    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_and (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error)
{
  *status = json_node_new (JSON_NODE_VALUE);

  if (tb_jsonpath_exec_filter_value (first) == TRUE
      && tb_jsonpath_exec_filter_value (second) == TRUE)
    json_node_set_boolean (*status, TRUE);
  else
    json_node_set_boolean (*status, FALSE);

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_or (JsonNode * first,
				      JsonNode * second,
				      JsonNode ** status,
				      GError ** error)
{
  *status = json_node_new (JSON_NODE_VALUE);

  if (tb_jsonpath_exec_filter_value (first) == TRUE
      || tb_jsonpath_exec_filter_value (second) == TRUE)
    json_node_set_boolean (*status, TRUE);
  else
    json_node_set_boolean (*status, FALSE);

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_gt (JsonNode * first,
				      JsonNode * second,
				      JsonNode ** status,
				      GError ** error)
{
  *status = json_node_new (JSON_NODE_VALUE);

  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      json_node_set_boolean (*status, FALSE);
      return TRUE;
    }

  switch (json_node_get_node_type (first))
    {   
      case JSON_NODE_VALUE:
        {   
          if (json_node_get_value_type (first) == G_TYPE_STRING)
          { 
	    gboolean ret;
            gchar *string1 = (gchar *) json_node_get_string (first);
            gchar *string2 = (gchar *) json_node_get_string (second);

            ret = g_utf8_collate (string1, string2);

            json_node_set_boolean (*status, ret == 1 ? TRUE : FALSE);
            return TRUE;
          } 

          if (json_node_get_value_type (first) == G_TYPE_DOUBLE
                || json_node_get_value_type (first) == G_TYPE_FLOAT)
          {
	    gdouble number1 = json_node_get_double (first);
	    gdouble number2 = json_node_get_double (second);

	    json_node_set_boolean (*status, number1 > number2 ? TRUE : FALSE);
	    return TRUE;
          }

	  if (json_node_get_value_type (first) == G_TYPE_INT
                || json_node_get_value_type (first) == G_TYPE_INT64
                || json_node_get_value_type (first) == G_TYPE_UINT)
          { 
            gint number1 = (gint) json_node_get_int (first);
            gint number2 = (gint) json_node_get_int (second);

	    json_node_set_boolean (*status, number1 > number2 ? TRUE : FALSE);
	    return TRUE;
          } 
          if (json_node_get_value_type (first) == G_TYPE_BOOLEAN)
          {
	    gboolean boolean1 = json_node_get_boolean (first);
	    gboolean boolean2 = json_node_get_boolean (second);

	    json_node_set_boolean (*status,
				   boolean1 > boolean2 ? TRUE : FALSE); /* does not make sense ? */
	    return TRUE;
          }
        } 

      case JSON_NODE_OBJECT:
      case JSON_NODE_ARRAY:
        json_node_set_boolean (*status, FALSE);
        return TRUE;

      case JSON_NODE_NULL:
        json_node_set_boolean (*status, FALSE);
        return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_ge (JsonNode * first,
				      JsonNode * second,
				      JsonNode ** status,
				      GError ** error)
{
  *status = json_node_new (JSON_NODE_VALUE);

  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      json_node_set_boolean (*status, FALSE);
      return TRUE;
    }

  switch (json_node_get_node_type (first))
    {
      case JSON_NODE_VALUE:
        {
          if (json_node_get_value_type (first) == G_TYPE_STRING)
          {
            gboolean ret;
            gchar *string1 = (gchar *) json_node_get_string (first);
            gchar *string2 = (gchar *) json_node_get_string (second);

            ret = g_utf8_collate (string1, string2);

	    json_node_set_boolean (*status, ret == 1 || ret == 0 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_DOUBLE
                || json_node_get_value_type (first) == G_TYPE_FLOAT)
          {
            gdouble number1 = json_node_get_double (first);
            gdouble number2 = json_node_get_double (second);

            json_node_set_boolean (*status, number1 >= number2 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_INT
                || json_node_get_value_type (first) == G_TYPE_INT64
                || json_node_get_value_type (first) == G_TYPE_UINT)
          {
            gint number1 = (gint) json_node_get_int (first);
            gint number2 = (gint) json_node_get_int (second);

            json_node_set_boolean (*status, number1 >= number2 ? TRUE : FALSE);
            return TRUE;
          }
          if (json_node_get_value_type (first) == G_TYPE_BOOLEAN)
          {
            gboolean boolean1 = json_node_get_boolean (first);
            gboolean boolean2 = json_node_get_boolean (second);

            json_node_set_boolean (*status,
                                   boolean1 >= boolean2 ? TRUE : FALSE); /* does not make sense ? */
            return TRUE;
          }
        }

      case JSON_NODE_OBJECT:
      case JSON_NODE_ARRAY:
        json_node_set_boolean (*status, FALSE);
        return TRUE;

      case JSON_NODE_NULL:
        json_node_set_boolean (*status, FALSE);
        return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_lt (JsonNode * first,
				      JsonNode * second,
				      JsonNode ** status,
				      GError ** error)
{
  *status = json_node_new (JSON_NODE_VALUE);

  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      json_node_set_boolean (*status, FALSE);
      return TRUE;
    }

  switch (json_node_get_node_type (first))
    {
      case JSON_NODE_VALUE:
        {
          if (json_node_get_value_type (first) == G_TYPE_STRING)
          {
            gboolean ret;
            gchar *string1 = (gchar *) json_node_get_string (first);
            gchar *string2 = (gchar *) json_node_get_string (second);

            ret = g_utf8_collate (string1, string2);

            json_node_set_boolean (*status, ret == -1 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_DOUBLE
                || json_node_get_value_type (first) == G_TYPE_FLOAT)
          {
            gdouble number1 = json_node_get_double (first);
            gdouble number2 = json_node_get_double (second);

            json_node_set_boolean (*status, number1 < number2 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_INT
                || json_node_get_value_type (first) == G_TYPE_INT64
                || json_node_get_value_type (first) == G_TYPE_UINT)
          {
            gint number1 = (gint) json_node_get_int (first);
            gint number2 = (gint) json_node_get_int (second);

            json_node_set_boolean (*status, number1 < number2 ? TRUE : FALSE);
            return TRUE;
          }
          if (json_node_get_value_type (first) == G_TYPE_BOOLEAN)
          {
            gboolean boolean1 = json_node_get_boolean (first);
            gboolean boolean2 = json_node_get_boolean (second);

            json_node_set_boolean (*status,
                                   boolean1 < boolean2 ? TRUE : FALSE); /* does not make sense ? */
            return TRUE;
          }
        }

      case JSON_NODE_OBJECT:
      case JSON_NODE_ARRAY:
        json_node_set_boolean (*status, FALSE);
        return TRUE;

      case JSON_NODE_NULL:
        json_node_set_boolean (*status, FALSE);
        return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_le (JsonNode * first,
				      JsonNode * second,
				      JsonNode ** status,
				      GError ** error)
{
  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      json_node_set_boolean (*status, FALSE);
      return TRUE;
    }

  switch (json_node_get_node_type (first))
    {
      case JSON_NODE_VALUE:
        {
          if (json_node_get_value_type (first) == G_TYPE_STRING)
          {
            gboolean ret;
            gchar *string1 = (gchar *) json_node_get_string (first);
            gchar *string2 = (gchar *) json_node_get_string (second);

            ret = g_utf8_collate (string1, string2);

            json_node_set_boolean (*status, ret == 0 || ret == -1 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_DOUBLE
                || json_node_get_value_type (first) == G_TYPE_FLOAT)
          {
            gdouble number1 = json_node_get_double (first);
            gdouble number2 = json_node_get_double (second);

            json_node_set_boolean (*status, number1 <= number2 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_INT
                || json_node_get_value_type (first) == G_TYPE_INT64
                || json_node_get_value_type (first) == G_TYPE_UINT)
          {
            gint number1 = (gint) json_node_get_int (first);
            gint number2 = (gint) json_node_get_int (second);

            json_node_set_boolean (*status, number1 <= number2 ? TRUE : FALSE);
            return TRUE;
          }
          if (json_node_get_value_type (first) == G_TYPE_BOOLEAN)
          {
            gboolean boolean1 = json_node_get_boolean (first);
            gboolean boolean2 = json_node_get_boolean (second);

            json_node_set_boolean (*status,
                                   boolean1 <= boolean2 ? TRUE : FALSE); /* does not make sense ? */
            return TRUE;
          }
        }

      case JSON_NODE_OBJECT:
      case JSON_NODE_ARRAY:
        json_node_set_boolean (*status, FALSE);
        return TRUE;

      case JSON_NODE_NULL:
        json_node_set_boolean (*status, FALSE);
        return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_eq (JsonNode * first,
				      JsonNode * second,
				      JsonNode ** status,
				      GError ** error)
{
  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      json_node_set_boolean (*status, FALSE);
      return TRUE;
    }
  
  switch (json_node_get_node_type (first))
    {
      case JSON_NODE_VALUE:
        {
          if (json_node_get_value_type (first) == G_TYPE_STRING)
          {
            gboolean ret;
            gchar *string1 = (gchar *) json_node_get_string (first);
            gchar *string2 = (gchar *) json_node_get_string (second);
        
            ret = g_utf8_collate (string1, string2);

            json_node_set_boolean (*status, ret == 0 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_DOUBLE
                || json_node_get_value_type (first) == G_TYPE_FLOAT)
          {
            gdouble number1 = json_node_get_double (first);
            gdouble number2 = json_node_get_double (second);

            json_node_set_boolean (*status, number1 == number2 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_INT
                || json_node_get_value_type (first) == G_TYPE_INT64
                || json_node_get_value_type (first) == G_TYPE_UINT)
          {
            gint number1 = (gint) json_node_get_int (first);
            gint number2 = (gint) json_node_get_int (second);

            json_node_set_boolean (*status, number1 == number2 ? TRUE : FALSE);
            return TRUE;
          }
          if (json_node_get_value_type (first) == G_TYPE_BOOLEAN)
          {
            gboolean boolean1 = json_node_get_boolean (first);
            gboolean boolean2 = json_node_get_boolean (second);

            json_node_set_boolean (*status,
                                   boolean1 == boolean2 ? TRUE : FALSE); /* does not make sense ? */
            return TRUE;
          }
        }

      case JSON_NODE_OBJECT:
      case JSON_NODE_ARRAY:
        json_node_set_boolean (*status, FALSE);
        return TRUE;

      case JSON_NODE_NULL:
        json_node_set_boolean (*status, FALSE);
        return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_ne (JsonNode * first,
				      JsonNode * second,
				      JsonNode ** status,
				      GError ** error)
{
  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      json_node_set_boolean (*status, FALSE);
      return TRUE;
    }

  switch (json_node_get_node_type (first))
    {
      case JSON_NODE_VALUE:
        {
          if (json_node_get_value_type (first) == G_TYPE_STRING)
          {
            gboolean ret;
            gchar *string1 = (gchar *) json_node_get_string (first);
            gchar *string2 = (gchar *) json_node_get_string (second);
        
            ret = g_utf8_collate (string1, string2);

            json_node_set_boolean (*status, ret != 0 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_DOUBLE
                || json_node_get_value_type (first) == G_TYPE_FLOAT)
          {
            gdouble number1 = json_node_get_double (first);
            gdouble number2 = json_node_get_double (second);

            json_node_set_boolean (*status, number1 != number2 ? TRUE : FALSE);
            return TRUE;
          }

          if (json_node_get_value_type (first) == G_TYPE_INT
                || json_node_get_value_type (first) == G_TYPE_INT64
                || json_node_get_value_type (first) == G_TYPE_UINT)
          {
            gint number1 = (gint) json_node_get_int (first);
            gint number2 = (gint) json_node_get_int (second);

            json_node_set_boolean (*status, number1 != number2 ? TRUE : FALSE);
            return TRUE;
          }
          if (json_node_get_value_type (first) == G_TYPE_BOOLEAN)
          {
            gboolean boolean1 = json_node_get_boolean (first);
            gboolean boolean2 = json_node_get_boolean (second);

            json_node_set_boolean (*status,
                                   boolean1 != boolean2 ? TRUE : FALSE); /* does not make sense ? */
            return TRUE;
          }
        }

      case JSON_NODE_OBJECT:
      case JSON_NODE_ARRAY:
        json_node_set_boolean (*status, FALSE);
        return TRUE;

      case JSON_NODE_NULL:
        json_node_set_boolean (*status, FALSE);
        return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_mul (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error)
{
  gdouble number1, number2;

  /* WARNING - following some hacks - check if there is a better way to do it in json-glib - json_node_set_value() is not allowed for NULL / G_TYPE_INVALID */

  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      *status = json_node_new (JSON_NODE_NULL);
      return TRUE;
    }

  if (json_node_get_value_type (first) != G_TYPE_UINT &&
      json_node_get_value_type (first) != G_TYPE_INT64 &&
      json_node_get_value_type (first) != G_TYPE_INT &&
      json_node_get_value_type (first) != G_TYPE_DOUBLE)
    {
      *status = json_node_new (JSON_NODE_NULL);
      return TRUE;
    }

  *status = json_node_new (JSON_NODE_VALUE);

  number1 = json_node_get_double (first);
  number2 = json_node_get_double (second);

  json_node_set_double (*status, number1 * number2);
  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_div (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error)
{
  gdouble number1, number2;

  /* WARNING - following some hacks - check if there is a better way to do it in json-glib - json_node_set_value() is not allowed for NULL / G_TYPE_INVALID */

  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      *status = json_node_new (JSON_NODE_NULL);
      return TRUE;
    }

  if (json_node_get_value_type (first) != G_TYPE_UINT &&
      json_node_get_value_type (first) != G_TYPE_INT64 &&
      json_node_get_value_type (first) != G_TYPE_INT &&
      json_node_get_value_type (first) != G_TYPE_DOUBLE)
    {
      *status = json_node_new (JSON_NODE_NULL);
      return TRUE;
    }

  number1 = json_node_get_double (first);
  number2 = json_node_get_double (second);

  if (number2)
    {
      *status = json_node_new (JSON_NODE_VALUE);
      json_node_set_double (*status, number1 / number2);
    }
  else
    *status = json_node_new (JSON_NODE_NULL);

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_add (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error)
{
  gdouble number1, number2;

  /* WARNING - following some hacks - check if there is a better way to do it in json-glib - json_node_set_value() is not allowed for NULL / G_TYPE_INVALID */

  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      *status = json_node_new (JSON_NODE_NULL);
      return TRUE;
    }

  if (json_node_get_value_type (first) != G_TYPE_UINT &&
      json_node_get_value_type (first) != G_TYPE_INT64 &&
      json_node_get_value_type (first) != G_TYPE_INT &&
      json_node_get_value_type (first) != G_TYPE_DOUBLE)
    {
      *status = json_node_new (JSON_NODE_NULL);
      return TRUE;
    }

  *status = json_node_new (JSON_NODE_VALUE);

  number1 = json_node_get_double (first);
  number2 = json_node_get_double (second);

  json_node_set_double (*status, number1 + number2);

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_sub (JsonNode * first,
				       JsonNode * second,
				       JsonNode ** status,
				       GError ** error)
{
  gdouble number1, number2;

  /* WARNING - following some hacks - check if there is a better way to do it in json-glib - json_node_set_value() is not allowed for NULL / G_TYPE_INVALID */

  if (json_node_get_value_type (first) != json_node_get_value_type (second))
    {
      *status = json_node_new (JSON_NODE_NULL);
      return TRUE;
    }

  if (json_node_get_value_type (first) != G_TYPE_UINT &&
      json_node_get_value_type (first) != G_TYPE_INT64 &&
      json_node_get_value_type (first) != G_TYPE_INT &&
      json_node_get_value_type (first) != G_TYPE_DOUBLE)
    {
      *status = json_node_new (JSON_NODE_NULL);
      return TRUE;
    }

  *status = json_node_new (JSON_NODE_VALUE);

  number1 = json_node_get_double (first);
  number2 = json_node_get_double (second);

  json_node_set_double (*status, number1 - number2);

  return TRUE;
}

/* RESULTS **************************************************************/

/**
 * tb_jsonpath_result_next:
 * @result: JSONPath result
 * @value: a pointer for a JSONValue
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * This function returns the next object (or the first) from a 
 * JSONPath result. If value will be NULL, no other results exist.
 */
gboolean
tb_jsonpath_result_next (tb_jsonpath_result_t * result,
			 JsonNode ** value)
{
  g_return_val_if_fail (result != NULL, FALSE);
  g_return_val_if_fail (value != NULL, FALSE);

  if (!result->current)
    {
      result->current = result->values;
      *value = result->current->data;
      return TRUE;
    }

  if (!result->current->next)
    return FALSE;

  result->current = result->current->next;
  *value = result->current->data;
  return TRUE;
}

/**
 * tb_jsonpath_result_prev:
 * @result: JSONPath result
 * @value: a pointer for a JSONValue
 * @returns: TRUE on success, FALSE if an error occurred
 *
 * This function returns the prev object from a JSONPath result.
 */
gboolean
tb_jsonpath_result_prev (tb_jsonpath_result_t * result,
			 JsonNode ** value)
{
  g_return_val_if_fail (result != NULL, FALSE);
  g_return_val_if_fail (value != NULL, FALSE);

  if (!result->current || !result->current->prev)
    return FALSE;

  result->current = result->current->prev;
  *value = result->current->data;
  return TRUE;
}

/**
 * tb_jsonpath_result_length:
 * @result: JSONPath result
 * @returns: the number of results
 *
 * Returns the number of results
 */
gint
tb_jsonpath_result_length (tb_jsonpath_result_t * result)
{
  g_return_val_if_fail (result != NULL, -1);

  return g_list_length (result->values);
}

/**
 * tb_jsonpath_result_free:
 * @result: the JSONPath result
 *
 * frees the results
 */
void
tb_jsonpath_result_free (tb_jsonpath_result_t * result)
{
  if (!result)
    return;

  for (result->current = result->values; result->current;
       result->current = result->current->next)
    json_node_free (result->current->data); /* TODO - check it is actually correct to free JsonNode and we do not have JsonObject or JsonArray as well */

  g_list_free (result->values);
  g_free (result);
}

/* FUNCTIONS ***************************************************************/

static gboolean
tb_jsonpath_set_functions_filter (tb_jsonpath_filter_t * filter,
				  tb_jsonpath_functions_t *
				  functions, GError ** error);
static gboolean
tb_jsonpath_set_functions_script (tb_jsonpath_script_t * script,
				  tb_jsonpath_functions_t *
				  functions, GError ** error);
static gboolean tb_jsonpath_set_functions_query (tb_jsonpath_query_t
						 * query,
						 tb_jsonpath_functions_t
						 * functions,
						 GError ** error);
static gboolean tb_jsonpath_set_functions_match (tb_jsonpath_match_t
						 * match,
						 tb_jsonpath_functions_t
						 * functions,
						 GError ** error);

gboolean
tb_jsonpath_set_functions (tb_jsonpath_item_t * item,
			   tb_jsonpath_functions_t * functions,
			   GError ** error)
{
  if (item->filter
      && tb_jsonpath_set_functions_filter (item->filter, functions,
					   error) == FALSE)
    return FALSE;

  if (item->script
      && tb_jsonpath_set_functions_script (item->script, functions,
					   error) == FALSE)
    return FALSE;

  if (item->query
      && tb_jsonpath_set_functions_query (item->query, functions,
					  error) == FALSE)
    return FALSE;

  item->functions = functions;
  return TRUE;
}

static gboolean
tb_jsonpath_set_functions_filter (tb_jsonpath_filter_t * filter,
				  tb_jsonpath_functions_t *
				  functions, GError ** error)
{
  if (filter->first
      && tb_jsonpath_set_functions (filter->first, functions, error) == FALSE)
    return FALSE;

  if (filter->second
      && tb_jsonpath_set_functions (filter->second, functions,
				    error) == FALSE)
    return FALSE;

  return TRUE;
}

static gboolean
tb_jsonpath_set_functions_script (tb_jsonpath_script_t * script,
				  tb_jsonpath_functions_t *
				  functions, GError ** error)
{
  GList *list;

  for (list = script->items; list; list = list->next)
    if (tb_jsonpath_set_functions (list->data, functions, error) == FALSE)
      return FALSE;

  return TRUE;
}

static gboolean
tb_jsonpath_set_functions_query (tb_jsonpath_query_t * query,
				 tb_jsonpath_functions_t * functions,
				 GError ** error)
{
  GList *list;

  for (list = query->matches; list; list = list->next)
    if (tb_jsonpath_set_functions_match (list->data, functions, error) ==
	FALSE)
      return FALSE;

  return TRUE;
}

static gboolean
tb_jsonpath_set_functions_match (tb_jsonpath_match_t * match,
				 tb_jsonpath_functions_t * functions,
				 GError ** error)
{
  if (match->type != TB_JSONPATH_MATCH_TYPE_CONDITION)
    return TRUE;

  if (match->condition.start
      && tb_jsonpath_set_functions (match->condition.start, functions,
				    error) == FALSE)
    return FALSE;

  if (match->condition.end
      && tb_jsonpath_set_functions (match->condition.end, functions,
				    error) == FALSE)
    return FALSE;

  if (match->condition.step
      && tb_jsonpath_set_functions (match->condition.step, functions,
				    error) == FALSE)
    return FALSE;

  return TRUE;
}

/* ERRORS **********************************************************/
/**
 * tb_jsonpath_error_quark:
 * @returns: The error domain of for the tb jsonpath
 *
 * The error domain for the tb jsonpath
 **/
GQuark
tb_jsonpath_error_quark (void)
{
  return g_quark_from_static_string ("thebox-jsonpath-error-quark");
}

/* EOF */
