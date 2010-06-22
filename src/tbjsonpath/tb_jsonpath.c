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

#include "../tbjson/tb_scanner.h"

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
    tb_json_value_destroy (item->value);

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

static gboolean tb_jsonpath_parser_item (tb_scanner_t * scanner,
					 tb_jsonpath_item_t ** item,
					 gboolean operation, GError ** error);
static gboolean tb_jsonpath_parser_query (tb_scanner_t * scanner,
					  tb_jsonpath_query_t ** jp,
					  GError ** error);
static gboolean tb_jsonpath_parser_condition (tb_scanner_t * scanner,
					      tb_jsonpath_match_t **
					      match_ret, GError ** error);
static gboolean tb_jsonpath_parser_filter (tb_scanner_t * scanner,
					   tb_jsonpath_filter_t ** filter,
					   GError ** error);
static gboolean tb_jsonpath_parser_script (tb_scanner_t * scanner,
					   tb_jsonpath_script_t ** script,
					   GError ** error);

/* This function parses a string: */
gboolean
tb_jsonpath_parser (gchar * jsonpath, gssize size,
		    tb_jsonpath_item_t ** item_ret, GError ** error)
{
  tb_jsonpath_item_t *item;
  tb_scanner_t *scanner;

  /* It uses our scanner parser: */
  scanner = tb_scanner_new (error);
  tb_scanner_input_text (scanner, jsonpath, size);
  tb_scanner_set_qname (scanner, FALSE);

  tb_scanner_get_next_token (scanner);

  /* Parse the item. An item is something like @.aa[conditions/filters/...] */
  if (tb_jsonpath_parser_item (scanner, &item, FALSE, error) == FALSE)
    {
      tb_scanner_destroy (scanner);
      return FALSE;
    }

  /* End of the string: */
  if (tb_scanner_cur_token (scanner) != TB_SCANNER_EOF)
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_EOF, NULL);
      tb_scanner_destroy (scanner);
      tb_jsonpath_free (item);
      return FALSE;
    }

  tb_scanner_destroy (scanner);
  *item_ret = item;
  return TRUE;
}

/* This function parse an item: */
static gboolean
tb_jsonpath_parser_item (tb_scanner_t * scanner,
			 tb_jsonpath_item_t ** item_ret, gboolean operation,
			 GError ** error)
{
  tb_jsonpath_item_t *item;

  item = g_malloc0 (sizeof (tb_jsonpath_item_t));

  /* A @ or $ stuff: */
  if (tb_scanner_cur_token (scanner) == TB_SCANNER_DOLLAR
      || tb_scanner_cur_token (scanner) == TB_SCANNER_AT)
    {
      if (tb_jsonpath_parser_query (scanner, &item->query, error) == FALSE)
	{
	  tb_jsonpath_free (item);
	  return FALSE;
	}
    }

  /* Filter: */
  else if (tb_scanner_cur_token (scanner) == TB_SCANNER_LEFT_PAREN)
    {
      if (tb_jsonpath_parser_filter (scanner, &item->filter, error) == FALSE)
	{
	  tb_jsonpath_free (item);
	  return FALSE;
	}
    }

  /* Script: */
  else if (tb_scanner_cur_token (scanner) == TB_SCANNER_IDENTIFIER
	   && g_str_has_prefix (tb_scanner_cur_value_identifier (scanner),
				"?") == TRUE)
    {
      if (tb_jsonpath_parser_script (scanner, &item->script, error) == FALSE)
	{
	  tb_jsonpath_free (item);
	  return FALSE;
	}
    }

  /* Value: */
  else if (tb_scanner_cur_token (scanner) == TB_SCANNER_STRING)
    {
      tb_json_value_new (&item->value);

      tb_json_value_set_string (item->value,
				tb_scanner_cur_value_string (scanner));
      tb_scanner_get_next_token (scanner);
    }

  else if (tb_scanner_cur_token (scanner) == TB_SCANNER_NUMBER)
    {
      tb_json_value_new (&item->value);

      tb_json_value_set_number (item->value,
				tb_scanner_cur_value_number (scanner));
      tb_scanner_get_next_token (scanner);
    }

  /* Operation: */
  else if (operation == TRUE
	   && tb_scanner_cur_token (scanner) == TB_SCANNER_IDENTIFIER)
    {
      if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "+"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_ADD;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "-"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_SUB;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "="))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_EQ;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "*"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_MUL;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "/"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_DIV;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), ">"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_GT;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), ">="))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_GE;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "<"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_LT;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "<="))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_LE;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "!="))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_NE;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "&&"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_AND;
	}

      else
	if (!g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "||"))
	{
	  item->is_operation = TRUE;
	  item->operation = TB_JSONPATH_FILTER_OPERATION_OR;
	}
      else
	{
	  tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER,
				  "Valid jsonpath item or operation");
	  tb_jsonpath_free (item);
	  return FALSE;
	}

      tb_scanner_get_next_token (scanner);
    }
  else
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER,
			      "Valid jsonpath item");
      tb_jsonpath_free (item);
      return FALSE;
    }

  *item_ret = item;
  return TRUE;
}

/* This function parse an item: */
static gboolean
tb_jsonpath_parser_query (tb_scanner_t * scanner,
			  tb_jsonpath_query_t ** query_ret, GError ** error)
{
  tb_jsonpath_query_t *query;
  gboolean dot = FALSE;

  query = g_malloc0 (sizeof (tb_jsonpath_query_t));

  /* How does it start: */
  if (tb_scanner_cur_token (scanner) == TB_SCANNER_DOLLAR)
    query->root = TRUE;
  else if (tb_scanner_cur_token (scanner) == TB_SCANNER_AT)
    query->root = FALSE;

  else
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER, "$ or @");
      tb_jsonpath_free_query (query);
      return FALSE;
    }

  tb_scanner_get_next_token (scanner);

  /* List of nodes/conditions: */
  while (tb_scanner_cur_token (scanner) != TB_SCANNER_EOF)
    {
      gboolean recursive = FALSE;

      /* Node: */
      if (tb_scanner_cur_token (scanner) == TB_SCANNER_IDENTIFIER)
	{
	  gchar **split;
	  gint i;

	  split =
	    g_strsplit (tb_scanner_cur_value_identifier (scanner), ".", -1);

	  /* If the first is not '.': */
	  if (*split[0])
	    {
	      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER,
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
		      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER,
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
      else if (tb_scanner_cur_token (scanner) == TB_SCANNER_LEFT_BRACE)
	{
	  /* Node in braces: */
	  if (dot == TRUE)
	    {
	      tb_jsonpath_match_t *match;

	      if (tb_scanner_get_next_token (scanner) != TB_SCANNER_STRING)
		{
		  tb_scanner_unexp_token (scanner, TB_SCANNER_STRING, NULL);
		  tb_jsonpath_free_query (query);
		  return FALSE;
		}

	      match = g_malloc0 (sizeof (tb_jsonpath_match_t));
	      match->type = TB_JSONPATH_MATCH_TYPE_NODE;

	      match->node.recursive = recursive;
	      recursive = FALSE;

	      match->node.node =
		g_strdup (tb_scanner_cur_value_string (scanner));

	      query->matches = g_list_append (query->matches, match);

	      /* After the "string" I look for a ']': */
	      if (tb_scanner_get_next_token (scanner) !=
		  TB_SCANNER_RIGHT_BRACE)
		{
		  tb_scanner_unexp_token (scanner, TB_SCANNER_RIGHT_BRACE,
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

      tb_scanner_get_next_token (scanner);
    }

  *query_ret = query;
  return TRUE;
}

/* Condition parser: */
static gboolean
tb_jsonpath_parser_condition (tb_scanner_t * scanner,
			      tb_jsonpath_match_t ** match_ret,
			      GError ** error)
{
  tb_jsonpath_match_t *match;
  gint i;

  tb_scanner_get_next_token (scanner);

  /* * */
  if (tb_scanner_cur_token (scanner) == TB_SCANNER_IDENTIFIER
      && !g_utf8_collate (tb_scanner_cur_value_identifier (scanner), "*"))
    {
      if (tb_scanner_get_next_token (scanner) != TB_SCANNER_RIGHT_BRACE)
	{
	  tb_scanner_unexp_token (scanner, TB_SCANNER_RIGHT_BRACE, NULL);
	  return FALSE;
	}

      match = g_malloc0 (sizeof (tb_jsonpath_match_t));
      match->type = TB_JSONPATH_MATCH_TYPE_CONDITION;

      match->condition.start = g_malloc0 (sizeof (tb_jsonpath_item_t));
      tb_json_value_new (&match->condition.start->value);
      tb_json_value_set_number (match->condition.start->value, 0);

      match->condition.end = g_malloc0 (sizeof (tb_jsonpath_item_t));
      tb_json_value_new (&match->condition.end->value);
      tb_json_value_set_number (match->condition.end->value, -1);

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

      if (tb_scanner_cur_token (scanner) != TB_SCANNER_COLON
	  && tb_scanner_cur_token (scanner) != TB_SCANNER_RIGHT_BRACE)
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

      if (tb_scanner_cur_token (scanner) == TB_SCANNER_RIGHT_BRACE)
	{
	  *match_ret = match;
	  return TRUE;
	}

      if (tb_scanner_cur_token (scanner) != TB_SCANNER_COLON)
	{
	  tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER, "] or :");
	  tb_jsonpath_free_match (match);
	  return FALSE;
	}

      tb_scanner_get_next_token (scanner);
    }

  if (tb_scanner_get_next_token (scanner) == TB_SCANNER_RIGHT_BRACE)
    {
      *match_ret = match;
      return TRUE;
    }

  tb_scanner_unexp_token (scanner, TB_SCANNER_RIGHT_BRACE, NULL);
  tb_jsonpath_free_match (match);
  return FALSE;
}

/* This function parse a script really: */
static gboolean
tb_jsonpath_parser_script (tb_scanner_t * scanner,
			   tb_jsonpath_script_t ** script_ret,
			   GError ** error)
{
  tb_jsonpath_script_t *script;

  script = g_malloc0 (sizeof (tb_jsonpath_script_t));

  /* the name: */
  if (tb_scanner_cur_token (scanner) != TB_SCANNER_IDENTIFIER)
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER, NULL);
      tb_jsonpath_free_script (script);
      return FALSE;
    }

  script->name = g_strdup (tb_scanner_cur_value_identifier (scanner) + 1);

  /* '(' */
  if (tb_scanner_get_next_token (scanner) != TB_SCANNER_LEFT_PAREN)
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_LEFT_PAREN, NULL);
      tb_jsonpath_free_script (script);
      return FALSE;
    }

  tb_scanner_get_next_token (scanner);

  /* List of arguments: */
  while (tb_scanner_cur_token (scanner) != TB_SCANNER_RIGHT_PAREN
	 && tb_scanner_cur_token (scanner) != TB_SCANNER_EOF)
    {
      tb_jsonpath_item_t *item;

      if (tb_jsonpath_parser_item (scanner, &item, FALSE, error) == FALSE)
	{
	  tb_jsonpath_free_script (script);
	  return FALSE;
	}

      script->items = g_list_append (script->items, item);

      if (tb_scanner_cur_token (scanner) == TB_SCANNER_RIGHT_PAREN)
	break;

      /* Comma between the arguments: */
      if (tb_scanner_cur_token (scanner) != TB_SCANNER_COMMA)
	{
	  tb_scanner_unexp_token (scanner, TB_SCANNER_COMMA, NULL);
	  tb_jsonpath_free_script (script);
	  return FALSE;
	}

      tb_scanner_get_next_token (scanner);
    }

  /* ')' */
  if (tb_scanner_cur_token (scanner) != TB_SCANNER_RIGHT_PAREN)
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_RIGHT_PAREN, NULL);
      tb_jsonpath_free_script (script);
      return FALSE;
    }

  tb_scanner_get_next_token (scanner);
  *script_ret = script;
  return TRUE;
}

static gboolean tb_jsonpath_parser_filter_sort (tb_scanner_t * scanner,
						GList ** list,
						tb_jsonpath_filter_t **
						filter, GError ** error);

/* This function parses a filter: */
static gboolean
tb_jsonpath_parser_filter (tb_scanner_t * scanner,
			   tb_jsonpath_filter_t ** filter, GError ** error)
{
  GList *list = NULL;
  gboolean ret;

  tb_scanner_get_next_token (scanner);

  /* Untill the filter exists: */
  while (tb_scanner_cur_token (scanner) != TB_SCANNER_EOF
	 && tb_scanner_cur_token (scanner) != TB_SCANNER_RIGHT_PAREN)
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
  if (tb_scanner_cur_token (scanner) != TB_SCANNER_RIGHT_PAREN)
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_RIGHT_PAREN, NULL);

      if (list)
	{
	  g_list_foreach (list, (GFunc) tb_jsonpath_free, NULL);
	  g_list_free (list);
	}
      return FALSE;
    }

  tb_scanner_get_next_token (scanner);

  /* No elements inside? */
  if (!list)
    {
      tb_scanner_unexp_token (scanner, TB_SCANNER_IDENTIFIER,
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
tb_jsonpath_parser_filter_sort (tb_scanner_t * scanner, GList ** items,
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
	  g_set_error (tb_scanner_error (scanner), tb_jsonpath_error_quark (),
		       TB_ERROR_JSONPATH,
		       "Error in the filter (Line: %d, Position: %d).",
		       tb_scanner_get_cur_line (scanner),
		       tb_scanner_get_cur_position (scanner));
	  return FALSE;
	}

      else if (operation == TRUE && (!item->is_operation || !list->next))
	{
	  g_set_error (tb_scanner_error (scanner), tb_jsonpath_error_quark (),
		       TB_ERROR_JSONPATH,
		       "Error in the filter (Line: %d, Position: %d).",
		       tb_scanner_get_cur_line (scanner),
		       tb_scanner_get_cur_position (scanner));
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
					tb_json_object_t * parent,
					tb_json_object_t * object,
					tb_jsonpath_query_t * query,
					tb_jsonpath_result_t ** result,
					GError ** error);
static gboolean tb_jsonpath_exec_filter (tb_jsonpath_item_t * item,
					 tb_json_object_t * parent,
					 tb_json_object_t * object,
					 tb_jsonpath_filter_t * filter,
					 tb_jsonpath_result_t ** result,
					 GError ** error);
static gboolean tb_jsonpath_exec_script (tb_jsonpath_item_t * item,
					 tb_json_object_t * parent,
					 tb_json_object_t * object,
					 tb_jsonpath_script_t * script,
					 tb_jsonpath_result_t ** result,
					 GError ** error);

static gboolean tb_jsonpath_exec_query_real (tb_jsonpath_item_t * item,
					     tb_json_object_t * parent,
					     tb_json_object_t * object,
					     tb_jsonpath_query_t * query,
					     GList ** result,
					     GError ** error);
static gboolean tb_jsonpath_exec_query_node_rec (tb_jsonpath_item_t * item,
						 tb_json_object_t * parent,
						 tb_json_object_t * object,
						 tb_jsonpath_query_t * query,
						 gchar * node,
						 GList ** results,
						 GError ** error);
static gboolean tb_jsonpath_exec_query_node_rec_array (tb_jsonpath_item_t *
						       item,
						       tb_json_object_t *
						       parent,
						       tb_json_array_t *
						       array,
						       tb_jsonpath_query_t *
						       query, gchar * node,
						       GList ** results,
						       GError ** error);
static gboolean tb_jsonpath_exec_query_node (tb_jsonpath_item_t * item,
					     tb_json_object_t * parent,
					     tb_json_object_t * object,
					     tb_jsonpath_query_t * query,
					     tb_json_node_t * node,
					     GList ** results,
					     GError ** error);
static gboolean tb_jsonpath_exec_query_value (tb_jsonpath_item_t * item,
					      tb_json_object_t * parent,
					      tb_json_object_t * object,
					      tb_jsonpath_query_t * query,
					      tb_json_value_t * value,
					      GList ** result,
					      GError ** error);
static gboolean tb_jsonpath_exec_query_value_node (tb_jsonpath_item_t * item,
						   tb_json_object_t * parent,
						   tb_json_object_t * object,
						   tb_jsonpath_query_t *
						   query,
						   tb_json_value_t * value,
						   GList ** result,
						   GError ** error);
static gboolean tb_jsonpath_exec_query_condition (tb_jsonpath_item_t * item,
						  tb_json_object_t * parent,
						  tb_json_object_t * object,
						  tb_jsonpath_query_t * query,
						  tb_json_value_t * value,
						  GList ** result,
						  GError ** error);
static gboolean tb_jsonpath_exec_query_condition_slice (tb_jsonpath_item_t *
							item,
							tb_json_object_t *
							parent,
							tb_json_object_t *
							object,
							tb_jsonpath_query_t *
							query,
							tb_json_array_t *
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
							  tb_json_object_t *
							  parent,
							  tb_json_object_t *
							  object,
							  tb_jsonpath_query_t
							  * query,
							  tb_json_value_t *
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
tb_jsonpath_exec (gchar * jsonpath, gssize size, tb_json_object_t * object,
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
tb_jsonpath_exec_real (tb_jsonpath_item_t * item, tb_json_object_t * parent,
		       tb_json_object_t * object,
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
      tb_json_value_t *value;

      tb_json_value_duplicate (item->value, &value);

      *result = g_malloc0 (sizeof (tb_jsonpath_result_t));
      (*result)->values = g_list_append (NULL, value);
    }

  return TRUE;
}

/* This function exec a normal query and return a result data struct: */
static gboolean
tb_jsonpath_exec_query (tb_jsonpath_item_t * item, tb_json_object_t * parent,
			tb_json_object_t * object,
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
			     tb_json_object_t * parent,
			     tb_json_object_t * object,
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
	  && tb_json_object_has_node (object, match->node.node) == TRUE)
	return tb_jsonpath_exec_query_node (item, parent, object, query,
					    tb_json_object_get_node (object,
								     match->node.node),
					    result, error);

      /* If the match is a '*': */
      if (!match->node.node)
	{
	  GList *list;
	  GList *matches;

	  /* A copy of the list: */
	  matches = g_list_copy (query->matches);
	  query->matches = g_list_remove (query->matches, match);

	  for (list = tb_json_object_get_nodes (object); list;
	       list = list->next)
	    {
	      if (tb_jsonpath_exec_query_value
		  (item, parent, object, query,
		   tb_json_node_get_value (list->data), result,
		   error) == FALSE)
		return FALSE;
	    }

	  g_list_free (query->matches);
	  query->matches = matches;

	  return TRUE;
	}

      /* No other options: */
      return TRUE;

    default:
      {
	GList *list;

	/* A copy of the list: */
	for (list = tb_json_object_get_nodes (object); list;
	     list = list->next)
	  {
	    if (tb_jsonpath_exec_query_value
		(item, parent, object, query,
		 tb_json_node_get_value (list->data), result, error) == FALSE)
	      return FALSE;
	  }

	return TRUE;
      }
      break;
    }

  return TRUE;
}

/* Recurive node search: */
static gboolean
tb_jsonpath_exec_query_node_rec (tb_jsonpath_item_t * item,
				 tb_json_object_t * parent,
				 tb_json_object_t * object,
				 tb_jsonpath_query_t * query, gchar * node,
				 GList ** result, GError ** error)
{
  GList *list;

  if (tb_json_object_has_node (object, node)
      && tb_jsonpath_exec_query_node (item, parent, object, query,
				      tb_json_object_get_node (object, node),
				      result, error) == FALSE)
    return FALSE;

  for (list = tb_json_object_get_nodes (object); list; list = list->next)
    {
      tb_json_value_t *value = tb_json_node_get_value (list->data);

      if (tb_json_value_get_type (value) == TB_JSON_VALUE_OBJECT
	  && tb_jsonpath_exec_query_node_rec (item, parent,
					      tb_json_value_get_object
					      (value), query, node, result,
					      error) == FALSE)
	return FALSE;

      if (tb_json_value_get_type (value) == TB_JSON_VALUE_ARRAY
	  && tb_jsonpath_exec_query_node_rec_array (item, parent,
						    tb_json_value_get_array
						    (value), query, node,
						    result, error) == FALSE)
	return FALSE;
    }

  return TRUE;
}

/* Recursive node search into array: */
static gboolean
tb_jsonpath_exec_query_node_rec_array (tb_jsonpath_item_t * item,
				       tb_json_object_t * parent,
				       tb_json_array_t * array,
				       tb_jsonpath_query_t * query,
				       gchar * node, GList ** result,
				       GError ** error)
{
  gint i, len;

  for (i = 0, len = tb_json_array_length (array); i < len; i++)
    {
      tb_json_value_t *value = tb_json_array_get (array, i);

      if (tb_json_value_get_type (value) == TB_JSON_VALUE_OBJECT
	  && tb_jsonpath_exec_query_node_rec (item, parent,
					      tb_json_value_get_object
					      (value), query, node, result,
					      error) == FALSE)
	return FALSE;

      if (tb_json_value_get_type (value) == TB_JSON_VALUE_ARRAY
	  && tb_jsonpath_exec_query_node_rec_array (item, parent,
						    tb_json_value_get_array
						    (value), query, node,
						    result, error) == FALSE)
	return FALSE;
    }

  return TRUE;
}

/* Match a single node: */
static gboolean
tb_jsonpath_exec_query_node (tb_jsonpath_item_t * item,
			     tb_json_object_t * parent,
			     tb_json_object_t * object,
			     tb_jsonpath_query_t * query,
			     tb_json_node_t * node, GList ** result,
			     GError ** error)
{
  tb_jsonpath_match_t *match;
  GList *matches;
  gchar *string;
  gboolean ret;

  string = tb_json_node_get_string (node);

  matches = g_list_copy (query->matches);
  match = query->matches->data;

  if (match->type == TB_JSONPATH_MATCH_TYPE_NODE
      && !g_utf8_collate (match->node.node, string))
    query->matches = g_list_remove (query->matches, match);

  ret =
    tb_jsonpath_exec_query_value (item, parent, object, query,
				  tb_json_node_get_value (node), result,
				  error);

  g_list_free (query->matches);
  query->matches = matches;

  return ret;
}

/* Check a single value: */
static gboolean
tb_jsonpath_exec_query_value (tb_jsonpath_item_t * item,
			      tb_json_object_t * parent,
			      tb_json_object_t * object,
			      tb_jsonpath_query_t * query,
			      tb_json_value_t * value, GList ** result,
			      GError ** error)
{
  tb_jsonpath_match_t *match;

  if (!query->matches)
    {
      tb_json_value_t *new;

      tb_json_value_duplicate (value, &new);
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
				   tb_json_object_t * parent,
				   tb_json_object_t * object,
				   tb_jsonpath_query_t * query,
				   tb_json_value_t * value, GList ** result,
				   GError ** error)
{
  if (tb_json_value_get_type (value) == TB_JSON_VALUE_OBJECT)
    return tb_jsonpath_exec_query_real (item, parent,
					tb_json_value_get_object (value),
					query, result, error);

  if (tb_json_value_get_type (value) == TB_JSON_VALUE_ARRAY)
    {
      tb_json_array_t *array = tb_json_value_get_array (value);
      gint i, len;

      for (i = 0, len = tb_json_array_length (array); i < len; i++)
	{
	  value = tb_json_array_get (array, i);

	  if (tb_jsonpath_exec_query_value
	      (item, parent, object, query, value, result, error) == FALSE)
	    return FALSE;
	}
    }

  return TRUE;
}

/* Exec the condition: */
static gboolean
tb_jsonpath_exec_query_condition (tb_jsonpath_item_t * item,
				  tb_json_object_t * parent,
				  tb_json_object_t * object,
				  tb_jsonpath_query_t * query,
				  tb_json_value_t * value, GList ** result,
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

  if (tb_json_value_get_type (value) == TB_JSON_VALUE_ARRAY)
    ret =
      tb_jsonpath_exec_query_condition_slice (item, parent, object, query,
					      tb_json_value_get_array (value),
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
					tb_json_object_t * parent,
					tb_json_object_t * object,
					tb_jsonpath_query_t * query,
					tb_json_array_t * array,
					tb_jsonpath_result_t * rstart,
					tb_jsonpath_result_t * rend,
					tb_jsonpath_result_t * rstep,
					GList ** result, GError ** error)
{
  tb_json_value_t *vstart, *vend, *vstep;
  gint start, end, step;
  gint len;

  if (!rstart || tb_jsonpath_result_next (rstart, &vstart) == FALSE
      || tb_json_value_get_type (vstart) != TB_JSON_VALUE_NUMBER)
    return TRUE;

  len = tb_json_array_length (array);
  start = (gint) tb_json_value_get_number (rstart->values->data);

  if (start < 0)
    start = len + start;

  if (rend && tb_jsonpath_result_next (rend, &vend) == TRUE
      && tb_json_value_get_type (vend) == TB_JSON_VALUE_NUMBER)
    {
      end = (gint) tb_json_value_get_number (vend);

      if (end < 0)
	end = len + end;

      end++;

      if (len < end || end >= start)
	end = len;
    }
  else
    end = start + 1;


  if (rstep && tb_jsonpath_result_next (rstep, &vstep) == TRUE
      && tb_json_value_get_type (vstep) == TB_JSON_VALUE_NUMBER)
    {
      step = (gint) tb_json_value_get_number (vstep);

      if (step <= 0)
	step = 1;
    }
  else
    step = 1;

  for (; start < end; start += step)
    {
      tb_json_value_t *value = tb_json_array_get (array, start);

      if (!value)
	continue;

      switch (tb_json_value_get_type (value))
	{
	case TB_JSON_VALUE_OBJECT:
	  {
	    tb_json_object_t *obj = tb_json_value_get_object (value);

	    if (tb_jsonpath_exec_query_value
		(item, parent, obj, query, value, result, error) == FALSE)
	      return FALSE;
	  }

	  break;

	case TB_JSON_VALUE_ARRAY:
	  {
	    tb_json_array_t *array = tb_json_value_get_array (value);
	    gint i, len;

	    for (i = 0, len = tb_json_array_length (array); i < len; i++)
	      {
		value = tb_json_array_get (array, i);

		if (tb_jsonpath_exec_query_value
		    (item, parent, object, query, value, result,
		     error) == FALSE)
		  return FALSE;
	      }
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
					  tb_json_object_t * parent,
					  tb_json_object_t * object,
					  tb_jsonpath_query_t *
					  query,
					  tb_json_value_t * value,
					  tb_jsonpath_result_t *
					  rstart,
					  tb_jsonpath_result_t *
					  rend,
					  tb_jsonpath_result_t *
					  rstep, GList ** result,
					  GError ** error)
{
  tb_json_value_t *tmp;

  /* It is not an array: */
  if (rend || rstep)
    return TRUE;

  while (tb_jsonpath_result_next (rstart, &tmp) == TRUE)
    {
      gboolean todo = FALSE;

      switch (tb_json_value_get_type (tmp))
	{
	case TB_JSON_VALUE_STRING:
	  {
	    gchar *string = tb_json_value_get_string (tmp);
	    todo = *string ? TRUE : FALSE;
	    break;
	  }

	case TB_JSON_VALUE_NUMBER:
	  {
	    gdouble numb = tb_json_value_get_number (tmp);
	    todo = numb ? TRUE : FALSE;
	    break;
	  }

	case TB_JSON_VALUE_OBJECT:
	case TB_JSON_VALUE_ARRAY:
	  todo = TRUE;
	  break;

	case TB_JSON_VALUE_BOOLEAN:
	  todo = tb_json_value_get_boolean (tmp);
	  break;

	case TB_JSON_VALUE_NULL:
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
			 tb_json_object_t * parent,
			 tb_json_object_t * object,
			 tb_jsonpath_script_t * script,
			 tb_jsonpath_result_t ** result, GError ** error)
{
  gint len;
  GList *args = NULL;
  gboolean ret = TRUE;
  tb_json_value_t *value;

  tb_jsonpath_function_t *func;

  if (!(func = tb_jsonpath_function_find (item, script->name)))
    {
      g_set_error (error, tb_jsonpath_error_quark (),
		   TB_ERROR_JSONPATH, "The function '%s' doesn't exist",
		   script->name);
      return FALSE;
    }

  len = g_list_length (script->items);

  if (func->numb_args != -1 && func->numb_args != len)
    {
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
	      tb_json_value_new (&value);
	      tmp->values = g_list_append (NULL, value);
	    }

	  args = g_list_append (args, tmp);
	}
    }

  tb_json_value_new (&value);
  *result = g_malloc0 (sizeof (tb_jsonpath_result_t));
  (*result)->values = g_list_append (NULL, value);

  ret = func->func (args, value, error);

  g_list_foreach (args, (GFunc) tb_jsonpath_result_free, NULL);
  g_list_free (args);

  return ret;
}

/* Filter exec */
static gboolean tb_jsonpath_exec_filter_value (tb_json_value_t * status);

static gboolean
tb_jsonpath_exec_filter_value (tb_json_value_t * status)
{
  switch (tb_json_value_get_type (status))
    {
    case TB_JSON_VALUE_STRING:
      {
	gchar *string = tb_json_value_get_string (status);
	if (*string)
	  return TRUE;

	return FALSE;
      }

    case TB_JSON_VALUE_NUMBER:
      if (tb_json_value_get_number (status))
	return TRUE;

      return FALSE;

    case TB_JSON_VALUE_OBJECT:
    case TB_JSON_VALUE_ARRAY:
      return TRUE;

    case TB_JSON_VALUE_BOOLEAN:
      return tb_json_value_get_boolean (status);

    case TB_JSON_VALUE_NULL:
      return FALSE;
    }

  return TRUE;
}

static gboolean tb_jsonpath_exec_filter_operation (tb_json_value_t *
						   first,
						   tb_json_value_t *
						   second,
						   tb_jsonpath_operation_type_t
						   operation,
						   tb_json_value_t *
						   status, GError ** error);

static gboolean
tb_jsonpath_exec_filter (tb_jsonpath_item_t * item,
			 tb_json_object_t * parent,
			 tb_json_object_t * object,
			 tb_jsonpath_filter_t * filter,
			 tb_jsonpath_result_t ** result, GError ** error)
{
  tb_jsonpath_result_t *first, *second;
  tb_json_value_t *value;
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

  tb_json_value_new (&value);
  *result = g_malloc0 (sizeof (tb_jsonpath_result_t));
  (*result)->values = g_list_append (NULL, value);

  ret =
    tb_jsonpath_exec_filter_operation (first->values->data,
				       second->values->data,
				       filter->operation, value, error);

  tb_jsonpath_result_free (first);
  tb_jsonpath_result_free (second);

  return ret;
}

static gboolean
tb_jsonpath_exec_filter_operation_and (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_or (tb_json_value_t
						      * first,
						      tb_json_value_t
						      * second,
						      tb_json_value_t
						      * status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_gt (tb_json_value_t
						      * first,
						      tb_json_value_t
						      * second,
						      tb_json_value_t
						      * status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_ge (tb_json_value_t
						      * first,
						      tb_json_value_t
						      * second,
						      tb_json_value_t
						      * status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_lt (tb_json_value_t
						      * first,
						      tb_json_value_t
						      * second,
						      tb_json_value_t
						      * status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_le (tb_json_value_t
						      * first,
						      tb_json_value_t
						      * second,
						      tb_json_value_t
						      * status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_ne (tb_json_value_t
						      * first,
						      tb_json_value_t
						      * second,
						      tb_json_value_t
						      * status,
						      GError ** error);
static gboolean tb_jsonpath_exec_filter_operation_eq (tb_json_value_t
						      * first,
						      tb_json_value_t
						      * second,
						      tb_json_value_t
						      * status,
						      GError ** error);
static gboolean
tb_jsonpath_exec_filter_operation_mul (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error);
static gboolean
tb_jsonpath_exec_filter_operation_div (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error);
static gboolean
tb_jsonpath_exec_filter_operation_add (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error);
static gboolean
tb_jsonpath_exec_filter_operation_sub (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error);

static gboolean
tb_jsonpath_exec_filter_operation (tb_json_value_t * first,
				   tb_json_value_t * second,
				   tb_jsonpath_operation_type_t
				   operation,
				   tb_json_value_t * status, GError ** error)
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
tb_jsonpath_exec_filter_operation_and (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error)
{
  if (tb_jsonpath_exec_filter_value (first) == TRUE
      && tb_jsonpath_exec_filter_value (second) == TRUE)
    tb_json_value_set_boolean (status, TRUE);
  else
    tb_json_value_set_boolean (status, FALSE);

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_or (tb_json_value_t * first,
				      tb_json_value_t * second,
				      tb_json_value_t * status,
				      GError ** error)
{
  if (tb_jsonpath_exec_filter_value (first) == TRUE
      || tb_jsonpath_exec_filter_value (second) == TRUE)
    tb_json_value_set_boolean (status, TRUE);
  else
    tb_json_value_set_boolean (status, FALSE);

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_gt (tb_json_value_t * first,
				      tb_json_value_t * second,
				      tb_json_value_t * status,
				      GError ** error)
{
  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  switch (tb_json_value_get_type (first))
    {
    case TB_JSON_VALUE_STRING:
      {
	gboolean ret;
	gchar *string1 = tb_json_value_get_string (first);
	gchar *string2 = tb_json_value_get_string (second);

	ret = g_utf8_collate (string1, string2);

	tb_json_value_set_boolean (status, ret == 1 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NUMBER:
      {
	gdouble number1 = tb_json_value_get_number (first);
	gdouble number2 = tb_json_value_get_number (second);

	tb_json_value_set_boolean (status, number1 > number2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_OBJECT:
    case TB_JSON_VALUE_ARRAY:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;

    case TB_JSON_VALUE_BOOLEAN:
      {
	gboolean boolean1 = tb_json_value_get_boolean (first);
	gboolean boolean2 = tb_json_value_get_boolean (second);

	tb_json_value_set_boolean (status,
				   boolean1 > boolean2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NULL:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_ge (tb_json_value_t * first,
				      tb_json_value_t * second,
				      tb_json_value_t * status,
				      GError ** error)
{
  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  switch (tb_json_value_get_type (first))
    {
    case TB_JSON_VALUE_STRING:
      {
	gboolean ret;
	gchar *string1 = tb_json_value_get_string (first);
	gchar *string2 = tb_json_value_get_string (second);

	ret = g_utf8_collate (string1, string2);

	tb_json_value_set_boolean (status, ret == 1
				   || ret == 0 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NUMBER:
      {
	gdouble number1 = tb_json_value_get_number (first);
	gdouble number2 = tb_json_value_get_number (second);

	tb_json_value_set_boolean (status, number1 >= number2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_OBJECT:
    case TB_JSON_VALUE_ARRAY:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;

    case TB_JSON_VALUE_BOOLEAN:
      {
	gboolean boolean1 = tb_json_value_get_boolean (first);
	gboolean boolean2 = tb_json_value_get_boolean (second);

	tb_json_value_set_boolean (status,
				   boolean1 >= boolean2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NULL:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_lt (tb_json_value_t * first,
				      tb_json_value_t * second,
				      tb_json_value_t * status,
				      GError ** error)
{
  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  switch (tb_json_value_get_type (first))
    {
    case TB_JSON_VALUE_STRING:
      {
	gboolean ret;
	gchar *string1 = tb_json_value_get_string (first);
	gchar *string2 = tb_json_value_get_string (second);

	ret = g_utf8_collate (string1, string2);

	tb_json_value_set_boolean (status, ret == -1 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NUMBER:
      {
	gdouble number1 = tb_json_value_get_number (first);
	gdouble number2 = tb_json_value_get_number (second);

	tb_json_value_set_boolean (status, number1 < number2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_OBJECT:
    case TB_JSON_VALUE_ARRAY:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;

    case TB_JSON_VALUE_BOOLEAN:
      {
	gboolean boolean1 = tb_json_value_get_boolean (first);
	gboolean boolean2 = tb_json_value_get_boolean (second);

	tb_json_value_set_boolean (status,
				   boolean1 < boolean2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NULL:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_le (tb_json_value_t * first,
				      tb_json_value_t * second,
				      tb_json_value_t * status,
				      GError ** error)
{
  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  switch (tb_json_value_get_type (first))
    {
    case TB_JSON_VALUE_STRING:
      {
	gboolean ret;
	gchar *string1 = tb_json_value_get_string (first);
	gchar *string2 = tb_json_value_get_string (second);

	ret = g_utf8_collate (string1, string2);

	tb_json_value_set_boolean (status, ret == 0
				   || ret == -1 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NUMBER:
      {
	gdouble number1 = tb_json_value_get_number (first);
	gdouble number2 = tb_json_value_get_number (second);

	tb_json_value_set_boolean (status, number1 <= number2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_OBJECT:
    case TB_JSON_VALUE_ARRAY:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;

    case TB_JSON_VALUE_BOOLEAN:
      {
	gboolean boolean1 = tb_json_value_get_boolean (first);
	gboolean boolean2 = tb_json_value_get_boolean (second);

	tb_json_value_set_boolean (status,
				   boolean1 <= boolean2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NULL:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_eq (tb_json_value_t * first,
				      tb_json_value_t * second,
				      tb_json_value_t * status,
				      GError ** error)
{
  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  switch (tb_json_value_get_type (first))
    {
    case TB_JSON_VALUE_STRING:
      {
	gboolean ret;
	gchar *string1 = tb_json_value_get_string (first);
	gchar *string2 = tb_json_value_get_string (second);

	ret = g_utf8_collate (string1, string2);

	tb_json_value_set_boolean (status, ret == 0 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NUMBER:
      {
	gdouble number1 = tb_json_value_get_number (first);
	gdouble number2 = tb_json_value_get_number (second);

	tb_json_value_set_boolean (status, number1 == number2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_OBJECT:
    case TB_JSON_VALUE_ARRAY:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;

    case TB_JSON_VALUE_BOOLEAN:
      {
	gboolean boolean1 = tb_json_value_get_boolean (first);
	gboolean boolean2 = tb_json_value_get_boolean (second);

	tb_json_value_set_boolean (status,
				   boolean1 == boolean2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NULL:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_ne (tb_json_value_t * first,
				      tb_json_value_t * second,
				      tb_json_value_t * status,
				      GError ** error)
{
  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  switch (tb_json_value_get_type (first))
    {
    case TB_JSON_VALUE_STRING:
      {
	gboolean ret;
	gchar *string1 = tb_json_value_get_string (first);
	gchar *string2 = tb_json_value_get_string (second);

	ret = g_utf8_collate (string1, string2);

	tb_json_value_set_boolean (status, ret != 0 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NUMBER:
      {
	gdouble number1 = tb_json_value_get_number (first);
	gdouble number2 = tb_json_value_get_number (second);

	tb_json_value_set_boolean (status, number1 != number2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_OBJECT:
    case TB_JSON_VALUE_ARRAY:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;

    case TB_JSON_VALUE_BOOLEAN:
      {
	gboolean boolean1 = tb_json_value_get_boolean (first);
	gboolean boolean2 = tb_json_value_get_boolean (second);

	tb_json_value_set_boolean (status,
				   boolean1 != boolean2 ? TRUE : FALSE);
	return TRUE;
      }

    case TB_JSON_VALUE_NULL:
      tb_json_value_set_boolean (status, FALSE);
      return TRUE;
    }

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_mul (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error)
{
  gdouble number1, number2;

  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_null (status);
      return TRUE;
    }

  if (tb_json_value_get_type (first) != TB_JSON_VALUE_NUMBER)
    {
      tb_json_value_set_null (status);
      return TRUE;
    }

  number1 = tb_json_value_get_number (first);
  number2 = tb_json_value_get_number (second);

  tb_json_value_set_number (status, number1 * number2);
  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_div (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error)
{
  gdouble number1, number2;

  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_null (status);
      return TRUE;
    }

  if (tb_json_value_get_type (first) != TB_JSON_VALUE_NUMBER)
    {
      tb_json_value_set_null (status);
      return TRUE;
    }

  number1 = tb_json_value_get_number (first);
  number2 = tb_json_value_get_number (second);

  if (number2)
    tb_json_value_set_number (status, number1 / number2);
  else
    tb_json_value_set_null (status);

  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_add (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error)
{
  gdouble number1, number2;

  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_null (status);
      return TRUE;
    }

  if (tb_json_value_get_type (first) != TB_JSON_VALUE_NUMBER)
    {
      tb_json_value_set_null (status);
      return TRUE;
    }

  number1 = tb_json_value_get_number (first);
  number2 = tb_json_value_get_number (second);

  tb_json_value_set_number (status, number1 + number2);
  return TRUE;
}

static gboolean
tb_jsonpath_exec_filter_operation_sub (tb_json_value_t * first,
				       tb_json_value_t * second,
				       tb_json_value_t * status,
				       GError ** error)
{
  gdouble number1, number2;

  if (tb_json_value_get_type (first) != tb_json_value_get_type (second))
    {
      tb_json_value_set_null (status);
      return TRUE;
    }

  if (tb_json_value_get_type (first) != TB_JSON_VALUE_NUMBER)
    {
      tb_json_value_set_null (status);
      return TRUE;
    }

  number1 = tb_json_value_get_number (first);
  number2 = tb_json_value_get_number (second);

  tb_json_value_set_number (status, number1 - number2);
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
 * JSONPath result. Il value will be NULL, no other results exist.
 */
gboolean
tb_jsonpath_result_next (tb_jsonpath_result_t * result,
			 tb_json_value_t ** value)
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
			 tb_json_value_t ** value)
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
    tb_json_value_destroy (result->current->data);

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
