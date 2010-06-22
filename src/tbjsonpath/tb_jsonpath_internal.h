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

#ifndef _TB_JSONPATH_INTERNAL_H_
#define _TB_JSONPATH_INTERNAL_H_

#include "tb_jsonpath.h"

typedef struct tb_jsonpath_query_t	tb_jsonpath_query_t;
typedef struct tb_jsonpath_match_t	tb_jsonpath_match_t;
typedef struct tb_jsonpath_filter_t	tb_jsonpath_filter_t;
typedef struct tb_jsonpath_item_t	tb_jsonpath_item_t;
typedef struct tb_jsonpath_script_t	tb_jsonpath_script_t;

typedef enum
{
  TB_JSONPATH_MATCH_TYPE_NODE,
  TB_JSONPATH_MATCH_TYPE_CONDITION
} tb_jsonpath_match_type_t;

struct tb_jsonpath_query_t
{
  gboolean	root;
  GList *	matches;
};

struct tb_jsonpath_match_t
{
  tb_jsonpath_match_type_t	type;

  union
  {
    /* Node: */
    struct
    {
      gchar *	node;
      gboolean	recursive;
    } node;

    /* Condition: */
    struct
    {
      tb_jsonpath_item_t *	start;
      tb_jsonpath_item_t *	end;
      tb_jsonpath_item_t *	step;
    } condition;
  };
};

typedef enum
{
  TB_JSONPATH_FILTER_AND,
  TB_JSONPATH_FILTER_OR,
  TB_JSONPATH_FILTER_OPERATION
} tb_jsonpath_filter_type_t;

typedef enum
{
  TB_JSONPATH_FILTER_OPERATION_AND,
  TB_JSONPATH_FILTER_OPERATION_OR,
  TB_JSONPATH_FILTER_OPERATION_GT,
  TB_JSONPATH_FILTER_OPERATION_GE,
  TB_JSONPATH_FILTER_OPERATION_LT,
  TB_JSONPATH_FILTER_OPERATION_LE,
  TB_JSONPATH_FILTER_OPERATION_NE,
  TB_JSONPATH_FILTER_OPERATION_EQ,
  TB_JSONPATH_FILTER_OPERATION_MUL,
  TB_JSONPATH_FILTER_OPERATION_DIV,
  TB_JSONPATH_FILTER_OPERATION_ADD,
  TB_JSONPATH_FILTER_OPERATION_SUB
} tb_jsonpath_operation_type_t;

struct tb_jsonpath_filter_t
{
  tb_jsonpath_item_t *		first;
  tb_jsonpath_item_t *		second;

  tb_jsonpath_operation_type_t	operation;
};

struct tb_jsonpath_item_t
{
  tb_jsonpath_filter_t *	filter;
  tb_jsonpath_script_t *	script;
  tb_jsonpath_query_t *		query;
  tb_json_value_t *		value;

  gboolean			is_operation;
  tb_jsonpath_operation_type_t	operation;

  tb_jsonpath_functions_t *	functions;
};

struct tb_jsonpath_script_t
{
  gchar *	name;
  GList *	items;
};

struct tb_jsonpath_result_t
{
  GList *	values;
  GList *	current;
};

gboolean	tb_jsonpath_parser	(gchar *		jsonpath,
					 gssize			size,
					 tb_jsonpath_item_t **	item_ret,
					 GError **		error);

void		tb_jsonpath_free	(tb_jsonpath_item_t *	item);

gboolean	tb_jsonpath_exec_real	(tb_jsonpath_item_t *	item,
					 tb_json_object_t *	parent,
					 tb_json_object_t *	object,
					 tb_jsonpath_result_t ** result,
					 GError **		error);

gboolean	tb_jsonpath_set_functions
					(tb_jsonpath_item_t *	item,
					 tb_jsonpath_functions_t *
					                      functions,
					 GError **		error);
#endif

/* EOF */
