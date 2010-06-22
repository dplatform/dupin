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

#ifndef _TB_JSON_H_
#define _TB_JSON_H_

#include <glib.h>

G_BEGIN_DECLS

typedef struct tb_json_t		tb_json_t;
typedef struct tb_json_object_t		tb_json_object_t;
typedef struct tb_json_node_t		tb_json_node_t;
typedef struct tb_json_array_t		tb_json_array_t;
typedef struct tb_json_value_t		tb_json_value_t;

typedef enum
{
  TB_JSON_VALUE_NULL = 0,
  TB_JSON_VALUE_STRING,
  TB_JSON_VALUE_NUMBER,
  TB_JSON_VALUE_OBJECT,
  TB_JSON_VALUE_ARRAY,
  TB_JSON_VALUE_BOOLEAN
} tb_json_value_type_t;

/* Data */
tb_json_t *	tb_json_new			(void) G_GNUC_WARN_UNUSED_RESULT;

gboolean	tb_json_load_from_file		(tb_json_t *	data,
						 gchar *	filename,
						 GError **	error);

gboolean	tb_json_load_from_buffer	(tb_json_t *	data,
						 gchar *	buffer,
						 gssize		size,
						 GError **	error);

void		tb_json_destroy			(tb_json_t *	data);						 
gboolean	tb_json_is_object		(tb_json_t *	data);

gboolean	tb_json_is_array		(tb_json_t *	data);

tb_json_object_t *
		tb_json_object			(tb_json_t *	data);

tb_json_object_t *
		tb_json_object_and_detach	(tb_json_t *	data);

tb_json_array_t *
		tb_json_array			(tb_json_t *	data);

tb_json_array_t *
		tb_json_array_and_detach	(tb_json_t *	data);

/* Write */
gboolean	tb_json_write_to_file		(tb_json_t *	data,
						 gchar *	filename,
						 GError **	error);

gboolean	tb_json_write_to_buffer		(tb_json_t *	data,
						 gchar **	buffer,
						 gsize *	size,
						 GError **	error);

/* Object */
gboolean	tb_json_object_write_to_file	(tb_json_object_t *	object,
						 gchar *		filename,
						 GError **		error);

gboolean	tb_json_object_write_to_buffer	(tb_json_object_t *	object,
						 gchar **		buffer,
						 gsize *		size,
						 GError **		error);

gboolean	tb_json_object_new		(tb_json_object_t **	object);

void		tb_json_object_destroy		(tb_json_object_t *	object);

gboolean	tb_json_object_duplicate	(tb_json_object_t *	src,
						 tb_json_object_t **	dest);

tb_json_t *	tb_json_object_to_data		(tb_json_object_t *	object);

GList *		tb_json_object_get_nodes	(tb_json_object_t *	object);

tb_json_node_t *
		tb_json_object_get_node		(tb_json_object_t *	object,
						 gchar *		node);

gboolean	tb_json_object_has_node		(tb_json_object_t *	object,
						 gchar *		node);

gboolean	tb_json_object_add_node		(tb_json_object_t *	object,
						 gchar *		string,
						 tb_json_node_t **	node);

gboolean	tb_json_object_remove_node	(tb_json_object_t *	object,
						 tb_json_node_t *	node);

tb_json_value_t *
		tb_json_object_parent		(tb_json_object_t *	object);

/* Node */
void		tb_json_node_destroy		(tb_json_node_t *	node);

tb_json_value_t *
		tb_json_node_get_value		(tb_json_node_t *	node);

gchar *		tb_json_node_get_string		(tb_json_node_t *	node);

gboolean	tb_json_node_set_string		(tb_json_node_t *	node,
						 gchar *		string);

tb_json_object_t *
		tb_json_node_parent		(tb_json_node_t *	node);

/* Value */
gboolean	tb_json_value_new		(tb_json_value_t **	value);

void		tb_json_value_destroy		(tb_json_value_t *	value);

gboolean	tb_json_value_duplicate		(tb_json_value_t *	src,
						 tb_json_value_t **	dest);

gboolean	tb_json_value_duplicate_exists	(tb_json_value_t *	src,

						 tb_json_value_t *	dest);
tb_json_value_type_t
		tb_json_value_get_type		(tb_json_value_t *	value);

gboolean	tb_json_value_set_string	(tb_json_value_t *	value,
						 gchar *		string);

gchar *		tb_json_value_get_string	(tb_json_value_t *	value);

gboolean	tb_json_value_set_number	(tb_json_value_t *	value,
						 gdouble		number);

gdouble		tb_json_value_get_number	(tb_json_value_t *	value);

gboolean	tb_json_value_set_object	(tb_json_value_t *	value,
						 tb_json_object_t *	object);

gboolean	tb_json_value_set_object_new	(tb_json_value_t *	value,
						 tb_json_object_t **	object);

tb_json_object_t *
		tb_json_value_get_object	(tb_json_value_t *	value);

gboolean	tb_json_value_set_boolean	(tb_json_value_t *	value,
						 gboolean		boolean);

gboolean	tb_json_value_get_boolean	(tb_json_value_t *	value);

gboolean	tb_json_value_set_null		(tb_json_value_t *	value);

gboolean	tb_json_value_set_array		(tb_json_value_t *	value,
						 tb_json_array_t *	array);

gboolean	tb_json_value_set_array_new	(tb_json_value_t *	value,
						 tb_json_array_t **	array);

tb_json_array_t *
		tb_json_value_get_array		(tb_json_value_t *	value);

gboolean	tb_json_value_evaluate		(tb_json_value_t *	value,
						 gchar *		buffer,
						 gssize			size,
						 GError **		error);

gboolean	tb_json_value_parent_is_node	(tb_json_value_t *	value);

gboolean	tb_json_value_parent_is_array	(tb_json_value_t *	value);

tb_json_node_t *
		tb_json_value_parent_node	(tb_json_value_t *	value);

tb_json_array_t *
		tb_json_value_parent_array	(tb_json_value_t *	value);

/* Array */
gboolean	tb_json_array_write_to_file	(tb_json_array_t *	array,
						 gchar *		filename,
						 GError **		error);

gboolean	tb_json_array_write_to_buffer	(tb_json_array_t *	array,
						 gchar **		buffer,
						 gsize *		size,
						 GError **		error);

gboolean	tb_json_array_new		(tb_json_array_t **	array);

void		tb_json_array_destroy		(tb_json_array_t *	array);

gboolean	tb_json_array_duplicate		(tb_json_array_t *	src,
						 tb_json_array_t **	dest);

tb_json_t *	tb_json_array_to_data		(tb_json_array_t *	array);

guint		tb_json_array_length		(tb_json_array_t *	array);

gboolean	tb_json_array_remove		(tb_json_array_t *	array,
						 guint			id);

gboolean	tb_json_array_add		(tb_json_array_t *	array,
						 guint *		id,
						 tb_json_value_t **	value);

tb_json_value_t *
		tb_json_array_get		(tb_json_array_t *	array,
						 guint			id);

tb_json_value_t *
		tb_json_array_parent		(tb_json_array_t *	array);

/* ERROR */
enum {
  TB_ERROR_JSON
};

GQuark     	tb_json_error_quark		(void);	

G_END_DECLS

#endif

/* EOF */
