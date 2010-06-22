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

#ifndef _TB_SCANNER_H_
#define _TB_SCANNER_H_

#include <glib.h>

G_BEGIN_DECLS

typedef struct tb_scanner_t		tb_scanner_t;
typedef struct tb_scanner_token_t	tb_scanner_token_t;

typedef enum
{
  TB_SCANNER_NONE = 0,

  TB_SCANNER_LEFT_PAREN = '(',
  TB_SCANNER_RIGHT_PAREN = ')',

  TB_SCANNER_LEFT_BRACE = '[',
  TB_SCANNER_RIGHT_BRACE = ']',

  TB_SCANNER_LEFT_CURLY = '{',
  TB_SCANNER_RIGHT_CURLY = '}',

  TB_SCANNER_COLON = ':',
  TB_SCANNER_COMMA = ',',

  TB_SCANNER_AT = '@',
  TB_SCANNER_DOLLAR = '$',

  TB_SCANNER_EOF = 255,

  TB_SCANNER_IDENTIFIER,
  TB_SCANNER_STRING,
  TB_SCANNER_NUMBER,

} tb_scanner_token_type_t;


tb_scanner_t *	tb_scanner_new		(GError **	error);

void		tb_scanner_destroy	(tb_scanner_t * scanner);

GError **	tb_scanner_error	(tb_scanner_t *	scanner);

void		tb_scanner_input_text	(tb_scanner_t * scanner,
					 gchar *	buffer,
					 gssize		size);

void		tb_scanner_set_qname	(tb_scanner_t * scanner,
					 gboolean	qname);

gboolean	tb_scanner_get_qname	(tb_scanner_t * scanner);
					 
gchar *		tb_scanner_cur_value_string
					(tb_scanner_t * scanner);

gchar *		tb_scanner_cur_value_identifier
					(tb_scanner_t * scanner);

gdouble		tb_scanner_cur_value_number
					(tb_scanner_t * scanner);

tb_scanner_token_type_t
		tb_scanner_cur_token	(tb_scanner_t * scanner);

tb_scanner_token_type_t
		tb_scanner_get_next_token
					(tb_scanner_t * scanner);

tb_scanner_token_type_t
		tb_scanner_peek_next_token
					(tb_scanner_t * scanner);

gint		tb_scanner_get_cur_line	(tb_scanner_t * scanner);

gint		tb_scanner_get_cur_position
					(tb_scanner_t * scanner);

void		tb_scanner_unexp_token	(tb_scanner_t * scanner,
					 tb_scanner_token_type_t type,
					 gchar *	what);

/* ERROR */
enum {
  TB_ERROR_SCANNER
};

GQuark     	tb_scanner_error_quark	(void);	

#ifdef TBCOMMON_INTERNAL
#  include "tb_scanner_internal.h"
#endif

G_END_DECLS

#endif

/* EOF */
