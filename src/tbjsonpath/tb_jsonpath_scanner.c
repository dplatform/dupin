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

#include "tb_jsonpath_scanner.h"

#include <string.h>
#include <stdlib.h>

static void tb_jsonpath_scanner_token_destroy (tb_jsonpath_scanner_token_t * token);
static void tb_jsonpath_scanner_token (tb_jsonpath_scanner_t * scanner,
			      tb_jsonpath_scanner_token_t * token);

/**
 * tb_jsonpath_scanner_new:
 * @error: a location for a GError, or NULL
 * @returns: a new tb_jsonpath_scanner_t data struct
 *
 * Creates a new tb_jsonpath_scanner_t data struct
 */
tb_jsonpath_scanner_t *
tb_jsonpath_scanner_new (GError ** error)
{
  tb_jsonpath_scanner_t *new;

  new = g_malloc0 (sizeof (tb_jsonpath_scanner_t));
  new->error = error;

  return new;
}

/**
 * tb_jsonpath_scanner_destroy:
 * @scanner: a tb_jsonpath_scanner_t data struct
 * 
 * Destroies a tb_jsonpath_scanner_t data struct
 */
void
tb_jsonpath_scanner_destroy (tb_jsonpath_scanner_t * scanner)
{
  if (!scanner)
    return;

  if (scanner->input_buffer)
    g_free (scanner->input_buffer);

  tb_jsonpath_scanner_token_destroy (&scanner->token);
  tb_jsonpath_scanner_token_destroy (&scanner->token_next);

  g_free (scanner);
}

static void
tb_jsonpath_scanner_token_destroy (tb_jsonpath_scanner_token_t * token)
{
  switch (token->type)
    {
    case TB_JSONPATH_SCANNER_NONE:
    case TB_JSONPATH_SCANNER_LEFT_PAREN:
    case TB_JSONPATH_SCANNER_RIGHT_PAREN:
    case TB_JSONPATH_SCANNER_LEFT_BRACE:
    case TB_JSONPATH_SCANNER_RIGHT_BRACE:
    case TB_JSONPATH_SCANNER_LEFT_CURLY:
    case TB_JSONPATH_SCANNER_RIGHT_CURLY:
    case TB_JSONPATH_SCANNER_COLON:
    case TB_JSONPATH_SCANNER_COMMA:
    case TB_JSONPATH_SCANNER_AT:
    case TB_JSONPATH_SCANNER_DOLLAR:

    case TB_JSONPATH_SCANNER_EOF:
      break;

    case TB_JSONPATH_SCANNER_IDENTIFIER:
      g_free (token->identifier);
      break;

    case TB_JSONPATH_SCANNER_STRING:
      g_free (token->string);
      break;

    case TB_JSONPATH_SCANNER_NUMBER:
      break;
    }
}

/**
 * tb_jsonpath_scanner_error:
 * @scanner: the scanner
 * @returns: the GError pointer
 *
 * Returns the GError from the tb_jsonpath_scanner_new
 */
GError **
tb_jsonpath_scanner_error (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, NULL);
  return scanner->error;
}

/**
 * tb_jsonpath_scanner_input_text:
 * @scanner: the tb_jsonpath_scanner_t
 * @buffer: a buffer to parse
 * @size: the size of the buffer or -1
 *
 * This code inserts a buffer memory string into the scanner for the next
 * operations. The buffer will be copied.
 */
void
tb_jsonpath_scanner_input_text (tb_jsonpath_scanner_t * scanner, gchar * buffer, gssize size)
{
  g_return_if_fail (scanner != NULL);
  g_return_if_fail (buffer != NULL);

  if (scanner->input_buffer)
    g_free (scanner->input_buffer);

  if (size < 0)
    size = g_utf8_strlen (buffer, -1);

  scanner->input_buffer = scanner->input_cursor = g_strndup (buffer, size);
  scanner->line = 1;
  scanner->position = 0;
}

/**
 * tb_jsonpath_scanner_cur_value_string:
 * @scanner: the scanner
 * @returns: a internal string
 *
 * Returns the current token as a string if it is a string type. This string
 * is internal so, please, don't free it.
 */
gchar *
tb_jsonpath_scanner_cur_value_string (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, NULL);
  g_return_val_if_fail (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_STRING,
			NULL);

  return scanner->token.string;
}

/**
 * tb_jsonpath_scanner_cur_value_identifier:
 * @scanner: the scanner
 * @returns: the identifier string
 *
 * Returns the current token as a identifier string if it is a identifier type.
 * This string is a internal one. Don't free it.
 */
gchar *
tb_jsonpath_scanner_cur_value_identifier (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, NULL);
  g_return_val_if_fail (tb_jsonpath_scanner_cur_token (scanner) ==
			TB_JSONPATH_SCANNER_IDENTIFIER, NULL);

  return scanner->token.identifier;
}

/**
 * tb_jsonpath_scanner_cur_value_number:
 * @scanner: the scanner
 * @returns: a number
 *
 * returns a gdouble if the current token is a number
 */
gdouble
tb_jsonpath_scanner_cur_value_number (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, 0);
  g_return_val_if_fail (tb_jsonpath_scanner_cur_token (scanner) == TB_JSONPATH_SCANNER_NUMBER,
			0);

  return scanner->token.number;
}

/**
 * tb_jsonpath_scanner_cur_token:
 * @scanner: the scanner
 * @returns: the type of the current token
 *
 * Returns the type for the current token
 */
tb_jsonpath_scanner_token_type_t
tb_jsonpath_scanner_cur_token (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, TB_JSONPATH_SCANNER_NONE);

  return scanner->token.type;
}

/**
 * tb_jsonpath_scanner_get_next_token:
 * @scanner: scanner
 * @returns: the type of the next token
 *
 * Returns the type of the next token and moves it in the current position
 */
tb_jsonpath_scanner_token_type_t
tb_jsonpath_scanner_get_next_token (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, TB_JSONPATH_SCANNER_NONE);

  if (scanner->token_next.type == TB_JSONPATH_SCANNER_NONE)
    {
      tb_jsonpath_scanner_token_destroy (&scanner->token);
      tb_jsonpath_scanner_token (scanner, &scanner->token);
    }

  else
    {
      tb_jsonpath_scanner_token_destroy (&scanner->token);
      scanner->token = scanner->token_next;
      scanner->token_next.type = TB_JSONPATH_SCANNER_NONE;
    }

  return scanner->token.type;
}

/**
 * tb_jsonpath_scanner_peek_next_token:
 * @scanner: scanner
 * @returns: the type of the next token
 *
 * This function returns the type of the next token but doesn't move it in the
 * current position
 */
tb_jsonpath_scanner_token_type_t
tb_jsonpath_scanner_peek_next_token (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, TB_JSONPATH_SCANNER_NONE);

  if (scanner->token_next.type == TB_JSONPATH_SCANNER_NONE)
    tb_jsonpath_scanner_token (scanner, &scanner->token_next);

  return scanner->token_next.type;
}

/**
 * tb_jsonpath_scanner_get_cur_line:
 * @scanner: scanner
 * @returns: the current line
 *
 * Returns the current line
 */
gint
tb_jsonpath_scanner_get_cur_line (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, -1);

  return scanner->token.line;
}

/**
 * tb_jsonpath_scanner_get_cur_position:
 * @scanner: scanner
 * @returns: the current line
 *
 * Returns the current line
 */
gint
tb_jsonpath_scanner_get_cur_position (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, -1);

  return scanner->token.position;
}

/**
 * tb_jsonpath_scanner_unexp_token:
 * @scanner: the scanner
 * @type: the type that you are waiting
 * @what: a message, or NULL
 * 
 * This function writes a error message in the GError contained in the scanner
 * (do you rember the GError for the tb_jsonpath_scanner_new ?)
 */
void
tb_jsonpath_scanner_unexp_token (tb_jsonpath_scanner_t * scanner, tb_jsonpath_scanner_token_type_t type,
			gchar * what)
{
  GString *str = NULL;

  g_return_if_fail (scanner != NULL);

  switch (scanner->token.type)
    {
    case TB_JSONPATH_SCANNER_NONE:
      g_assert ("SCANNER NONE ?!?");
      break;

    case TB_JSONPATH_SCANNER_LEFT_PAREN:
    case TB_JSONPATH_SCANNER_RIGHT_PAREN:
    case TB_JSONPATH_SCANNER_LEFT_BRACE:
    case TB_JSONPATH_SCANNER_RIGHT_BRACE:
    case TB_JSONPATH_SCANNER_LEFT_CURLY:
    case TB_JSONPATH_SCANNER_RIGHT_CURLY:
    case TB_JSONPATH_SCANNER_COLON:
    case TB_JSONPATH_SCANNER_COMMA:
    case TB_JSONPATH_SCANNER_AT:
    case TB_JSONPATH_SCANNER_DOLLAR:
      str = g_string_new (NULL);
      g_string_append_printf (str, "unexpected '%c', expected",
			      scanner->token.type);
      break;

    case TB_JSONPATH_SCANNER_EOF:
      str = g_string_new ("unexpected end of file, expected");
      break;

    case TB_JSONPATH_SCANNER_IDENTIFIER:
      str = g_string_new (NULL);
      g_string_append_printf (str, "unexpected identifier '%s', expected",
			      scanner->token.identifier);
      break;

    case TB_JSONPATH_SCANNER_STRING:
      str = g_string_new (NULL);
      g_string_append_printf (str, "unexpected string '%s', expected",
			      scanner->token.string);
      break;

    case TB_JSONPATH_SCANNER_NUMBER:
      str = g_string_new (NULL);
      g_string_append_printf (str, "unexpected number '%f', expected",
			      scanner->token.number);
      break;
    }

  switch (type)
    {
    case TB_JSONPATH_SCANNER_NONE:
      g_assert ("SCANNER NONE ?!?");
      break;

    case TB_JSONPATH_SCANNER_LEFT_PAREN:
    case TB_JSONPATH_SCANNER_RIGHT_PAREN:
    case TB_JSONPATH_SCANNER_LEFT_BRACE:
    case TB_JSONPATH_SCANNER_RIGHT_BRACE:
    case TB_JSONPATH_SCANNER_LEFT_CURLY:
    case TB_JSONPATH_SCANNER_RIGHT_CURLY:
    case TB_JSONPATH_SCANNER_COLON:
    case TB_JSONPATH_SCANNER_COMMA:
    case TB_JSONPATH_SCANNER_AT:
    case TB_JSONPATH_SCANNER_DOLLAR:
      g_string_append_printf (str, " '%c'", type);
      break;

    case TB_JSONPATH_SCANNER_EOF:
      g_string_append_printf (str, " end of file");
      break;

    case TB_JSONPATH_SCANNER_IDENTIFIER:
      if (what)
	g_string_append_printf (str, " the identifier '%s'", what);
      else
	g_string_append_printf (str, " an identifier");
      break;

    case TB_JSONPATH_SCANNER_STRING:
      if (what)
	g_string_append_printf (str, " the string '%s'", what);
      else
	g_string_append_printf (str, " an string");
      break;

    case TB_JSONPATH_SCANNER_NUMBER:
      g_string_append_printf (str, " a number");
      break;
    }

  g_string_append_printf (str, " (Line: %d, Position: %d)",
			  scanner->token.line, scanner->token.position);

  if (scanner->error != NULL && *scanner->error == NULL)
    g_set_error ((GError **) scanner->error, tb_jsonpath_scanner_error_quark (),
	       TB_ERROR_SCANNER, "%s", str->str);

  g_string_free (str, TRUE);
}

static void tb_jsonpath_scanner_token_string (tb_jsonpath_scanner_t * scanner,
				     tb_jsonpath_scanner_token_t * token);
static void tb_jsonpath_scanner_token_something (tb_jsonpath_scanner_t * scanner,
					tb_jsonpath_scanner_token_t * token);
static gunichar tb_jsonpath_scanner_token_escape (tb_jsonpath_scanner_t * scanner);

static gunichar
tb_jsonpath_scanner_token_char (tb_jsonpath_scanner_t * scanner, gboolean * escape)
{
  gunichar ch;

  *escape = FALSE;

  if (!(ch = g_utf8_get_char (scanner->input_cursor)))
    return 0;

  if (ch == '\\')
    {
      *escape = TRUE;
      scanner->position++;

      scanner->input_cursor = g_utf8_next_char (scanner->input_cursor);

      if (!(ch = g_utf8_get_char (scanner->input_cursor)))
	return 0;

      switch (ch)
	{
	case 0x22:
	  return 0x22;

	case 0x5c:
	  return 0x5c;

	case 0x2f:
	  return 0x2f;

	case 0x62:
	  return 0x08;

	case 0x66:
	  return 0x0c;

	case 0x6e:
	  return 0x0a;

	case 0x72:
	  return 0x0d;

	case 0x74:
	  return 0x09;

	case 0x75:
	  scanner->input_cursor = g_utf8_next_char (scanner->input_cursor);
	  return tb_jsonpath_scanner_token_escape (scanner);

	default:
	  return '\\';
	}
    }

  if (ch == '\n')
    {
      scanner->line++;
      scanner->position = 0;
    }
  else
    scanner->position++;

  return ch;
}

static void
tb_jsonpath_scanner_token_char_next (tb_jsonpath_scanner_t * scanner)
{
  scanner->input_cursor = g_utf8_next_char (scanner->input_cursor);
}

static gunichar
tb_jsonpath_scanner_token_escape (tb_jsonpath_scanner_t * scanner)
{
  gunichar ch, ret;
  gboolean escape;
  gint i;

  for (i = 0, ret = 0; i < 4; i++)
    {
      ret *= 10;
      ch = tb_jsonpath_scanner_token_char (scanner, &escape);
      tb_jsonpath_scanner_token_char_next (scanner);

      if (escape == TRUE || !ch)
	return 0;

      if (ch >= '0' && ch <= '9')
	ret = ch - '0';
      else if (ch >= 'a' && ch <= 'f')
	ret = ch - 'a' + 10;
      else if (ch >= 'A' && ch <= 'F')
	ret = ch - 'A' + 10;
      else
	return 0;
    }

  return ret;
}

static void
tb_jsonpath_scanner_token (tb_jsonpath_scanner_t * scanner, tb_jsonpath_scanner_token_t * token)
{
  while (1)
    {
      gboolean escape;
      gunichar ch;

      if (!(ch = tb_jsonpath_scanner_token_char (scanner, &escape)))
	{
	  token->type = TB_JSONPATH_SCANNER_EOF;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;
	}

      switch (ch)
	{
	case TB_JSONPATH_SCANNER_LEFT_PAREN:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_LEFT_PAREN;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	case TB_JSONPATH_SCANNER_RIGHT_PAREN:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_RIGHT_PAREN;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	case TB_JSONPATH_SCANNER_LEFT_BRACE:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_LEFT_BRACE;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	case TB_JSONPATH_SCANNER_RIGHT_BRACE:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_RIGHT_BRACE;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	case TB_JSONPATH_SCANNER_LEFT_CURLY:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_LEFT_CURLY;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	case TB_JSONPATH_SCANNER_RIGHT_CURLY:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_RIGHT_CURLY;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	case TB_JSONPATH_SCANNER_COLON:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_COLON;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	case TB_JSONPATH_SCANNER_COMMA:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_COMMA;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	case TB_JSONPATH_SCANNER_AT:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_AT;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	case TB_JSONPATH_SCANNER_DOLLAR:
	  tb_jsonpath_scanner_token_char_next (scanner);

	  token->type = TB_JSONPATH_SCANNER_DOLLAR;
	  token->line = scanner->line;
	  token->position = scanner->position;
	  return;

	default:
	  if (g_unichar_isspace (ch) == TRUE)
	    {
	      tb_jsonpath_scanner_token_char_next (scanner);
	      continue;
	    }

	  if (ch == '\"' && escape == FALSE)
	    {
	      tb_jsonpath_scanner_token_char_next (scanner);
	      tb_jsonpath_scanner_token_string (scanner, token);
	      return;
	    }

	  tb_jsonpath_scanner_token_something (scanner, token);
	  return;
	}
    }
}

static void
tb_jsonpath_scanner_token_string (tb_jsonpath_scanner_t * scanner, tb_jsonpath_scanner_token_t * token)
{
  gboolean escape;
  GString *str;
  gunichar ch;

  str = g_string_new (NULL);
  token->line = scanner->line;
  token->position = scanner->position;

  while ((ch = tb_jsonpath_scanner_token_char (scanner, &escape))
	 && (ch != '\"' || escape == TRUE))
    {
      tb_jsonpath_scanner_token_char_next (scanner);
      str = g_string_append_unichar (str, ch);
    }

  if (ch == '\"')
    tb_jsonpath_scanner_token_char_next (scanner);

  token->type = TB_JSONPATH_SCANNER_STRING;
  token->string = g_string_free (str, FALSE);
}

static gboolean
tb_jsonpath_scanner_token_number (tb_jsonpath_scanner_t * scanner, tb_jsonpath_scanner_token_t * token,
			 gchar * string)
{
  gchar *ptr;
  GString *str;

  gboolean minus = FALSE;

  gint integer = 0;
  gint frac = 0;

  gboolean e_minus = FALSE;
  gboolean e_plus = FALSE;

  gint e = 0;

  ptr = string = g_strdup (string);

  if (*ptr == '-')
    {
      minus = TRUE;
      ptr++;
    }

  if (!*ptr)
    {
      g_free (string);
      return FALSE;
    }

  if (*ptr == '0')
    ptr++;

  else
    {
      while (*ptr >= '0' && *ptr <= '9')
	{
	  integer = integer * 10 + *ptr - '0';
	  ptr++;
	}
    }

  if (*ptr == '.')
    {
      ptr++;

      if (*ptr < '0' || *ptr > '9')
	{
	  g_free (string);
	  return FALSE;
	}

      while (*ptr >= '0' && *ptr <= '9')
	{
	  frac = frac * 10 + *ptr - '0';
	  ptr++;
	}
    }

  if (*ptr == 'e' || *ptr == 'E')
    {
      ptr++;

      if (*ptr == '-')
	{
	  e_minus = TRUE;
	  ptr++;
	}
      else if (*ptr == '+')
	{
	  e_plus = TRUE;
	  ptr++;
	}

      if (*ptr < '0' || *ptr > '9')
	{
	  g_free (string);
	  return FALSE;
	}

      while (*ptr >= '0' && *ptr <= '9')
	{
	  e = e * 10 + *ptr - '0';
	  ptr++;
	}
    }

  if (*ptr)
    {
      g_free (string);
      return FALSE;
    }

  g_free (string);

  str = g_string_new (NULL);

  if (minus)
    g_string_append_unichar (str, '-');

  g_string_append_printf (str, "%d", integer);

  if (frac)
    g_string_append_printf (str, ".%d", frac);

  if (e_minus || e_plus || e)
    {
      if (e_minus)
	g_string_append (str, "e-");
      else if (e_plus)
	g_string_append (str, "e+");
      else
	g_string_append (str, "e");

      g_string_append_printf (str, "%d", e);
    }

  token->type = TB_JSONPATH_SCANNER_NUMBER;
  string = g_string_free (str, FALSE);
  token->number = strtod (string, NULL);
  g_free (string);

  return TRUE;
}

static void
tb_jsonpath_scanner_token_something (tb_jsonpath_scanner_t * scanner,
			    tb_jsonpath_scanner_token_t * token)
{
  gboolean colon = FALSE;
  gboolean escape;
  GString *str;
  gunichar ch;
  gchar *out;

  str = g_string_new (NULL);

  token->line = scanner->line;
  token->position = scanner->position;

  while ((ch = tb_jsonpath_scanner_token_char (scanner, &escape)))
    {
      if (escape == FALSE)
	{
	  if (g_unichar_isspace (ch) == TRUE
	      || ch == '\n' || ch == TB_JSONPATH_SCANNER_LEFT_PAREN
	      || ch == TB_JSONPATH_SCANNER_RIGHT_PAREN
	      || ch == TB_JSONPATH_SCANNER_LEFT_BRACE
	      || ch == TB_JSONPATH_SCANNER_RIGHT_BRACE
	      || ch == TB_JSONPATH_SCANNER_LEFT_CURLY
	      || ch == TB_JSONPATH_SCANNER_RIGHT_CURLY
	      || ch == TB_JSONPATH_SCANNER_COMMA
	      || ch == TB_JSONPATH_SCANNER_AT || ch == TB_JSONPATH_SCANNER_DOLLAR)
	    break;

	  if (ch == TB_JSONPATH_SCANNER_COLON)
	    {
	      if (scanner->qname == TRUE)
		{
		  if (colon == TRUE)
		    break;
		  else
		    colon = TRUE;
		}
	      else
		break;
	    }
	}

      tb_jsonpath_scanner_token_char_next (scanner);
      str = g_string_append_unichar (str, ch);
    }

  out = g_string_free (str, FALSE);

  if (tb_jsonpath_scanner_token_number (scanner, token, out) == TRUE)
    {
      g_free (out);
      return;
    }

  token->type = TB_JSONPATH_SCANNER_IDENTIFIER;
  token->string = out;
}

/**
 * tb_jsonpath_scanner_set_qname:
 * @scanner: the scanner
 * @qname: a boolean value, TRUE to enable this function
 *
 * Actives the qname support
 */
void
tb_jsonpath_scanner_set_qname (tb_jsonpath_scanner_t * scanner, gboolean qname)
{
  g_return_if_fail (scanner != NULL);
  scanner->qname = qname;
}

/**
 * tb_jsonpath_scanner_get_qname:
 * @scanner: the scanner
 * @returns: if this scanner  uses the qname function or not
 *
 * Returns TRUE if this scanner uses the qname function
 */
gboolean
tb_jsonpath_scanner_get_qname (tb_jsonpath_scanner_t * scanner)
{
  g_return_val_if_fail (scanner != NULL, FALSE);
  return scanner->qname;
}

/* ERRORS **********************************************************/
/**
 * tb_jsonpath_scanner_error_quark:
 * @returns: The error domain of for the tb scanner
 *
 * The error domain for the tb scanner
 **/
GQuark
tb_jsonpath_scanner_error_quark (void)
{
  return g_quark_from_static_string ("jsonpath-scanner-error-quark");
}

/* EOF */
