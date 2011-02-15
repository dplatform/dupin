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

gboolean
dupin_util_is_valid_record_id (gchar * id)
{
  g_return_val_if_fail (id != NULL, FALSE);

  /* FIXME: something else? */

 /* TODO - consider '/' as valid document id sub-parts I.e. _design/foo/bar-attachment */

  return (strlen(id)<=DUPIN_ID_MAX_LEN) ? TRUE : FALSE;
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
void
dupin_util_generate_id (gchar id[DUPIN_ID_MAX_LEN])
{
  gchar guid[32];

  static const unsigned char rchars[] =
	"abcdefghijklmnopqrstuvwxyz"
	"0123456789";
   gint i;

   sqlite3_randomness(sizeof(guid), id);

   for (i=0; i<sizeof(guid); i++)
     {
       id[i] = rchars[ id[i] % (sizeof(rchars)-1) ];
     }
   id[sizeof(guid)] = '\0';

   gchar *md5 = g_compute_checksum_for_string (DUPIN_ID_HASH_ALGO, id, 32);

   snprintf (id, DUPIN_ID_MAX_LEN, "%s", md5);   

   g_free (md5);
}

gsize
dupin_util_timestamp_now ()
{
  gsize ttime=0;
  GTimeVal tnow;

  /* time in nanoseconds */
  g_get_current_time(&tnow); 
  ttime= tnow.tv_sec * 1000000 + tnow.tv_usec;

  return ttime;
}

gboolean
dupin_util_is_valid_mr_lang (gchar * lang)
{
  g_return_val_if_fail (lang != NULL, FALSE);

  if (!g_strcmp0 (lang, "javascript"))
    return TRUE;

  return FALSE;
}


DupinMRLang
dupin_util_mr_lang_to_enum (gchar * lang)
{
  g_return_val_if_fail (lang != NULL, 0);

  if (!g_strcmp0 (lang, "javascript"))
    return DP_MR_LANG_JAVASCRIPT;

  if (!g_strcmp0 (lang, "dupin_gi"))
    return DP_MR_LANG_DUPIN_GI;

  g_return_val_if_fail (dupin_util_is_valid_mr_lang (lang) == TRUE, 0);
  return 0;
}

const gchar *
dupin_util_mr_lang_to_string (DupinMRLang lang)
{
  switch (lang)
    {
    case DP_MR_LANG_JAVASCRIPT:
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

      json_generator_set_root (gen, node);

      node_serialized = json_generator_to_data (gen,NULL);

      g_object_unref (gen);
    }

  return node_serialized;
}

/* NOTE - proper deep copy/clone of given JsonNode */

JsonNode *
dupin_util_json_node_clone (JsonNode * node)
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
				dupin_util_json_node_clone (
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
        json_array_add_element(array, dupin_util_json_node_clone (n->data));
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
