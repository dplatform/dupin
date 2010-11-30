#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_utils.h"
#include "dupin_internal.h"

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
dupin_util_is_valid_view_name (gchar * view)
{
  g_return_val_if_fail (view != NULL, FALSE);

  if (*view == '_')
    return FALSE;

  /* FIXME: something else? */

  return TRUE;
}

gboolean
dupin_util_is_valid_record_id (gchar * id)
{
  g_return_val_if_fail (id != NULL, FALSE);

  /* FIXME: something else? */

  return (strlen(id)<=255) ? TRUE : FALSE;
}

/* see also http://engineering.twitter.com/2010/06/announcing-snowflake.html */

/* roughly we want an ID which is unique per thread, machine/server and sequential, and sortable */
void
dupin_util_generate_id (gchar id[255])
{
  GRand *rand;
  gint32 i;
  /*gsize ttime=0;
  GTimeVal tnow;*/

  /* TODO - rework this function to be network portable (indep. of NTP) and sequential etc */

  /* time in nanoseconds */
  /*
  g_get_current_time(&tnow); 
  ttime= tnow.tv_sec * 1000000 + tnow.tv_usec;
  */

  rand = g_rand_new ();

  i = g_rand_int_range (rand, 1, G_MAXINT32);
  /*snprintf (id, 255, "%X-%X", i, ttime);*/
  snprintf (id, 255, "%X", i);

  g_rand_free (rand);
}

gboolean
dupin_util_is_valid_mr_lang (gchar * lang)
{
  g_return_val_if_fail (lang != NULL, FALSE);

  if (!strcmp (lang, "javascript"))
    return TRUE;

  return FALSE;
}


DupinMRLang
dupin_util_mr_lang_to_enum (gchar * lang)
{
  g_return_val_if_fail (lang != NULL, 0);

  if (!strcmp (lang, "javascript"))
    return DP_MR_LANG_JAVASCRIPT;

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

/* EOF */
