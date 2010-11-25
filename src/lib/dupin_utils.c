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

/* EOF */
