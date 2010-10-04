#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <string.h>

#include "dupin.h"
#include "httpd.h"
#include "map.h"

/* INITIALIZE ***************************************************************/
void map_free (DSMap * map);

/* Generic init: */
gboolean
map_init (DSGlobal * data, GError ** error)
{
  data->map_mutex = g_mutex_new ();
  data->map_table =
    g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
			   (GDestroyNotify) map_free);

  return TRUE;
}

/* Close the socket */
void
map_close (DSGlobal * data)
{
  if (data->map_mutex)
    g_mutex_free (data->map_mutex);

  if (data->map_table)
    g_hash_table_destroy (data->map_table);

  if (data->map_unreflist)
    g_list_free (data->map_unreflist);
}

void
map_free (DSMap * map)
{
  if (map->filename)
    g_free (map->filename);

  if (map->map)
    g_mapped_file_free (map->map);

  if (map->mime)
    g_free (map->mime);

  g_free (map);
}

/* MAP FUNC ****************************************************************/
DSMap *
map_find (DSGlobal * data, gchar * filename, time_t mtime)
{
  DSMap *map;

  g_mutex_lock (data->map_mutex);

  if (!(map = g_hash_table_lookup (data->map_table, filename)))
    {
      GMappedFile *mf;
      struct stat st;

      if (g_stat (filename, &st) != 0)
	{
	  g_mutex_unlock (data->map_mutex);
	  return NULL;
	}

      if (data->limit_cachemaxfilesize != 0
	  && st.st_size >= data->limit_cachemaxfilesize)
	{
	  g_mutex_unlock (data->map_mutex);
	  return NULL;
	}

      if (data->limit_cachesize != 0
	  && g_hash_table_size (data->map_table) >= data->limit_cachesize)
	{
	  if (!data->map_unreflist)
	    {
	      g_mutex_unlock (data->map_mutex);
	      return NULL;
	    }

	  map = data->map_unreflist->data;
	  g_hash_table_remove (data->map_table, map->filename);
	  data->map_unreflist = g_list_remove (data->map_unreflist, map);
	}

      if (!(mf = g_mapped_file_new (filename, FALSE, NULL)))
	{
	  g_mutex_unlock (data->map_mutex);
	  return NULL;
	}

      map = g_malloc0 (sizeof (DSMap));

      map->filename = g_strdup (filename);
      map->mtime = st.st_mtime;
      map->map = mf;

      gboolean uncertain;

#ifndef MIMEGUESS_STRICT
      if (g_str_has_suffix (filename, ".html") == TRUE
	  || g_str_has_suffix (filename, ".htm") == TRUE)
	map->mime = g_strdup (HTTP_MIME_TEXTHTML);

      else if (g_str_has_suffix (filename, ".css") == TRUE)
	map->mime = g_strdup ("text/css");

      else if (g_str_has_suffix (filename, ".png") == TRUE)
	map->mime = g_strdup ("image/png");

      else if (g_str_has_suffix (filename, ".js") == TRUE)
	map->mime = g_strdup ("application/javascript");

      else if (!(map->mime = g_content_type_guess (filename, NULL, 0, &uncertain)))
	map->mime = g_strdup (HTTP_MIME_TEXTHTML);
#else
      if (!(map->mime = g_content_type_guess (filename, NULL, 0, &uncertain)))
	map->mime = g_strdup (HTTP_MIME_TEXTHTML);
#endif

      if( uncertain == TRUE )
	{
	  map->mime = g_strdup (HTTP_MIME_TEXTHTML);
	}

      g_hash_table_insert (data->map_table, g_strdup (filename), map);
    }

  if (map->mtime != mtime)
    {
      if (map->unrefnode)
	data->map_unreflist =
	  g_list_delete_link (data->map_unreflist, map->unrefnode);

      g_hash_table_remove (data->map_table, map->filename);
      g_mutex_unlock (data->map_mutex);
      return NULL;
    }

  map->ref++;

  if (map->unrefnode)
    {
      data->map_unreflist =
	g_list_delete_link (data->map_unreflist, map->unrefnode);
      map->unrefnode = NULL;
    }

  g_mutex_unlock (data->map_mutex);

  return map;
}

/* UNREF *******************************************************************/
void
map_unref (DSGlobal * data, DSMap * map)
{
  g_mutex_lock (data->map_mutex);

  map->ref--;

  if (map->ref == 0)
    {
      data->map_unreflist = g_list_prepend (data->map_unreflist, map);
      map->unrefnode = data->map_unreflist;
    }

  g_mutex_unlock (data->map_mutex);
}

/* EOF */
