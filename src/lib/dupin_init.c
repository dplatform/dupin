#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_init.h"

#include "../httpd/configure.h"

#include <string.h>

Dupin *
dupin_init (DSGlobal *data, GError ** error)
{
  Dupin *d;
  GDir *dir;
  const gchar *filename;

  if (g_file_test (DUPIN_DB_PATH, G_FILE_TEST_IS_DIR) == FALSE)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_INIT,
		   "Directory '%s' doesn't exist.", DUPIN_DB_PATH);
      return NULL;
    }

  if (!(dir = g_dir_open (DUPIN_DB_PATH, 0, error)))
    return NULL;

  d = g_malloc0 (sizeof (Dupin));

  d->conf = data; /* we just copy point from caller */

  d->mutex = g_mutex_new ();
  d->path = g_strdup (DUPIN_DB_PATH);

  d->dbs =
    g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
			   (GDestroyNotify) dupin_db_free);
  d->views =
    g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
			   (GDestroyNotify) dupin_view_free);

  d->sync_map_workers_pool = g_thread_pool_new (dupin_view_sync_map_func,
					        NULL,
						(d->conf != NULL) ? d->conf->limit_map_max_threads : 4,
						FALSE,
						NULL);

  d->sync_reduce_workers_pool = g_thread_pool_new (dupin_view_sync_reduce_func,
					           NULL,
						   (d->conf != NULL) ? d->conf->limit_reduce_max_threads : 4,
						   FALSE,
						   NULL);

  while ((filename = g_dir_read_name (dir)))
    {
      DupinDB *db;
      gchar *path;
      gchar *name;

      if (g_str_has_suffix (filename, DUPIN_DB_SUFFIX) == FALSE)
	continue;

      path = g_build_path (G_DIR_SEPARATOR_S, d->path, filename, NULL);

      name = g_strdup (filename);
      name[strlen (filename) - DUPIN_DB_SUFFIX_LEN] = 0;

      if (!(db = dupin_db_create (d, name, path, error)))
	{
	  dupin_shutdown (d);
	  g_free (path);
	  g_free (name);
	  return NULL;
	}

      g_hash_table_insert (d->dbs, g_strdup (name), db);
      g_free (path);
      g_free (name);
    }

  g_dir_rewind (dir);

  while ((filename = g_dir_read_name (dir)))
    {
      DupinView *view;
      gchar *path;
      gchar *name;

      if (g_str_has_suffix (filename, DUPIN_VIEW_SUFFIX) == FALSE)
	continue;

      path = g_build_path (G_DIR_SEPARATOR_S, d->path, filename, NULL);

      name = g_strdup (filename);
      name[strlen (filename) - DUPIN_VIEW_SUFFIX_LEN] = 0;

      if (!(view = dupin_view_create (d, name, path, error)))
	{
	  dupin_shutdown (d);
	  g_free (path);
	  g_free (name);
	  return NULL;
	}

      if (dupin_view_p_update (view, error) == FALSE)
	{
	  dupin_shutdown (d);
	  g_free (path);
	  g_free (name);
	  return NULL;
	}

      dupin_view_sync (view);

      g_hash_table_insert (d->views, g_strdup (name), view);
      g_free (path);
      g_free (name);
    }

  g_dir_close (dir);

  return d;
}

void
dupin_shutdown (Dupin * d)
{
  g_return_if_fail (d != NULL);

  /* NOTE - wait until all map and reduce threads are done */

  g_thread_pool_free (d->sync_map_workers_pool, TRUE, TRUE);
  g_thread_pool_free (d->sync_reduce_workers_pool, TRUE, TRUE);

g_message("dupin_shutdown: map and reduce worker pools freed\n");

  if (d->mutex)
    g_mutex_free (d->mutex);

  if (d->views)
    g_hash_table_destroy (d->views);

  if (d->dbs)
    g_hash_table_destroy (d->dbs);

  if (d->path)
    g_free (d->path);

  g_free (d);
}

/* Quark: */
GQuark
dupin_error_quark (void)
{
  return g_quark_from_static_string ("dupin-error-quark");
}

/* EOF */
