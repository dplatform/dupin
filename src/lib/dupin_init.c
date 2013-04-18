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

  if (g_file_test (data->sqlite_path, G_FILE_TEST_IS_DIR) == FALSE)
    {
      g_set_error (error, dupin_error_quark (), DUPIN_ERROR_INIT,
		   "Directory '%s' doesn't exist.", data->sqlite_path);
      return NULL;
    }

  if (!(dir = g_dir_open (data->sqlite_path, 0, error)))
    return NULL;

  d = g_malloc0 (sizeof (Dupin));

  d->conf = data; /* we just copy point from caller */

  d->rwlock = g_new0 (GRWLock, 1);
  g_rw_lock_init (d->rwlock);

  d->path = g_strdup (d->conf->sqlite_path);

  d->dbs =
    g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
			   (GDestroyNotify) dupin_db_disconnect);

  d->linkbs =
    g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
			   (GDestroyNotify) dupin_linkb_disconnect);

  d->views =
    g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
			   (GDestroyNotify) dupin_view_disconnect);

  d->attachment_dbs =
    g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
			   (GDestroyNotify) dupin_attachment_db_disconnect);

  d->db_compact_workers_pool = g_thread_pool_new (dupin_database_compact_func,
					        NULL,
						(d->conf != NULL) ? d->conf->limit_compact_max_threads : DS_LIMIT_COMPACT_MAXTHREADS_DEFAULT,
						FALSE,
						NULL);

  d->linkb_compact_workers_pool = g_thread_pool_new (dupin_linkbase_compact_func,
					        NULL,
						(d->conf != NULL) ? d->conf->limit_compact_max_threads : DS_LIMIT_COMPACT_MAXTHREADS_DEFAULT,
						FALSE,
						NULL);

  d->view_compact_workers_pool = g_thread_pool_new (dupin_view_compact_func,
					        NULL,
						(d->conf != NULL) ? d->conf->limit_compact_max_threads : DS_LIMIT_COMPACT_MAXTHREADS_DEFAULT,
						FALSE,
						NULL);

  d->linkb_check_workers_pool = g_thread_pool_new (dupin_linkbase_check_func,
					        NULL,
						(d->conf != NULL) ? d->conf->limit_checklinks_max_threads : DS_LIMIT_CHECKLINKS_MAXTHREADS_DEFAULT,
						FALSE,
						NULL);

  d->sync_map_workers_pool = g_thread_pool_new (dupin_view_sync_map_func,
					        NULL,
						(d->conf != NULL) ? d->conf->limit_map_max_threads : DS_LIMIT_MAP_MAXTHREADS_DEFAULT,
						FALSE,
						NULL);

  d->sync_reduce_workers_pool = g_thread_pool_new (dupin_view_sync_reduce_func,
					           NULL,
						   (d->conf != NULL) ? d->conf->limit_reduce_max_threads : DS_LIMIT_REDUCE_MAXTHREADS_DEFAULT,
						   FALSE,
						   NULL);

  d->bulk_transaction = FALSE;
  d->super_bulk_transaction = FALSE;

  while ((filename = g_dir_read_name (dir)))
    {
      DupinDB *db;
      gchar *path;
      gchar *name;

      if (g_str_has_suffix (filename, DUPIN_DB_SUFFIX) == FALSE)
	continue;

      /* NOTE - check if database needs to be connected */

      if (d->conf->sqlite_connect != NULL
	  && !g_regex_match (d->conf->sqlite_connect, filename, 0, NULL))
        {
#if DEBUG
	  g_message("dupin_init: skipped database %s (sqlite_connect)\n", filename);
#endif

          continue;
	}

      if (d->conf->sqlite_db_connect != NULL
	  && !g_regex_match (d->conf->sqlite_db_connect, filename, 0, NULL))
        {
#if DEBUG
	  g_message("dupin_init: skipped database %s (sqlite_db_connect)\n", filename);
#endif

          continue;
	}

      path = g_build_path (G_DIR_SEPARATOR_S, d->path, filename, NULL);

      name = g_strdup (filename);
      name[strlen (filename) - DUPIN_DB_SUFFIX_LEN] = 0;

      if (!(db = dupin_db_connect (d, name, path, d->conf->sqlite_db_mode, error)))
	{
	  dupin_shutdown (d);
	  g_free (path);
	  g_free (name);
	  return NULL;
	}

#if DEBUG
      g_message("dupin_init: connected database %s\n", name);
#endif

      g_hash_table_insert (d->dbs, g_strdup (name), db);
      g_free (path);
      g_free (name);
    }

  g_dir_rewind (dir);

  while ((filename = g_dir_read_name (dir)))
    {
      DupinLinkB *linkb;
      gchar *path;
      gchar *name;

      if (g_str_has_suffix (filename, DUPIN_LINKB_SUFFIX) == FALSE)
	continue;

      if (d->conf->sqlite_connect != NULL
	  && !g_regex_match (d->conf->sqlite_connect, filename, 0, NULL))
        {
#if DEBUG
          g_message("dupin_init: skipped linkbase %s (sqlite_connect)\n", filename);
#endif

          continue;
	}

      if (d->conf->sqlite_linkb_connect != NULL
	  && !g_regex_match (d->conf->sqlite_linkb_connect, filename, 0, NULL))
        {
#if DEBUG
          g_message("dupin_init: skipped linkbase %s (sqlite_linkb_connect)\n", filename);
#endif

          continue;
	}

      path = g_build_path (G_DIR_SEPARATOR_S, d->path, filename, NULL);

      name = g_strdup (filename);
      name[strlen (filename) - DUPIN_LINKB_SUFFIX_LEN] = 0;

      if (!(linkb = dupin_linkb_connect (d, name, path, d->conf->sqlite_linkb_mode, error)))
	{
	  dupin_shutdown (d);
	  g_free (path);
	  g_free (name);
	  return NULL;
	}

#if DEBUG
      g_message("dupin_init: connected linkbase %s\n", name);
#endif

      if (dupin_linkbase_p_update (linkb, error) == FALSE)
	{
	  dupin_shutdown (d);
	  g_free (path);
	  g_free (name);
	  return NULL;
	}

      g_hash_table_insert (d->linkbs, g_strdup (name), linkb);
      g_free (path);
      g_free (name);

      /* Set default parent linkbase if matching */

      if (g_hash_table_size (d->dbs))
        {
	  gpointer key;
	  gpointer value;
          GHashTableIter iter;

          g_hash_table_iter_init (&iter, d->dbs);
          while (g_hash_table_iter_next (&iter, &key, &value) == TRUE)
	    {
	      DupinDB * db = (DupinDB*) value;
	      DupinLinkB * default_linkbase = NULL;

              if ((!g_strcmp0 (dupin_database_get_default_linkbase_name (db), dupin_linkbase_get_name (linkb))) &&
		  (!(default_linkbase = dupin_database_get_default_linkbase (db))))
                {
                  db->default_linkbase = linkb;

                  dupin_linkbase_ref (linkb);
                } 
	    }
	}
    }

  g_dir_rewind (dir);

  while ((filename = g_dir_read_name (dir)))
    {
      DupinAttachmentDB *attachment_db;
      gchar *path;
      gchar *name;

      if (g_str_has_suffix (filename, DUPIN_ATTACHMENT_DB_SUFFIX) == FALSE)
	continue;

      if (d->conf->sqlite_connect != NULL
	  && !g_regex_match (d->conf->sqlite_connect, filename, 0, NULL))
        {
#if DEBUG
          g_message("dupin_init: skipped attachment database %s (sqlite_connect)\n", filename);
#endif

          continue;
	}

      if (d->conf->sqlite_attachment_db_connect != NULL
	  && !g_regex_match (d->conf->sqlite_attachment_db_connect, filename, 0, NULL))
        {
#if DEBUG
          g_message("dupin_init: skipped attachment database %s (sqlite_attachment_db_connect)\n", filename);
#endif

          continue;
	}

      path = g_build_path (G_DIR_SEPARATOR_S, d->path, filename, NULL);

      name = g_strdup (filename);
      name[strlen (filename) - DUPIN_ATTACHMENT_DB_SUFFIX_LEN] = 0;

      if (!(attachment_db = dupin_attachment_db_connect (d, name, path, d->conf->sqlite_attachment_db_mode, error)))
	{
	  dupin_shutdown (d);
	  g_free (path);
	  g_free (name);
	  return NULL;
	}

#if DEBUG
      g_message("dupin_init: connected attachment database %s\n", name);
#endif

      if (dupin_attachment_db_p_update (attachment_db, error) == FALSE)
	{
	  dupin_shutdown (d);
	  g_free (path);
	  g_free (name);
	  return NULL;
	}

      g_hash_table_insert (d->attachment_dbs, g_strdup (name), attachment_db);
      g_free (path);
      g_free (name);

      /* Set default parent attachment database if matching */

      if (g_hash_table_size (d->dbs))
        {
          gpointer key;
          gpointer value;
          GHashTableIter iter;

          g_hash_table_iter_init (&iter, d->dbs);
          while (g_hash_table_iter_next (&iter, &key, &value) == TRUE)
            {
              DupinDB * db = (DupinDB*) value;
	      DupinAttachmentDB * default_attachment_db = NULL;

	      if ((!g_strcmp0 (dupin_database_get_default_attachment_db_name (db), dupin_attachment_db_get_name (attachment_db))) &&
		  (!(default_attachment_db = dupin_database_get_default_attachment_db (db))))
                {
                  db->default_attachment_db = attachment_db;

		  dupin_attachment_db_ref (attachment_db);
                }
            }
        }
    }

  g_dir_rewind (dir);

  while ((filename = g_dir_read_name (dir)))
    {
      DupinView *view;
      gchar *path;
      gchar *name;

      if (g_str_has_suffix (filename, DUPIN_VIEW_SUFFIX) == FALSE)
	continue;

      if (d->conf->sqlite_connect != NULL
	  && !g_regex_match (d->conf->sqlite_connect, filename, 0, NULL))
        {
#if DEBUG
          g_message("dupin_init: skipped view %s (sqlite_connect)\n", filename);
#endif

          continue;
	}

      if (d->conf->sqlite_view_connect != NULL
	  && !g_regex_match (d->conf->sqlite_view_connect, filename, 0, NULL))
        {
#if DEBUG
          g_message("dupin_init: skipped view %s (sqlite_view_connect)\n", filename);
#endif

          continue;
	}

      path = g_build_path (G_DIR_SEPARATOR_S, d->path, filename, NULL);

      name = g_strdup (filename);
      name[strlen (filename) - DUPIN_VIEW_SUFFIX_LEN] = 0;

      if (!(view = dupin_view_connect (d, name, path, d->conf->sqlite_view_mode, error)))
	{
	  dupin_shutdown (d);
	  g_free (path);
	  g_free (name);
	  return NULL;
	}

#if DEBUG
      g_message("dupin_init: connected view %s\n", name);
#endif

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

  g_thread_pool_free (d->db_compact_workers_pool, TRUE, TRUE);
  g_thread_pool_free (d->linkb_compact_workers_pool, TRUE, TRUE);
  g_thread_pool_free (d->view_compact_workers_pool, TRUE, TRUE);
  g_thread_pool_free (d->linkb_check_workers_pool, TRUE, TRUE);
  g_thread_pool_free (d->sync_map_workers_pool, TRUE, TRUE);
  g_thread_pool_free (d->sync_reduce_workers_pool, TRUE, TRUE);

#if DEBUG
  g_message("dupin_shutdown: worker pools freed\n");
#endif

  if (d->views)
    g_hash_table_destroy (d->views);

  if (d->attachment_dbs)
    g_hash_table_destroy (d->attachment_dbs);

  if (d->linkbs)
    g_hash_table_destroy (d->linkbs);

  if (d->dbs)
    g_hash_table_destroy (d->dbs);

  if (d->path)
    g_free (d->path);

  if (d->rwlock)
    {
      g_rw_lock_clear (d->rwlock);
      g_free (d->rwlock);
    }

  g_free (d);
}

/* Quark: */
GQuark
dupin_error_quark (void)
{
  return g_quark_from_static_string ("dupin-error-quark");
}

/* EOF */
