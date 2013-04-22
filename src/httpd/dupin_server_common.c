#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_server_common.h"

/* LOG SYSTEM OF GLIB *******************************************************/
GQuark
dupin_server_common_error_quark (void)
{
  static GQuark q = 0;

  if (!q)
    q = g_quark_from_static_string ("dupin-server-error-quark");

  return q;
}

/* PID CHECK ****************************************************************/
#ifdef G_OS_UNIX
gboolean
dupin_server_common_pid_check (DSGlobal * data, GError ** error)
{
  return TRUE;

  gchar *buffer;
  gint pid;

  if (g_file_test (data->pidfile, G_FILE_TEST_EXISTS) == FALSE)
    return dupin_server_common_pid_check_write (data, error);

  if (g_file_get_contents (data->pidfile, &buffer, NULL, error) == FALSE)
    return FALSE;

  if (!(pid = atoi (buffer)))
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_server_common_error_quark (), 0,
		   "Pid file contains strange data. Please, remove or check the file: %s",
		   data->pidfile);
      g_free (buffer);
      return FALSE;
    }

  g_free (buffer);

  /* Only for linux */
  buffer = g_strdup_printf ("/proc/%d", pid);

  if (g_file_test (buffer, G_FILE_TEST_EXISTS | G_FILE_TEST_IS_DIR) == TRUE)
    {
      if (error != NULL && *error != NULL)
        g_set_error (error, dupin_server_common_error_quark (), 0,
		   "The previous process '%d' is running!", pid);
      g_free (buffer);
      return FALSE;
    }

  g_free (buffer);
  return dupin_server_common_pid_check_write (data, error);
}

gboolean
dupin_server_common_pid_check_write (DSGlobal * data, GError ** error)
{
  gchar *buffer;
  gboolean ret;

  buffer = g_strdup_printf ("%d", getpid ());

  ret = g_file_set_contents (data->pidfile, buffer, 0, error);
  g_free (buffer);
  return ret;
}

void
dupin_server_common_pid_check_close (DSGlobal * data)
{
  g_remove (data->pidfile);
}

/* PERMISSIONS **************************************************************/
gboolean
dupin_server_common_permission (DSGlobal * data, GError ** error)
{
  if (data->group)
    {
      struct group *gr;

      if (!(gr = getgrnam (data->group)) || setgid (gr->gr_gid))
	{
	  if (error != NULL && *error != NULL)
	    g_set_error (error, dupin_server_common_error_quark (), 0,
		       "Error setting the GROUP permission of '%s'",
		       data->group);

	  return FALSE;
	}
    }

  if (data->user)
    {
      struct passwd *pw;

      if (!(pw = getpwnam (data->user)) || setuid (pw->pw_uid))
	{
	  if (error != NULL && *error != NULL)
	    g_set_error (error, dupin_server_common_error_quark (), 0,
		       "Error setting the USER permission of '%s'",
		       data->user);

	  return FALSE;
	}
    }

  return TRUE;
}
#endif
