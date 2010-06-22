#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin.h"

#include "log.h"
#include "httpd.h"
#include "configure.h"
#include "map.h"

#include <stdlib.h>

#ifdef G_OS_UNIX
#  include <sys/types.h>
#  include <unistd.h>
#  include <stdlib.h>
#  include <grp.h>
#  include <pwd.h>
#  include <signal.h>
#  include <sys/wait.h>
#endif

#ifdef G_OS_UNIX
static gboolean pid_check (DSGlobal * data, GError ** error);
static gboolean pid_check_write (DSGlobal * data, GError ** error);
static void pid_check_close (DSGlobal * data);
static gboolean permission (DSGlobal * data, GError ** error);
#endif

int
main (int argc, char **argv)
{
  GError *error = NULL;
  DSGlobal *data;

  g_thread_init (NULL);

  /* Read the config file: */
  if (!(data = configure_init (argc, argv, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
      return 1;
    }

#ifdef G_OS_UNIX
  /* Background: */
  if (data->background == TRUE && fork ())
    exit (0);
#endif

#ifdef G_OS_UNIX
  /* Pid check: */
  if (pid_check (data, &error) == FALSE)
    {
      fprintf (stderr, "Error about the Pid File: %s\n", error->message);
      goto main_error_pid;
    }
#endif

  /* Open the log file: */
  if (log_open (data, &error) == FALSE)
    {
      fprintf (stderr, "Error about the Log File: %s\n", error->message);
      goto main_error_log;
    }

#ifdef G_OS_UNIX
  /* Permissions */
  if (permission (data, &error) == FALSE)
    {
      fprintf (stderr, "Error about the permissions: %s\n", error->message);
      goto main_error_permission;
    }
#endif

#ifdef G_OS_UNIX
  signal (SIGPIPE, SIG_IGN);
#endif

  /* LibDupin: */
  if (!(data->dupin = dupin_init (&error)))
    {
      fprintf (stderr, "Error connecting to the database: %s\n",
	       error->message);
      goto main_error_dupin;
    }

  /* Mapped file cache: */
  if (map_init (data, &error) == FALSE)
    {
      fprintf (stderr, "Error activing the cache: %s\n", error->message);
      goto main_error_map;
    }

  /* HTTP Server: */
  if (httpd_init (data, &error) == FALSE)
    {
      fprintf (stderr, "Error activing the network interface: %s\n",
	       error->message);
      goto main_error_httpd;
    }

  log_write (data, LOG_VERBOSE_INFO, LOG_STARTUP, "pid", LOG_VERBOSE_INFO,
	     LOG_TYPE_INTEGER, getpid (), NULL);

  g_get_current_time (&data->start_timeval);

  /* Glib Loop: */
  data->loop = g_main_loop_new (NULL, FALSE);
  g_main_loop_run (data->loop);

  httpd_close (data);

  map_close (data);

  dupin_shutdown (data->dupin);

  log_write (data, LOG_VERBOSE_INFO, LOG_QUIT, NULL);

  /* Close everything: */
  g_main_loop_unref (data->loop);

  log_close (data);

#ifdef G_OS_UNIX
  pid_check_close (data);
#endif

  /* Free the memory: */
  configure_free (data);
  return 0;

main_error_httpd:
  map_close (data);

main_error_map:
  dupin_shutdown (data->dupin);

main_error_dupin:
  g_error_free (error);

main_error_permission:
  log_close (data);

main_error_log:
#ifdef G_OS_UNIX
  pid_check_close (data);
#endif

main_error_pid:
  configure_free (data);
  return 1;
}

/* LOG SYSTEM OF GLIB *******************************************************/
GQuark
ds_error_quark (void)
{
  static GQuark q = 0;

  if (!q)
    q = g_quark_from_static_string ("dupin-server-error-quark");

  return q;
}

/* PID CHECK ****************************************************************/
#ifdef G_OS_UNIX
static gboolean
pid_check (DSGlobal * data, GError ** error)
{
  return TRUE;

  gchar *buffer;
  gint pid;

  if (g_file_test (data->pidfile, G_FILE_TEST_EXISTS) == FALSE)
    return pid_check_write (data, error);

  if (g_file_get_contents (data->pidfile, &buffer, NULL, error) == FALSE)
    return FALSE;

  if (!(pid = atoi (buffer)))
    {
      g_set_error (error, ds_error_quark (), 0,
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
      g_set_error (error, ds_error_quark (), 0,
		   "The previous process '%d' is running!", pid);
      g_free (buffer);
      return FALSE;
    }

  g_free (buffer);
  return pid_check_write (data, error);
}

static gboolean
pid_check_write (DSGlobal * data, GError ** error)
{
  gchar *buffer;
  gboolean ret;

  buffer = g_strdup_printf ("%d", getpid ());

  ret = g_file_set_contents (data->pidfile, buffer, 0, error);
  g_free (buffer);
  return ret;
}

static void
pid_check_close (DSGlobal * data)
{
  g_remove (data->pidfile);
}

/* PERMISSIONS **************************************************************/
static gboolean
permission (DSGlobal * data, GError ** error)
{
  if (data->group)
    {
      struct group *gr;

      if (!(gr = getgrnam (data->group)) || setgid (gr->gr_gid))
	{
	  g_set_error (error, ds_error_quark (), 0,
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
	  g_set_error (error, ds_error_quark (), 0,
		       "Error setting the USER permission of '%s'",
		       data->user);

	  return FALSE;
	}
    }

  return TRUE;
}
#endif
