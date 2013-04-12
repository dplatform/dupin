#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin.h"

#include "log.h"
#include "httpd.h"
#include "configure.h"
#include "map.h"
#include "dupin_server_common.h"

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

/* TODO - turn this into #if defined(__APPLE__) */

#if defined(DARWIN) && !defined(__cplusplus) && !defined(_ANSI_SOURCE)
/* work around Darwin header file bugs
 *   http://www.opensource.apple.com/bugs/X/BSD%20Kernel/2657228.html
 */
#undef SIG_DFL
#undef SIG_IGN
#undef SIG_ERR
#define SIG_DFL (void (*)(int))0
#define SIG_IGN (void (*)(int))1
#define SIG_ERR (void (*)(int))-1
#endif

int
main (int argc, char **argv)
{
  GError *error = NULL;
  DSGlobal *data;

#if !GLIB_CHECK_VERSION (2, 31, 0)
  // better make double-sure glib itself is initialized properly.
  if (!g_thread_supported ())
	g_thread_init (NULL);
#endif

  g_type_init();

  /* Read the config file: */
  if (!(data = configure_init (argc, argv, &error)))
    {
      fprintf (stderr, "Error: %s\n", (error) ? error->message : DUPIN_UNKNOWN_ERROR);
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
  if (dupin_server_common_pid_check (data, &error) == FALSE)
    {
      fprintf (stderr, "Error about the Pid File: %s\n", (error) ? error->message : DUPIN_UNKNOWN_ERROR);
      goto main_error_pid;
    }
#endif

  /* Open the log file: */
  if (log_open (data, &error) == FALSE)
    {
      fprintf (stderr, "Error about the Log File: %s\n", (error) ? error->message : DUPIN_UNKNOWN_ERROR);
      goto main_error_log;
    }

#ifdef G_OS_UNIX
  /* Permissions */
  if (dupin_server_common_permission (data, &error) == FALSE)
    {
      fprintf (stderr, "Error about the permissions: %s\n", (error) ? error->message : DUPIN_UNKNOWN_ERROR);
      goto main_error_permission;
    }
#endif

#ifdef G_OS_UNIX
  signal (SIGPIPE, SIG_IGN);
#endif

  /* LibDupin: */
  if (!(data->dupin = dupin_init (data, &error)))
    {
      fprintf (stderr, "Error connecting to database: %s\n",
	       (error) ? error->message : DUPIN_UNKNOWN_ERROR);
      goto main_error_dupin;
    }

  /* Mapped file cache: */
  if (map_init (data, &error) == FALSE)
    {
      fprintf (stderr, "Error activing the cache: %s\n", (error) ? error->message : DUPIN_UNKNOWN_ERROR);
      goto main_error_map;
    }

  /* HTTP Server: */
  if (httpd_init (data, &error) == FALSE)
    {
      fprintf (stderr, "Error activing the network interface: %s\n",
	       (error) ? error->message : DUPIN_UNKNOWN_ERROR);
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
  dupin_server_common_pid_check_close (data);
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
  dupin_server_common_pid_check_close (data);
#endif

main_error_pid:
  configure_free (data);
  return 1;
}
