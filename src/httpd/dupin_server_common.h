#ifndef _DUPIN_SERVER_COMMON_H_
#define _DUPIN_SERVER_COMMON_H_

#include "dupin.h"

#ifdef G_OS_UNIX
#  include <sys/types.h>
#  include <unistd.h>
#  include <stdlib.h>
#  include <grp.h>
#  include <pwd.h>
#  include <signal.h>
#  include <sys/wait.h>
#endif

GQuark          dupin_server_common_error_quark (void);

#ifdef G_OS_UNIX
gboolean dupin_server_common_pid_check (DSGlobal * data, GError ** error);
gboolean dupin_server_common_pid_check_write (DSGlobal * data, GError ** error);
void dupin_server_common_pid_check_close (DSGlobal * data);
gboolean dupin_server_common_permission (DSGlobal * data, GError ** error);
#endif

#endif

/* EOF */
