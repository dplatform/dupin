#ifndef _DS_CONFIGURE_H_
#define _DS_CONFIGURE_H_

#include "dupin.h"

#ifndef DS_CONFIG_FILE
#  define DS_CONFIG_FILE	"/etc/controller.cfg"
#endif

#ifndef DS_WWW_PATH
#  define DS_WWW_PATH		"/var/www"
#endif

#define DS_ROOT_TAG		"DupinServer"
#define DS_GENERAL_TAG		"General"
#define DS_LOGFILE_TAG		"LogFile"
#define DS_LOGVERBOSE_TAG	"LogVerbose"
#define DS_PIDFILE_TAG		"PidFile"
#define DS_BACKGROUND_TAG	"Background"
#define DS_USER_TAG		"User"
#define DS_GROUP_TAG		"Group"

#define DS_HTTPD_TAG		"Network"
#define DS_HTTPD_IPV6_TAG	"Ipv6"
#define DS_HTTPD_INTERFACE_TAG	"Interface"
#define DS_HTTPD_PORT_TAG	"Port"
#define DS_HTTPD_LISTEN_TAG	"Listen"

#define DS_HTTPD_LISTEN_DEFAULT	5

#define DS_LIMIT_TAG			"Limits"
#define DS_LIMIT_MAXHEADERS_TAG 	"MaxHeaders"
#define DS_LIMIT_MAXCLIENTS_TAG 	"MaxClients"
#define DS_LIMIT_MAXCONTENTLENGTH_TAG 	"MaxContentLength"
#define DS_LIMIT_CLIENTSFORTHREAD_TAG	"ClientsForThread"
#define DS_LIMIT_THREADNUMB_TAG		"ThreadNumb"
#define DS_LIMIT_TIMEOUT_TAG		"Timeout"
#define DS_LIMIT_TIMEOUTFORTHREAD_TAG	"TimeoutForThread"
#define DS_LIMIT_CACHESIZE_TAG		"CacheSize"
#define DS_LIMIT_CACHEMAXFILE_TAG	"CacheMaxFileSize"
#define DS_LIMIT_MAP_MAXTHREADS_TAG 	"MapMaxThreads"
#define DS_LIMIT_REDUCE_MAXTHREADS_TAG 	"ReduceMaxThreads"
#define DS_LIMIT_SYNC_INTERVAL_TAG	"SyncInterval"
#define DS_LIMIT_COMPACT_MAXTHREADS_TAG	"CompactMaxThreads"

#define DS_LIMIT_TIMEOUT_DEFAULT		5
#define DS_LIMIT_CLIENTSFORTHREAD_DEFAULT	5
#define DS_LIMIT_TIMEOUTFORTHREAD_DEFAULT	2
#define DS_LIMIT_MAP_MAXTHREADS_DEFAULT		4
#define DS_LIMIT_REDUCE_MAXTHREADS_DEFAULT	4
#define DS_LIMIT_SYNC_INTERVAL_DEFAULT		60 /* every minute */
#define DS_LIMIT_COMPACT_MAXTHREADS_DEFAULT	2

typedef enum {
  LOG_VERBOSE_ERROR,
  LOG_VERBOSE_WARNING,
  LOG_VERBOSE_INFO,
  LOG_VERBOSE_DEBUG
} LogVerbose;

struct ds_global_t
{
  gchar *       configfile;             /* Config File */

  GMutex *      logmutex;               /* Mutex about the log */
  GIOChannel *  logio;                  /* Log IO Channel */
  gchar *       logfile;                /* Log File */
  LogVerbose    logverbose;

  gboolean      background;             /* Demonize or not */
  gchar *       pidfile;                /* Pid File */

  gchar *       user;                   /* Permissions */
  gchar *       group;

  GMainLoop *   loop;

  /* Info about the socket: */
  gchar *       httpd_interface;
  gint          httpd_port;
  gint          httpd_listen;
  gboolean      httpd_ipv6;

  GIOChannel *  httpd_socket;
  GSource *     httpd_socket_source;
  gint          httpd_socket_fd;

  GMutex *      httpd_mutex;
  GList *       httpd_threads;

  guint         httpd_clients_numb;
  guint         httpd_threads_numb;

  guint         limit_maxheaders;
  guint         limit_maxclients;
  guint         limit_maxcontentlength;
  guint         limit_clientsforthread;
  guint         limit_threadnumb;

  guint         limit_timeout;
  guint         limit_timeoutforthread;
  guint         limit_cachesize;
  guint         limit_cachemaxfilesize;

  guint         limit_compact_max_threads;

  guint         limit_map_max_threads;
  guint         limit_reduce_max_threads;
  guint         limit_sync_interval;

  /* TimeVal: */
  GTimeVal      start_timeval;

  GMutex *      map_mutex;
  GHashTable *  map_table;
  GList *       map_unreflist;

  /* Dupin: */
  Dupin *       dupin;
};

DSGlobal *	configure_init	(int		argc,
				 char **	argv,
				 GError **	error);

void		configure_free	(DSGlobal *	data);

#endif

/* EOF */
