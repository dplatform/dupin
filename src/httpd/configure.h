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

#define DS_LIMIT_TIMEOUT_DEFAULT		5
#define DS_LIMIT_CLIENTSFORTHREAD_DEFAULT	5
#define DS_LIMIT_TIMEOUTFORTHREAD_DEFAULT	2

DSGlobal *	configure_init	(int		argc,
				 char **	argv,
				 GError **	error);

void		configure_free	(DSGlobal *	data);

#endif

/* EOF */
