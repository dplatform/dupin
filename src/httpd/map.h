#ifndef _DS_MAP_H_
#define _DS_MAP_H_

#include "dupin.h"

gboolean	map_init		(DSGlobal *	data,
					 GError **	error);

void		map_close		(DSGlobal *	data);

DSMap *		map_find		(DSGlobal *	data,
					 gchar *	filename,
					 time_t		mtime);

void		map_unref		(DSGlobal *	data,
					 DSMap *	map);

#endif
/* EOF */
