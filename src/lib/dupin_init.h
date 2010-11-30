#ifndef _DUPIN_INIT_H_
#define _DUPIN_INIT_H_

#include <dupin.h>

G_BEGIN_DECLS

typedef struct dupin_t	Dupin;

typedef struct ds_global_t DSGlobal;

Dupin *		dupin_init		(DSGlobal *data,
					 GError **	error);

void		dupin_shutdown		(Dupin *	d);

G_END_DECLS

#endif

/* EOF */
