#ifndef _DUPIN_INIT_H_
#define _DUPIN_INIT_H_

#include <dupin.h>

G_BEGIN_DECLS

typedef struct dupin_t	Dupin;

Dupin *		dupin_init		(GError **	error);

void		dupin_shutdown		(Dupin *	d);

G_END_DECLS

#endif

/* EOF */
