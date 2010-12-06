#ifndef _DUPIN_ATTACHMENT_DB_H_
#define _DUPIN_ATTACHMENT_DB_H_

#include <dupin.h>
#include <dupin_db.h>

G_BEGIN_DECLS

gchar **	dupin_get_attachment_dbs		(Dupin *	d);

gboolean	dupin_attachment_db_exists		(Dupin *	d,
					 		 gchar *	attachment_db);

DupinAttachmentDB *
		dupin_attachment_db_open		(Dupin *	d,
					 		 gchar *	attachment_db,
					 		 GError **	error);

DupinAttachmentDB *
		dupin_attachment_db_new			(Dupin *	d,
					 		 gchar *	attachment_db,
					 		 gchar *	parent,
					 		 GError **	error);

void		dupin_attachment_db_ref			(DupinAttachmentDB *	attachment_db);

void		dupin_attachment_db_unref		(DupinAttachmentDB *	attachment_db);

gboolean	dupin_attachment_db_delete		(DupinAttachmentDB *	attachment_db,
					 		 GError **	error);

gboolean        dupin_attachment_db_force_quit   	(DupinAttachmentDB *    attachment_db,
					 		 GError **	error);

const gchar *	dupin_attachment_db_get_name		(DupinAttachmentDB *	attachment_db);

const gchar *	dupin_attachment_db_get_parent		(DupinAttachmentDB *	attachment_db);

gsize		dupin_attachment_db_get_size		(DupinAttachmentDB *	attachment_db);

gsize		dupin_attachment_db_count		(DupinAttachmentDB *	attachment_db);

G_END_DECLS

#endif

/* EOF */
