#ifndef _DUPIN_ATTACHMENT_RECORD_H_
#define _DUPIN_ATTACHMENT_RECORD_H_

#include <dupin.h>

G_BEGIN_DECLS

gboolean	dupin_attachment_record_insert
					(DupinAttachmentDB * attachment_db,
					 gchar *       id,
                                         guint         revision,
                                         gchar *       title,
                                         gsize         length,
                                         gchar *       type,
                                         gchar *       hash,
                                         const void *  content);

gboolean	dupin_attachment_record_delete
					(DupinAttachmentDB * attachment_db,
                                	 gchar *        id,
                                 	 guint          revision,
                                	 gchar *        title);

gboolean	dupin_attachment_record_exists
					(DupinAttachmentDB * attachment_db,
                                	 gchar *        id,
                                 	 guint          revision,
                                	 gchar *        title);

DupinAttachmentRecord *
		dupin_attachment_record_read
					(DupinAttachmentDB *		attachment_db,
                                	 gchar *        id,
                                 	 guint          revision,
                                	 gchar *        title,
					 GError **		error);

/* get total of records into attachment_db */
gboolean        dupin_attachment_record_get_total_records
					(DupinAttachmentDB * attachment_db,
                                         gsize * total,
                                         gchar * id,
                                         guint   revision,
                                         gchar * start_title,
                                         gchar * end_title,
                                         gboolean inclusive_end,
					 GError **		error);

/* get max rowid for attachment_db DB */
gboolean        dupin_attachment_record_get_max_rowid
					(DupinAttachmentDB * attachment_db,
					 gsize * max_rowid);

/* List of DupinAttachmentRecord: */
gboolean	dupin_attachment_record_get_list
					(DupinAttachmentDB * attachment_db,
					 guint count,
					 guint offset,
                            		 gsize rowid_start,
					 gsize rowid_end,
                            		 DupinOrderByType orderby_type,
                            		 gboolean descending,
                            		 gchar * id,
                            		 guint   revision,
                            		 gchar * start_title,
                            		 gchar * end_title,
                            		 gboolean inclusive_end,
                            		 GList ** list,
					 GError ** error);

void		dupin_attachment_record_get_list_close
					(GList *		list);

void		dupin_attachment_record_close
					(DupinAttachmentRecord *	record);

const gchar *	dupin_attachment_record_get_id
					(DupinAttachmentRecord *	record);

const gchar *	dupin_attachment_record_get_title
					(DupinAttachmentRecord *	record);

const gchar *	dupin_attachment_record_get_type
					(DupinAttachmentRecord *	record);

const gchar *	dupin_attachment_record_get_hash
					(DupinAttachmentRecord *	record);

gsize		dupin_attachment_record_get_length
					(DupinAttachmentRecord *	record);

gsize 	        dupin_attachment_record_get_rowid
					(DupinAttachmentRecord *	record);

JsonNode *
		dupin_attachment_record_get
					(DupinAttachmentRecord *	record);

G_END_DECLS

#endif

/* EOF */
