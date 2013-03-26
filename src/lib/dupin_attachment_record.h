#ifndef _DUPIN_ATTACHMENT_RECORD_H_
#define _DUPIN_ATTACHMENT_RECORD_H_

#include <dupin.h>

G_BEGIN_DECLS

/* TODO - check, bug ? shouldn't be DupinAttachmentRecord ?
          at the moment it is a special case due we need to access DupinRecord revision
          information when adding/updating the attachment - we just been lazy? */

gboolean	dupin_attachment_record_create
					(DupinAttachmentDB * attachment_db,
                                	 gchar *       id,
                                	 gchar *       title,
                                	 gsize         length,
                                	 gchar *       type,
                                	 const void ** content);

gboolean	dupin_attachment_record_delete
					(DupinAttachmentDB * attachment_db,
                                	 gchar *        id,
                                	 gchar *        title);

gboolean	dupin_attachment_record_delete_all
					(DupinAttachmentDB * attachment_db,
                                	 gchar *        id);

gboolean	dupin_attachment_record_exists
					(DupinAttachmentDB * attachment_db,
                                	 gchar *        id,
                                	 gchar *        title);

DupinAttachmentRecord *
		dupin_attachment_record_read
					(DupinAttachmentDB *		attachment_db,
                                	 gchar *        id,
                                	 gchar *        title,
					 GError **		error);

/* get max rowid for attachment_db DB */
gboolean        dupin_attachment_record_get_max_rowid
					(DupinAttachmentDB * attachment_db,
					 gsize * max_rowid);

/* List of DupinAttachmentRecord: */

gsize		dupin_attachment_record_get_list_total
					(DupinAttachmentDB * attachment_db,
                                         gsize rowid_start,
					 gsize rowid_end,
                                         gchar * id,
                                         gchar * start_title,
                                         gchar * end_title,
                                         gboolean inclusive_end,
					 GError ** error);

gboolean	dupin_attachment_record_get_list
					(DupinAttachmentDB * attachment_db,
					 guint count,
					 guint offset,
                            		 gsize rowid_start,
					 gsize rowid_end,
                            		 DupinOrderByType orderby_type,
                            		 gboolean descending,
                            		 gchar * id,
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

gboolean	dupin_attachment_record_blob_open
					(DupinAttachmentRecord * record,
					 gboolean read_write);

gboolean	dupin_attachment_record_blob_close
					(DupinAttachmentRecord * record);

gboolean	dupin_attachment_record_blob_read
					(DupinAttachmentRecord * record,
					 gchar *buf,
                                         gsize count,
                                         gsize offset,
                                         gsize *bytes_read,
                                         GError **error);

gboolean	dupin_attachment_record_blob_write
					(DupinAttachmentRecord * record,
					 const gchar *buf,
				         gsize count,
                                         gsize offset,
                                         gsize *bytes_written,
                                         GError **error);

gchar *		dupin_attachment_record_get_aggregated_hash
					(DupinAttachmentDB * attachment_db,
                                         gchar *        id);

/* insert = create or update */

gboolean	dupin_attachment_record_insert
					(DupinAttachmentDB * attachment_db,
                                	 gchar * id,
                                	 gchar * caller_mvcc,
                                	 GList * title_parts,
                                	 gsize  attachment_body_size,
                                	 gchar * attachment_input_mime,
                                	 const void ** attachment_body, // try to avoid to pass megabytes on stack
                                	 GList ** response_list,
					 GError ** error);

G_END_DECLS

#endif

/* EOF */
