#ifndef _DUPIN_UTILS_H_
#define _DUPIN_UTILS_H_

#include <dupin.h>

G_BEGIN_DECLS

#define DUPIN_UTIL_DUMP_JSON(msg, node) do { \
        gchar * string = dupin_util_json_serialize (node); \
	g_message ("%s: %s", msg, string); \
	g_free (string); \
        } while (0)

gboolean	dupin_util_is_valid_db_name	(gchar *	db);

gboolean	dupin_util_is_valid_linkb_name	(gchar *	linkb);

gboolean	dupin_util_is_valid_view_name	(gchar *	view);

gboolean	dupin_util_is_valid_attachment_db_name
						(gchar * attachment_db);

gboolean	dupin_util_is_valid_record_id	(gchar *	id);

gboolean	dupin_util_is_valid_absolute_uri
						(gchar *	uri);

gchar *		dupin_util_json_strescape	(const gchar *	string);

gchar *		dupin_util_json_serialize	(JsonNode * node);

JsonNode *	dupin_util_json_node_clone	(JsonNode * node);

gboolean	dupin_util_is_valid_obj		(JsonObject *obj);

void		dupin_util_generate_id		(gchar		id[DUPIN_ID_MAX_LEN]);

gsize		dupin_util_timestamp_now	();

gchar *		dupin_util_timestamp_to_iso8601	(gsize timestamp);

gboolean	dupin_util_is_valid_mr_lang	(gchar *	lang);

DupinMRLang	dupin_util_mr_lang_to_enum	(gchar *	lang);

const gchar *	dupin_util_mr_lang_to_string	(DupinMRLang	lang);

gchar *		dupin_util_utf8_normalize	(const gchar *text);

gchar *		dupin_util_utf8_casefold_normalize
						(const gchar *text);

gint		dupin_util_utf8_compare
						(const gchar *t1, const gchar *t2);

gint		dupin_util_utf8_ncompare
						(const gchar *t1, const gchar *t2);

gint		dupin_util_utf8_casecmp		(const gchar *t1, const gchar *t2);

gint		dupin_util_utf8_ncasecmp	(const gchar *t1, const gchar *t2);

gchar *		dupin_util_utf8_create_key_gen 	(const gchar *text, gint case_sen,
						 gchar * (*keygen) (const gchar * text, gssize size));

gchar *		dupin_util_utf8_create_key	(const gchar *text, gint case_sen);

gchar *		dupin_util_utf8_create_key_for_filename
						(const gchar *text, gint case_sen);

gboolean        dupin_util_mvcc_new   		(guint revision,
                                       		 gchar * hash,
                                         	 gchar mvcc[DUPIN_ID_MAX_LEN]);

gboolean        dupin_util_is_valid_mvcc	(gchar * mvcc);

gint		dupin_util_mvcc_revision_cmp	(gchar * mvcc_a,
						 gchar * mvcc_b);

gboolean        dupin_util_mvcc_get_revision	(gchar * mvcc,
                                         	 guint * revision);

gboolean        dupin_util_mvcc_get_hash	(gchar * mvcc,
                                         	 gchar hash[DUPIN_ID_HASH_ALGO_LEN]);

DupinCollateType
		dupin_util_get_collate_type	(JsonNode * node);

int		dupin_util_collation		(void        * ref,
						 int         left_len,
						 const void  *left_void,
						 int         right_len,
						 const void  *right_void);

int		dupin_util_collation_compare_pair
						(JsonNode * left_node,
						 JsonNode * right_node);

gchar *        dupin_util_json_string_normalize	(gchar * input_string);

gchar *        dupin_util_json_string_normalize_docid
						(gchar * input_string_docid);

/* k/v pairs for argument lists */

typedef struct dupin_keyvalue_t    dupin_keyvalue_t;

struct dupin_keyvalue_t
{
  gchar *       key;
  gchar *       value;
};

dupin_keyvalue_t * dupin_keyvalue_new         (gchar *        key,
                                         gchar *        value) G_GNUC_WARN_UNUSED_RESULT;

void            dupin_keyvalue_destroy     (dupin_keyvalue_t * data);

G_END_DECLS

#endif

/* EOF */
