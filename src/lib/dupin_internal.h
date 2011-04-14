#ifndef _DUPIN_INTERNAL_H_
#define _DUPIN_INTERNAL_H_

/* see GChecksumType */
#define DUPIN_ID_MAX_LEN	255
#define DUPIN_ID_HASH_ALGO	G_CHECKSUM_SHA1
#define DUPIN_ID_HASH_ALGO_LEN	32

/* see dupin_link_record.c */
#define DUPIN_LINKS_PATH_CACHE	100000

#include "dupin.h"

#include <glib/gstdio.h>
#include <sqlite3.h>

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#ifdef WEBKIT_FRAMEWORK
#  include <JavaScriptCore.h>
#else
#  include <JavaScriptCore/JavaScript.h>
#endif

#define DUPIN_DB_SUFFIX		".db.dupin"
#define DUPIN_DB_SUFFIX_LEN	9

#define DUPIN_VIEW_SUFFIX	".view.dupin"
#define DUPIN_VIEW_SUFFIX_LEN	11

#define DUPIN_ATTACHMENT_DB_SUFFIX	".attachments.dupin"
#define DUPIN_ATTACHMENT_DB_SUFFIX_LEN	18

#define DUPIN_LINKB_SUFFIX	".linkb.dupin"
#define DUPIN_LINKB_SUFFIX_LEN	12

struct dupin_t
{
  GMutex *	mutex;

  gchar *	path;
  GHashTable *	dbs;
  GHashTable *	views;
  GHashTable *	attachment_dbs;
  GHashTable *	linkbs;

  DSGlobal *	conf;

  GThreadPool * db_compact_workers_pool;
  GThreadPool * linkb_compact_workers_pool;
  GThreadPool * linkb_check_workers_pool;
  GThreadPool * sync_map_workers_pool;
  GThreadPool * sync_reduce_workers_pool;
};

typedef struct dupin_linkb_p_t DupinLinkBP;
struct dupin_linkb_p_t
{
  DupinLinkB ** linkbs;

  gsize		numb;
  gsize		size;
};

typedef struct dupin_attachment_db_p_t DupinAttachmentDBP;
struct dupin_attachment_db_p_t
{
  DupinAttachmentDB **	attachment_dbs;

  gsize		numb;
  gsize		size;
};

typedef struct dupin_view_p_t DupinViewP;
struct dupin_view_p_t
{
  DupinView **	views;

  gsize		numb;
  gsize		size;
};

struct dupin_db_t
{
  Dupin *	d;
  GMutex *	mutex;

  gchar *	name;
  gchar *	path;

  guint		ref;

  gboolean	todelete;

  sqlite3 *	db;

  DupinViewP	views;
  DupinAttachmentDBP	attachment_dbs;
  DupinLinkBP	linkbs;

  gchar *	default_attachment_db_name;
  gchar *	default_linkbase_name;

  gboolean	tocompact;
  gboolean	compact_toquit;
  GThread *	compact_thread;
  gsize		compact_processed_count; /* incremental counter of compacted records */

  gchar *       error_msg;
  gchar *       warning_msg;
};

struct dupin_linkb_t
{
  Dupin *	d;
  GMutex *	mutex;

  gchar *	name;
  gchar *	path;

  gchar *	parent;
  gboolean	parent_is_db;

  guint		ref;

  gboolean	todelete;

  sqlite3 *	db;

  DupinViewP	views;
  /* no attacthments for link bases */
  DupinLinkBP	linkbs;

  gboolean	tocompact;
  gboolean	compact_toquit;
  GThread *	compact_thread;
  gsize		compact_processed_count; /* incremental counter of compacted records */

  gboolean	tocheck;
  gboolean	check_toquit;
  GThread *	check_thread;
  gsize		check_processed_count; /* incremental counter of checked records */

  gchar *       error_msg;
  gchar *       warning_msg;
};

struct dupin_view_t
{
  Dupin *	d;
  GMutex *	mutex;

  gchar *	name;
  gchar *	path;

  gchar *	parent;
  gboolean	parent_is_db;
  gboolean	parent_is_linkb;

  guint		ref;

  gboolean	todelete;
  gboolean	tosync;

  GThread *	sync_map_thread;
  GThread *	sync_reduce_thread;
  gsize		sync_map_offset;
  gsize		sync_map_processed_count; /* incremental counter of mapped records */
  gsize		sync_reduce_total_records; /* total records to reduce from view table */
  gsize		sync_reduce_processed_count; /* incremental counter of reduced records */
  gboolean	sync_toquit;
  GCond *	sync_map_has_new_work; /* for communication between map to reduce threads */

  sqlite3 *	db;

  gchar *	map;
  DupinMRLang	map_lang;

  gchar *	reduce;
  DupinMRLang	reduce_lang;

  JsonParser *	collation_parser;

  DupinViewP	views;

  gchar *       error_msg;
  gchar *       warning_msg;
};

struct dupin_attachment_db_t
{
  Dupin *	d;
  GMutex *	mutex;

  gchar *	name;
  gchar *	path;

  gchar *	parent;

  guint		ref;

  gboolean	todelete;

  sqlite3 *	db;

  gchar *       error_msg;
  gchar *       warning_msg;
};

struct dupin_attachment_record_t
{
  DupinAttachmentDB * attachment_db;

  gchar	*	id;
  gsize		id_len;
  gchar *	title;
  gsize		title_len;

  gsize		length;
  gchar *	type;
  gsize		type_len;
  gchar *	hash;
  gsize		hash_len;

  gsize		rowid;

  sqlite3_blob * blob;
};

typedef struct dupin_record_rev_t DupinRecordRev;

struct dupin_record_t
{
  DupinDB *	db;

  gchar	*	id;

  DupinRecordRev * last;
  GHashTable *	revisions;
};

struct dupin_record_rev_t
{
  guint		revision;
  gchar *	hash;
  gsize		hash_len;
  gchar *	mvcc;
  gsize		mvcc_len;

  gboolean	deleted;

  gsize		created;

  gsize		rowid;

  gchar *	type;

  gchar *	obj_serialized;
  gsize		obj_serialized_len;
  JsonNode *    obj;
};

typedef struct dupin_link_record_rev_t DupinLinkRecordRev;

struct dupin_link_record_t
{
  DupinLinkB *	linkb;

  gchar	*	id;

  DupinLinkRecordRev * last;
  GHashTable *	revisions;
};

struct dupin_link_record_rev_t
{
  guint		revision;
  gchar *	hash;
  gsize		hash_len;
  gchar *	mvcc;
  gsize		mvcc_len;

  gchar	*	context_id;
  gchar	*	label;
  gchar	*	href;
  gchar	*	rel;
  gboolean	is_weblink;
  gchar	*	tag; /* E.g. to tag named hierarchies */

  gboolean	deleted;

  gsize		created;

  gsize		rowid;

  gchar *	obj_serialized;
  gsize		obj_serialized_len;
  JsonNode *    obj;
};

struct dupin_view_record_t
{
  DupinView *	view;

  gchar	*	id;
  gsize		rowid;

  gchar *	pid_serialized;
  gsize		pid_serialized_len;
  JsonNode *    pid;

  gchar *	key_serialized;
  gsize		key_serialized_len;
  JsonNode *    key;

  gchar *	obj_serialized;
  gsize		obj_serialized_len;
  JsonNode *    obj;
};

struct dupin_js_t
{
  JsonNode *	reduceResult;
  JsonArray *	mapResults;
};

DupinDB *	dupin_db_connect
				(Dupin *	     d,
				 gchar *	     name,
				 gchar *	     path,
				 DupinSQLiteOpenType mode,
				 GError **	     error);

void		dupin_db_disconnect
				(DupinDB *	db);

DupinLinkB *	dupin_linkb_connect
				(Dupin *	     d,
				 gchar *	     name,
				 gchar *	     path,
				 DupinSQLiteOpenType mode,
				 GError **	     error);

void		dupin_linkb_disconnect
				(DupinLinkB *	linkb);

DupinView *	dupin_view_connect
				(Dupin *	     d,
				 gchar *	     name,
				 gchar *	     path,
				 DupinSQLiteOpenType mode,
				 GError **	     error);

void		dupin_view_disconnect
				(DupinView *	view);

DupinAttachmentDB *	dupin_attachment_db_connect
				(Dupin *	     d,
				 gchar *	     name,
				 gchar *	     path,
				 DupinSQLiteOpenType mode,
				 GError **	     error);

void		dupin_attachment_db_disconnect
				(DupinAttachmentDB *	attachment_db);

gchar *		dupin_database_generate_id_real
				(DupinDB *	db,
				 GError **	error,
				 gboolean	lock);

gboolean	dupin_record_exists_real
				(DupinDB *	db,
				 gchar *	id,
				 gboolean	lock);

gchar *		dupin_linkbase_generate_id_real
				(DupinLinkB *	linkb,
				 GError **	error,
				 gboolean	lock);

gboolean	dupin_link_record_exists_real
				(DupinLinkB *	linkb,
				 gchar *	id,
				 gboolean	lock);

gboolean	dupin_linkbase_p_update
				(DupinLinkB *	linkb,
				 GError **	error);

void		dupin_linkbase_p_record_insert
				(DupinLinkBP *	p,
				 gchar *	id,
				 JsonObject *obj);

void		dupin_linkbase_p_record_delete
				(DupinLinkBP *	p,
				 gchar *	id);

gboolean	dupin_view_p_update
				(DupinView *	view,
				 GError **	error);

void		dupin_view_p_record_insert
				(DupinViewP *	p,
				 gchar *	id,
				 JsonObject *obj);

void		dupin_view_p_record_delete
				(DupinViewP *	p,
				 gchar *	id);

void		dupin_view_record_save_map
				(DupinView *	view,
				 JsonNode     * pid,
				 JsonNode     * key,
				 JsonNode     * obj);

void		dupin_view_record_delete
				(DupinView *	view,
				 gchar *	pid);

gboolean	dupin_view_record_exists_real
				(DupinView *	view,
				 gchar *	id,
				 gboolean	lock);

void		dupin_view_sync	(DupinView *	view);

gboolean	dupin_attachment_db_p_update
				(DupinAttachmentDB *	attachment_db,
				 GError **	error);

void		dupin_attachment_db_p_record_insert
			 	(DupinAttachmentDBP * p,
                                     gchar *       id,
                                     gchar *       title,
                                     gsize         length,
                                     gchar *       type,
                                     const void ** content);	

void		dupin_attachment_db_p_record_delete
				(DupinAttachmentDBP * p,
                                     gchar *       id,
                                     gchar *       title);

#endif

/* EOF */
