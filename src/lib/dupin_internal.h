#ifndef _DUPIN_INTERNAL_H_
#define _DUPIN_INTERNAL_H_

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

struct dupin_t
{
  GMutex *	mutex;

  gchar *	path;
  GHashTable *	dbs;
  GHashTable *	views;
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
};

struct dupin_view_t
{
  Dupin *	d;
  GMutex *	mutex;

  gchar *	name;
  gchar *	path;

  gchar *	parent;
  gboolean	parent_is_db;

  guint		ref;

  gboolean	todelete;

  GThread *	sync_thread;
  gboolean	sync_toquit;
  gsize		sync_offset;

  sqlite3 *	db;

  gchar *	map;
  DupinMRLang	map_lang;

  gchar *	reduce;
  DupinMRLang	reduce_lang;

  DupinViewP	views;
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

  gboolean	deleted;

  gchar *	obj_serialized;
  gsize		obj_serialized_len;

  JsonObject *obj;
};

struct dupin_view_record_t
{
  DupinView *	view;

  gchar	*	id;
  gchar *	pid;

  gchar *	obj_serialized;
  gsize		obj_serialized_len;

  tb_json_object_t *obj;
};

struct dupin_js_t
{
  tb_json_object_t *	emit;
  tb_json_array_t *	emitIntermediate;
};

DupinDB *	dupin_db_create	(Dupin *	d,
				 gchar *	name,
				 gchar *	path,
				 GError **	error);

void		dupin_db_free	(DupinDB *	db);

DupinView *	dupin_view_create
				(Dupin *	d,
				 gchar *	name,
				 gchar *	path,
				 GError **	error);

void		dupin_view_free	(DupinView *	view);

gchar *		dupin_database_generate_id_real
				(DupinDB *	db,
				 GError **	error,
				 gboolean	lock);

gboolean	dupin_record_exists_real
				(DupinDB *	db,
				 gchar *	id,
				 gboolean	lock);

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

void		dupin_view_record_save
				(DupinView *	view,
				 gchar *	pid,
				 tb_json_object_t * obj);

void		dupin_view_record_delete
				(DupinView *	view,
				 gchar *	pid);

gboolean	dupin_view_record_exists_real
				(DupinView *	view,
				 gchar *	id,
				 gboolean	lock);

void		dupin_view_sync	(DupinView *	view);

#endif

/* EOF */
