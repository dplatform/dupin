#ifndef _DUPIN_H_
#define _DUPIN_H_

#include <glib.h>

#include <glib-object.h>

/* ERROR */
enum
{
  DUPIN_ERROR_INIT,
  DUPIN_ERROR_OPEN,
  DUPIN_ERROR_CRUD
};

GQuark     	dupin_error_quark	(void);	

/* Count type: */
typedef enum
{
  DP_COUNT_EXIST,
  DP_COUNT_DELETE,
  DP_COUNT_CHANGES,
  DP_COUNT_ALL
} DupinCountType;

/* Get Links type: */
typedef enum
{
  DP_LINK_TYPE_NONE,
  DP_LINK_TYPE_ANY,
  DP_LINK_TYPE_WEB_LINK,
  DP_LINK_TYPE_RELATIONSHIP
} DupinLinksType;

/* Changes type: */
typedef enum
{
  DP_CHANGES_MAIN_ONLY,
  DP_CHANGES_ALL_DOCS,
  DP_CHANGES_ALL_LINKS,
  DP_CHANGES_WEB_LINKS,
  DP_CHANGES_RELATIONSHIPS
} DupinChangesType;

/* Changes feed type: */
typedef enum
{
  DP_CHANGES_FEED_POLL,
  DP_CHANGES_FEED_LONGPOLL,
  DP_CHANGES_FEED_CONTINUOUS
} DupinChangesFeedType;

/* Languages: */
typedef enum
{
  DP_MR_LANG_JAVASCRIPT = 0
} DupinMRLang;

/* OrderBy type: */ 
typedef enum
{
  DP_ORDERBY_ROWID,
  DP_ORDERBY_KEY,
  DP_ORDERBY_TITLE,
  DP_ORDERBY_REV,
  DP_ORDERBY_HASH,
  DP_ORDERBY_LABEL,
  DP_ORDERBY_TAG,
  DP_ORDERBY_LINK_TYPE_LABEL	/* 2 levels sort key sort weblinks and relationships with respective labels */
} DupinOrderByType;

/* SQLite key collation */
typedef enum
{
  DP_COLLATE_TYPE_NULL,
  DP_COLLATE_TYPE_BOOLEAN,
  DP_COLLATE_TYPE_NUMBER,
  DP_COLLATE_TYPE_TEXT,
  DP_COLLATE_TYPE_ARRAY,
  DP_COLLATE_TYPE_OBJECT,
  DP_COLLATE_TYPE_EMPTY,
  DP_COLLATE_TYPE_ANY
} DupinCollateType;

typedef struct dupin_db_t		DupinDB;
typedef struct dupin_record_t		DupinRecord;
typedef struct dupin_view_record_t	DupinViewRecord;
typedef struct dupin_view_t		DupinView;
typedef struct dupin_attachment_db_t		DupinAttachmentDB;
typedef struct dupin_attachment_record_t	DupinAttachmentRecord;
typedef struct dupin_js_t		DupinJs;
typedef struct dupin_linkb_t		DupinLinkB;
typedef struct dupin_link_record_t	DupinLinkRecord;

#include <dupin_init.h>

#include <dupin_record.h>
#include <dupin_view_record.h>
#include <dupin_attachment_record.h>
#include <dupin_link_record.h>

#include <dupin_db.h>
#include <dupin_view.h>
#include <dupin_attachment_db.h>
#include <dupin_linkb.h>
#include <dupin_mr.h>
#include <dupin_js.h>

#include <dupin_internal.h>
#include <dupin_utils.h>

#endif

/* EOF */
