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
  DP_COUNT_ALL
} DupinCountType;

/* Languages: */
typedef enum
{
  DP_MR_LANG_JAVASCRIPT = 0
} DupinMRLang;

/* OrderBy type: */ 
typedef enum
{
  DP_ORDERBY_ROWID,
  DP_ORDERBY_UPDATED,
  DP_ORDERBY_KEY,
  DP_ORDERBY_TITLE,
  DP_ORDERBY_REV,
  DP_ORDERBY_HASH
} DupinOrderByType;

typedef struct dupin_db_t		DupinDB;
typedef struct dupin_record_t		DupinRecord;
typedef struct dupin_view_record_t	DupinViewRecord;
typedef struct dupin_view_t		DupinView;
typedef struct dupin_attachment_db_t		DupinAttachmentDB;
typedef struct dupin_attachment_record_t	DupinAttachmentRecord;
typedef struct dupin_js_t		DupinJs;

#include <dupin_init.h>

#include <dupin_record.h>
#include <dupin_view_record.h>
#include <dupin_attachment_record.h>

#include <dupin_db.h>
#include <dupin_view.h>
#include <dupin_attachment_db.h>
#include <dupin_mr.h>
#include <dupin_js.h>

#include <dupin_internal.h>
#include <dupin_utils.h>

#endif

/* EOF */
