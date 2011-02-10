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

/* NOTE - requests and record API common macros - to be renamed/rearranged later */

#define REQUEST_OBJ_ID                  "_id"
#define REQUEST_OBJ_REV                 "_rev"
#define REQUEST_OBJ_ATTACHMENTS         "_attachments"
#define REQUEST_OBJ_LINKS               "_links"
#define REQUEST_OBJ_RELATIONSHIPS       "_relationships"
#define REQUEST_OBJ_CONTENT             "_content"
#define REQUEST_OBJ_DELETED             "_deleted"

#define REQUEST_LINK_OBJ_HREF           "_href"
#define REQUEST_LINK_OBJ_REL            "_rel"
#define REQUEST_LINK_OBJ_TAG            "_tag"
#define REQUEST_LINK_OBJ_LABEL          "_label"

#define REQUEST_OBJ_INLINE_ATTACHMENTS_STUB     "stub"
#define REQUEST_OBJ_INLINE_ATTACHMENTS_DATA     "data"
#define REQUEST_OBJ_INLINE_ATTACHMENTS_TYPE     "content_type"

#define RESPONSE_OBJ_ID                 "id"
#define RESPONSE_OBJ_REV                "rev"
#define RESPONSE_OBJ_ATTACHMENTS        REQUEST_OBJ_ATTACHMENTS
#define RESPONSE_OBJ_LINKS              REQUEST_OBJ_LINKS
#define RESPONSE_OBJ_RELATIONSHIPS      REQUEST_OBJ_RELATIONSHIPS
#define RESPONSE_OBJ_CONTENT            REQUEST_OBJ_CONTENT
#define RESPONSE_OBJ_DELETED            REQUEST_OBJ_DELETED
#define RESPONSE_OBJ_EMPTY              "_empty"
#define RESPONSE_OBJ_LINKS_PAGING       "_paging"
#define RESPONSE_OBJ_DOC                "doc"

#define RESPONSE_LINK_OBJ_ID            RESPONSE_OBJ_ID
#define RESPONSE_LINK_OBJ_REV           RESPONSE_OBJ_REV
#define RESPONSE_LINK_OBJ_HREF          "href"
#define RESPONSE_LINK_OBJ_REL           "rel"
#define RESPONSE_LINK_OBJ_TAG           "tag"
#define RESPONSE_LINK_OBJ_LABEL         REQUEST_LINK_OBJ_LABEL
#define RESPONSE_LINK_OBJ_DOC           RESPONSE_OBJ_DOC
#define RESPONSE_LINK_OBJ_LINK          "link"
#define RESPONSE_LINK_OBJ_EMPTY         RESPONSE_OBJ_EMPTY

#define RESPONSE_VIEW_OBJ_DOC           RESPONSE_LINK_OBJ_DOC

#define REQUEST_STRICT                  "strict"
#define REQUEST_STRICT_LINKS            "links"

#define DUPIN_DB_MAX_DOCS_COUNT     50
#define DUPIN_LINKB_MAX_LINKS_COUNT 50
#define DUPIN_VIEW_MAX_DOCS_COUNT   50
#define DUPIN_ATTACHMENTS_COUNT     100
#define DUPIN_REVISIONS_COUNT       100
#define DUPIN_DB_MAX_CHANGES_COUNT  100

#define RESPONSE_STATUS_OK                      "ok"
#define RESPONSE_STATUS_ERROR                   "error"
#define RESPONSE_STATUS_WARNING                 "warning"
#define RESPONSE_STATUS_REASON                  "reason"

/* GET */
#define REQUEST_ALL_DBS         "_all_dbs"
#define REQUEST_ALL_LINKBS      "_all_linkbs"
#define REQUEST_ALL_ATTACH_DBS  "_all_attachment_dbs"
#define REQUEST_ALL_VIEWS       "_all_views"
#define REQUEST_ALL_CHANGES     "_changes"
#define REQUEST_ALL_DOCS        "_all_docs"
#define REQUEST_ATTACH_DBS      "_attach_dbs"
#define REQUEST_ALL_LINKS       "_all_links"
#define REQUEST_LINKBS          "_linkbs"
#define REQUEST_VIEWS           "_views"
#define REQUEST_SYNC            "_sync"
#define REQUEST_UUIDS           "_uuids"
#define REQUEST_UUIDS_COUNT     "count"

#define REQUEST_GET_ALL_DOCS_DESCENDING           "descending"
#define REQUEST_GET_ALL_DOCS_COUNT                "count"
#define REQUEST_GET_ALL_DOCS_OFFSET               "offset"
#define REQUEST_GET_ALL_DOCS_KEY                  "key"
#define REQUEST_GET_ALL_DOCS_STARTKEY             "startkey"
#define REQUEST_GET_ALL_DOCS_ENDKEY               "endkey"
#define REQUEST_GET_ALL_DOCS_INCLUSIVEEND         "inclusive_end"
#define REQUEST_GET_ALL_DOCS_INCLUDE_DOCS         "include_docs"
#define REQUEST_GET_ALL_DOCS_INCLUDE_PARENT_DOCS  "include_parent_docs"

#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE                 "include_links"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_ALL_LINKS       REQUEST_GET_ALL_LINKS_LINK_TYPE_ALL_LINKS
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_WEBLINKS        REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_RELATIONSHIPS   REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TAG                  "include_links_tag"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS               "include_links_labels"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_DESCENDING  "include_links_weblinks_descending"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_COUNT       "include_links_weblinks_count"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_OFFSET      "include_links_weblinks_offset"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_DESCENDING     "include_links_relationships_descending"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_COUNT          "include_links_relationships_count"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_OFFSET         "include_links_relationships_offset"

#define REQUEST_GET_ALL_LINKS_DESCENDING        REQUEST_GET_ALL_DOCS_DESCENDING
#define REQUEST_GET_ALL_LINKS_COUNT             REQUEST_GET_ALL_DOCS_COUNT
#define REQUEST_GET_ALL_LINKS_OFFSET            REQUEST_GET_ALL_DOCS_OFFSET
#define REQUEST_GET_ALL_LINKS_KEY               REQUEST_GET_ALL_DOCS_KEY

#define REQUEST_GET_ALL_LINKS_IDSPATH_KEY  		"idspath_key"
#define REQUEST_GET_ALL_LINKS_IDSPATH_STARTKEY  	"idspath_startkey"
#define REQUEST_GET_ALL_LINKS_IDSPATH_ENDKEY            "idspath_endkey"
#define REQUEST_GET_ALL_LINKS_IDSPATH_INCLUSIVEEND      "idspath_inclusive_end"
#define REQUEST_GET_ALL_LINKS_LABELSPATH_KEY  		"labelspath_key"
#define REQUEST_GET_ALL_LINKS_LABELSPATH_STARTKEY  	"labelspath_startkey"
#define REQUEST_GET_ALL_LINKS_LABELSPATH_ENDKEY         "labelspath_endkey"
#define REQUEST_GET_ALL_LINKS_LABELSPATH_INCLUSIVEEND   "labelspath_inclusive_end"

#define REQUEST_GET_ALL_LINKS_LINKBASE                  "linkbase"

#define REQUEST_GET_ALL_LINKS_CONTEXT_ID                "context_id"
#define REQUEST_GET_ALL_LINKS_TAG                       "tag"
#define REQUEST_GET_ALL_LINKS_LABELS                    "labels"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE                 "link_type"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE_ALL_LINKS       "all_links"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS        "web_links"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS   "relationships"

#define REQUEST_GET_ALL_CHANGES_SINCE         "since"
#define REQUEST_GET_ALL_CHANGES_STYLE         "style"
#define REQUEST_GET_ALL_CHANGES_FEED          "feed"
#define REQUEST_GET_ALL_CHANGES_INCLUDE_DOCS  REQUEST_GET_ALL_DOCS_INCLUDE_DOCS
#define REQUEST_GET_ALL_CHANGES_HEARTBEAT     "heartbeat"
#define REQUEST_GET_ALL_CHANGES_TIMEOUT       "timeout"
#define REQUEST_GET_ALL_CHANGES_INCLUDE_LINKS "include_links"
#define REQUEST_GET_ALL_CHANGES_CONTEXT_ID    REQUEST_GET_ALL_LINKS_CONTEXT_ID
#define REQUEST_GET_ALL_CHANGES_TAG           REQUEST_GET_ALL_LINKS_TAG

#define REQUEST_GET_ALL_CHANGES_HEARTBEAT_DEFAULT  30000
#define REQUEST_GET_ALL_CHANGES_TIMEOUT_DEFAULT    60000

#define REQUEST_GET_ALL_CHANGES_STYLE_DEFAULT      "main_only"

#define REQUEST_GET_ALL_CHANGES_STYLE_ALL_LINKS         REQUEST_GET_ALL_LINKS_LINK_TYPE_ALL_LINKS
#define REQUEST_GET_ALL_CHANGES_STYLE_WEBLINKS          REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS
#define REQUEST_GET_ALL_CHANGES_STYLE_RELATIONSHIPS     REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS

#define REQUEST_GET_ALL_CHANGES_FEED_POLL       "poll"
#define REQUEST_GET_ALL_CHANGES_FEED_LONGPOLL   "longpoll"
#define REQUEST_GET_ALL_CHANGES_FEED_CONTINUOUS "continuous"

#define REQUEST_QUERY           "_query"

#define REQUEST_RECORD_ARG_REV  "rev"
#define REQUEST_RECORD_ARG_REVS "revs"

#define REQUEST_RECORD_ARG_LINKS_TAG    "links_tag"

#define REQUEST_FIELDS          "_fields"

/* POST */
#define REQUEST_POST_BULK_DOCS          "_bulk_docs"
#define REQUEST_POST_BULK_DOCS_DOCS     "docs"
#define REQUEST_POST_COMPACT_DATABASE   "_compact"

#define REQUEST_POST_BULK_LINKS         "_bulk_links"
#define REQUEST_POST_BULK_LINKS_LINKS   "links"
#define REQUEST_POST_COMPACT_LINKBASE   REQUEST_POST_COMPACT_DATABASE
#define REQUEST_POST_CHECK_LINKBASE     "_check"

/* DATA STRUCT */
#define REQUEST_WWW             "_www"
#define REQUEST_QUIT            "_quit"
#define REQUEST_STATUS          "_status"

/* Changes API */

#define DUPIN_DB_MAX_CHANGES_COMET_COUNT  1
#define DUPIN_LINKB_MAX_CHANGES_COMET_COUNT  1


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
