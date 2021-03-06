#ifndef _DUPIN_H_
#define _DUPIN_H_

#include <glib.h>

#include <glib-object.h>

/* ERROR */
enum
{
  DUPIN_ERROR_INIT,
  DUPIN_ERROR_OPEN,
  DUPIN_ERROR_CRUD,
  DUPIN_ERROR_RECORD_CONFLICT
};

GQuark     	dupin_error_quark	(void);	

/* SQLite Open type: */
typedef enum
{
  DP_SQLITE_OPEN_READONLY,
  DP_SQLITE_OPEN_READWRITE,
  DP_SQLITE_OPEN_CREATE
} DupinSQLiteOpenType;

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

/* Linkbase Include Docs type: */
typedef enum
{
  DP_LINKBASE_INCLUDE_DOC_TYPE_NONE,
  DP_LINKBASE_INCLUDE_DOC_TYPE_ALL,
  DP_LINKBASE_INCLUDE_DOC_TYPE_IN,
  DP_LINKBASE_INCLUDE_DOC_TYPE_OUT
} DupinLinkbaseIncludeDocsType;

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

/* View Engine Languages: */
typedef enum
{
  DP_VIEW_ENGINE_LANG_JAVASCRIPT = 0,
  DP_VIEW_ENGINE_LANG_DUPIN_GI /* TODO */
} DupinViewEngineLang;

/* OrderBy type: */ 
typedef enum
{
  DP_ORDERBY_ROWID,
  DP_ORDERBY_ID,
  DP_ORDERBY_KEY,
  DP_ORDERBY_TITLE,
  DP_ORDERBY_REV,
  DP_ORDERBY_HASH,
  DP_ORDERBY_LABEL,
  DP_ORDERBY_AUTHORITY,
  DP_ORDERBY_LINK_TYPE_LABEL	/* 2 levels sort key sort weblinks and relationships with respective labels */
} DupinOrderByType;

/* Created type: */ 
typedef enum
{
  DP_CREATED_SINCE,
  DP_CREATED_UNTIL
} DupinCreatedType;

/* FilterBy type: */ 
typedef enum
{
  DP_FILTERBY_UNDEF,
  DP_FILTERBY_EQUALS,
  DP_FILTERBY_CONTAINS,
  DP_FILTERBY_STARTS_WITH,
  DP_FILTERBY_PRESENT
} DupinFilterByType;

/* FieldsFormat type: */ 
typedef enum
{
  DP_FIELDS_FORMAT_NONE,
  DP_FIELDS_FORMAT_DOTTED,
  DP_FIELDS_FORMAT_JSONPATH
} DupinFieldsFormatType;

/* SQLite key collation */
typedef enum
{
  DP_COLLATE_TYPE_NULL,
  DP_COLLATE_TYPE_BOOLEAN,
  DP_COLLATE_TYPE_INTEGER,
  DP_COLLATE_TYPE_DOUBLE,
  DP_COLLATE_TYPE_STRING,
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
typedef struct dupin_webkit_t		DupinWebKit;
typedef struct dupin_view_engine_t	DupinViewEngine;
typedef struct dupin_linkb_t		DupinLinkB;
typedef struct dupin_link_record_t	DupinLinkRecord;

#define DUPIN_DEBUG		0
#define DUPIN_VIEW_DEBUG	0
#define DUPIN_VIEW_BENCHMARK	0

#define DUPIN_UNKNOWN_ERROR 	"Unknown Error"

/* wait 30 second timeout by default if "database is locked" before failing */
#define DUPIN_SQLITE_TIMEOUT		30000
#define DUPIN_SQLITE_CACHE_SIZE		50000

#define DUPIN_SQLITE_MAX_USER_VERSION	100

/* NOTE - requests and record API common macros - to be renamed/rearranged later */

#define REQUEST_OBJ_ID                  "_id"
#define REQUEST_OBJ_REV                 "_rev"
#define REQUEST_OBJ_TYPE                "_type"
#define REQUEST_OBJ_ATTACHMENTS         "_attachments"
#define REQUEST_OBJ_LINKS               "_links"
#define REQUEST_OBJ_RELATIONSHIPS       "_relationships"
#define REQUEST_OBJ_CONTENT             "_content"
#define REQUEST_OBJ_DELETED             "_deleted"
#define REQUEST_OBJ_PATCHED		"_patched"
#define REQUEST_OBJ_PATCHED_FIELDS	"_patched_fields"
#define REQUEST_OBJ_EXPIRE_AFTER        "_expire_after"
#define REQUEST_OBJ_EXPIRE_AT           "_expire_at"

#define REQUEST_LINK_OBJ_ID           	REQUEST_OBJ_ID
#define REQUEST_LINK_OBJ_REV        	REQUEST_OBJ_REV 
#define REQUEST_LINK_OBJ_DELETED        REQUEST_OBJ_DELETED
#define REQUEST_LINK_OBJ_HREF           "_href"
#define REQUEST_LINK_OBJ_REL            "_rel"
#define REQUEST_LINK_OBJ_AUTHORITY      "_authority"
#define REQUEST_LINK_OBJ_LABEL          "_label"
#define REQUEST_LINK_OBJ_CONTEXT_ID     "_context_id"
#define REQUEST_LINK_OBJ_EXPIRE_AFTER   REQUEST_OBJ_EXPIRE_AFTER
#define REQUEST_LINK_OBJ_EXPIRE_AT      REQUEST_OBJ_EXPIRE_AT

#define REQUEST_OBJ_INLINE_ATTACHMENTS_STUB     "_stub"
#define REQUEST_OBJ_INLINE_ATTACHMENTS_DATA     "_data"
#define REQUEST_OBJ_INLINE_ATTACHMENTS_TYPE     "_content_type"

#define RESPONSE_OBJ_INLINE_ATTACHMENTS_TYPE     REQUEST_OBJ_INLINE_ATTACHMENTS_TYPE
#define RESPONSE_OBJ_INLINE_ATTACHMENTS_HASH	"_hash"     
#define RESPONSE_OBJ_INLINE_ATTACHMENTS_LENGTH	"_length"     

#define RESPONSE_OBJ_ID                 "id"
#define RESPONSE_OBJ_REV                "rev"
#define RESPONSE_OBJ_CREATED            "created"
#define RESPONSE_OBJ_EXPIRE             "expire"
#define RESPONSE_OBJ_TYPE		"type"
#define RESPONSE_OBJ_ATTACHMENTS        REQUEST_OBJ_ATTACHMENTS
#define RESPONSE_OBJ_LINKS              REQUEST_OBJ_LINKS
#define RESPONSE_OBJ_RELATIONSHIPS      REQUEST_OBJ_RELATIONSHIPS
#define RESPONSE_OBJ_CONTENT            REQUEST_OBJ_CONTENT
#define RESPONSE_OBJ_DELETED            REQUEST_OBJ_DELETED
#define RESPONSE_OBJ_STATUS	        "status"
#define RESPONSE_OBJ_STATUS_AVAILABLE	"available"
#define RESPONSE_OBJ_STATUS_DELETED	"deleted"
#define RESPONSE_OBJ_EMPTY              "_empty"
#define RESPONSE_OBJ_INCLUDED           "_included"
#define RESPONSE_OBJ_LINKS_PAGING       "_paging"
#define RESPONSE_OBJ_DOC                "doc"

#define RESPONSE_LINK_OBJ_ID            RESPONSE_OBJ_ID
#define RESPONSE_LINK_OBJ_REV           RESPONSE_OBJ_REV
#define RESPONSE_LINK_OBJ_HREF          "href"
#define RESPONSE_LINK_OBJ_REL           "rel"
#define RESPONSE_LINK_OBJ_AUTHORITY     "authority"
#define RESPONSE_LINK_OBJ_LABEL         "label"
#define RESPONSE_LINK_OBJ_CONTEXT_ID    "context_id"
#define RESPONSE_LINK_OBJ_DOC		RESPONSE_OBJ_DOC
#define RESPONSE_LINK_OBJ_DOC_IN        "doc_in"
#define RESPONSE_LINK_OBJ_DOC_OUT       "doc_out"
#define RESPONSE_LINK_OBJ_LINK          "doc"
#define RESPONSE_LINK_OBJ_EMPTY         RESPONSE_OBJ_EMPTY
#define RESPONSE_LINK_OBJ_DELETED       REQUEST_OBJ_DELETED
#define RESPONSE_LINK_OBJ_INCLUDED      RESPONSE_OBJ_INCLUDED

#define RESPONSE_VIEW_OBJ_ID            RESPONSE_OBJ_ID
#define RESPONSE_VIEW_OBJ_DOC           RESPONSE_OBJ_DOC
#define RESPONSE_VIEW_OBJ_KEY           "key"
#define RESPONSE_VIEW_OBJ_VALUE         "value"

#define DUPIN_VIEW_KEY		        "key"
#define DUPIN_VIEW_KEYS		        "keys"
#define DUPIN_VIEW_VALUE	        "value"
#define DUPIN_VIEW_VALUES	        "values"
#define DUPIN_VIEW_PIDS			"pids"

#define REQUEST_STRICT                  "strict"
#define REQUEST_STRICT_LINKS            "links"

#define DUPIN_DB_MAX_DOCS_COUNT     50
#define DUPIN_LINKB_MAX_LINKS_COUNT 50
#define DUPIN_VIEW_MAX_DOCS_COUNT   50
#define DUPIN_ATTACHMENTS_COUNT     100
#define DUPIN_REVISIONS_COUNT       100

#define DUPIN_INCLUDE_DEFAULT_LEVEL	1	
#define DUPIN_INCLUDE_MAX_LEVEL		2	

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
#define REQUEST_ALL_LINKS       REQUEST_ALL_DOCS
#define REQUEST_LINKBS          "_linkbs"
#define REQUEST_VIEWS           "_views"
#define REQUEST_SYNC            "_sync"
#define REQUEST_UUIDS           "_uuids"
#define REQUEST_UUIDS_COUNT     "count"

#define REQUEST_GET_ALL_ANY_FILTER_FIELDS			"fields"
#define REQUEST_GET_ALL_ANY_FILTER_LINK_FIELDS			"link_fields"
#define REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL			"@all"
#define REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL_FIELDS		"@all_fields"
#define REQUEST_GET_ALL_ANY_FILTER_FIELDS_ALL_RELATIONSHIPS	"@all_relationships"
#define REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT		"fieldsFormat"
#define REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED		"dotted"
#define REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_JSONPATH	"jsonpath"
#define REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS			"equals"
#define REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS			"contains"
#define REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH		"starts_with"
#define REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT			"present"
#define REQUEST_GET_ALL_ANY_FILTER_CREATED_SINCE   		"created_since"
#define REQUEST_GET_ALL_ANY_FILTER_CREATED_UNTIL   		"created_until"
#define REQUEST_GET_ALL_ANY_FILTER_BY				"filter_by"
#define REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT			"filter_by_format"
#define REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_DOTTED		REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED
#define REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_JSONPATH		REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_JSONPATH
#define REQUEST_GET_ALL_ANY_FILTER_OP				"filter_op"
#define REQUEST_GET_ALL_ANY_FILTER_VALUES			"filter_value"

#define REQUEST_GET_ALL_DOCS_DESCENDING           "descending"
#define REQUEST_GET_ALL_DOCS_LIMIT                "limit"
#define REQUEST_GET_ALL_DOCS_OFFSET               "offset"

#define REQUEST_GET_ALL_ATTACHMENTS_DESCENDING  "attachments_descending"
#define REQUEST_GET_ALL_ATTACHMENTS_LIMIT       "attachments_limit"
#define REQUEST_GET_ALL_ATTACHMENTS_OFFSET      "attachments_offset"
#define RESPONSE_OBJ_ATTACHMENTS_PAGING		RESPONSE_OBJ_LINKS_PAGING

#define REQUEST_GET_ALL_DOCS_KEY                  "key"
#define REQUEST_GET_ALL_DOCS_KEYS                 "keys"
#define REQUEST_GET_ALL_DOCS_STARTKEY             "startkey"
#define REQUEST_GET_ALL_DOCS_ENDKEY               "endkey"
#define REQUEST_GET_ALL_DOCS_INCLUSIVEEND         "inclusive_end"
#define REQUEST_GET_ALL_DOCS_VALUE                "value"
#define REQUEST_GET_ALL_DOCS_STARTVALUE           "startvalue"
#define REQUEST_GET_ALL_DOCS_ENDVALUE             "endvalue"
#define REQUEST_GET_ALL_DOCS_INCLUSIVEEND_VALUE   "inclusive_end_value"
#define REQUEST_GET_ALL_DOCS_INCLUDE_DOCS         "include_docs"

#define REQUEST_GET_ALL_DOCS_TYPES                "types"
#define REQUEST_GET_ALL_DOCS_TYPES_OP             "types_op"

#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE                 "include_links"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_ALL_LINKS       REQUEST_GET_ALL_LINKS_LINK_TYPE_ALL_LINKS
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_WEBLINKS        REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_TYPE_RELATIONSHIPS   REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELS                 "include_links_rels"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELS_OP              "include_links_rels_op"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS               "include_links_labels"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_LABELS_OP            "include_links_labels_op"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_HREFS                "include_links_hrefs"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_HREFS_OP             "include_links_hrefs_op"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_AUTHORITIES          "include_links_authorities"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_AUTHORITIES_OP       "include_links_authorities_op"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_CREATED_SINCE "include_links_created_since"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_CREATED_UNTIL "include_links_created_until"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_DESCENDING  "include_links_weblinks_descending"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_LIMIT       "include_links_weblinks_limit"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_WEBLINKS_OFFSET      "include_links_weblinks_offset"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_DESCENDING     "include_links_relationships_descending"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_LIMIT          "include_links_relationships_limit"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_RELATIONSHIPS_OFFSET         "include_links_relationships_offset"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_BY			"include_links_filter_by"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_BY_FORMAT		"include_links_filter_by_format"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_BY_FORMAT_DOTTED	REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_DOTTED
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_BY_FORMAT_JSONPATH	REQUEST_GET_ALL_ANY_FILTER_FIELDS_FORMAT_JSONPATH
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_OP			"include_links_filter_op"
#define REQUEST_GET_ALL_DOCS_INCLUDE_LINKS_FILTER_VALUES		"include_links_filter_value"

#define REQUEST_GET_ALL_LINKS_DESCENDING        REQUEST_GET_ALL_DOCS_DESCENDING
#define REQUEST_GET_ALL_LINKS_LIMIT             REQUEST_GET_ALL_DOCS_LIMIT
#define REQUEST_GET_ALL_LINKS_OFFSET            REQUEST_GET_ALL_DOCS_OFFSET
#define REQUEST_GET_ALL_LINKS_KEY               REQUEST_GET_ALL_DOCS_KEY
#define REQUEST_GET_ALL_LINKS_KEYS              REQUEST_GET_ALL_DOCS_KEYS

#define REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS	"include_linked_docs"
#define REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_IN	"in"
#define REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_OUT	"out"
#define REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_ALL	"all"
#define REQUEST_GET_ALL_LINKS_INCLUDE_LINKED_DOCS_LEVEL	"include_linked_docs_level"

#define REQUEST_GET_ALL_LINKS_LINKBASE                  "linkbase"

#define REQUEST_GET_ALL_LINKS_CREATED_SINCE   		REQUEST_GET_ALL_DOCS_CREATED_SINCE
#define REQUEST_GET_ALL_LINKS_CREATED_UNTIL   		REQUEST_GET_ALL_DOCS_CREATED_UNTIL
#define REQUEST_GET_ALL_LINKS_CONTEXT_ID                "context_id"
#define REQUEST_GET_ALL_LINKS_RELS                      "rels"
#define REQUEST_GET_ALL_LINKS_RELS_OP                   "rels_op"
#define REQUEST_GET_ALL_LINKS_HREFS                     "hrefs"
#define REQUEST_GET_ALL_LINKS_HREFS_OP                  "hrefs_op"
#define REQUEST_GET_ALL_LINKS_AUTHORITIES               "authorities"
#define REQUEST_GET_ALL_LINKS_AUTHORITIES_OP            "authorities_op"
#define REQUEST_GET_ALL_LINKS_LABELS                    "labels"
#define REQUEST_GET_ALL_LINKS_LABELS_OP                 "labels_op"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE                 "link_type"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE_ALL_LINKS       "all_links"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS        "web_links"
#define REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS   "relationships"

#define REQUEST_GET_ALL_CHANGES_SINCE         	"since"
#define REQUEST_GET_ALL_CHANGES_STYLE         	"style"
#define REQUEST_GET_ALL_CHANGES_FEED          	"feed"
#define REQUEST_GET_ALL_CHANGES_INCLUDE_DOCS  	REQUEST_GET_ALL_DOCS_INCLUDE_DOCS
#define REQUEST_GET_ALL_CHANGES_TYPES         	REQUEST_GET_ALL_DOCS_TYPES
#define REQUEST_GET_ALL_CHANGES_TYPES_OP      	REQUEST_GET_ALL_DOCS_TYPES_OP
#define REQUEST_GET_ALL_CHANGES_HEARTBEAT     	"heartbeat"
#define REQUEST_GET_ALL_CHANGES_TIMEOUT       	"timeout"
#define REQUEST_GET_ALL_CHANGES_CONTEXT_ID    	REQUEST_GET_ALL_LINKS_CONTEXT_ID
#define REQUEST_GET_ALL_CHANGES_AUTHORITIES   	REQUEST_GET_ALL_LINKS_AUTHORITIES
#define REQUEST_GET_ALL_CHANGES_AUTHORITIES_OP  REQUEST_GET_ALL_LINKS_AUTHORITIES_OP

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
#define REQUEST_RECORD_ARG_REVS "revs_info"

#define REQUEST_FIELDS          "_fields"

/* POST */
#define REQUEST_POST_BULK_DOCS          	    "_bulk_docs"
#define REQUEST_POST_ALL_DOCS          	    	    REQUEST_ALL_DOCS
#define REQUEST_POST_ALL_DOCS_KEYS                  "keys"
#define REQUEST_POST_ALL_LINKS          	    REQUEST_ALL_LINKS 
#define REQUEST_POST_ALL_LINKS_KEYS                 "keys"
#define REQUEST_POST_BULK_DOCS_DOCS     	    "docs"
#define REQUEST_POST_BULK_DOCS_USE_LATEST_REVISION  "_use_latest_revision"
#define REQUEST_POST_BULK_DOCS_IGNORE_IF_UNMODIFIED "_ignore_updates_if_unmodified"
#define REQUEST_POST_COMPACT_DATABASE		    "_compact"
#define REQUEST_POST_COMPACT_DATABASE_PURGE	    "purge"

#define REQUEST_POST_BULK_LINKS         	    "_bulk_links"
#define REQUEST_POST_BULK_LINKS_LINKS   	    REQUEST_POST_BULK_DOCS_DOCS
#define REQUEST_POST_BULK_LINKS_USE_LATEST_REVISION REQUEST_POST_BULK_DOCS_USE_LATEST_REVISION
#define REQUEST_POST_BULK_LINKS_IGNORE_IF_UNMODIFIED	REQUEST_POST_BULK_DOCS_IGNORE_IF_UNMODIFIED
#define REQUEST_POST_COMPACT_LINKBASE   	    REQUEST_POST_COMPACT_DATABASE
#define REQUEST_POST_COMPACT_LINKBASE_PURGE   	    REQUEST_POST_COMPACT_DATABASE_PURGE
#define REQUEST_POST_CHECK_LINKBASE     	    "_check"

#define REQUEST_POST_COMPACT_VIEW		    REQUEST_POST_COMPACT_DATABASE

/* DATA STRUCT */
#define REQUEST_WWW             "_www"
#define REQUEST_QUIT            "_quit"
#define REQUEST_STATUS          "_status"

/* Changes API */

#define DUPIN_DB_MAX_CHANGES_COUNT           255
#define DUPIN_LINKB_MAX_CHANGES_COUNT        DUPIN_DB_MAX_CHANGES_COUNT

/* Portable Listings */

#define REQUEST_PORTABLE_LISTINGS	"_portable_listings"

#define REQUEST_GET_PORTABLE_LISTINGS_FILTER_BY			"filterBy"
#define REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP			"filterOp"
#define REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP_EQUALS		"equals"
#define REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP_CONTAINS	"contains"
#define REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP_STARTS_WITH	"starts_with"
#define REQUEST_GET_PORTABLE_LISTINGS_FILTER_OP_PRESENT		"present"
#define REQUEST_GET_PORTABLE_LISTINGS_FILTER_VALUES		"filterValue"
#define REQUEST_GET_PORTABLE_LISTINGS_FILTER_OBJECT_TYPE	"filterObjectType"
#define REQUEST_GET_PORTABLE_LISTINGS_UPDATED_SINCE   		"updatedSince"
#define REQUEST_GET_PORTABLE_LISTINGS_UPDATED_UNTIL   		"updatedUntil"
#define REQUEST_GET_PORTABLE_LISTINGS_SORT_BY			"sortBy"
#define REQUEST_GET_PORTABLE_LISTINGS_SORT_ORDER           	"sortOrder"
#define REQUEST_GET_PORTABLE_LISTINGS_START_INDEX              	"startIndex"
#define REQUEST_GET_PORTABLE_LISTINGS_COUNT                	"count"
#define REQUEST_GET_PORTABLE_LISTINGS_FIELDS			"fields"
#define REQUEST_GET_PORTABLE_LISTINGS_FIELDS_ALL		"@all"
#define REQUEST_GET_PORTABLE_LISTINGS_FIELDS_ALL_FIELDS		"@all_fields"
#define REQUEST_GET_PORTABLE_LISTINGS_FIELDS_ALL_RELATIONSHIPS	"@all_relationships"
#define REQUEST_GET_PORTABLE_LISTINGS_INCLUDE_RELATIONSHIPS    	"includeRelationships"
#define REQUEST_GET_PORTABLE_LISTINGS_FORMAT              	"format"
#define REQUEST_GET_PORTABLE_LISTINGS_FORMAT_JSON              	"json"

#include <dupin_init.h>

#include <dupin_record.h>
#include <dupin_view_record.h>
#include <dupin_attachment_record.h>
#include <dupin_link_record.h>

#include <dupin_db.h>
#include <dupin_view.h>
#include <dupin_attachment_db.h>
#include <dupin_linkb.h>
#include <dupin_view_engine.h>
#include <dupin_webkit.h>

#include <dupin_internal.h>
#include <dupin_utils.h>
#include <dupin_date.h>

#endif

/* EOF */
