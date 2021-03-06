#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_internal.h"
#include "dupin_utils.h"
#include "dupin_date.h"
#include "dupin_webkit.h"

#include <string.h>

#define DUPIN_WEBKIT_FUNCTION_SUM "function sum (values) { var rv = 0; for (var i in values) { rv += values[i]; } return rv; };\n"

static JSValueRef dupin_webkit_emit	(JSContextRef ctx,
			 	     	 JSObjectRef object,
				     	 JSObjectRef thisObject,
				     	 size_t argumentCount,
				     	 const JSValueRef arguments[],
				     	 JSValueRef * exception);

static gchar* dupin_webkit_string_utf8	(JSStringRef js_string);

static void dupin_webkit_obj 		(JSContextRef ctx,
					 JSValueRef object_value,
			  		 JsonNode ** obj_node);

static void dupin_webkit_value 		(JSContextRef ctx,
					 JSValueRef value,
                          		 JsonNode ** v);

static JSValueRef dupin_webkit_dupin_class_log
					(JSContextRef ctx,
					 JSObjectRef object,
					 JSObjectRef thisObject,
					 size_t argumentCount,
					 const JSValueRef arguments[],
					 JSValueRef * exception);

static JSValueRef dupin_webkit_dupin_class_view_lookup
					(JSContextRef ctx,
					 JSObjectRef object,
					 JSObjectRef thisObject,
					 size_t argumentCount,
					 const JSValueRef arguments[],
					 JSValueRef * exception);

static JSValueRef dupin_webkit_dupin_class_links
					(JSContextRef ctx,
					 JSObjectRef object,
		   	       		 JSObjectRef thisObject,
					 size_t argumentCount,
		   	       		 const JSValueRef arguments[],
		   	       		 JSValueRef * exception);

static JSValueRef dupin_webkit_dupin_class_util_hash
					(JSContextRef ctx,
					 JSObjectRef object,
					 JSObjectRef thisObject,
					 size_t argumentCount,
					 const JSValueRef arguments[],
					 JSValueRef * exception);

static JSValueRef dupin_webkit_dupin_class_util_base64_encode
					(JSContextRef ctx,
					 JSObjectRef object,
					 JSObjectRef thisObject,
					 size_t argumentCount,
					 const JSValueRef arguments[],
					 JSValueRef * exception);

static JSValueRef dupin_webkit_dupin_class_util_base64_decode
					(JSContextRef ctx,
					 JSObjectRef object,
					 JSObjectRef thisObject,
					 size_t argumentCount,
					 const JSValueRef arguments[],
					 JSValueRef * exception);

static JSValueRef dupin_webkit_dupin_class_util_http_client
					(JSContextRef ctx,
					 JSObjectRef object,
		   	       		 JSObjectRef thisObject,
					 size_t argumentCount,
		   	       		 const JSValueRef arguments[],
		   	       		 JSValueRef * exception);

static JSStaticFunction dupin_webkit_dupin_class_static_functions[] = {
    { "log", dupin_webkit_dupin_class_log, kJSPropertyAttributeNone },
    { "view_lookup", dupin_webkit_dupin_class_view_lookup, kJSPropertyAttributeNone },
    { "links", dupin_webkit_dupin_class_links, kJSPropertyAttributeNone },
    { "util_hash", dupin_webkit_dupin_class_util_hash, kJSPropertyAttributeNone },
    { "util_base64_encode", dupin_webkit_dupin_class_util_base64_encode, kJSPropertyAttributeNone },
    { "util_base64_decode", dupin_webkit_dupin_class_util_base64_decode, kJSPropertyAttributeNone },
    { "util_http_client", dupin_webkit_dupin_class_util_http_client, kJSPropertyAttributeNone },
    { 0, 0, 0 }
};

/* TODO */
static JSStaticValue dupin_webkit_dupin_class_static_values[] = {
    { 0, 0, 0, 0 }
};

DupinWebKit *
dupin_webkit_new (Dupin * d,
	          GError ** error)
{
  g_return_val_if_fail (d != NULL, NULL);

  DupinWebKit *js;

  JSStringRef str;
  JSObjectRef func;

  js = g_malloc0 (sizeof (DupinWebKit));

  js->d = d;
  js->ctx = JSGlobalContextCreate (NULL);

  /*
     TODO
        -> we should have ctx in scope of of caller to avoid too many of these and
           if we do it, note we could have multiple views, so we should have the
           global state in context kept per view (E.g. an object or array of states)
  */

  /* Setup JavaScript environment */

  static JSClassRef jsDupinClass;
  JSClassDefinition dupin_class_def = kJSClassDefinitionEmpty;
  dupin_class_def.className = "Dupin";
  dupin_class_def.staticFunctions = dupin_webkit_dupin_class_static_functions;
  dupin_class_def.staticValues = dupin_webkit_dupin_class_static_values;

  if (!jsDupinClass)
    jsDupinClass = JSClassCreate (&dupin_class_def);

  JSObjectRef globalObject = JSContextGetGlobalObject(js->ctx);

  JSObjectRef DupinObject = JSObjectMake(js->ctx, jsDupinClass, js->d);
  str = JSStringCreateWithUTF8CString("dupin");
  JSObjectSetProperty(js->ctx, globalObject, str, DupinObject, kJSPropertyAttributeNone, NULL);
  JSStringRelease(str);

  /* register call back fro emit (k,v) */

  str = JSStringCreateWithUTF8CString ("emit");
  func = JSObjectMakeFunctionWithCallback (js->ctx, str, dupin_webkit_emit);
  JSObjectSetProperty (js->ctx, globalObject, str, func,
                       kJSPropertyAttributeNone, NULL);
  JSStringRelease (str);

  return js;
}

void
dupin_webkit_free (DupinWebKit * js)
{
  g_return_if_fail (js != NULL);

  JSGlobalContextRelease (js->ctx);

  g_free (js);
}

/*
 See http://wiki.apache.org/couchdb/Introduction_to_CouchDB_views#Concept

 function(doc) {
    ...
    emit(key, value);
  }
 */

JsonNode *
dupin_webkit_map (DupinWebKit *  js,
		  gchar *        js_json_doc,
                  gchar *        js_code,
                  gchar **       exception_string)
{
  g_return_val_if_fail (js != NULL, NULL);

  g_return_val_if_fail (js_json_doc != NULL, NULL);
  g_return_val_if_fail (js_code != NULL, NULL);

  if (g_utf8_validate (js_json_doc, -1, NULL) == FALSE)
    return NULL;

  if (g_utf8_validate (js_code, -1, NULL) == FALSE)
    return NULL;

  JSStringRef str;
  JSObjectRef array;
  JSValueRef result;
  JSValueRef js_exception=NULL;
  JSValueRef p;
  GString *buffer;
  gchar *b=NULL;

  /* we will keep callback result pairs (key,value) in two separated arrays keys[i]->values[i] for simplicity */

  JSObjectRef globalObject = JSContextGetGlobalObject(js->ctx);

  str = JSStringCreateWithUTF8CString ("return new Array");
  array = JSObjectMakeFunction(js->ctx, NULL, 0, NULL, str, NULL, 1, NULL);
  JSStringRelease (str);
  p = JSObjectCallAsFunction(js->ctx, array, NULL, 0, NULL, NULL);
  str = JSStringCreateWithUTF8CString ("__dupin_emit_keys");
  JSObjectSetProperty (js->ctx, globalObject, str, p,
                       kJSPropertyAttributeDontDelete, NULL); /* or kJSPropertyAttributeNone ? */
  JSStringRelease (str);

  str = JSStringCreateWithUTF8CString ("return new Array");
  array = JSObjectMakeFunction(js->ctx, NULL, 0, NULL, str, NULL, 1, NULL);
  JSStringRelease (str);
  p = JSObjectCallAsFunction(js->ctx, array, NULL, 0, NULL, NULL);
  str = JSStringCreateWithUTF8CString ("__dupin_emit_values");
  JSObjectSetProperty (js->ctx, globalObject, str, p,
                       kJSPropertyAttributeDontDelete, NULL); /* or kJSPropertyAttributeNone ? */
  JSStringRelease (str);

  /* make map function (doc) { ... passed JS code calling eventually emit(k,v) ... }  */
  buffer = g_string_new (DUPIN_WEBKIT_FUNCTION_SUM);
  buffer = g_string_append (buffer, "var __dupin_map_function = ");
  buffer = g_string_append_len (buffer, js_code, strlen (js_code));
  buffer = g_string_append (buffer, "\n"); /* no semicolon to avoid checking input js_code for it */
  buffer = g_string_append (buffer, "__dupin_map_function (");
  buffer = g_string_append_len (buffer, js_json_doc, strlen (js_json_doc));
  buffer = g_string_append (buffer, ");\n");
  b = g_string_free (buffer, FALSE);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_webkit_map():\n");
  g_message("\tscript is: %s\n",js_code);
  g_message("\tjs_json_doc is: %s\n",js_json_doc);
  g_message("\twhole is: %s\n",b);
#endif

  str = JSStringCreateWithUTF8CString (b);
  result = JSEvaluateScript (js->ctx, str, NULL, NULL, 0, &js_exception);
  JSStringRelease (str);

  g_free (b);

  if (!result)
    {
      JSStringRef js_message = JSValueToStringCopy (js->ctx, js_exception, NULL);
      gchar* value = dupin_webkit_string_utf8 (js_message);

      if (exception_string)
        *exception_string = value;
      else
        {
          g_warning ("dupin_webkit_map: %s", value);

          g_warning("\n\tscript is: %s\n",js_code);
          g_warning("\n\tjs_json_doc is: %s\n",js_json_doc);

          g_free (value);
        }

      JSStringRelease (js_message);

      return NULL;
    }
  else
    {
      /* convert matching Javascript objects to JSON */

      /* return an array of mapped objects in JSON like [ { "key": key, "value": value } ... { ... } ] where key and objects can be arbitrary objects */

      /* mapped keys and values */

      str = JSStringCreateWithUTF8CString ("__dupin_emit_keys");
      JSValueRef mkeys = JSObjectGetProperty (js->ctx, globalObject, str, NULL);
      JSStringRelease (str);

      str = JSStringCreateWithUTF8CString ("__dupin_emit_values");
      JSValueRef mvalues = JSObjectGetProperty (js->ctx, globalObject, str, NULL);
      JSStringRelease (str);

      if (mkeys && mvalues)
        {
          JSObjectRef map_keys = JSValueToObject(js->ctx, mkeys, NULL);
          JSObjectRef map_values = JSValueToObject(js->ctx, mvalues, NULL);
          JSPropertyNameArrayRef maps_names = JSObjectCopyPropertyNames (js->ctx, map_keys);

	  /* NOTE - we assumed emit keys and value to have the same cardinality */

          gsize nmaps = JSPropertyNameArrayGetCount (maps_names);

	  JsonArray * mapResults = json_array_new ();

          gint i;
          for (i = 0; i < nmaps; i++)
            {
              JsonNode * key_node;
              JsonNode * value_node;
              JSValueRef key = JSObjectGetPropertyAtIndex (js->ctx, map_keys, i, NULL);
              JSValueRef value = JSObjectGetPropertyAtIndex (js->ctx, map_values, i, NULL);

	      /* TODO - check if we migth not want to emit an empty key or empty value */

              dupin_webkit_value (js->ctx, key, &key_node); /* CHECK - possible bug when mapped key=NULL but seen as string by dupin_webkit_value() ?!? */
              dupin_webkit_value (js->ctx, value, &value_node);

	      JsonObject *map_object = json_object_new (); /* TODO - make double sure we do not need a json node object for GC reasons */
	      json_object_set_member (map_object, DUPIN_VIEW_KEY, key_node);
	      json_object_set_member (map_object, DUPIN_VIEW_VALUE, value_node);

              json_array_add_object_element(mapResults, map_object);
            }

	  JsonNode * result_node = json_node_new (JSON_NODE_ARRAY);
          json_node_set_array (result_node, mapResults);

#if DUPIN_VIEW_DEBUG
	  g_message("dupin_webkit_map(): mapResults: %s\n", dupin_util_json_serialize (result_node));
#endif

          JSPropertyNameArrayRelease (maps_names);

	  return result_node;
        }
    }

  return NULL;
}

/*

See http://wiki.apache.org/couchdb/Introduction_to_CouchDB_views#Concept

function (key, values, rereduce) {
    return sum(values);
}
Reduce functions are passed three arguments in the order key, values and rereduce

Reduce functions must handle two cases:

1. When rereduce is false:

- key will be an array whose elements are arrays of the form [key,id], where key is a key emitted by the map function and id is that of the document from which the key was generated.
- values will be an array of the values emitted for the respective elements in keys
- i.e. reduce([ [key1,id1], [key2,id2], [key3,id3] ], [value1,value2,value3], false)

2. When rereduce is true:

- key will be null
- values will be an array of values returned by previous calls to the reduce function
- i.e. reduce(null, [intermediate1,intermediate2,intermediate3], true)

Reduce functions should return a single value, suitable for both the value field of the final view and as a member of the values array passed to the reduce function.

CouchDB doesn't necessarily pass in all the values for a unique key to the reduce function. This means that the reduce function needs to handle values potentially being an array of previous outputs.

 */

JsonNode *
dupin_webkit_reduce (DupinWebKit *  js,
		     gchar *        js_json_keys,
		     gchar *        js_json_values,
                     gboolean       rereduce,
                     gchar *        js_code,
                     gchar **       exception_string)
{
  g_return_val_if_fail (js != NULL, NULL);
  g_return_val_if_fail (js_json_values != NULL, NULL);
  g_return_val_if_fail (js_code != NULL, NULL);

  if (g_utf8_validate (js_json_values, -1, NULL) == FALSE)
    return NULL;

  if (js_json_keys != NULL
      && g_utf8_validate (js_json_keys, -1, NULL) == FALSE)
    return NULL;

  if (g_utf8_validate (js_code, -1, NULL) == FALSE)
    return NULL;

  JSStringRef str;
  JSValueRef result;
  JSValueRef js_exception=NULL;
  GString *buffer;
  gchar *b=NULL;

  JSObjectRef globalObject = JSContextGetGlobalObject(js->ctx);

  /* make map function (keys,values,rereduce) { ... passed JS code returning an object ... } */
  //buffer = g_string_new (DUPIN_WEBKIT_FUNCTION_SUM); // Already defined in dupin_webkit_map () - see above
  buffer = g_string_new ("var __dupin_reduce_function = ");
  buffer = g_string_append_len (buffer, js_code, strlen (js_code));
  buffer = g_string_append (buffer, "\n"); /* no semicolon to avoid checking input js_code for it */
  buffer = g_string_append (buffer, "__dupin_reduce_result = __dupin_reduce_function (");

  /* TODO - check reduce passed function takes three params - or return error */

  if (js_json_keys != NULL)
    buffer = g_string_append_len (buffer, js_json_keys, strlen (js_json_keys));
  else
    buffer = g_string_append_len (buffer, "null", 4);

  buffer = g_string_append (buffer, ", ");
  buffer = g_string_append_len (buffer, js_json_values, strlen (js_json_values));
  buffer = g_string_append (buffer, ", ");
  gchar * rereduce_param = (rereduce == TRUE) ? "true" : "false";
  buffer = g_string_append_len (buffer, rereduce_param, strlen(rereduce_param));
  buffer = g_string_append (buffer, ");\n");
  b = g_string_free (buffer, FALSE);

#if DUPIN_VIEW_DEBUG
  g_message("dupin_webkit_reduce():\n");
  g_message("\tscript is: %s\n",js_code);
  g_message("\tjs_json_keys is: %s\n",js_json_keys);
  g_message("\tjs_json_values is: %s\n",js_json_values);
  g_message("\trereduce is: %s\n",rereduce_param);
  g_message("\twhole is: %s\n",b);
#endif

  str = JSStringCreateWithUTF8CString (b);
  result = JSEvaluateScript (js->ctx, str, NULL, NULL, 0, &js_exception);
  JSStringRelease (str);

  g_free (b);

  if (!result)
    {
      JSStringRef js_message = JSValueToStringCopy (js->ctx, js_exception, NULL);
      gchar* value = dupin_webkit_string_utf8 (js_message);

      if (exception_string)
        *exception_string = value;
      else
        {
          g_warning ("dupin_webkit_reduce: %s", value);

          g_warning("\n\tscript is: %s\n",js_code);
          g_warning("\n\tjs_json_keys is: %s\n",js_json_keys);
          g_warning("\n\tjs_json_values is: %s\n",js_json_values);
          g_warning("\n\trereduce is: %s\n",rereduce_param);

          g_free (value);
        }

      JSStringRelease (js_message);

      return NULL;
    }
  else
    {
      /* convert matching Javascript objects to JSON */
      /* process __dupin_reduce_result */

      str = JSStringCreateWithUTF8CString ("__dupin_reduce_result");
      JSValueRef reduce_results = JSObjectGetProperty (js->ctx, globalObject, str, NULL);
      JSStringRelease (str);

      JsonNode * result_node = NULL;

      dupin_webkit_value (js->ctx, reduce_results, &result_node);

      /* debug print what's there */

#if DUPIN_VIEW_DEBUG
      g_message("dupin_webkit_reduce(): reduceResult: %s\n", dupin_util_json_serialize (result_node));
#endif

      return result_node;
    }

  return NULL;
}

static gchar*
dupin_webkit_string_utf8 (JSStringRef js_string)
{
    size_t size_utf8;
    gchar* string_utf8;

    g_return_val_if_fail (js_string, NULL);

    size_utf8 = JSStringGetMaximumUTF8CStringSize (js_string);
    string_utf8 = g_new (gchar, size_utf8);
    JSStringGetUTF8CString (js_string, string_utf8, size_utf8);
    return string_utf8;
}

gchar *
dupin_webkit_string (JSStringRef js_string)
{
  gsize size;
  gchar *string;

  size = JSStringGetMaximumUTF8CStringSize (js_string);
  string = g_malloc (size + 1);
  JSStringGetUTF8CString (js_string, string, size);

  return string;
}

static void
dupin_webkit_value (JSContextRef ctx,
		    JSValueRef value,
		    JsonNode ** v)
{
  switch (JSValueGetType (ctx, value))
    {
    case kJSTypeUndefined:
    case kJSTypeNull:
      *v = json_node_new (JSON_NODE_NULL);

      break;

    case kJSTypeBoolean:
      *v = json_node_new (JSON_NODE_VALUE);

      json_node_set_boolean (*v, 
				 JSValueToBoolean (ctx,
						   value) ==
				 true ? TRUE : FALSE);
      break;

    case kJSTypeNumber:
      *v = json_node_new (JSON_NODE_VALUE);

      json_node_set_double (*v, 
				(gdouble) JSValueToNumber (ctx, value, NULL));
      break;

    case kJSTypeString:
      {
	JSStringRef string;
	gchar *str;

	string = JSValueToStringCopy (ctx, value, NULL);
	//str = dupin_webkit_string (string);
	str = dupin_webkit_string_utf8 (string);
	JSStringRelease (string);

        *v = json_node_new (JSON_NODE_VALUE);

        json_node_set_string (*v, str);

	g_free (str);
	break;
      }

    case kJSTypeObject:
      {
        dupin_webkit_obj (ctx, value, v);

	break;
      }
    }

  /* FIXME: array?!? integer?!?
            -> probably arrays are considered instances of Array() Javascript object ?!

            see http://developer.apple.com/library/mac/#documentation/Carbon/Reference/WebKit_JavaScriptCore_Ref/JSValueRef_h/index.html%23//apple_ref/c/func/JSValueGetType */
}

static void
dupin_webkit_obj (JSContextRef ctx,
	          JSValueRef object_value,
	          JsonNode ** obj_node)
{
  JSPropertyNameArrayRef props;
  gsize nprops, i;

  JSObjectRef object = JSValueToObject (ctx, object_value,NULL);
  props = JSObjectCopyPropertyNames (ctx, object);
  nprops = JSPropertyNameArrayGetCount (props);

  /* hack to check if object is instance of array */
  JSStringRef array = JSStringCreateWithUTF8CString("Array");
  JSObjectRef arrayConstructor = JSValueToObject(ctx, JSObjectGetProperty(ctx, JSContextGetGlobalObject(ctx), array, NULL), NULL);
  JSStringRelease(array);
  gboolean is_array = JSValueIsInstanceOfConstructor (ctx, object_value, arrayConstructor, NULL);

  JsonObject * obj=NULL;
  JsonArray * arr=NULL;
  if (is_array == TRUE)
    {
      *obj_node = json_node_new (JSON_NODE_ARRAY);
      arr = json_array_new ();
    }
  else
    {
      *obj_node = json_node_new (JSON_NODE_OBJECT);
      obj = json_object_new ();
    }

  for (i = 0; i < nprops; i++)
    {
      JSStringRef prop = JSPropertyNameArrayGetNameAtIndex (props, i);

      JSValueRef value;
      JsonNode *node;
      gchar *p;

      p = dupin_webkit_string_utf8 (prop);

      value = JSObjectGetProperty (ctx, object, prop, NULL);
      dupin_webkit_value (ctx, value, &node);

      if (is_array == TRUE)
        {
          json_array_add_element (arr, node);
        }
      else
        {
          json_object_set_member (obj, p, node);
        }

      g_free (p);
    }

  if (is_array == TRUE)
    {
      json_node_take_array (*obj_node, arr);
    }
  else
    {
      json_node_take_object (*obj_node, obj);
    }

  JSPropertyNameArrayRelease (props);
}

/* emit(key,value) { ... } */
static JSValueRef
dupin_webkit_emit (JSContextRef ctx,
	           JSObjectRef object,
	           JSObjectRef thisObject, size_t argumentCount,
	           const JSValueRef arguments[],
	           JSValueRef * exception)
{
  if (argumentCount != 2) /* does it work if key or value are null/empty ? */
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return JSValueMakeNull(ctx);
    }

  /* does nothing if both NULL */
  if (!arguments[0] && !arguments[1])
    {
      return JSValueMakeNull(ctx);
    }

  JSStringRef str;
  JSObjectRef array;
  gsize last;
  JSPropertyNameArrayRef array_names;

  /* keep key and object in global scope */

  /* TODO - check if we need to allow/disallow key or value null/empty and how */

  JSObjectRef globalObject = JSContextGetGlobalObject(ctx);

  /* key */
  str = JSStringCreateWithUTF8CString ("__dupin_emit_keys");
  JSValueRef o = JSObjectGetProperty (ctx, globalObject, str, NULL);
  JSStringRelease (str);
  array = JSValueToObject(ctx,o, NULL);
  array_names = JSObjectCopyPropertyNames (ctx, array);
  last = JSPropertyNameArrayGetCount (array_names);
  JSPropertyNameArrayRelease (array_names);
  JSObjectSetPropertyAtIndex(ctx,array,last, arguments[0], NULL); /* push */

  /* value */
  str = JSStringCreateWithUTF8CString ("__dupin_emit_values");
  o = JSObjectGetProperty (ctx, globalObject, str, NULL);
  JSStringRelease (str);
  array = JSValueToObject(ctx,o, NULL);
  array_names = JSObjectCopyPropertyNames (ctx, array);
  last = JSPropertyNameArrayGetCount (array_names);
  JSPropertyNameArrayRelease (array_names);
  JSObjectSetPropertyAtIndex(ctx,array,last, arguments[1], NULL); /* push */

  return JSValueMakeNull(ctx);
}

static JSValueRef
dupin_webkit_dupin_class_log (JSContextRef ctx,
             		      JSObjectRef object,
	     		      JSObjectRef thisObject, size_t argumentCount,
             		      const JSValueRef arguments[],
	     		      JSValueRef * exception)
{
  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return JSValueMakeNull(ctx);
    }

  JsonNode * node = NULL;
  dupin_webkit_value (ctx, arguments[0], &node);
  gchar * json = dupin_util_json_serialize (node);
  g_message("dupin_webkit_dupin_class_log: %s\n", json);
  g_free (json);
  json_node_free (node);

  return JSValueMakeNull(ctx);
}

/* Dupin.view_lookup (viewname, key, include_docs) */

/* NOTE - we start by doing simple single key lookup ok/fail */

static JSValueRef
dupin_webkit_dupin_class_view_lookup (JSContextRef ctx,
                   		      JSObjectRef object,
		   		      JSObjectRef thisObject, size_t argumentCount,
		   		      const JSValueRef arguments[],
		   		      JSValueRef * exception)
{
  JSValueRef result=NULL;
  Dupin * d = (Dupin *) JSObjectGetPrivate(thisObject);
  DupinView *view=NULL;
  DupinDB *parent_db=NULL;
  DupinLinkB *parent_linkb=NULL;
  DupinView *parent_view=NULL;

  GList *results;

  gboolean descending = FALSE;
  guint count = 1;
  guint offset = 0;
  gchar * lookupkey = NULL;
  gboolean inclusive_end = TRUE;

  DupinViewRecord *record=NULL;
  JsonNode * match=NULL;

  if (argumentCount != 3)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return JSValueMakeNull(ctx);
    }

//g_message("dupin_webkit_dupin_class_view_lookup: checking params...\n");

  if ((!JSValueIsString(ctx, arguments[0]))
       || (!arguments[1])
       || (!JSValueIsBoolean(ctx, arguments[2])))
    return JSValueMakeNull(ctx);

//g_message("dupin_webkit_dupin_class_view_lookup: ok params...\n");

  /* view name */
  JSStringRef string = JSValueToStringCopy (ctx, arguments[0], NULL);
  gchar * view_name = dupin_webkit_string_utf8 (string);
  JSStringRelease (string);

  /* key is node */
  JsonNode * key = NULL;
  dupin_webkit_value (ctx, arguments[1], &key);
  if (key == NULL)
    {
      g_free (view_name);
      return JSValueMakeNull(ctx);
    }
  lookupkey = dupin_util_json_serialize (key); 

  /* include_docs */
  gboolean include_docs = (JSValueToBoolean (ctx, arguments[2]) == true) ? TRUE : FALSE;

//g_message("dupin_webkit_dupin_class_view_lookup: view_name=%s include_docs=%d (dupin_path=%s)\n", view_name, (gint)include_docs, d->path);

  if (!
      (view =
       dupin_view_open (d, view_name, NULL)))
    {
      g_free (lookupkey);
      g_free (view_name);
      json_node_free (key);
      return JSValueMakeNull(ctx);
    }

  if (include_docs == TRUE)
    {
      if (dupin_view_get_parent_is_db (view) == TRUE)
        {
          if (!(parent_db = dupin_database_open (view->d, view->parent, NULL)))
            {
              dupin_view_unref (view);
              g_free (lookupkey);
              g_free (view_name);
              json_node_free (key);
              return JSValueMakeNull(ctx);
            }
        }
      else if (dupin_view_get_parent_is_linkb (view) == TRUE)
        {
          if (!(parent_linkb = dupin_linkbase_open (view->d, view->parent, NULL)))
            {
              dupin_view_unref (view);
              g_free (lookupkey);
              g_free (view_name);
              json_node_free (key);
              return JSValueMakeNull(ctx);
            }
        }
      else
        {
          if (!(parent_view = dupin_view_open (view->d, view->parent, NULL)))
            {
              dupin_view_unref (view);
              g_free (lookupkey);
              g_free (view_name);
              json_node_free (key);
              return JSValueMakeNull(ctx);
            }
        }
    }

  if ((dupin_view_record_get_list (view, count, offset, 0, 0, DP_ORDERBY_KEY, descending,
                                  NULL, lookupkey, lookupkey, inclusive_end,
                                  NULL, NULL, TRUE,
				  NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL,
                                  &results, NULL) == FALSE)
       || (g_list_length (results) <= 0))
    {
      result = NULL;
      goto dupin_webkit_dupin_class_view_lookup_error;
    }

  record = results->data;

  if (! (match = dupin_view_record_get (record)))
    {
      result = NULL;
      goto dupin_webkit_dupin_class_view_lookup_error;
    }
  match = json_node_copy (match);

  if (include_docs == TRUE)
    {
      gchar * record_id;
      JsonNode * doc = NULL;
      JsonObject * match_obj;

      if (json_node_get_node_type (match) == JSON_NODE_OBJECT)
        match_obj = json_node_get_object (match);

      if (match_obj != NULL
          && json_object_has_member (match_obj, REQUEST_OBJ_ID))
        record_id = (gchar *) json_object_get_string_member (match_obj, REQUEST_OBJ_ID);
      else if (match_obj != NULL
	       && json_object_has_member (match_obj, RESPONSE_OBJ_ID))
        record_id = (gchar *) json_object_get_string_member (match_obj, RESPONSE_OBJ_ID);
      else
        {
          JsonNode * pid = dupin_view_record_get_pid (record);
          record_id = (gchar *)json_array_get_string_element (json_node_get_array (pid), 0);
        }

      if (dupin_view_get_parent_is_db (view) == TRUE)
        {
          DupinRecord * db_record=NULL;
          if (!(db_record = dupin_record_read (parent_db, record_id, NULL)))
            {
              doc = json_node_new (JSON_NODE_NULL);
            }
          else
            {
              doc = json_node_copy (dupin_record_get_revision_node (db_record, NULL));

              dupin_record_close (db_record);
            }
        }
      else if (dupin_view_get_parent_is_linkb (view) == TRUE)
        {
          DupinLinkRecord * linkb_record=NULL;
          if (!(linkb_record = dupin_link_record_read (parent_linkb, record_id, NULL)))
            {
              doc = json_node_new (JSON_NODE_NULL);
            }
          else
            {
              doc = json_node_copy (dupin_link_record_get_revision_node (linkb_record, NULL));

              dupin_link_record_close (linkb_record);
            }
        }
      else
        {
          DupinViewRecord * view_record=NULL;
          if (!(view_record = dupin_view_record_read (parent_view, record_id, NULL)))
            {
              doc = json_node_new (JSON_NODE_NULL);
            }
          else
            {
              doc = json_node_copy (dupin_view_record_get (view_record));

              dupin_view_record_close (view_record);
            }
        }

      json_object_set_member (match_obj, "doc", doc);
    }

  /* now match has the node we wanted */
  gchar * match_json = dupin_util_json_serialize (match); 

  gchar *b=NULL;
  GString * buffer = g_string_new ("var result = ");
  buffer = g_string_append (buffer, match_json);
  buffer = g_string_append (buffer, "; result;");
  b = g_string_free (buffer, FALSE);
  string=JSStringCreateWithUTF8CString(b);
  result = JSEvaluateScript (ctx, string, NULL, NULL, 1, NULL);
  JSStringRelease(string);
  g_free (b);
  g_free (match_json);

dupin_webkit_dupin_class_view_lookup_error:

  if (match != NULL)
    json_node_free (match);

  if( results )
    dupin_view_record_get_list_close(results);

  if (parent_db != NULL)
    dupin_database_unref (parent_db);

  if (parent_linkb != NULL)
    dupin_linkbase_unref (parent_linkb);

  if (parent_view != NULL)
    dupin_view_unref (parent_view);

  dupin_view_unref (view);
  g_free (lookupkey);
  g_free (view_name);
  json_node_free (key);

  return result;
}

static JSValueRef
dupin_webkit_dupin_class_links (JSContextRef ctx,
                   	        JSObjectRef object,
		   	        JSObjectRef thisObject, size_t argumentCount,
		   	        const JSValueRef arguments[],
		   	        JSValueRef * exception)
{
  JSValueRef result=NULL;

  Dupin * d = (Dupin *) JSObjectGetPrivate(thisObject);
  DupinLinkB *linkb=NULL;

  /*
     we pass two arguments:
	-> the document
	-> the request parameters (named as in the src/lib/dupin.h and used in request.c):

	{
	  "count": number,
	  "offset": number,
	  "rowid_start": number,
	  "rowid_end": number,
	  "link_type": type,
	  "keys": [ string, ..., string ],
	  "key": string,
	  "start_key": string,
	  "end_key": string,
	  "inclusive_end": boolean,
	  "count_type": string,
	  "orderby_type": string,
	  "descending": boolean,
	  "rels": "string, string...",
	  "rels_type": string,
	  "labels": "string, string ... ",
	  "labels_type": string,
	  "hrefs": "string, string ... ",
	  "hrefs_type": string,
	  "authorities": "string, string .. ",
	  "authorities_type": string,
	  "filter_by": string,
	  "filter_by_format": string,
	  "filter_op": string,
	  "filter_values": "string, string ...",
	  "include_linked_docs_out": boolean
        }
   */
  if (argumentCount != 2)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return JSValueMakeNull(ctx);
    }
//g_message("dupin_webkit_dupin_class_links: checking params...\n");

  if (((!arguments[0]))
      || (!arguments[1]))
    return JSValueMakeNull(ctx);

//g_message("dupin_webkit_dupin_class_links: ok params...\n");

  JsonNode * doc_node = NULL;
  dupin_webkit_value (ctx, arguments[0], &doc_node);
  if (doc_node == NULL
      || json_node_get_node_type (doc_node) != JSON_NODE_OBJECT
      || json_object_has_member (json_node_get_object (doc_node), "_linkbase") == FALSE)
    {
      if (doc_node != NULL) 
        json_node_free (doc_node);

      return JSValueMakeNull(ctx);
    }

  JsonNode * params_node = NULL;
  dupin_webkit_value (ctx, arguments[1], &params_node);
  if (params_node == NULL
      || json_node_get_node_type (params_node) != JSON_NODE_OBJECT)
    {
      if (doc_node != NULL) 
        json_node_free (doc_node);
      if (params_node != NULL) 
        json_node_free (params_node);

      return JSValueMakeNull(ctx);
    }
  JsonObject * params_node_obj = json_node_get_object (params_node);

//DUPIN_UTIL_DUMP_JSON ("dupin_webkit_dupin_class_links: doc:", doc_node);
//DUPIN_UTIL_DUMP_JSON ("dupin_webkit_dupin_class_links: params:", params_node);

  /* parse parameters */

  gchar * context_id = (gchar *)json_object_get_string_member (json_node_get_object (doc_node), "_id");
  if (context_id == NULL)
    {
      if (doc_node != NULL) 
        json_node_free (doc_node);
      if (params_node != NULL) 
        json_node_free (params_node);

      return JSValueMakeNull(ctx);
    }

  gboolean descending = FALSE;
  guint count = DUPIN_LINKB_MAX_LINKS_COUNT;
  guint offset = 0;
  GList * keys = NULL;
  gchar * startkey = NULL;
  gchar * endkey = NULL;
  gboolean inclusive_end = TRUE;
  gchar ** link_rels = NULL;
  DupinFilterByType link_rels_op = DP_FILTERBY_EQUALS;
  gchar ** link_labels = NULL;
  DupinFilterByType link_labels_op = DP_FILTERBY_EQUALS;
  gchar ** link_hrefs = NULL;
  DupinFilterByType link_hrefs_op = DP_FILTERBY_EQUALS;
  gchar ** link_authorities = NULL;
  DupinFilterByType link_authorities_op = DP_FILTERBY_EQUALS;
  DupinLinksType link_type = DP_LINK_TYPE_ANY;
  gchar * filter_by = NULL;
  DupinFieldsFormatType filter_by_format = DP_FIELDS_FORMAT_DOTTED;
  DupinFilterByType filter_op = DP_FILTERBY_UNDEF;
  gchar * filter_values = NULL;
  gsize created = 0;
  DupinCreatedType created_op = DP_CREATED_SINCE;

  gboolean include_linked_docs_out = false;

  GList * nodes = json_object_get_members (params_node_obj);
  GList * l=NULL;

  for (l = nodes; l != NULL ; l = l->next)
    {
      gchar * member_name = (gchar *)l->data;
      JsonNode * member = json_object_get_member (params_node_obj, member_name);

      if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_DESCENDING))
        {
 	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && json_node_get_value_type (member) == G_TYPE_BOOLEAN)
	    {
              descending = json_node_get_boolean (member);
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_LIMIT))
        {
 	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && (json_node_get_value_type (member) == G_TYPE_INT
	          || json_node_get_value_type (member) == G_TYPE_INT64
		  || json_node_get_value_type (member) == G_TYPE_UINT))
            {
              count = json_node_get_int (member);
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_OFFSET))
        {
 	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && (json_node_get_value_type (member) == G_TYPE_INT
	          || json_node_get_value_type (member) == G_TYPE_INT64
		  || json_node_get_value_type (member) == G_TYPE_UINT))
            {
              offset = json_node_get_int (member);
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_ANY_FILTER_CREATED_SINCE))
        {
 	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && (json_node_get_value_type (member) == G_TYPE_DOUBLE
	          || json_node_get_value_type (member) == G_TYPE_FLOAT))
	    {
	      created = (gsize) json_node_get_double (member);
              created_op = DP_CREATED_SINCE;
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_ANY_FILTER_CREATED_UNTIL))
        {
 	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && (json_node_get_value_type (member) == G_TYPE_DOUBLE
	          || json_node_get_value_type (member) == G_TYPE_FLOAT))
	    {
	      created = (gsize) json_node_get_double (member);
              created_op = DP_CREATED_SINCE;
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_RELS))
        {
 	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && json_node_get_value_type (member) == G_TYPE_STRING)
            {
	      link_rels = g_strsplit (json_node_get_string (member), ",", -1);
	    }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_RELS_OP))
        {
	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && json_node_get_value_type (member) == G_TYPE_STRING)
            {
	      gchar * op = (gchar *)json_node_get_string (member);
              if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                link_rels_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                link_rels_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                link_rels_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                link_rels_op = DP_FILTERBY_PRESENT;
	    }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_LABELS))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              link_labels = g_strsplit (json_node_get_string (member), ",", -1);
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_LABELS_OP))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              gchar * op = (gchar *)json_node_get_string (member);
              if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                link_labels_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                link_labels_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                link_labels_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                link_labels_op = DP_FILTERBY_PRESENT;
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_HREFS))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              link_hrefs = g_strsplit (json_node_get_string (member), ",", -1);
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_HREFS_OP))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              gchar * op = (gchar *)json_node_get_string (member);
              if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                link_hrefs_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                link_hrefs_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                link_hrefs_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                link_hrefs_op = DP_FILTERBY_PRESENT;
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_AUTHORITIES))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              link_authorities = g_strsplit (json_node_get_string (member), ",", -1);
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_AUTHORITIES_OP))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              gchar * op = (gchar *)json_node_get_string (member);
              if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                link_authorities_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                link_authorities_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                link_authorities_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                link_authorities_op = DP_FILTERBY_PRESENT;
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_LINKS_LINK_TYPE))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              gchar * t = (gchar *)json_node_get_string (member);
              if (!g_strcmp0 (t, REQUEST_GET_ALL_LINKS_LINK_TYPE_WEBLINKS))
                link_type = DP_LINK_TYPE_WEB_LINK;
              else if (!g_strcmp0 (t, REQUEST_GET_ALL_LINKS_LINK_TYPE_RELATIONSHIPS))
                link_type = DP_LINK_TYPE_RELATIONSHIP;
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_DOCS_INCLUSIVEEND))
        {
 	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && json_node_get_value_type (member) == G_TYPE_BOOLEAN)
	    {
              inclusive_end = json_node_get_boolean (member);
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_DOCS_KEYS))
        {
	  if (json_node_get_node_type (member) == JSON_NODE_ARRAY)
            {
	      keys = json_array_get_elements (json_node_get_array (member));

              if (keys == NULL)
                {
                  if (link_rels)
                    g_strfreev (link_rels);

                  if (link_labels)
                    g_strfreev (link_labels);

                  if (link_hrefs)
                    g_strfreev (link_hrefs);

                  if (link_authorities)
                    g_strfreev (link_authorities);

                  if (endkey != NULL)
                    g_free (endkey);

                  if (doc_node != NULL)
                    json_node_free (doc_node);

                  if (params_node != NULL)
                    json_node_free (params_node);

                  return JSValueMakeNull(ctx);
                }
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_DOCS_KEY))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              if (startkey != NULL)
                g_free (startkey);

              startkey = dupin_util_json_string_normalize_docid ((gchar *)json_node_get_string (member));
              if (startkey == NULL)
                {
                  if (link_rels)
                    g_strfreev (link_rels);

                  if (link_labels)
                    g_strfreev (link_labels);

                  if (link_hrefs)
                    g_strfreev (link_hrefs);

                  if (link_authorities)
                    g_strfreev (link_authorities);

                  if (endkey != NULL)
                    g_free (endkey);

                  if (doc_node != NULL) 
                    json_node_free (doc_node);

                  if (params_node != NULL) 
                    json_node_free (params_node);

		  if (keys != NULL)
		    g_list_free (keys);

                  return JSValueMakeNull(ctx);
                }

              endkey = g_strdup (startkey);
	    }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_DOCS_STARTKEY))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              if (startkey != NULL)
                g_free (startkey);

              startkey = dupin_util_json_string_normalize_docid ((gchar *)json_node_get_string (member));
              if (startkey == NULL)
                {
                  if (link_rels)
                    g_strfreev (link_rels);

                  if (link_labels)
                    g_strfreev (link_labels);

                  if (link_hrefs)
                    g_strfreev (link_hrefs);

                  if (link_authorities)
                    g_strfreev (link_authorities);

                  if (endkey != NULL)
                    g_free (endkey);

                  if (doc_node != NULL) 
                    json_node_free (doc_node);

                  if (params_node != NULL) 
                    json_node_free (params_node);

		  if (keys != NULL)
                    g_list_free (keys);

                  return JSValueMakeNull(ctx);
                }
	    }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_DOCS_ENDKEY))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              if (endkey != NULL)
                g_free (endkey);

              endkey = dupin_util_json_string_normalize_docid ((gchar *)json_node_get_string (member));
              if (endkey == NULL)
                {
                  if (link_rels)
                    g_strfreev (link_rels);

                  if (link_labels)
                    g_strfreev (link_labels);

                  if (link_hrefs)
                    g_strfreev (link_hrefs);

                  if (link_authorities)
                    g_strfreev (link_authorities);

                  if (startkey != NULL)
                    g_free (startkey);

                  if (doc_node != NULL) 
                    json_node_free (doc_node);

                  if (params_node != NULL) 
                    json_node_free (params_node);

		  if (keys != NULL)
                    g_list_free (keys);

                  return JSValueMakeNull(ctx);
                }
	    }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_ANY_FILTER_BY))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
	      filter_by = (gchar *)json_node_get_string (member);
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              gchar * f = (gchar *)json_node_get_string (member);
              if (!g_strcmp0 (f, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_DOTTED))
                filter_by_format = DP_FIELDS_FORMAT_DOTTED;
              else if (!g_strcmp0 (f, REQUEST_GET_ALL_ANY_FILTER_BY_FORMAT_JSONPATH))
                filter_by_format = DP_FIELDS_FORMAT_JSONPATH;
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_ANY_FILTER_OP))
        {
          if (json_node_get_node_type (member) == JSON_NODE_VALUE
              && json_node_get_value_type (member) == G_TYPE_STRING)
            {
              gchar * op = (gchar *)json_node_get_string (member);
              if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_EQUALS))
                filter_op = DP_FILTERBY_EQUALS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_CONTAINS))
                filter_op = DP_FILTERBY_CONTAINS;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_STARTS_WITH))
                filter_op = DP_FILTERBY_STARTS_WITH;
              else if (!g_strcmp0 (op, REQUEST_GET_ALL_ANY_FILTER_OP_PRESENT))
                filter_op = DP_FILTERBY_PRESENT;
            }
        }

      else if (!g_strcmp0 (member_name, REQUEST_GET_ALL_ANY_FILTER_VALUES))
        {
 	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && json_node_get_value_type (member) == G_TYPE_STRING)
            {
	      filter_values = (gchar *)json_node_get_string (member);
	    }
        }

      else if (!g_strcmp0 (member_name, "include_linked_docs_out"))
        {
 	  if (json_node_get_node_type (member) == JSON_NODE_VALUE
	      && json_node_get_value_type (member) == G_TYPE_BOOLEAN)
	    {
              include_linked_docs_out = json_node_get_boolean (member);
            }
        }

    }
  g_list_free (nodes);

  gchar * linkbase_name = (gchar *)json_object_get_string_member (json_node_get_object (doc_node), "_linkbase");

  if (!
      (linkb = dupin_linkbase_open (d, linkbase_name, NULL)))
    {
      if (link_rels)
        g_strfreev (link_rels);

      if (link_labels)
        g_strfreev (link_labels);

      if (link_hrefs)
        g_strfreev (link_hrefs);

      if (link_authorities)
        g_strfreev (link_authorities);

      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      if (doc_node != NULL) 
        json_node_free (doc_node);

      if (params_node != NULL) 
        json_node_free (params_node);

      if (keys != NULL)
        g_list_free (keys);

      return JSValueMakeNull(ctx);
    }

  GList *list;
  GList *results;

  if (dupin_link_record_get_list (linkb, count, offset, 0, 0, link_type, keys, startkey, endkey, inclusive_end, DP_COUNT_EXIST, DP_ORDERBY_ID, descending,
                                  context_id, link_rels, link_rels_op, link_labels, link_labels_op,
                                  link_hrefs, link_hrefs_op, link_authorities, link_authorities_op,
                                  filter_by, filter_by_format, filter_op, filter_values, &results, NULL) == FALSE)
    {
      if (link_rels)
        g_strfreev (link_rels);

      if (link_labels)
        g_strfreev (link_labels);

      if (link_hrefs)
        g_strfreev (link_hrefs);

      if (link_authorities)
        g_strfreev (link_authorities);

      if (startkey != NULL)
        g_free (startkey);

      if (endkey != NULL)
        g_free (endkey);

      if (doc_node != NULL) 
        json_node_free (doc_node);

      if (params_node != NULL) 
        json_node_free (params_node);

      if (keys != NULL)
        g_list_free (keys);

      dupin_linkbase_unref (linkb);

      return JSValueMakeNull(ctx);
    }

  JsonNode * links = json_node_new (JSON_NODE_OBJECT);
  JsonObject * links_obj = json_object_new ();
  json_node_take_object (links, links_obj);

  JsonNode * weblinks_node = NULL;
  JsonObject * weblinks_obj = NULL;
  JsonNode * relationships_node = NULL;
  JsonObject * relationships_obj = NULL;

  for (list = results; list; list = list->next)
    {
      DupinLinkRecord *record = list->data;

      gchar * mvcc = dupin_link_record_get_last_revision (record);

      JsonNode * obj_node = NULL;
      JsonObject * obj = NULL;

      if (dupin_link_record_is_deleted (record, mvcc) == TRUE)
        {
          obj_node = json_node_new (JSON_NODE_OBJECT);
          obj = json_object_new ();
          json_node_take_object (obj_node, obj);
          json_object_set_boolean_member (obj, RESPONSE_OBJ_DELETED, TRUE);
        }
      else
        {
          obj_node = json_node_copy (dupin_link_record_get_revision_node (record, mvcc));
          obj = json_node_get_object (obj_node);
	}

      gchar * href = (gchar *)dupin_link_record_get_href (record);

      if (dupin_link_record_is_weblink (record) == FALSE
	  && include_linked_docs_out == TRUE)
        {
          DupinDB * parent_db=NULL;
          DupinLinkB * parent_linkb=NULL;
          JsonNode * node_out = NULL;
          JsonObject * node_out_obj = NULL;

//g_message("request_link_record_revision_obj: generating links for link record id=%s with mvcc=%s for context_id=%s href=%s\n", id, mvcc, context_id, href);

          if (dupin_linkbase_get_parent_is_db (linkb) == TRUE )
            {
              if (! (parent_db = dupin_database_open (d, linkb->parent, NULL)))
                {
      		  if (link_rels)
        	    g_strfreev (link_rels);

      		  if (link_labels)
        	    g_strfreev (link_labels);

      		  if (link_hrefs)
        	    g_strfreev (link_hrefs);

      		  if (link_authorities)
        	    g_strfreev (link_authorities);

      		  if (startkey != NULL)
        	    g_free (startkey);

      		  if (endkey != NULL)
        	    g_free (endkey);

      		  if (doc_node != NULL) 
         	    json_node_free (doc_node);

      		  if (params_node != NULL) 
        	    json_node_free (params_node);

		  if (keys != NULL)
		    g_list_free (keys);

      		  if (obj_node != NULL) 
        	    json_node_free (obj_node);

      		  dupin_linkbase_unref (linkb);

      		  return JSValueMakeNull(ctx);
                }

              DupinRecord * doc_id_record = dupin_record_read (parent_db, href, NULL);

              if (doc_id_record == NULL)
	        {
                  node_out = json_node_new (JSON_NODE_OBJECT);
                  node_out_obj = json_object_new ();
                  json_node_take_object (node_out, node_out_obj);
                  json_object_set_boolean_member (node_out_obj, RESPONSE_OBJ_EMPTY, TRUE);
		}
              else
                {
                  if (dupin_record_is_deleted (doc_id_record, NULL) == FALSE)
                    {
		      node_out = json_node_copy (dupin_record_get_revision_node (doc_id_record,
							(gchar *)dupin_record_get_last_revision (doc_id_record)));
		      node_out_obj = json_node_get_object (node_out);
                    }
                  else
		    {
                      node_out = json_node_new (JSON_NODE_OBJECT);
            	      node_out_obj = json_object_new ();
            	      json_node_take_object (node_out, node_out_obj);
            	      json_object_set_boolean_member (node_out_obj, RESPONSE_OBJ_DELETED, TRUE);
		    }

                  json_object_set_string_member (node_out_obj, "_id", (gchar *) dupin_record_get_id (doc_id_record));
                  json_object_set_string_member (node_out_obj, "_rev", dupin_record_get_last_revision (doc_id_record));

                  if (json_object_has_member (node_out_obj, "_created") == TRUE)
                    json_object_remove_member (node_out_obj, "_created"); // ignore any record one if set by user, ever
                  gchar * created = dupin_date_timestamp_to_iso8601 (dupin_record_get_created (doc_id_record));
                  json_object_set_string_member (node_out_obj, "_created", created);
                  g_free (created);

                  if (json_object_has_member (node_out_obj, "_expire") == TRUE)
                    json_object_remove_member (node_out_obj, "_expire"); // ignore any record one if set by user, ever
		  if (dupin_record_get_expire (doc_id_record) != 0)
		    {
                      gchar * expire = dupin_date_timestamp_to_iso8601 (dupin_record_get_expire (doc_id_record));
                      json_object_set_string_member (node_out_obj, "_expire", expire);
                      g_free (expire);
		    }

                  if (json_object_has_member (node_out_obj, "_type") == TRUE)
                    json_object_remove_member (node_out_obj, "_type"); // ignore any record one if set by user, ever
                  json_object_set_string_member (node_out_obj, "_type", (gchar *)dupin_record_get_type (doc_id_record));

                  dupin_record_close (doc_id_record);
                }

              if (json_object_has_member (node_out_obj, "_linkbase") == TRUE)
                json_object_remove_member (node_out_obj, "_linkbase"); // ignore any record one if set by user, ever
              json_object_set_string_member (node_out_obj, "_linkbase", dupin_linkbase_get_name (dupin_database_get_default_linkbase (parent_db)));

              dupin_database_unref (parent_db);
	  }
	else
	  {
	    if (!(parent_linkb = dupin_linkbase_open (d, linkb->parent, NULL)))
              {
      	        if (link_rels)
        	  g_strfreev (link_rels);

      		if (link_labels)
        	  g_strfreev (link_labels);

      		if (link_hrefs)
        	  g_strfreev (link_hrefs);

      		if (link_authorities)
        	  g_strfreev (link_authorities);

      		if (startkey != NULL)
        	  g_free (startkey);

      		if (endkey != NULL)
        	  g_free (endkey);

      		if (doc_node != NULL) 
         	  json_node_free (doc_node);

      		if (params_node != NULL) 
        	  json_node_free (params_node);

		if (keys != NULL)
		  g_list_free (keys);

      		if (obj_node != NULL) 
        	  json_node_free (obj_node);

      		dupin_linkbase_unref (linkb);

      		return JSValueMakeNull(ctx);
              }

            DupinLinkRecord * link_id_record = dupin_link_record_read (parent_linkb, href, NULL);

            if (link_id_record == NULL)
	      {
                node_out = json_node_new (JSON_NODE_OBJECT);
                node_out_obj = json_object_new ();
                json_node_take_object (node_out, node_out_obj);
                json_object_set_boolean_member (node_out_obj, RESPONSE_OBJ_EMPTY, TRUE);
	      }
            else
              {
                if (dupin_link_record_is_deleted (link_id_record, NULL) == FALSE)
                  {
                    node_out = json_node_copy (dupin_link_record_get_revision_node (link_id_record,
                                                        (gchar *)dupin_link_record_get_last_revision (link_id_record)));
		    node_out_obj = json_node_get_object (node_out);
                  }
                else
		  {
                    node_out = json_node_new (JSON_NODE_OBJECT);
            	    node_out_obj = json_object_new ();
            	    json_node_take_object (node_out, node_out_obj);
            	    json_object_set_boolean_member (node_out_obj, RESPONSE_OBJ_DELETED, TRUE);
		  }

	        json_object_set_string_member (node_out_obj, "_id", (gchar *) dupin_link_record_get_id (link_id_record));
                json_object_set_string_member (node_out_obj, "_rev", dupin_link_record_get_last_revision (link_id_record));

                if (json_object_has_member (node_out_obj, "_created") == TRUE)
                  json_object_remove_member (node_out_obj, "_created"); // ignore any record one if set by user, ever
                gchar * created = dupin_date_timestamp_to_iso8601 (dupin_link_record_get_created (link_id_record));
                json_object_set_string_member (node_out_obj, "_created", created);
                g_free (created);

                if (json_object_has_member (node_out_obj, "_expire") == TRUE)
                  json_object_remove_member (node_out_obj, "_expire"); // ignore any record one if set by user, ever
		if (dupin_link_record_get_expire (link_id_record) != 0)
                  {
		    gchar * expire = dupin_date_timestamp_to_iso8601 (dupin_link_record_get_expire (link_id_record));
                    json_object_set_string_member (node_out_obj, "_expire", expire);
                    g_free (expire);
		  }

                dupin_link_record_close (link_id_record);
              }

            if (json_object_has_member (node_out_obj, "_linkbase") == TRUE)
              json_object_remove_member (node_out_obj, "_linkbase"); // ignore any record one if set by user, ever
            json_object_set_string_member (node_out_obj, "_linkbase", linkbase_name);

            dupin_linkbase_unref (parent_linkb);
	  }
	

        if (node_out != NULL)
          json_object_set_member (obj, RESPONSE_LINK_OBJ_DOC_OUT, node_out);
      }

      /* NOTE - set standard internal fields */

      json_object_set_string_member (obj, REQUEST_LINK_OBJ_HREF, href);

      gchar * rel = (gchar *)dupin_link_record_get_rel (record);

      if (rel != NULL)
        json_object_set_string_member (obj, REQUEST_LINK_OBJ_REL, rel);

      gchar * authority = (gchar *)dupin_link_record_get_authority (record);

      if (authority != NULL)
        json_object_set_string_member (obj, REQUEST_LINK_OBJ_AUTHORITY, authority);

      /* Setting _id and _rev: */
      json_object_set_string_member (obj, REQUEST_LINK_OBJ_ID, (gchar *) dupin_link_record_get_id (record));
      json_object_set_string_member (obj, REQUEST_LINK_OBJ_REV, mvcc);

      gchar * created = dupin_date_timestamp_to_http_date (dupin_link_record_get_created (record));
      json_object_set_string_member (obj, RESPONSE_OBJ_CREATED, created);
      g_free (created);

      if (dupin_link_record_get_expire (record) != 0)
        {
          gchar * expire = dupin_date_timestamp_to_http_date (dupin_link_record_get_expire (record));
          json_object_set_string_member (obj, RESPONSE_OBJ_EXPIRE, expire);
          g_free (expire);
	}

      gchar * label = (gchar *)dupin_link_record_get_label (record);

      if (dupin_link_record_is_weblink (record) == TRUE)
        {
	  if (weblinks_node == NULL)
            {
              weblinks_node = json_node_new (JSON_NODE_OBJECT);
              weblinks_obj = json_object_new ();
              json_node_take_object (weblinks_node, weblinks_obj);
              json_object_set_member (links_obj, RESPONSE_OBJ_LINKS, weblinks_node);
            }

          if (json_object_has_member (weblinks_obj, label) == FALSE)
            {
              JsonNode * links_label_node = json_node_new (JSON_NODE_ARRAY);
              JsonArray * links_label_array = json_array_new ();
              json_node_take_array (links_label_node, links_label_array);
              json_object_set_member (weblinks_obj, label, links_label_node);
            }

          json_array_add_element( json_node_get_array ( json_object_get_member (weblinks_obj, label)), obj_node);
        }
      else
        {
	  if (relationships_node == NULL)
            {
              relationships_node = json_node_new (JSON_NODE_OBJECT);
              relationships_obj = json_object_new ();
              json_node_take_object (relationships_node, relationships_obj);
              json_object_set_member (links_obj, RESPONSE_OBJ_RELATIONSHIPS, relationships_node);
            }

          if (json_object_has_member (relationships_obj, label) == FALSE)
            {
              JsonNode * relationships_label_node = json_node_new (JSON_NODE_ARRAY);
              JsonArray * relationships_label_array = json_array_new ();
              json_node_take_array (relationships_label_node, relationships_label_array);
              json_object_set_member (relationships_obj, label, relationships_label_node);
            } 

          json_array_add_element( json_node_get_array ( json_object_get_member (relationships_obj, label)), obj_node);
        }
    }

  gchar * links_json = dupin_util_json_serialize (links); 

  gchar *b=NULL;
  GString * buffer = g_string_new ("var result = ");
  buffer = g_string_append (buffer, links_json);
  buffer = g_string_append (buffer, "; result;");
  b = g_string_free (buffer, FALSE);
  JSStringRef string=JSStringCreateWithUTF8CString(b);
  result = JSEvaluateScript (ctx, string, NULL, NULL, 1, NULL);
  JSStringRelease(string);
  g_free (b);
  g_free (links_json);

  if (link_rels)
    g_strfreev (link_rels);

  if (link_labels)
    g_strfreev (link_labels);

  if (link_hrefs)
    g_strfreev (link_hrefs);

  if (link_authorities)
    g_strfreev (link_authorities);

  if (startkey != NULL)
    g_free (startkey);

  if (endkey != NULL)
    g_free (endkey);

  if( results )
    dupin_link_record_get_list_close (results);

  dupin_linkbase_unref (linkb);

  if (doc_node != NULL)
    json_node_free (doc_node);

  if (params_node != NULL)
    json_node_free (params_node);

  if (keys != NULL)
    g_list_free (keys);
 
  json_node_free (links);

  return result;
}

static void
dupin_webkit_dupin_class_util_http_client_return_headers (const gchar *name,
						          const gchar *value,
						          gpointer data)
{
  json_object_set_string_member (data, name, value);
}

static gboolean
_dupin_webkit_dupin_class_util_http_client_session_authenticate (SoupSession *session,
							         SoupMessage *msg,
							         SoupAuth *auth,
							         gboolean retrying,
							         gpointer callback_data)
{
  g_return_val_if_fail (callback_data != NULL, FALSE);

  /* TODO - we do not use retry yet */

  JsonObject * params_node_obj = (JsonObject *)callback_data;

  gchar * username = NULL;
  if (json_object_has_member (params_node_obj, "username"))
    username = (gchar *)json_object_get_string_member (params_node_obj, "username");

  gchar * password = NULL;
  if (json_object_has_member (params_node_obj, "password"))
    password = (gchar *)json_object_get_string_member (params_node_obj, "password");

  soup_auth_authenticate (auth, username, password);

  return TRUE;
}

static JSValueRef
dupin_webkit_dupin_class_util_http_client (JSContextRef ctx,
                   	    	           JSObjectRef object,
		   	    	           JSObjectRef thisObject, size_t argumentCount,
		   	    	           const JSValueRef arguments[],
		   	    	           JSValueRef * exception)
{
  JSValueRef result=NULL;

  SoupSession *session = NULL;
  SoupURI *proxy = NULL, *parsed;
  SoupMessage *msg;

  /*
     input one argument:

	{
	  "headers": { .... },
	  "params": { .... },
	  "method": "get / post",
	  "body": " body to post ... ",
	  "username": "foo",
	  "password": "password",
	  "user_agent": "Foo/Bar",
	  "debug": true
        }

     returns:

	{
	  "status": ...,
	  "content-type": ....,
	  "headers": ....
	  "rawbody": alway base64 encoded
	  "body": if application/json returns the JSON node, otherwise a string
	}
   */
  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return JSValueMakeNull(ctx);
    }

  if ((!arguments[0]))
    return JSValueMakeNull(ctx);

  JsonNode * params_node = NULL;
  dupin_webkit_value (ctx, arguments[0], &params_node);
  if (params_node == NULL
      || json_node_get_node_type (params_node) != JSON_NODE_OBJECT)
    {
      if (params_node != NULL) 
        json_node_free (params_node);

      return JSValueMakeNull(ctx);
    }
  JsonObject * params_node_obj = json_node_get_object (params_node);

  /* NOTE - parse paramaters */

  gchar * url = NULL;

  if (json_object_has_member (params_node_obj, "url"))
    url = (gchar *)json_object_get_string_member (params_node_obj, "url");
  if (url == NULL)
    {
      if (params_node != NULL) 
        json_node_free (params_node);

      return JSValueMakeNull(ctx);
    }
  parsed = soup_uri_new (url);
  if (parsed == NULL)
    {
      g_warning ("Could not parse '%s' as a URL\n", url);

      if (params_node != NULL) 
        json_node_free (params_node);

      return JSValueMakeNull(ctx);
    }
  soup_uri_free (parsed);

  gchar * proxy_str = NULL;

  if (json_object_has_member (params_node_obj, "proxy"))
    proxy_str = (gchar *)json_object_get_string_member (params_node_obj, "proxy");
  if (proxy_str != NULL)
    {
      proxy = soup_uri_new (proxy_str);
      if (proxy == NULL)
        {
          g_warning ("Could not parse proxy '%s' as a URL\n", proxy_str);

          if (params_node != NULL) 
            json_node_free (params_node);

          return JSValueMakeNull(ctx);
        }
    }

  gchar * method = NULL;
  if (json_object_has_member (params_node_obj, "method"))
    method = (gchar *)json_object_get_string_member (params_node_obj, "method");
  if ((method == NULL)
       || (!g_strcmp0 (method, "get")))
    {
      method = (gchar *)SOUP_METHOD_GET;
    }
  else if (!g_strcmp0 (method, "head"))
    {
      method = (gchar *)SOUP_METHOD_HEAD;
    }
  else
    {
      if (params_node != NULL) 
        json_node_free (params_node);

      return JSValueMakeNull(ctx);
    }

  gchar * user_agent = NULL;
  if (json_object_has_member (params_node_obj, "user_agent"))
    user_agent = (gchar *)json_object_get_string_member (params_node_obj, "user_agent");
  if (user_agent == NULL)
    {
      user_agent = "Dupin/" VERSION;
    }

  gboolean debug = FALSE;

  if (json_object_has_member (params_node_obj, "debug"))
    debug = json_object_get_boolean_member (params_node_obj, "debug");

//DUPIN_UTIL_DUMP_JSON ("dupin_webkit_dupin_class_util_http_client: params:", params_node);

  session = soup_session_sync_new_with_options (
#ifdef HAVE_GNOME
              SOUP_SESSION_ADD_FEATURE_BY_TYPE, SOUP_TYPE_GNOME_FEATURES_2_26,
#endif
              SOUP_SESSION_ADD_FEATURE_BY_TYPE, SOUP_TYPE_CONTENT_DECODER,
              SOUP_SESSION_ADD_FEATURE_BY_TYPE, SOUP_TYPE_COOKIE_JAR,
              SOUP_SESSION_USER_AGENT, user_agent,
              SOUP_SESSION_ACCEPT_LANGUAGE_AUTO, TRUE,
              NULL);

  /* Need to do this after creating the session, since adding
   * SOUP_TYPE_GNOME_FEATURE_2_26 will add a proxy resolver, thereby
   * bashing over the manually-set proxy.
   */
  if (proxy)
    {
      g_object_set (G_OBJECT (session), SOUP_SESSION_PROXY_URI, proxy, NULL);
    }

  /* auth stuff */
  g_signal_connect (session, "authenticate", G_CALLBACK (_dupin_webkit_dupin_class_util_http_client_session_authenticate), params_node_obj);

  /* NOTE - do HTTP GET */

  msg = soup_message_new (method, url);

  soup_session_send_message (session, msg);

  gchar * name = soup_message_get_uri (msg)->path;

  if (debug)
    {
      SoupMessageHeadersIter iter;
      const gchar *hname, *value;
      gchar *path = soup_uri_to_string (soup_message_get_uri (msg), TRUE);

      g_message ("%s %s HTTP/1.%d\n", method, path,
                        soup_message_get_http_version (msg));
      soup_message_headers_iter_init (&iter, msg->request_headers);
      while (soup_message_headers_iter_next (&iter, &hname, &value))
        g_message ("%s: %s\r\n", hname, value);
      g_message ("\n");

      g_message ("HTTP/1.%d %d %s\n", soup_message_get_http_version (msg),
                        msg->status_code, msg->reason_phrase);

      soup_message_headers_iter_init (&iter, msg->response_headers);
      while (soup_message_headers_iter_next (&iter, &hname, &value))
        g_message ("%s: %s\r\n", hname, value);
      g_message ("\n");
    }
  else if (SOUP_STATUS_IS_TRANSPORT_ERROR (msg->status_code))
    g_message ("%s: %d %s\n", name, msg->status_code, msg->reason_phrase);

  JsonNode * response = json_node_new (JSON_NODE_OBJECT);
  JsonObject * response_obj = json_object_new ();
  json_node_take_object (response, response_obj);

  JsonNode * response_headers = json_node_new (JSON_NODE_OBJECT);
  JsonObject * response_headers_obj = json_object_new ();
  json_node_take_object (response_headers, response_headers_obj);

  json_object_set_int_member (response_obj, "status_code", (gint)msg->status_code);
  json_object_set_member (response_obj, "response_headers", response_headers);

  soup_message_headers_foreach (msg->response_headers,
			        dupin_webkit_dupin_class_util_http_client_return_headers,
				response_headers_obj);

  if (msg->status_code == SOUP_STATUS_OK)
    {
      const gchar *header = soup_message_headers_get_one (msg->response_headers,
								"Content-Type");

      if (g_str_has_prefix (header, "application/json") == TRUE)
        {
          /* NOTE - try to parse response */
          JsonParser *parser = json_parser_new ();

          if (json_parser_load_from_data (parser, msg->response_body->data,
						  msg->response_body->length, NULL))
            {
              json_object_set_member (response_obj, "doc",
			json_node_copy (json_parser_get_root (parser)));
            }
          else
            {
              json_object_set_null_member (response_obj, "doc");
            }

          g_object_unref (parser);
        }
      else
        {
          json_object_set_null_member (response_obj, "doc");
	}
      
      gchar *base64 = g_base64_encode ((const guchar *)msg->response_body->data,
				       msg->response_body->length);

      json_object_set_string_member (response_obj, "response_body", base64);

      g_free (base64);
    }
  else
    {
      json_object_set_string_member (response_obj, "reason_phrase", msg->reason_phrase);
    }

  soup_session_abort (session);
  g_object_unref (session);

  if (proxy != NULL)
    soup_uri_free (proxy);

  gchar * response_json = dupin_util_json_serialize (response); 

  gchar *b=NULL;
  GString * buffer = g_string_new ("var result = ");
  buffer = g_string_append (buffer, response_json);
  buffer = g_string_append (buffer, "; result;");
  b = g_string_free (buffer, FALSE);
  JSStringRef string=JSStringCreateWithUTF8CString(b);
  result = JSEvaluateScript (ctx, string, NULL, NULL, 1, NULL);
  JSStringRelease(string);
  g_free (b);
  g_free (response_json);

  if (params_node != NULL)
    json_node_free (params_node);
 
  json_node_free (response);

  return result;
}

static JSValueRef
dupin_webkit_dupin_class_util_hash (JSContextRef ctx,
                   	            JSObjectRef object,
		   	            JSObjectRef thisObject, size_t argumentCount,
		   	            const JSValueRef arguments[],
		   	            JSValueRef * exception)
{
  JSValueRef result=NULL;

  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return JSValueMakeNull(ctx);
    }

  if ((!arguments[0])
      || (!JSValueIsString(ctx, arguments[0])))
    return JSValueMakeNull(ctx);

  JSStringRef string = JSValueToStringCopy (ctx, arguments[0], NULL);
  gchar * input = dupin_webkit_string_utf8 (string);
  JSStringRelease (string);

//g_message ("dupin_webkit_dupin_class_util_hash: input=%s\n", input);

  gchar *md5 = g_compute_checksum_for_string (DUPIN_ID_HASH_ALGO, input, -1);
  string=JSStringCreateWithUTF8CString(md5);
  g_free (input);

//g_message ("dupin_webkit_dupin_class_util_hash: hash=%s\n", md5);

  g_free (md5);
  result = JSValueMakeString(ctx, string);
  JSStringRelease(string);

  return result;
}

static JSValueRef
dupin_webkit_dupin_class_util_base64_encode (JSContextRef ctx,
                   	        	     JSObjectRef object,
		   	        	     JSObjectRef thisObject, size_t argumentCount,
		   	        	     const JSValueRef arguments[],
		   	        	     JSValueRef * exception)
{
  JSValueRef result=NULL;

  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return JSValueMakeNull(ctx);
    }

  if ((!arguments[0])
      || (!JSValueIsString(ctx, arguments[0])))
    return JSValueMakeNull(ctx);

  JSStringRef string = JSValueToStringCopy (ctx, arguments[0], NULL);
  gchar * input = dupin_webkit_string_utf8 (string);
  JSStringRelease (string);

//g_message ("dupin_webkit_dupin_class_util_base64_encode: input=%s\n", input);

  gchar *base64 = g_base64_encode ((const guchar *)input, strlen(input));
  string=JSStringCreateWithUTF8CString(base64);
  g_free (input);

//g_message ("dupin_webkit_dupin_class_util_base64_encode: base64=%s\n", base64);

  g_free (base64);
  result = JSValueMakeString(ctx, string);
  JSStringRelease(string);

  return result;
}

static JSValueRef
dupin_webkit_dupin_class_util_base64_decode (JSContextRef ctx,
                   	        	     JSObjectRef object,
		   	        	     JSObjectRef thisObject, size_t argumentCount,
		   	        	     const JSValueRef arguments[],
		   	        	     JSValueRef * exception)
{
  JSValueRef result=NULL;

  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return JSValueMakeNull(ctx);
    }

  if ((!arguments[0])
      || (!JSValueIsString(ctx, arguments[0])))
    return JSValueMakeNull(ctx);

  JSStringRef string = JSValueToStringCopy (ctx, arguments[0], NULL);
  gchar * base64 = dupin_webkit_string_utf8 (string);
  JSStringRelease (string);

//g_message ("dupin_webkit_dupin_class_util_base64_decode: base64=%s\n", base64);

  gsize buff_size;
  guchar * buff = g_base64_decode ((const gchar *)base64, &buff_size);
  string=JSStringCreateWithUTF8CString((gchar *)buff);
  g_free (base64);

//g_message ("dupin_webkit_dupin_class_util_base64_decode: buff=%s\n", buff);

  g_free (buff);
  result = JSValueMakeString(ctx, string);
  JSStringRelease(string);

  return result;
}

/* EOF */
