#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_js.h"
#include "dupin_internal.h"

#include <string.h>

static void
debug_print_json_node (char * msg, JsonNode * node)
{
  g_assert (node != NULL);
 
  JsonGenerator *gen = json_generator_new();
  json_generator_set_root (gen, node);
  gchar * buffer = json_generator_to_data (gen,NULL);
  g_message("%s - Json Node of type %d: %s\n",msg, (gint)json_node_get_node_type (node), buffer);
  g_free (buffer);
  g_object_unref (gen);
}

static JSValueRef dupin_js_emitIntermediate (JSContextRef ctx,
					     JSObjectRef object,
					     JSObjectRef thisObject,
					     size_t argumentCount,
					     const JSValueRef arguments[],
					     JSValueRef * exception);

static JSValueRef dupin_js_emit (JSContextRef ctx, JSObjectRef object,
				 JSObjectRef thisObject, size_t argumentCount,
				 const JSValueRef arguments[],
				 JSValueRef * exception);

static gchar* dupin_js_string_utf8 (JSStringRef js_string);

static void dupin_js_obj (JSContextRef ctx, JSValueRef object_value,
			  JsonNode ** obj_node);

static void dupin_js_value (JSContextRef ctx, JSValueRef value,
                          JsonNode ** v);

DupinJs *
dupin_js_new (gchar *        js_json,
              gchar *        js_code,
              gchar *        what,
              gchar**        exception_string)
{
  DupinJs *js;
  JSGlobalContextRef ctx;

  JSStringRef str;
  JSObjectRef func, array, globalObject;
  JSValueRef p, result;
  JSValueRef js_exception=NULL;

  g_return_val_if_fail (js_json != NULL, NULL);
  g_return_val_if_fail (js_code != NULL, NULL);
  g_return_val_if_fail (what != NULL, NULL);

  if (g_utf8_validate (js_json, -1, NULL) == FALSE)
    return NULL;

  if (g_utf8_validate (js_code, -1, NULL) == FALSE)
    return NULL;

  ctx = JSGlobalContextCreate (NULL);
  globalObject = JSContextGetGlobalObject(ctx);

  js = g_malloc0 (sizeof (DupinJs));

  /* work around 32 vs 64 bits points problem of original solution using GPOINTER_TO_INT / GINT_TO_POINTER
     in combination with webkit/gtk (I.e. 0x10248dff0 vs 0x248dff0 nightmare) by passing explicitly DupinJs
     structure pointer js to Javascript and back we keep a global in scope of context array and object
     to be filled in by the various calls to emitIntermediate() and emit()

     TODO:
	-> we should have ctx in scope of of caller to avoid too many of these and
           if we do it, note we could have multiple views, so we should have the
           global state in context kept per view (E.g. an object or array of states)


     ALGORITHM:
	-> make a pice of Javascript contaning the JSON + code (map/reduce)
	-> set call back for map and reduce to which fills in global javascript array of results
	-> post-process arrays of results and build up JSON structure to return
  */

  /* NOTE - we could actually have the follwing written in Javascript directly and pre-pended to script */

  /* hack to use built-in Array constructor - we can not use JSObjectMakeArray for this */

  /* array for maps */
  str = JSStringCreateWithUTF8CString ("return new Array");
  array = JSObjectMakeFunction(ctx, NULL, 0, NULL, str, NULL, 1, NULL);
  JSStringRelease (str);
  p = JSObjectCallAsFunction(ctx, array, NULL, 0, NULL, NULL);
  str = JSStringCreateWithUTF8CString ("__dupin_emitIntermediate");
  JSObjectSetProperty (ctx, globalObject, str, p,
		       kJSPropertyAttributeDontDelete, NULL); /* or kJSPropertyAttributeNone ? */
  JSStringRelease (str);

  /* array for reduce(s) - yes we do use an array of one element - can't get JS object to work */
  str = JSStringCreateWithUTF8CString ("return new Array");
  array = JSObjectMakeFunction(ctx, NULL, 0, NULL, str, NULL, 1, NULL);
  JSStringRelease (str);
  p = JSObjectCallAsFunction(ctx, array, NULL, 0, NULL, NULL);
  str = JSStringCreateWithUTF8CString ("__dupin_emit");
  JSObjectSetProperty (ctx, globalObject, str, p,
		       kJSPropertyAttributeDontDelete, NULL); /* or kJSPropertyAttributeNone ? */
  JSStringRelease (str);

  str = JSStringCreateWithUTF8CString ("emitIntermediate");
  func =
    JSObjectMakeFunctionWithCallback (ctx, str, dupin_js_emitIntermediate);
  JSObjectSetProperty (ctx, globalObject, str, func,
		       kJSPropertyAttributeNone, NULL);
  JSStringRelease (str);

  str = JSStringCreateWithUTF8CString ("emit");
  func = JSObjectMakeFunctionWithCallback (ctx, str, dupin_js_emit);
  JSObjectSetProperty (ctx, globalObject, str, func,
		       kJSPropertyAttributeNone, NULL);
  JSStringRelease (str);

  GString *buffer;
  gchar *b=NULL;
  if (g_strcmp0 (what, "map")==0)
    {
      buffer = g_string_new ("var mapObject = ");
    }
  else
    {
      buffer = g_string_new ("var mapObjects = ");
    }

  /* we need to split up js_json strings in chunks of to avoid problems with JS parsing of big >27k JS */
  buffer = g_string_append_len (buffer, js_json, strlen(js_json));
  buffer = g_string_append (buffer, ";\n");
  buffer = g_string_append (buffer, js_code);
  b = g_string_free (buffer, FALSE);

  //g_message("script is: %s\n",js_code);
  //g_message("json is: %s\n",js_json);
  //g_message("whole is: %s\n",b);

  str = JSStringCreateWithUTF8CString (b);
  result = JSEvaluateScript (ctx, str, NULL, NULL, 0, &js_exception);
  JSStringRelease (str);

  if (!result)
    {
      JSStringRef js_message = JSValueToStringCopy (ctx, js_exception, NULL);
      gchar* value = dupin_js_string_utf8 (js_message);
      if (exception_string)
        *exception_string = value;
      else
        {
          g_warning ("dupin_js_new: %s", value);
          g_free (value);
        }
      JSStringRelease (js_message);

      JSGlobalContextRelease (ctx);
      dupin_js_destroy (js);
      return NULL;
    }
  else
    {
      /* convert matching Javascript objects to JSON */

      /* TODO - we should either set reduce if there/set or just map */

      /* mapped values */
      gint i;
      gsize nmaps;
      JSStringRef str;
      JSObjectRef maps;
      JSPropertyNameArrayRef maps_names;

      str = JSStringCreateWithUTF8CString ("__dupin_emitIntermediate");
      JSValueRef mo = JSObjectGetProperty (ctx, globalObject, str, NULL);
      JSStringRelease (str);
      if (mo)
        {
          maps = JSValueToObject(ctx, mo, NULL);
          maps_names = JSObjectCopyPropertyNames (ctx, maps);
          nmaps = JSPropertyNameArrayGetCount (maps_names);

          if (!js->emitIntermediate)
            js->emitIntermediate = json_array_new ();

          for (i = 0; i < nmaps; i++)
            {
              JsonNode *v;
              JSValueRef  ele = JSObjectGetPropertyAtIndex (ctx,maps,i,NULL);

              dupin_js_value (ctx, ele, &v);

              json_array_add_element(js->emitIntermediate, v);
            }

	  /* debug print what's there */
#if 0
	  JsonNode *node = json_node_new (JSON_NODE_ARRAY);
          json_node_set_array (node, js->emitIntermediate);
          debug_print_json_node ("emitIntermediate: ",node);
          json_node_free (node);
#endif

          JSPropertyNameArrayRelease (maps_names);
        }

      /* reduced value */
      gsize nreduces;
      JSObjectRef reduces;
      JSPropertyNameArrayRef reduces_names;
      str = JSStringCreateWithUTF8CString ("__dupin_emit");
      JSValueRef ro = JSObjectGetProperty (ctx, globalObject, str, NULL);
      JSStringRelease (str);
      if (ro)
        {
          reduces = JSValueToObject(ctx, ro, NULL);
          reduces_names = JSObjectCopyPropertyNames (ctx, reduces);
          nreduces = JSPropertyNameArrayGetCount (reduces_names);
          JSPropertyNameArrayRelease (reduces_names);

          /* we just consider the first element of reduces array I.e. we could not get a proper JS object to work :( */
          JSValueRef  ele = JSObjectGetPropertyAtIndex (ctx,reduces,0,NULL);

          if (ele
              && JSValueGetType (ctx, ele) == kJSTypeObject)
            {
              if (js->emit)
                json_node_free (js->emit);

	      /* this is not dupin_js_value() */
              dupin_js_obj (ctx, ele, &js->emit);

	      /* debug print what's there */
#if 0
              debug_print_json_node ("emit: ",js->emit);
#endif
            }
        }
    }

  JSGlobalContextRelease (ctx);

  return js;
}

static gchar*
dupin_js_string_utf8 (JSStringRef js_string)
{
    size_t size_utf8;
    gchar* string_utf8;

    g_return_val_if_fail (js_string, NULL);

    size_utf8 = JSStringGetMaximumUTF8CStringSize (js_string);
    string_utf8 = g_new (gchar, size_utf8);
    JSStringGetUTF8CString (js_string, string_utf8, size_utf8);
    return string_utf8;
}

void
dupin_js_destroy (DupinJs * js)
{
  if (!js)
    return;

  if (js->emit)
    json_node_free (js->emit);

  if (js->emitIntermediate)
    json_array_unref (js->emitIntermediate);

  g_free (js);
}

const JsonNode *
dupin_js_get_emit (DupinJs * js)
{
  g_return_val_if_fail (js != NULL, NULL);

  return js->emit;
}

const JsonArray *
dupin_js_get_emitIntermediate (DupinJs * js)
{
  g_return_val_if_fail (js != NULL, NULL);

  return js->emitIntermediate;
}

gchar *
dupin_js_string (JSStringRef js_string)
{
  gsize size;
  gchar *string;

  size = JSStringGetMaximumUTF8CStringSize (js_string);
  string = g_malloc (size + 1);
  JSStringGetUTF8CString (js_string, string, size);

  return string;
}

static void
dupin_js_value (JSContextRef ctx, JSValueRef value, JsonNode ** v)
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
	str = dupin_js_string (string);
	JSStringRelease (string);

        *v = json_node_new (JSON_NODE_VALUE);

        json_node_set_string (*v, str);

	g_free (str);
	break;
      }

    case kJSTypeObject:
      {
        dupin_js_obj (ctx, value, v);

	break;
      }
    }

  /* FIXME: array?!? integer?!?
            -> probably arrays are considered instances of Array() Javascript object ?!

            see http://developer.apple.com/library/mac/#documentation/Carbon/Reference/WebKit_JavaScriptCore_Ref/JSValueRef_h/index.html%23//apple_ref/c/func/JSValueGetType */
}

static void
dupin_js_obj (JSContextRef ctx, JSValueRef object_value, JsonNode ** obj_node)
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

  JsonObject * obj;
  JsonArray * arr;
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

      p = dupin_js_string (prop);

      value = JSObjectGetProperty (ctx, object, prop, NULL);
      dupin_js_value (ctx, value, &node);

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

static JSValueRef
dupin_js_emitIntermediate (JSContextRef ctx, JSObjectRef object,
			   JSObjectRef thisObject, size_t argumentCount,
			   const JSValueRef arguments[],
			   JSValueRef * exception)
{
  /* we should have this in JS very simply like function emitIntermediate(mapObject) { window.__dupin_emitIntermediate.push(mapObject); } */
  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return NULL;
    }

  JSStringRef str;
  JSObjectRef array;
  gsize last;
  JSPropertyNameArrayRef array_names;

  /* keep object in global scope */
  JSObjectRef globalObject = JSContextGetGlobalObject(ctx);
  str = JSStringCreateWithUTF8CString ("__dupin_emitIntermediate");
  JSValueRef o = JSObjectGetProperty (ctx, globalObject, str, NULL);
  JSStringRelease (str);
  array = JSValueToObject(ctx,o, NULL);
  array_names = JSObjectCopyPropertyNames (ctx, array);
  last = JSPropertyNameArrayGetCount (array_names);
  JSPropertyNameArrayRelease (array_names);

  /* push */
  JSObjectSetPropertyAtIndex(ctx,array,last, arguments[0], NULL);

  return NULL;
}

static JSValueRef
dupin_js_emit (JSContextRef ctx, JSObjectRef object, JSObjectRef thisObject,
	       size_t argumentCount, const JSValueRef arguments[],
	       JSValueRef * exception)
{
  /* we should have this in JS very simply like function emit(reduceObject) { if (typeof(reduceObject) != 'object') return; window.__dupin_emit = reduceObject; } */

  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return NULL;
    }

  if (JSValueIsObject (ctx, arguments[0]) == false)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return NULL;
    }

  JSStringRef str;
  JSObjectRef array;

  /* keep object in global scope */
  JSObjectRef globalObject = JSContextGetGlobalObject(ctx);
  str = JSStringCreateWithUTF8CString ("__dupin_emit");
  JSValueRef o = JSObjectGetProperty (ctx, globalObject, str, NULL);
  JSStringRelease (str);
  array = JSValueToObject(ctx,o, NULL);

  /* push */
  JSObjectSetPropertyAtIndex(ctx,array,0, arguments[0], NULL);

  return NULL;
}

/* EOF */
