#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include "dupin_js.h"
#include "dupin_internal.h"

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

static void dupin_js_obj (JSContextRef ctx, JSObjectRef object,
			  JsonObject * obj);

DupinJs *
dupin_js_new (gchar * script)
{
  DupinJs *js;
  JSGlobalContextRef ctx;

  JSStringRef str;
  JSObjectRef func;
  JSValueRef p, result;

  g_return_val_if_fail (script != NULL, NULL);

  if (g_utf8_validate (script, -1, NULL) == FALSE)
    return NULL;

  ctx = JSGlobalContextCreate (NULL);
  js = g_malloc0 (sizeof (DupinJs));

  // FIXME: this is an horrible hack:
  str = JSStringCreateWithUTF8CString ("__dupin_p");
  p = JSValueMakeNumber (ctx, (gdouble) GPOINTER_TO_INT (js));
  JSObjectSetProperty (ctx, JSContextGetGlobalObject (ctx), str, p,
		       kJSPropertyAttributeReadOnly, NULL);
  JSStringRelease (str);

  str = JSStringCreateWithUTF8CString ("emitIntermediate");
  func =
    JSObjectMakeFunctionWithCallback (ctx, str, dupin_js_emitIntermediate);
  JSObjectSetProperty (ctx, JSContextGetGlobalObject (ctx), str, func,
		       kJSPropertyAttributeNone, NULL);
  JSStringRelease (str);

  str = JSStringCreateWithUTF8CString ("emit");
  func = JSObjectMakeFunctionWithCallback (ctx, str, dupin_js_emit);
  JSObjectSetProperty (ctx, JSContextGetGlobalObject (ctx), str, func,
		       kJSPropertyAttributeNone, NULL);
  JSStringRelease (str);

  str = JSStringCreateWithUTF8CString (script);
  result = JSEvaluateScript (ctx, str, NULL, NULL, 0, NULL);
  JSStringRelease (str);

  if (!result)
    {
      JSGlobalContextRelease (ctx);
      dupin_js_destroy (js);
      return NULL;
    }

  JSGlobalContextRelease (ctx);
  return js;
}

void
dupin_js_destroy (DupinJs * js)
{
  if (!js)
    return;

  if (js->emit)
    g_object_unref (js->emit);

  if (js->emitIntermediate)
    g_object_unref (js->emitIntermediate);

  g_free (js);
}

const JsonObject *
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
        *v = json_node_new (JSON_NODE_OBJECT);

	dupin_js_obj (ctx, JSValueToObject (ctx, value, NULL), json_node_get_object (*v));
	break;
      }
    }

  /* FIXME: array?!? integer?!?
            -> probably arrays are considered instances of Array() Javascript object ?!

            see http://developer.apple.com/library/mac/#documentation/Carbon/Reference/WebKit_JavaScriptCore_Ref/JSValueRef_h/index.html%23//apple_ref/c/func/JSValueGetType */
}

static void
dupin_js_obj (JSContextRef ctx, JSObjectRef object, JsonObject * obj)
{
  JSPropertyNameArrayRef props;
  gsize nprops, i;

  props = JSObjectCopyPropertyNames (ctx, object);
  nprops = JSPropertyNameArrayGetCount (props);

  for (i = 0; i < nprops; i++)
    {
      JSStringRef prop = JSPropertyNameArrayGetNameAtIndex (props, i);

      JSValueRef value;
      JsonNode *node;
      gchar *p;

      p = dupin_js_string (prop);

      value = JSObjectGetProperty (ctx, object, prop, NULL);
      dupin_js_value (ctx, value, &node);

      json_object_set_member (obj, p, node);

      g_free (p);
      JSStringRelease (prop);
    }

  JSPropertyNameArrayRelease (props);
}

static JSValueRef
dupin_js_emitIntermediate (JSContextRef ctx, JSObjectRef object,
			   JSObjectRef thisObject, size_t argumentCount,
			   const JSValueRef arguments[],
			   JSValueRef * exception)
{
  JSValueRef value;
  JSStringRef str;
  DupinJs *js;

  JsonNode *v;

  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return NULL;
    }

  str = JSStringCreateWithUTF8CString ("__dupin_p");
  value =
    JSObjectGetProperty (ctx, JSContextGetGlobalObject (ctx), str, NULL);
  JSStringRelease (str);

  if (!value
      || !(js = GINT_TO_POINTER ((gint) JSValueToNumber (ctx, value, NULL))))
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return NULL;
    }

  if (!js->emitIntermediate)
    js->emitIntermediate = json_array_new ();

  dupin_js_value (ctx, (JSObjectRef) arguments[0], &v);

  json_array_add_element(js->emitIntermediate, v);

  return NULL;
}

static JSValueRef
dupin_js_emit (JSContextRef ctx, JSObjectRef object, JSObjectRef thisObject,
	       size_t argumentCount, const JSValueRef arguments[],
	       JSValueRef * exception)
{
  JSValueRef value;
  JSStringRef str;
  DupinJs *js;

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

  str = JSStringCreateWithUTF8CString ("__dupin_p");
  value =
    JSObjectGetProperty (ctx, JSContextGetGlobalObject (ctx), str, NULL);
  JSStringRelease (str);

  if (!value
      || !(js = GINT_TO_POINTER ((gint) JSValueToNumber (ctx, value, NULL))))
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return NULL;
    }

  if (js->emit)
    g_object_unref (js->emit);

  js->emit = json_object_new ();

  dupin_js_obj (ctx, (JSObjectRef) arguments[0], js->emit);

  return NULL;
}

/* EOF */
