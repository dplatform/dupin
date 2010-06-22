#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <JavaScriptCore/JavaScript.h>
#include<glib.h>
#include <stdio.h>

#include "../tbjson/tb_json.h"

static JSValueRef js_emitIntermediate (JSContextRef ctx, JSObjectRef object,
				       JSObjectRef thisObject,
				       size_t argumentCount,
				       const JSValueRef arguments[],
				       JSValueRef * exception);
static JSValueRef js_emit (JSContextRef ctx, JSObjectRef object,
			   JSObjectRef thisObject, size_t argumentCount,
			   const JSValueRef arguments[],
			   JSValueRef * exception);

static void js_value (JSContextRef ctx, JSValueRef value,
		      tb_json_value_t * v);
static void js_obj (JSContextRef ctx, JSObjectRef object,
		    tb_json_object_t * obj);

gint
main (gint argc, gchar ** argv)
{
  JSGlobalContextRef ctx;

  JSStringRef str;
  JSObjectRef func;
  JSValueRef result;
  JSValueRef exception = NULL;

  tb_json_object_t *obj;
  tb_json_node_t *node;

  gchar *buffer;

  if (argc != 2)
    {
      printf ("Usage: %s <script>\n", argv[0]);
      return 1;
    }

  ctx = JSGlobalContextCreate (NULL);

  str = JSStringCreateWithUTF8CString ("emitIntermediate");
  func = JSObjectMakeFunctionWithCallback (ctx, str, js_emitIntermediate);
  JSObjectSetProperty (ctx, JSContextGetGlobalObject (ctx), str, func,
		       kJSPropertyAttributeNone, NULL);
  JSStringRelease (str);

  str = JSStringCreateWithUTF8CString ("emit");
  func = JSObjectMakeFunctionWithCallback (ctx, str, js_emit);
  JSObjectSetProperty (ctx, JSContextGetGlobalObject (ctx), str, func,
		       kJSPropertyAttributeNone, NULL);
  JSStringRelease (str);

  str = JSStringCreateWithUTF8CString (argv[1]);
  result = JSEvaluateScript (ctx, str, NULL, NULL, 0, &exception);
  JSStringRelease (str);

  tb_json_object_new (&obj);

  if (!result || exception)
    {
      tb_json_object_add_node (obj, "error", &node);
      js_value (ctx, exception, tb_json_node_get_value (node));

      tb_json_object_add_node (obj, "status", &node);
      tb_json_value_set_boolean (tb_json_node_get_value (node), FALSE);

      JSGlobalContextRelease (ctx);
    }
  else
    {
      tb_json_object_add_node (obj, "status", &node);
      tb_json_value_set_boolean (tb_json_node_get_value (node), TRUE);
    }

  tb_json_object_write_to_buffer (obj, &buffer, NULL, NULL);
  tb_json_object_destroy (obj);
  puts (buffer);
  g_free (buffer);

  JSGlobalContextRelease (ctx);
  return 0;
}

static gchar *
js_string (JSStringRef js_string)
{
  gsize size;
  gchar *string;

  size = JSStringGetMaximumUTF8CStringSize (js_string);
  string = g_malloc (size + 1);
  JSStringGetUTF8CString (js_string, string, size);

  return string;
}

static void
js_value (JSContextRef ctx, JSValueRef value, tb_json_value_t * v)
{
  switch (JSValueGetType (ctx, value))
    {
    case kJSTypeUndefined:
    case kJSTypeNull:
      break;

    case kJSTypeBoolean:
      tb_json_value_set_boolean (v,
				 JSValueToBoolean (ctx,
						   value) ==
				 true ? TRUE : FALSE);
      break;

    case kJSTypeNumber:
      tb_json_value_set_number (v,
				(gdouble) JSValueToNumber (ctx, value, NULL));
      break;

    case kJSTypeString:
      {
	JSStringRef string;
	gchar *str;

	string = JSValueToStringCopy (ctx, value, NULL);
	str = js_string (string);
	JSStringRelease (string);

	tb_json_value_set_string (v, str);
	g_free (str);
	break;
      }

    case kJSTypeObject:
      {
	tb_json_object_t *obj;
	tb_json_value_set_object_new (v, &obj);
	js_obj (ctx, JSValueToObject (ctx, value, NULL), obj);
	break;
      }
    }

  /* FIXME: array?!? */
}

static void
js_obj (JSContextRef ctx, JSObjectRef object, tb_json_object_t * obj)
{
  JSPropertyNameArrayRef props;
  gsize nprops, i;

  props = JSObjectCopyPropertyNames (ctx, object);
  nprops = JSPropertyNameArrayGetCount (props);

  for (i = 0; i < nprops; i++)
    {
      JSStringRef prop = JSPropertyNameArrayGetNameAtIndex (props, i);

      JSValueRef value;
      tb_json_node_t *node;
      gchar *p;

      p = js_string (prop);
      tb_json_object_add_node (obj, p, &node);
      g_free (p);

      value = JSObjectGetProperty (ctx, object, prop, NULL);
      js_value (ctx, value, tb_json_node_get_value (node));

      JSStringRelease (prop);
    }

  JSPropertyNameArrayRelease (props);
}

static JSValueRef
js_emitIntermediate (JSContextRef ctx, JSObjectRef object,
		     JSObjectRef thisObject, size_t argumentCount,
		     const JSValueRef arguments[], JSValueRef * exception)
{
  tb_json_object_t *obj;
  tb_json_node_t *node;
  gchar *buffer;

  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return NULL;
    }

  tb_json_object_new (&obj);
  tb_json_object_add_node (obj, "emitIntermediate", &node);
  js_value (ctx, (JSObjectRef) arguments[0], tb_json_node_get_value (node));
  tb_json_object_write_to_buffer (obj, &buffer, NULL, NULL);
  tb_json_object_destroy (obj);
  puts (buffer);
  g_free (buffer);

  return NULL;
}

static JSValueRef
js_emit (JSContextRef ctx, JSObjectRef object, JSObjectRef thisObject,
	 size_t argumentCount, const JSValueRef arguments[],
	 JSValueRef * exception)
{
  tb_json_object_t *obj;
  tb_json_node_t *node;
  gchar *buffer;

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

  tb_json_object_new (&obj);
  tb_json_object_add_node (obj, "emit", &node);
  js_value (ctx, (JSObjectRef) arguments[0], tb_json_node_get_value (node));
  tb_json_object_write_to_buffer (obj, &buffer, NULL, NULL);
  tb_json_object_destroy (obj);
  puts (buffer);
  g_free (buffer);

  return NULL;
}

/* EOF */
