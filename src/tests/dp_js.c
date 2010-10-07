#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <JavaScriptCore/JavaScript.h>
#include <glib.h>
#include <stdio.h>

#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

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
		      JsonNode ** v);
static void js_obj (JSContextRef ctx, JSObjectRef object,
		    JsonObject * obj);

gint
main (gint argc, gchar ** argv)
{
  JSGlobalContextRef ctx;

  JSStringRef str;
  JSObjectRef func;
  JSValueRef result;
  JSValueRef exception = NULL;

  JsonObject *obj;
  JsonNode *node;

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

  obj = json_object_new ();

  if (!result || exception)
    {
      js_value (ctx, exception, &node);

      json_object_set_member (obj, "error", node);

      json_object_set_boolean_member (obj, "status", FALSE);

      JSGlobalContextRelease (ctx);
    }
  else
    {
      json_object_set_boolean_member (obj, "status", TRUE);
    }

  JsonNode *node1 = json_node_new (JSON_NODE_OBJECT);

  if (node1 == NULL)
    {
      g_object_unref (obj);
      JSGlobalContextRelease (ctx);
      return 1;
    }

  json_node_set_object (node1, obj);

  JsonGenerator *gen = json_generator_new();

  if (gen == NULL)
    {
      g_object_unref (node1);
      JSGlobalContextRelease (ctx);
      return 1;
    }

  json_generator_set_root (gen, node1 );
  buffer = json_generator_to_data (gen,NULL);

  if (buffer == NULL)
    {
      g_object_unref (node1);
      JSGlobalContextRelease (ctx);
      return 1;
    }

  g_object_unref (node1);

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
js_value (JSContextRef ctx, JSValueRef value, JsonNode ** v)
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
        str = js_string (string);
        JSStringRelease (string);

        *v = json_node_new (JSON_NODE_VALUE);

        json_node_set_string (*v, str);

        g_free (str);
        break;
      }

    case kJSTypeObject:
      {
        *v = json_node_new (JSON_NODE_OBJECT);

        js_obj (ctx, JSValueToObject (ctx, value, NULL), json_node_get_object (*v));
        break;
      }
    }

  /* FIXME: array?!? integer?!?
            -> probably arrays are considered instances of Array() Javascript object ?!

            see http://developer.apple.com/library/mac/#documentation/Carbon/Reference/WebKit_JavaScriptCore_Ref/JSValueRef_h/index.html%23//apple_ref/c/func/JSValueGetType */
}

static void
js_obj (JSContextRef ctx, JSObjectRef object, JsonObject * obj)
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

      p = js_string (prop);

      value = JSObjectGetProperty (ctx, object, prop, NULL);
      js_value (ctx, value, &node);

      json_object_set_member (obj, p, node);

      g_free (p);
      JSStringRelease (prop);
    }

  JSPropertyNameArrayRelease (props);
}

static JSValueRef
js_emitIntermediate (JSContextRef ctx, JSObjectRef object,
                           JSObjectRef thisObject, size_t argumentCount,
                           const JSValueRef arguments[],
                           JSValueRef * exception)
{
  JsonObject *obj;
  JsonNode *node;
  gchar *buffer;
  if (argumentCount != 1)
    {
      *exception = JSValueMakeNumber (ctx, 1);
      return NULL;
    }

  obj = json_object_new();

  js_value (ctx, (JSObjectRef) arguments[0], &node);

  json_object_set_member (obj, "emitIntermediate", node);

  JsonNode *node1 = json_node_new (JSON_NODE_OBJECT);

  if (node1 == NULL)
    {
      g_object_unref (obj);
      return NULL;
    }

  json_node_set_object (node1, obj);

  JsonGenerator *gen = json_generator_new();

  if (gen == NULL)
    {
      g_object_unref (node1);
      return NULL;
    }

  json_generator_set_root (gen, node1 );
  buffer = json_generator_to_data (gen,NULL);

  if (buffer == NULL)
    {
      g_object_unref (node1);
      g_object_unref (gen);
      return NULL;
    }

  g_object_unref (node1);
  g_object_unref (gen);

  puts (buffer);
  g_free (buffer);

  return NULL; /* shouldn't be an object ? */
}

static JSValueRef
js_emit (JSContextRef ctx, JSObjectRef object, JSObjectRef thisObject,
	 size_t argumentCount, const JSValueRef arguments[],
	 JSValueRef * exception)
{
  JsonObject *obj;
  JsonNode *node;
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

  obj = json_object_new();

  js_value (ctx, (JSObjectRef) arguments[0], &node);
  json_object_set_member (obj, "emit", node);

  JsonNode *node1 = json_node_new (JSON_NODE_OBJECT);

  if (node1 == NULL)
    {
      g_object_unref (obj);
      return NULL;
    }

  json_node_set_object (node1, obj);

  JsonGenerator *gen = json_generator_new();

  if (gen == NULL)
    {
      g_object_unref (node1);
      return NULL;
    }

  json_generator_set_root (gen, node1 );
  buffer = json_generator_to_data (gen,NULL);

  if (buffer == NULL)
    {
      g_object_unref (node1);
      g_object_unref (gen);
      return NULL;
    }

  g_object_unref (node1);
  g_object_unref (gen);

  puts (buffer);
  g_free (buffer);

  return NULL; /* shouldn't be an object ? */
}
/* EOF */
