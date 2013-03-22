#include <dupin.h>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#ifndef G_OS_WIN32
#  include <unistd.h>
#else
#  ifndef STDIN_FILENO
#    define STDIN_FILENO 0
#  endif
#endif

#define DP_HELP	15

/* Global variables: */
Dupin *d = NULL;
DupinDB *db = NULL;
DupinView *view = NULL;
DupinRecord *record = NULL;
DupinViewRecord *viewRecord = NULL;
gboolean dp_exit = FALSE;

/* Propotypes: */
static void prompt (void);
static void parse (GList * list);
static GList *split (gchar * ar);
static JsonNode *json (gchar * str);
static void showRecord (DupinRecord * record);
static void showViewRecord (DupinViewRecord * record);

/* Command Prototypes: */
static void command_help (GList *);
static void command_exit (GList *);
static void command_getDbs (GList *);
static void command_openDb (GList *);
static void command_newDb (GList *);
static void command_deleteDb (GList *);
static void command_countDb (GList *);
static void command_getViews (GList *);
static void command_openView (GList *);
static void command_newView (GList *);
static void command_deleteView (GList *);
static void command_countView (GList *);
static void command_createRecord (GList *);
static void command_createRecordFromFile (GList *);
static void command_readRecord (GList *);
static void command_getListRecord (GList *);
static void command_updateRecord (GList *);
static void command_deleteRecord (GList *);
static void command_showRecord (GList *);
static void command_readViewRecord (GList *);
static void command_getListViewRecord (GList *);
static void command_showViewRecord (GList *);

struct dp_args
{
  gint args;
  gchar *name;
  gchar *description;
  void (*func) (GList *);
} dp_args[] =
{
  {
  0, "help", "Show the help menu.", command_help},
  {
  0, "exit", "Exit from this tool.", command_exit},
  {
  0, "getDbs", "Show the list of databases.", command_getDbs},
  {
  1, "openDb", "Open a database.", command_openDb},
  {
  1, "newDb", "Create a new database.", command_newDb},
  {
  0, "deleteDb", "Delete a database.", command_deleteDb},
  {
  0, "countDb", "Count the record into a database.", command_countDb},
  {
  0, "getViews", "Show the list of views.", command_getViews},
  {
  1, "openView", "Open a view.", command_openView},
  {
  7, "newView", "Create a new view.", command_newView},
  {
  0, "deleteView", "Delete a view.", command_deleteView},
  {
  0, "countView", "Count the record into a view.", command_countView},
  {
  1, "createRecord", "Insert a record.", command_createRecord},
  {
  1, "createRecordFromFile", "Insert a record from a file.", command_createRecordFromFile},
  {
  1, "readRecord", "Get a record.", command_readRecord},
  {
  3, "getListRecord", "Get a list of records.", command_getListRecord},
  {
  1, "updateRecord", "Update a record.", command_updateRecord},
  {
  0, "deleteRecord", "Delete a record.", command_deleteRecord},
  {
  0, "showRecord", "Show a record.", command_showRecord},
  {
  1, "readViewRecord", "Get a view record.", command_readViewRecord},
  {
  3, "getListViewRecord", "Get a list of view records.",
      command_getListViewRecord},
  {
  0, "showViewRecord", "Show a view record.", command_showViewRecord},
  {
  0, NULL, NULL, NULL}
};

int
main (void)
{
  GIOChannel *io;
  GError *error = NULL;

#if !GLIB_CHECK_VERSION (2, 31, 0)
  // better make double-sure glib itself is initialized properly.
  if (!g_thread_supported ())
        g_thread_init (NULL);
#endif

  g_type_init();

  if (!(d = dupin_init (NULL, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
      return 1;
    }

#ifdef G_OS_WIN32
  io = g_io_channel_win32_new_fd (STDIN_FILENO);
#else
  io = g_io_channel_unix_new (STDIN_FILENO);
#endif

  while (dp_exit == FALSE)
    {
      gsize last;
      gchar *line;
      GIOStatus status;

      GError *error = NULL;

      prompt ();

      status = g_io_channel_read_line (io, &line, NULL, &last, &error);

      if (status == G_IO_STATUS_ERROR)
	{
	  fprintf (stderr, "Error: %s\n", error->message);
	  g_error_free (error);
	  break;
	}

      if (status == G_IO_STATUS_AGAIN)
	continue;

      if (status == G_IO_STATUS_EOF)
	break;

      line[last] = 0;

      if (line[0])
	{
	  GList *part;

	  part = split (line);
	  parse (part);

	  g_list_foreach (part, (GFunc) g_free, NULL);
	  g_list_free (part);
	}
      g_free (line);
    }

  if (record)
    dupin_record_close (record);

  if (db)
    dupin_database_unref (db);

  if (view)
    dupin_view_unref (view);

  g_io_channel_shutdown (io, FALSE, NULL);
  g_io_channel_unref (io);

  dupin_shutdown (d);
  return 0;
}

static void
prompt (void)
{
  fprintf (stdout, "Dupin");

  if (db)
    fprintf (stdout, " DB(%s)", dupin_database_get_name (db));

  if (view)
    fprintf (stdout, " VIEW(%s)", dupin_view_get_name (view));

  if (record)
    fprintf (stdout, " R(%s)", dupin_record_get_id (record));

  if (viewRecord)
    fprintf (stdout, " VR(%s)", dupin_view_record_get_id (viewRecord));

  fprintf (stdout, "$ ");
  fflush (stdout);
}

static GList *
split (gchar * ar)
{
  gchar opt[1024];
  gint a = 0;
  gint q1 = 0;
  gint q2 = 0;
  GList *ret = NULL;

  while (*ar)
    {
      if (*ar == '\"' && !q2)
	{
	  q1 = !q1;
	  ar++;
	}

      else if (*ar == '\'' && !q1)
	{
	  q2 = !q2;
	  ar++;
	}

      else if ((*ar == ' ' || *ar == '\t') && !q1 && !q2)
	{
	  ar++;
	  if (a)
	    {
	      opt[a] = 0;
	      ret = g_list_append (ret, g_strdup (opt));
	    }

	  a = 0;
	}
      else if (a < sizeof (opt) - 1)
	opt[a++] = *ar++;
    }

  if (a)
    {
      opt[a] = 0;
      ret = g_list_append (ret, g_strdup (opt));
    }

  return ret;
}

static void
parse (GList * list)
{
  gint i;

  for (i = 0; dp_args[i].name; i++)
    {
      if (!g_utf8_collate (list->data, dp_args[i].name))
	{
	  if (dp_args[i].args != g_list_length (list) - 1)
	    {
	      printf ("The command '%s' needs %d parameter(s).\n",
		      dp_args[i].name, dp_args[i].args);
	      return;
	    }

	  dp_args[i].func (list->next);
	  return;
	}
    }

  printf ("Command unknown. Write 'help' to get the list of commands.\n");
}

static JsonNode *
json (gchar * str)
{
  JsonParser *j;
  JsonNode *obj_node;

  j = json_parser_new ();

  if (j == NULL)
    return NULL;

  if (json_parser_load_from_data (j, str, -1, NULL) == FALSE)
    {
      g_object_unref (j);
      return NULL;
    }

  JsonNode * node = json_parser_get_root (j);

  if (node == NULL)
    {
      g_object_unref (j);
      return NULL;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      g_object_unref (j);
      return NULL;
    }

  obj_node = json_node_copy (node);

  g_object_unref (j);

  return obj_node;
}

static JsonNode *
json_file (gchar * filename)
{
  JsonParser *j;
  JsonNode *obj_node;

  j = json_parser_new ();

  if (j == NULL)
    return NULL;

  if (json_parser_load_from_file (j, filename, NULL) == FALSE)
    {
      g_object_unref (j);
      return NULL;
    }

  JsonNode * node = json_parser_get_root (j);

  if (node == NULL)
    {
      g_object_unref (j);
      return NULL;
    }

  if (json_node_get_node_type (node) != JSON_NODE_OBJECT)
    {
      g_object_unref (j);
      return NULL;
    }

  obj_node = json_node_copy (node);

  g_object_unref (j);

  return obj_node;
}

/* Commands: */
static void
command_help (GList * list)
{
  gint i, j, k;
  gchar **split;

  for (i = 0; dp_args[i].name; i++)
    {
      printf ("%s", dp_args[i].name);

      for (j = strlen (dp_args[i].name); j < DP_HELP; j++)
	printf (" ");

      printf (" - ");

      split = g_strsplit (dp_args[i].description, "\n", -1);
      for (j = 0; split[j]; j++)
	{
	  if (j)
	    {
	      for (k = 0; k < DP_HELP; k++)
		printf (" ");

	      printf ("   ");
	    }

	  printf ("%s\n", split[j]);
	}

      g_strfreev (split);
    }

  printf ("\n");
}

static void
command_exit (GList * list)
{
  dp_exit = TRUE;
}

static void
command_getDbs (GList * list)
{
  gchar **ret;
  gint i;

  for (i = 0, ret = dupin_get_databases (d); ret && ret[i]; i++)
    fprintf (stdout, "%s\n", ret[i]);

  if (ret)
    g_strfreev (ret);

  fprintf (stdout, "Total: %d databases\n", i);
}

static void
command_openDb (GList * list)
{
  GError *error = NULL;

  if (db)
    dupin_database_unref (db);

  if (!(db = dupin_database_open (d, list->data, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }
}

static void
command_newDb (GList * list)
{
  GError *error = NULL;

  if (db)
    dupin_database_unref (db);

  if (!(db = dupin_database_new (d, list->data, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }
}

static void
command_deleteDb (GList * list)
{
  GError *error = NULL;

  if (!db)
    {
      fprintf (stderr, "Open a Db.\n");
      return;
    }

  if (dupin_database_delete (db, &error) == FALSE)
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }

  if (db)
    {
      dupin_database_unref (db);
      db = NULL;
    }
}

static void
command_countDb (GList * list)
{
  if (!db)
    {
      fprintf (stderr, "Open a Db.\n");
      return;
    }

  fprintf (stdout, "Total: %" G_GSIZE_FORMAT "\n",
	   dupin_database_count (db, DP_COUNT_ALL));
  fprintf (stdout, "Existing: %" G_GSIZE_FORMAT "\n",
	   dupin_database_count (db, DP_COUNT_EXIST));
  fprintf (stdout, "Deleted: %" G_GSIZE_FORMAT "\n",
	   dupin_database_count (db, DP_COUNT_DELETE));
}

static void
command_getViews (GList * list)
{
  gchar **ret;
  gint i;

  for (i = 0, ret = dupin_get_views (d); ret && ret[i]; i++)
    fprintf (stdout, "%s\n", ret[i]);

  if (ret)
    g_strfreev (ret);

  fprintf (stdout, "Total: %d views\n", i);
}

static void
command_openView (GList * list)
{
  GError *error = NULL;

  if (view)
    dupin_view_unref (view);

  if (!(view = dupin_view_open (d, list->data, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }
}

static void
command_newView (GList * list)
{
  GError *error = NULL;

  gchar *name = list->data;
  gchar *parent = g_list_nth_data (list, 1);
  gchar *is_db = g_list_nth_data (list, 2);
  gchar *is_linkb = g_list_nth_data (list, 3);
  gchar *map = g_list_nth_data (list, 4);
  gchar *map_lang = g_list_nth_data (list, 5);
  gchar *reduce = g_list_nth_data (list, 6);
  gchar *reduce_lang = g_list_nth_data (list, 7);

  if (view)
    dupin_view_unref (view);

  if (!
      (view =
       dupin_view_new (d, name, parent,
		       !g_strcmp0 (is_db, "true") ? TRUE : FALSE,
		       !g_strcmp0 (is_linkb, "true") ? TRUE : FALSE,
		       map,
		       dupin_util_mr_lang_to_enum (map_lang), reduce,
		       dupin_util_mr_lang_to_enum (reduce_lang),
		       NULL, FALSE, FALSE, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }
}

static void
command_deleteView (GList * list)
{
  GError *error = NULL;

  if (!view)
    {
      fprintf (stderr, "Open a View.\n");
      return;
    }

  if (dupin_view_delete (view, &error) == FALSE)
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }

  if (view)
    {
      dupin_view_unref (view);
      view = NULL;
    }
}

static void
command_countView (GList * list)
{
  if (!view)
    {
      fprintf (stderr, "Open a View.\n");
      return;
    }

  fprintf (stdout, "Total: %" G_GSIZE_FORMAT "\n", dupin_view_count (view));
}

static void
command_createRecord (GList * list)
{
  JsonNode *obj_node;
  GError *error = NULL;

  if (!db)
    {
      fprintf (stderr, "Open a Db.\n");
      return;
    }

  if (!(obj_node = json (list->data)))
    {
      fprintf (stderr, "The input is not a json object.\n");
      return;
    }

  if (record)
    dupin_record_close (record);

  if (!(record = dupin_record_create (db, obj_node, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }

  json_node_free (obj_node);
}

static void
command_createRecordFromFile (GList * list)
{
  JsonNode *obj_node;
  GError *error = NULL;

  if (!db)
    {
      fprintf (stderr, "Open a Db.\n");
      return;
    }

  if (!(obj_node = json_file (list->data)))
    {
      fprintf (stderr, "The input is not a file containing a json object.\n");
      return;
    }

  if (record)
    dupin_record_close (record);

  if (!(record = dupin_record_create (db, obj_node, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }

  json_node_free (obj_node);
}

static void
command_readRecord (GList * list)
{
  GError *error = NULL;

  if (!db)
    {
      fprintf (stderr, "Open a Db.\n");
      return;
    }

  if (record)
    dupin_record_close (record);

  if (!(record = dupin_record_read (db, list->data, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }
}

static void
command_getListRecord (GList * list)
{
  GError *error = NULL;

  guint count = 0;
  guint offset = 0;
  gboolean descending = FALSE;

  GList *results = NULL;

  if (!db)
    {
      fprintf (stderr, "Open a Db.\n");
      return;
    }

  count = atoi (list->data);
  offset = atoi (list->next->data);

  if (!g_strcmp0 (list->next->next->data, "true"))
    descending = TRUE;

  if (dupin_record_get_list (db, count, offset, 0, 0, NULL, NULL, NULL, TRUE, DP_COUNT_EXIST, DP_ORDERBY_ROWID, descending, NULL, DP_FILTERBY_EQUALS,
					NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, &error)
      == FALSE)
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }

  for (list = results; list; list = list->next)
    {
      DupinRecord *record = list->data;
      showRecord (record);
    }

  dupin_record_get_list_close (results);
}

static void
command_updateRecord (GList * list)
{
  GError *error = NULL;
  JsonNode *obj_node;

  if (!db)
    {
      fprintf (stderr, "Open a Db.\n");
      return;
    }

  if (!record)
    {
      fprintf (stderr, "Open a record.\n");
      return;
    }

  if (!(obj_node = json (list->data)))
    {
      fprintf (stderr, "The input is not a json object.\n");
      return;
    }

  if (dupin_record_update (record, obj_node, &error) == FALSE)
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }

  json_node_free (obj_node);
}

static void
command_deleteRecord (GList * list)
{
  GError *error = NULL;

  if (!db)
    {
      fprintf (stderr, "Open a Db.\n");
      return;
    }

  if (!record)
    {
      fprintf (stderr, "Open a record.\n");
      return;
    }

  if (dupin_record_delete (record, &error) == FALSE)
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }
}

static void
command_showRecord (GList * list)
{
  if (!db)
    {
      fprintf (stderr, "Open a Db.\n");
      return;
    }

  if (!record)
    {
      fprintf (stderr, "Open a record.\n");
      return;
    }

  showRecord (record);
}

static void
showRecord (DupinRecord * record)
{
  GList * revisions=NULL;
  GList * list=NULL;
  
  if (dupin_record_get_revisions_list (record,
                                       100,
                                       0, 1, 0, DP_COUNT_ALL, DP_ORDERBY_REV, FALSE,
                                       &revisions, NULL) == FALSE)
    {
      fprintf (stdout, "Error: cannot generate JSON output for record with id=%s\n", dupin_record_get_id (record));
      return;
    }

  for (list = revisions; list; list = list->next)
    {

      if (dupin_record_is_deleted (record, (gchar *)list->data) == TRUE)
	fprintf (stdout, "Rev: %s - Deleted\n", (gchar *)list->data);

      else
	{
	  gchar *buffer;
	  gsize size;

	  JsonNode *record_node = dupin_record_get_revision_node (record, (gchar *)list->data);

          if (record_node == NULL)
            {
              return;
            }

	  JsonNode *node = json_node_copy (record_node);

	  JsonGenerator *gen = json_generator_new();

          if (gen == NULL)
            {
              json_node_free (node);
              return;
            }

  	  json_generator_set_root (gen, node );
	  buffer = json_generator_to_data (gen,&size);

	  if (buffer == NULL )
	    {
  	      fprintf (stdout, "Rev %s - Error: cannot generate JSON output for record with id=%s\n", (gchar *)list->data, dupin_record_get_id (record));
	      json_node_free (node);
	      g_object_unref (gen);
	    }

	  else
	    {
	      fprintf (stdout, "Rev: %s - Id %s - %s\n", (gchar *)list->data, dupin_record_get_id (record), buffer);
	      g_free (buffer);
	      json_node_free (node);
	      g_object_unref (gen);
	    }
	}
    }
  dupin_record_get_revisions_list_close (revisions);
}

static void
command_readViewRecord (GList * list)
{
  GError *error = NULL;

  if (!view)
    {
      fprintf (stderr, "Open a View.\n");
      return;
    }

  if (viewRecord)
    dupin_view_record_close (viewRecord);

  if (!(viewRecord = dupin_view_record_read (view, list->data, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }
}

static void
command_getListViewRecord (GList * list)
{
  GError *error = NULL;

  guint count = 0;
  guint offset = 0;
  gboolean descending = FALSE;

  GList *results = NULL;

  if (!view)
    {
      fprintf (stderr, "Open a View.\n");
      return;
    }

  count = atoi (list->data);
  offset = atoi (list->next->data);

  if (!g_strcmp0 (list->next->next->data, "true"))
    descending = TRUE;

  if (dupin_view_record_get_list (view, count, offset, 0, 0, DP_ORDERBY_KEY, descending, NULL, NULL, TRUE, NULL, NULL, TRUE,
				     NULL, DP_FIELDS_FORMAT_DOTTED, DP_FILTERBY_EQUALS, NULL, &results, &error) == FALSE)
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
    }

  for (list = results; list; list = list->next)
    {
      DupinViewRecord *record = list->data;
      showViewRecord (record);
    }

  dupin_view_record_get_list_close (results);
}

static void
command_showViewRecord (GList * list)
{
  if (!view)
    {
      fprintf (stderr, "Open a View.\n");
      return;
    }

  if (!viewRecord)
    {
      fprintf (stderr, "Open a view record.\n");
      return;
    }

  showViewRecord (viewRecord);
}

static void
showViewRecord (DupinViewRecord * record)
{
  JsonObject *obj;
  gchar *buffer;
  gsize size;

  obj = json_node_get_object (json_node_copy (dupin_view_record_get (record)));

  JsonNode *node = json_node_new (JSON_NODE_OBJECT);

  if (node == NULL)
    return;

  json_node_set_object (node, obj);

  JsonGenerator *gen = json_generator_new();

  if (gen == NULL)
    {
      json_node_free (node);
      return;
    }

  json_generator_set_root (gen, node );

  buffer = json_generator_to_data (gen,&size);

  if (buffer == NULL )
    {
      fprintf (stdout, "Error: cannot generate JSON output of size %d\n",(gint)size);
      json_node_free (node);
      g_object_unref (gen);
    }

  else
    {
      fprintf (stdout, "%s\n", buffer);
      g_free (buffer);
      json_node_free (node);
      g_object_unref (gen);
    }
}

/* EOF */
