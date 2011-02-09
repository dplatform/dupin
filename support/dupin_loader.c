#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <dupin.h>
#include "configure.h"
#include "dupin_server_common.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#ifdef G_OS_UNIX
#  include <sys/types.h>
#  include <unistd.h>
#  include <stdlib.h>
#  include <grp.h>
#  include <pwd.h>
#  include <signal.h>
#  include <sys/wait.h>
#endif

#ifndef G_OS_WIN32
#  include <unistd.h>
#else
#  ifndef STDIN_FILENO
#    define STDIN_FILENO 0
#  endif
#endif

#ifdef HAVE_LOCALE
#include <locale.h>
#endif

/* Global variables: */
DSGlobal *d_conf = NULL;
Dupin *d = NULL;
DupinDB *db = NULL;
DupinLinkB *linkb = NULL;
DupinAttachmentDB *attachment_db = NULL;
gboolean dp_exit = FALSE;
gchar * error_msg = NULL;
gchar * warning_msg = NULL;

gchar * db_name=NULL;
gchar * json_data_file=NULL;
GIOChannel *io;
GError *error = NULL;
gint argc_left;

typedef struct _dupin_loader_options {
	gboolean bulk;
	gboolean links;
	gboolean create_db;
	gboolean verbose;
	gboolean silent;
	gboolean strict_links;
	gchar * context_id;
	gboolean links_cache;
} dupin_loader_options;

gchar * dupin_loader_extract_context_id (JsonNode * node);

static JsonNode * dupin_loader_read_json_object (gchar * line);

void dupin_loader_set_error (gchar * msg);
void dupin_loader_clear_error (void);
gchar * dupin_loader_get_error (void);
void dupin_loader_set_warning (gchar * msg);
void dupin_loader_clear_warning (void);
gchar * dupin_loader_get_warning (void);

static void
dupin_loader_usage (char *argv[])
{
  printf("usage:\n"
	 "   %s [options] <db-name> [<json-data-file>] [<dupin-configuration-file>]\n"
	 , argv[0]);
  puts("\n"
       "options:\n"
       "   --help             this usage statement\n"
       "   --bulk             read one bulk per line\n"
       "   --links            input is only about links (weblinks or relationships) no documents\n"
       "   --strict_links     check that the context_id document is valid and not deleted\n"
       "   --create-db        force (re)creation of database if it doesn't exist\n"
       "   --verbose          verbose logging, more than normal logging\n"
       "   --silent           silent, prints only errors\n"
       "   --version          prints version information\n"
       "   --context_id ID    the context_id to use to create the links if --links is used\n"
       "   --no-links-cache   do not use links cache on insertion (it will be much slower)\n"
       "   \n"
       "   When the --links option is specified without --context_id each input JSON object must have a 'context_id' field, including bulks.\n"
);
}

static void
dupin_loader_version (void)
{
  puts(PACKAGE " Metadata Loader " VERSION " \n"
	       "\n"
	     "Supported formats:\n");

  puts("\tJSON (one record per line)\n\n");
}

static void
dupin_loader_parse_options (int argc, char **argv,
			    dupin_loader_options * options)
{
  argc_left = argc;

  options->bulk = FALSE;
  options->links = FALSE;
  options->create_db = FALSE;
  options->silent = FALSE;
  options->verbose = FALSE;
  options->context_id = NULL;
  options->strict_links = FALSE;
  options->links_cache = TRUE;

  if (argc > 1)
    {
      gint i = 1;
      while (i < argc)
        {
	   if (g_str_has_prefix (argv[i], "--"))
             {
//g_message("option = %s\n", argv[i]);
	       if (!g_strcmp0 (argv[i], "--help"))
                 {
	           dupin_loader_usage (argv);
		   exit (EXIT_SUCCESS);
		 }
               else if (!g_strcmp0 (argv[i], "--bulk"))
                 {
		   options->bulk = TRUE;
		   argc_left--;
		 }
               else if (!g_strcmp0 (argv[i], "--links"))
                 {
		   options->links = TRUE;
		   argc_left--;
		 }
               else if (!g_strcmp0 (argv[i], "--strict_links"))
                 {
		   options->strict_links = TRUE;
		   argc_left--;
		 }
               else if (!g_strcmp0 (argv[i], "--context_id"))
                 {
                   if (argv[i+1] && dupin_link_record_util_is_valid_context_id (argv[i+1]) == FALSE)
                     {
		       fprintf (stderr, "not valid context_id: %s\n", argv[i+1]);
		       exit (EXIT_FAILURE);
                     }
                  
		   options->context_id = argv[i+1];
		   argc_left-=2;
		   i++;
		 }
               else if (!g_strcmp0 (argv[i], "--create-db"))
                 {
		   options->create_db = TRUE;
		   argc_left--;
		 }
               else if (!g_strcmp0 (argv[i], "--silent"))
                 {
		   options->silent = TRUE;
		   argc_left--;
		 }
	       else if (!g_strcmp0 (argv[i], "--version"))
                 {
	           dupin_loader_version ();
	           exit (EXIT_SUCCESS);
                 }
               else if (!g_strcmp0 (argv[i], "--no-links-cache"))
                 {
		   options->links_cache = FALSE;
		   argc_left--;
		 }
               else
                 {
		   fprintf (stderr, "unknown command line option: %s\n", argv[i]);
		   exit (EXIT_FAILURE);
	         }
	    }
          else
	    break;

	  i++;
        }
    }

  if (argc_left <= 4)
    {
      if (argc_left == 4)
        {
          db_name = g_strdup (argv[argc - 3]);
          json_data_file = g_strdup (argv[argc - 2]);
          return;
        }
      else if (argc_left == 3)
        {
          db_name = g_strdup (argv[argc - 2]);
          json_data_file = g_strdup (argv[argc - 1]);
          return;
	}
      else if (argc_left == 2)
        {
          db_name = g_strdup (argv[argc - 1]);
          return;
	}
    }

  dupin_loader_usage (argv);
  exit (EXIT_FAILURE);
}

void dupin_loader_close (void)
{
//g_message ("dupin_loader_close: closing down\n");

  g_io_channel_shutdown (io, FALSE, NULL);
  g_io_channel_unref (io);

  if (db)
    dupin_database_unref (db);

  if (linkb)
    dupin_linkbase_unref (linkb);

  if (db_name != NULL)
    g_free (db_name);

  if (json_data_file != NULL)
    g_free (json_data_file);

  configure_free (d_conf);

  dupin_shutdown (d);

  if (error != NULL)
    g_error_free (error);

  if (dupin_loader_get_error () != NULL)
    {
      fprintf (stderr, "Error: %s\n", dupin_loader_get_error ());

      dupin_loader_clear_error ();
      exit (EXIT_FAILURE);
    }
  else
    {
      if (dupin_loader_get_warning () != NULL)
        fprintf (stderr, "Warning: %s\n", dupin_loader_get_warning ());

      dupin_loader_clear_warning ();
      exit (EXIT_SUCCESS);
    }
}

static void dupin_loader_sig_int (int sig)
{
  dupin_loader_close ();
}

int
main (int argc, char *argv[])
{
  dupin_loader_options options;
  gchar *line;
  gsize last;
  GIOStatus status;

#ifdef HAVE_LOCALE
  /* initialize locale */
  setlocale(LC_CTYPE,"");
#endif

  signal(SIGINT, dupin_loader_sig_int);
#ifdef SIGQUIT
   signal(SIGQUIT, dupin_loader_sig_int);
#endif
#ifdef SIGTERM
  signal(SIGTERM, dupin_loader_sig_int);
#endif
#ifdef SIGSTOP
  signal(SIGSTOP, dupin_loader_sig_int);
#endif

  // better make double-sure glib itself is initialized properly.
  if (!g_thread_supported ())
        g_thread_init (NULL);
  g_type_init();

  /* NOTE - parse this command options */
  dupin_loader_parse_options (argc, argv, &options);

  if (options.context_id != NULL 
      && options.links == FALSE)
    {
      fprintf (stderr, "option context_id can only be used for loading links\n");
      dupin_loader_usage (argv);
      exit (EXIT_FAILURE);
    }

  //g_message("db_name=%s\n", db_name);
  //g_message("json_data_file=%s\n", json_data_file);
  //g_message("bulk=%d\n", options.bulk);

  /* Read the config file: */
  if (argc_left == 4)
    {
      argv[1] = argv[argc - 1];
      argc = 2;
    }
  else
    {
      argc = 1;
    }
  if (!(d_conf = configure_init (argc, argv, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);

      dupin_loader_usage (argv);
      exit (EXIT_FAILURE);
    }

#ifdef G_OS_UNIX
  /* Check permissions */
  if (dupin_server_common_permission (d_conf, &error) == FALSE)
    {
      fprintf (stderr, "Error about the permissions: %s\n", error->message);
      goto dupin_loader_end;
    }
#endif

  if (db_name == NULL)
    {
      dupin_loader_set_error ("Database name is missing");
      goto dupin_loader_end;
    }

  if (!(d = dupin_init (NULL, &error)))
    {
      fprintf (stderr, "Error: %s\n", error->message);
      g_error_free (error);
      return 1;
    }

  if (json_data_file == NULL
      || (!g_strcmp0 (json_data_file, "-")) )
    {
#ifdef G_OS_WIN32
      io = g_io_channel_win32_new_fd (STDIN_FILENO);
#else
      io = g_io_channel_unix_new (STDIN_FILENO);
#endif
    }
  else
    {
     io = g_io_channel_new_file (json_data_file, "r", &error);
    }

  if (io == NULL)
    {
      fprintf (stderr, "Can't read JSON data\n");
      exit (EXIT_FAILURE);
    }

  //g_io_channel_set_encoding (io, NULL, NULL);
  //g_io_channel_set_buffered(io, FALSE);
  g_io_channel_set_flags(io, G_IO_FLAG_NONBLOCK, NULL);

  if (options.create_db == TRUE)
    {
//g_message("creating db %s\n", db_name);

      if (!  (db = dupin_database_new (d, db_name, NULL)))
        {
          dupin_loader_set_error ("Cannot create database");
          goto dupin_loader_end;
        }
    }
  else
    {
//g_message("opening db %s\n", db_name);

      if (!  (db = dupin_database_open (d, db_name, NULL)))
        {
          dupin_loader_set_error ("Cannot connect to database");
          goto dupin_loader_end;
        }
    }
 
  if (options.links == TRUE)
    {
      if (!  (linkb = dupin_linkbase_open (d, dupin_database_get_default_linkbase_name (db), NULL)))
        {
          dupin_loader_set_error ("Cannot connect to linkbase");
          goto dupin_loader_end;
        }

      if (options.links_cache == TRUE)
        dupin_linkbase_cache_on (linkb, NULL);
    }

  while (TRUE)
    {
      GList * response_list=NULL;

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

      JsonNode * json_object_node = dupin_loader_read_json_object (line);

      if (json_object_node == NULL)
        goto dupin_loader_end;

      gboolean res;
      gchar * context_id = NULL;
      if (options.bulk == TRUE)
        {
          if (options.links == TRUE)
            {
	      if (options.context_id != NULL)
                context_id = g_strdup (options.context_id);

              if (context_id == NULL)
                context_id = dupin_loader_extract_context_id (json_object_node);

              if (context_id == NULL
                  || dupin_link_record_util_is_valid_context_id (context_id) == FALSE)
                {
                  dupin_loader_set_error ("Not a valid context_id specified");
                  goto dupin_loader_end;
                }

//g_message("context_id = %s\n", context_id);

	      res =  dupin_link_record_insert_bulk (linkb, json_object_node, context_id, &response_list,
						    options.strict_links);

	      g_free (context_id); 
            }
          else
            res = dupin_record_insert_bulk (db, json_object_node, &response_list);
        }
      else
       {
          if (options.links == TRUE)
            {
	      if (options.context_id != NULL)
                context_id = g_strdup (options.context_id);

              if (context_id == NULL)
                context_id = dupin_loader_extract_context_id (json_object_node);

              if (context_id == NULL
                  || dupin_link_record_util_is_valid_context_id (context_id) == FALSE)
                {
                  dupin_loader_set_error ("Not a valid context_id specified");
                  goto dupin_loader_end;
                }

	      res = dupin_link_record_insert (linkb, json_object_node, NULL, NULL, context_id,
					      DP_LINK_TYPE_ANY, &response_list, options.strict_links);

	      g_free (context_id); 
            }
          else
            res = dupin_record_insert (db, json_object_node, NULL, NULL, &response_list);
       }

      if (res == TRUE)
        {
//g_message("bulk=%d\n", options.bulk);

#if 0
      {
      gchar * str = dupin_util_json_serialize (json_object_node);
      g_message("%s", str);
      g_free (str);
      }
#endif
        }
      else
        {
          if (options.links == TRUE)
            dupin_loader_set_error (dupin_linkbase_get_error (linkb));
          else
            dupin_loader_set_error (dupin_database_get_error (db));
        }

      while (response_list)
        {
          json_node_free (response_list->data);
          response_list = g_list_remove (response_list, response_list->data);
        }

      json_node_free (json_object_node);

      g_free (line);
    }

dupin_loader_end:

  dupin_loader_close ();

  return EXIT_SUCCESS;
}

gchar *
dupin_loader_extract_context_id (JsonNode * node)
{
  g_return_val_if_fail (node != NULL, NULL);
  g_return_val_if_fail (json_node_get_node_type (node) == JSON_NODE_OBJECT, NULL);

  JsonObject * obj = json_node_get_object (node);

  gchar * context_id = NULL;
  if (json_object_has_member (obj, REQUEST_GET_ALL_LINKS_CONTEXT_ID) == TRUE)
    {
      context_id = g_strdup ((gchar *)json_object_get_string_member (obj, REQUEST_GET_ALL_LINKS_CONTEXT_ID));
      json_object_remove_member (obj, REQUEST_GET_ALL_LINKS_CONTEXT_ID);
    }

  return context_id;
}

static JsonNode *
dupin_loader_read_json_object (gchar * line)
{
  JsonNode * json_object_node = NULL;
  GError *error = NULL;

  JsonParser *parser = json_parser_new ();

  if (parser == NULL)
    {
      dupin_loader_set_error ("Cannot parse JSON object");
      goto dupin_loader_read_json_object_end;
    }

  if (!json_parser_load_from_data (parser, line, -1, &error))
    {
      dupin_loader_set_error (error->message);
      g_error_free (error);
      goto dupin_loader_read_json_object_end;
    }

  JsonNode * node = json_parser_get_root (parser);

  if (node == NULL)
    {
      dupin_loader_set_error ("Cannot parse JSON object");
      goto dupin_loader_read_json_object_end;
    }

  json_object_node = json_node_copy (node);

dupin_loader_read_json_object_end:

  if (parser != NULL)
    g_object_unref (parser);

  return json_object_node;
}

void dupin_loader_set_error (gchar * msg)
{
  g_return_if_fail (msg != NULL);

  dupin_loader_clear_error ();

  error_msg = g_strdup ( msg );

  return;
}

void dupin_loader_clear_error (void)
{
  if (error_msg != NULL)
    g_free (error_msg);

  error_msg = NULL;

  return;
}

gchar *
dupin_loader_get_error (void)
{
  return error_msg;
}

void dupin_loader_set_warning (gchar * msg)
{
  g_return_if_fail (msg != NULL);

  dupin_loader_clear_warning ();

  warning_msg = g_strdup ( msg );

  return;
}

void dupin_loader_clear_warning (void)
{
  if (warning_msg != NULL)
    g_free (warning_msg);

  warning_msg = NULL;

  return;
}

gchar *
dupin_loader_get_warning (void)
{
  return warning_msg;
}

/* EOF */
