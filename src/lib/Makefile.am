noinst_LTLIBRARIES = libdupin.la

INCLUDES = \
	-DDUPIN_DB_PATH=\"$(localstatedir)/@PACKAGE@/dbs\" \
	-I../tbjson

libdupin_la_SOURCES = \
	dupin.h \
	dupin_db.c \
	dupin_db.h \
	dupin_init.c \
	dupin_init.h \
	dupin_internal.h \
	dupin_js.c \
	dupin_js.h \
	dupin_mr.c \
	dupin_mr.h \
	dupin_record.c \
	dupin_record.h \
	dupin_utils.c \
	dupin_utils.h \
	dupin_view.c \
	dupin_view.h \
	dupin_view_record.c \
	dupin_view_record.h \
	dupin_attachment_db.c \
	dupin_attachment_db.h \
	dupin_attachment_record.c \
	dupin_attachment_record.h

libdupin_la_LIBADD = \
	../tbjson/libtbjson.la
