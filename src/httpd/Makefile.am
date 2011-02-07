bin_PROGRAMS = dupin_server

INCLUDES = \
	-DDS_WWW_PATH=\"$(datadir)/@PACKAGE@/www\" \
	-DDS_CONFIG_FILE=\"$(sysconfdir)/dupinserver.cfg\" \
	-I../lib \
	-I../tbjson \
	-I../tbjsonpath

dupin_server_SOURCES = \
	dupin_server_common.c \
	dupin_server_common.h \
	configure.c \
	configure.h \
	dupin.h \
	httpd.c \
	httpd.h \
	log.c \
	log.h \
	main.c \
	map.c \
	map.h \
	request.c \
	request.h

dupin_server_LDADD = \
	../lib/libdupin.la \
	../tbjson/libtbjson.la \
	../tbjsonpath/libtbjsonpath.la
