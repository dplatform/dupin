noinst_LTLIBRARIES = libsqlite.la

INCLUDES = \
	-DSQLITE_ENABLE_COLUMN_METADATA=1 \
	-DSQLITE_ENABLE_FTS4=1 \
	-DSQLITE_ENABLE_FTS4_UNICODE61=1 \
	-DSQLITE_ENABLE_FTS3_PARENTHESIS=1 \
	-DSQLITE_ENABLE_RTREE=1 \
	-DSQLITE_ENABLE_STAT3=1 \
	-DSQLITE_TEMP_STORE=2 \
	#-DSQLITE_MEMDEBUG=1 \
	-DSQLITE_THREADSAFE=1

libsqlite_la_SOURCES = \
	sqlite3.c \
	sqlite3.h
