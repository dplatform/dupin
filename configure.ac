AC_PREREQ(2.54)

AC_INIT(src/httpd/main.c)
AC_CANONICAL_HOST
AC_CANONICAL_TARGET

AC_PROG_MAKE_SET

AM_SANITY_CHECK
AM_INIT_AUTOMAKE(dupin, 0.1)
AM_CONFIG_HEADER(config.h)
AM_MAINTAINER_MODE

AC_PROG_CC
AC_PROG_LN_S
AC_ISC_POSIX
AC_PROG_LIBTOOL
AC_PROG_INSTALL
AM_PROG_CC_STDC

AC_C_CONST
AC_C_INLINE

AC_HEADER_STDC
AC_HEADER_DIRENT
AC_CHECK_HEADERS(stdio.h sys/stat.h sys/types.h unistd.h string.h)

AC_ARG_WITH(mimeguess,
   AC_HELP_STRING([--with-mimeguess=none|auto (default=auto)],
                  [do you want dupin following the couchdb protocol strictly?]),
   MIMEGUESS=$withval,
   MIMEGUESS=yes)

AH_TEMPLATE([MIMEGUESS_STRICT], [Define to 1 if you want to compile this code with g_content_type_guess])

if test "$MIMEGUESS" = yes; then
  AC_DEFINE(MIMEGUESS_STRICT)
fi

AC_ARG_WITH(couchdbstrict,
   AC_HELP_STRING([--with-couchdbstrict=none|auto (default=auto)],
                  [do you want dupin following the couchdb protocol strictly?]),
   COUCHDB=$withval,
   COUCHDB=yes)

AH_TEMPLATE([COUCHDB_STRICT], [Define to 1 if you want dupin following the couchdb protocol strictly?])

if test "$COUCHDB" = yes; then
  AC_DEFINE(COUCHDB_STRICT)
fi

AH_TEMPLATE([WEBKIT_FRAMEWORK], [Define to 1 if you want to compile this code with Webkit framework under Darwin])

if [ test "`echo $target_os | grep darwin`" != "" && which port &> /dev/null ] ||
   [ test "`echo $target_os | grep darwin`" = "" ] ; then

  # WebKit
  PKG_CHECK_MODULES(webkit, [
    webkit-1.0 >= 1.0.0
  ])

  LDFLAGS="$LDFLAGS $webkit_LIBS"
  CFLAGS="$CFLAGS $webkit_CFLAGS"

  # SQLite
  PKG_CHECK_MODULES(sqlite, [
    sqlite3 >= 3.4.0
  ])

  LDFLAGS="$LDFLAGS $sqlite_LIBS"
  CFLAGS="$CFLAGS $sqlite_CFLAGS"

  # libXML2
  PKG_CHECK_MODULES(libxml2, [
    libxml-2.0 >= 2.6.0
  ])

  LDFLAGS="$LDFLAGS $libxml2_LIBS"
  CFLAGS="$CFLAGS $libxml2_CFLAGS"

  # GLib
  PKG_CHECK_MODULES(glib, [
    glib-2.0    >= 2.6.0 
    gthread-2.0 >= 2.6.0
    gio-2.0     >= 2.6.0
  ])

  LDFLAGS="$LDFLAGS $glib_LIBS"
  CFLAGS="$CFLAGS $glib_CFLAGS -DG_DISABLE_DEPRECATED"

else

  AC_DEFINE(WEBKIT_FRAMEWORK)

  # WebKit
  CFLAGS="$CFLAGS -I /System/Library/Frameworks/JavaScriptCore.framework/Headers"
  LDFLAGS="$LDFLAGS  -framework JavaScriptCore"

  # SQLite
  AC_CHECK_HEADER(sqlite3.h,,AC_MSG_ERROR(sqlite3 is required to compile this software))
  AC_CHECK_LIB(sqlite3,sqlite3_exec,, AC_MSG_ERROR(sqlite3 is required to compile this software))

  # libXML2
  AC_CHECK_HEADER(libxml/parser.h,,AC_MSG_ERROR(libxml2 is required to compile this software))
  AC_CHECK_LIB(xml2,xmlParseFile,, AC_MSG_ERROR(libxml2 is required to compile this software))

  # GLib
  CFLAGS="$CFLAGS -I/Library/Frameworks/GLib.framework/Headers"
  LDFLAGS="$LDFLAGS -framework GLib"
fi

CFLAGS="$CFLAGS -Wall"

AC_OUTPUT([
Makefile

src/Makefile

src/tbjson/Makefile
src/tbjsonpath/Makefile
src/lib/Makefile
src/httpd/Makefile
src/www/Makefile
src/tests/Makefile
])

