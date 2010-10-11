AC_PREREQ(2.54)

AC_INIT(src/httpd/main.c)
AC_CANONICAL_HOST
AC_CANONICAL_TARGET

AC_PROG_MAKE_SET

AC_PATH_PROG(PKG_CONFIG, pkg-config)
AC_PATH_PROG(HOMEBREW, brew)
AC_PATH_PROG(HAVEPORTS, port)

# host checking - inspired by the Webkit/GTK+ configure.in
# TODO: move these to dupin.m4?
AC_MSG_CHECKING([for native Win32])
case "$host" in
     *-*-mingw*)
       os_win32=yes
       ;;
     *)
       os_win32=no
       ;;
esac
AC_MSG_RESULT([$os_win32])

case "$host" in
     *-*-linux*)
       os_linux=yes
       ;;
     *-*-freebsd*)
       os_freebsd=yes
       ;;
     *-*-darwin*)
       os_darwin=yes
       ;;
esac

case "$host_os" in
     gnu* | linux* | k*bsd*-gnu)
       os_gnu=yes
       ;;
     *)
       os_gnu=no
       ;;
esac

AM_SANITY_CHECK
AM_INIT_AUTOMAKE(dupin, 0.2)
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

if	test -n "$HOMEBREW" || \
	test -n "$HAVEPORTS" ] || \
	test "$os_darwin" != "yes" ; then

  if test -n "$PKG_CONFIG"; then

	# 1. yes we have pkg-config, yes we needed it for Webkit and Glib (we do not check all deps here yet...) but we could 
	#    not still have everything in it, so we have checkings to do :(
	# 2. we expanded the macro below because PKG_CHECK_MODULES seems not working as written on the thin for darwin...

	# WebKit
	PKG_CHECK_MODULES(webkit, [ webkit-1.0 >= 1.0.0 ],
		[	[webkit_LIBS=`$PKG_CONFIG --libs "webkit-1.0" 2>/dev/null`]
			[webkit_CFLAGS=`$PKG_CONFIG --cflags "webkit-1.0" 2>/dev/null`]
		],
		[
			AC_MSG_ERROR(WebkitGtk 1.x library is required to build Dupin)
		]
		)

	# SQLite
	PKG_CHECK_MODULES(sqlite, [ sqlite3 >= 3.4.0 ],
		[	[sqlite_LIBS=`$PKG_CONFIG --libs "sqlite" 2>/dev/null`]
			[sqlite_CFLAGS=`$PKG_CONFIG --cflags "sqlite" 2>/dev/null`]
		],
		[	AC_CHECK_HEADER(sqlite3.h,,AC_MSG_ERROR(SQLite3 is required to build Dupin))
			AC_CHECK_LIB(sqlite3,sqlite3_exec,, AC_MSG_ERROR(SQLite3 is required to build Dupin))
		]
		)

	# libXML2
	PKG_CHECK_MODULES(libxml2, [ libxml-2.0 >= 2.6.0 ],
		[	[libxml2_LIBS=`$PKG_CONFIG --libs "libxml-2.0" 2>/dev/null`]
			[libxml2_CFLAGS=`$PKG_CONFIG --cflags "libxml-2.0" 2>/dev/null`]
		],
		[	AC_CHECK_HEADER(libxml/parser.h,,AC_MSG_ERROR(libxml2 is required to build Dupin))
			AC_CHECK_LIB(xml2,xmlParseFile,, AC_MSG_ERROR(libxml2 is required to build Dupin))
		]
		)

	# GLib
	# TODO: make a full check eventually and fall-back
	PKG_CHECK_MODULES(glib, [
		glib-2.0    >= 2.6.0
		gthread-2.0 >= 2.6.0
		gio-2.0     >= 2.6.0 ])

	# json-glib
	PKG_CHECK_MODULES(jsonglib, [ json-glib-1.0 >= 0.12.0 ],
		[	[jsonglib_LIBS=`$PKG_CONFIG --libs "json-glib-1.0" 2>/dev/null`]
			[jsonglib_CFLAGS=`$PKG_CONFIG --cflags "json-glib-1.0" 2>/dev/null`]
		],
		[
  			AC_CHECK_HEADER(json-glib/json-glib.h,,AC_MSG_ERROR(jsonglib is required to build Dupin))
			AC_CHECK_LIB(json-glib,json_parser_load_from_file,, AC_MSG_ERROR(jsonglib is required to build Dupin))
		]
		)

  else
	AC_MSG_ERROR(pkg-config is required to build Dupin)
  fi

  # 3. set env
  LDFLAGS="$LDFLAGS $webkit_LIBS $sqlite_LIBS $libxml2_LIBS $glib_LIBS $jsonglib_LIBS"
  CFLAGS="$CFLAGS $webkit_CFLAGS $sqlite_CFLAGS $libxml2_CFLAGS $glib_CFLAGS $jsonglib_CFLAGS -DG_DISABLE_DEPRECATED"

else

  # Darwin with frameworks

  AC_DEFINE(WEBKIT_FRAMEWORK)

  # WebKit
  CFLAGS="$CFLAGS -I /System/Library/Frameworks/JavaScriptCore.framework/Headers"
  LDFLAGS="$LDFLAGS -framework JavaScriptCore"

  # SQLite
  AC_CHECK_HEADER(sqlite3.h,,AC_MSG_ERROR(sqlite3 is required to build Dupin))
  AC_CHECK_LIB(sqlite3,sqlite3_exec,, AC_MSG_ERROR(sqlite3 is required to build Dupin))

  # libXML2
  AC_CHECK_HEADER(libxml/parser.h,,AC_MSG_ERROR(libxml2 is required to build Dupin))
  AC_CHECK_LIB(xml2,xmlParseFile,, AC_MSG_ERROR(libxml2 is required to build Dupin))

  # json-glib
  AC_CHECK_HEADER(json-glib/json-glib.h,,AC_MSG_ERROR(jsonglib is required to build Dupin))
  AC_CHECK_LIB(json-glib,json_parser_load_from_file,, AC_MSG_ERROR(jsonglib is required to build Dupin))

  # GLib
  CFLAGS="$CFLAGS -I/Library/Frameworks/GLib.framework/Headers"
  LDFLAGS="$LDFLAGS -framework GLib"

fi

#CFLAGS="$CFLAGS -Wall"
CFLAGS="-g $CFLAGS -Wall"

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

