dnl Licensed to the Apache Software Foundation (ASF) under one or more
dnl contributor license agreements.  See the NOTICE file distributed with
dnl this work for additional information regarding copyright ownership.
dnl The ASF licenses this file to You under the Apache License, Version 2.0
dnl (the "License"); you may not use this file except in compliance with
dnl the License.  You may obtain a copy of the License at
dnl 
dnl   http://www.apache.org/licenses/LICENSE-2.0
dnl 
dnl Unless required by applicable law or agreed to in writing, software
dnl distributed under the License is distributed on an "AS IS" BASIS,
dnl WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
dnl See the License for the specific language governing permissions and
dnl limitations under the License.
dnl run autogen.sh to generate the configure script.

AC_PREREQ(2.54)

AC_INIT([dupin], [0.6])
AC_CONFIG_MACRO_DIR([m4])

AC_CANONICAL_HOST

AC_CONFIG_HEADER([config.h])
AC_CONFIG_SRCDIR([src/httpd/main.c])

#AC_CANONICAL_TARGET
#AC_PROG_MAKE_SET

AC_PATH_PROG(PKG_CONFIG, pkg-config)

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

AM_INIT_AUTOMAKE
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

# never run autotools and configure automatically
AM_MAINTAINER_MODE

AC_PROG_CC
AC_PROG_LN_S
AC_ISC_POSIX
AC_PROG_LIBTOOL
AC_PROG_INSTALL

AC_C_CONST
AC_C_INLINE

AC_HEADER_STDC
AC_HEADER_DIRENT
AC_CHECK_HEADERS(stdio.h sys/stat.h sys/types.h unistd.h string.h)

AC_MSG_CHECKING([if using g_content_type_guess])
AC_ARG_WITH(mimeguess,
            AC_HELP_STRING([--with-mimeguess],
                           [Build using with g_content_type_guess support [default=no]]),
            [],[with_mimeguess="no"])
AC_MSG_RESULT([$with_mimeguess])

AH_TEMPLATE([MIMEGUESS_STRICT], [Define to 1 if you want to compile this code with g_content_type_guess])

if test "$with_mimeguess" = yes; then
  AC_DEFINE(MIMEGUESS_STRICT)
fi

AC_MSG_CHECKING([if using WebKit Framework])
AC_ARG_WITH(webkitframework,
            AC_HELP_STRING([--with-webkitframework],
                           [Build using OSX Darwin Webkit built in Framework [default=no]]),
            [],[with_webkitframework="no"])
AC_MSG_RESULT([$with_webkitframework])

AH_TEMPLATE([WEBKIT_FRAMEWORK], [Define to 1 if you want to compile this code with Webkit framework under Darwin])

# minimum base dependencies
LIBXML_REQUIRED_VERSION=2.6.0
GLIB_REQUIRED_VERSION=2.32.0
GTHREAD_REQUIRED_VERSION=2.32.0
GIO_REQUIRED_VERSION=2.32.0
JSONGLIB_REQUIRED_VERSION=0.10.4
SQLITE_REQUIRED_VERSION=3.7.4 # SQLite WAL and FTS4 support needed
LIBSOUP_REQUIRED_VERSION=2.24.0
WEBKIT_REQUIRED_VERSION=1.0.0

# libXML2
PKG_CHECK_MODULES([LIBXML], [libxml-2.0 >= $LIBXML_REQUIRED_VERSION])
AC_SUBST([LIBXML_CFLAGS])
AC_SUBST([LIBXML_LIBS])

# GLib
PKG_CHECK_MODULES([GLIB], [glib-2.0 >= $GLIB_REQUIRED_VERSION])
AC_SUBST([GLIB_CFLAGS])
AC_SUBST([GLIB_LIBS])

# GThread
PKG_CHECK_MODULES([GTHREAD], [gthread-2.0 >= $GTHREAD_REQUIRED_VERSION])
AC_SUBST([GTHREAD_CFLAGS])
AC_SUBST([GTHREAD_LIBS])

# Gio
PKG_CHECK_MODULES([GIO], [gio-2.0 >= $GIO_REQUIRED_VERSION])
AC_SUBST([GIO_CFLAGS])
AC_SUBST([GIO_LIBS])

# json-glib
PKG_CHECK_MODULES([JSONGLIB], [json-glib-1.0 >= $JSONGLIB_REQUIRED_VERSION])
AC_SUBST([JSONGLIB_CFLAGS])
AC_SUBST([JSONGLIB_LIBS])

# SQLite
#
# TODO - Add FTS and ICU library built in support
#
PKG_CHECK_MODULES([SQLITE], [sqlite3 >= $SQLITE_REQUIRED_VERSION])
AC_SUBST([SQLITE3_CFLAGS])
AC_SUBST([SQLITE3_LIBS])

# libsoup
PKG_CHECK_MODULES([LIBSOUP], [libsoup-2.4 >= $LIBSOUP_REQUIRED_VERSION])
AC_SUBST([LIBSOUP_CFLAGS])
AC_SUBST([LIBSOUP_LIBS])

# Webkit
if test "$with_webkitframework" != "yes" ; then
  PKG_CHECK_MODULES([WEBKIT], [webkit-1.0 >= $WEBKIT_REQUIRED_VERSION])
  AC_SUBST([WEBKIT_CFLAGS])
  AC_SUBST([WEBKIT_LIBS])
else
  # Darwin with frameworks
  AC_DEFINE(WEBKIT_FRAMEWORK)

  WEBKIT_CFLAGS="-I /System/Library/Frameworks/JavaScriptCore.framework/Headers"
  WEBKIT_LIBS="-framework JavaScriptCore"
fi

LDFLAGS="$LDFLAGS $WEBKIT_LIBS $SQLITE_LIBS $LIBXML_LIBS $LIBSOUP_LIBS $GLIB_LIBS $GTHREAD_LIBS $GIO_LIBS $JSONGLIB_LIBS"
CFLAGS="$CFLAGS $WEBKIT_CFLAGS $SQLITE_CFLAGS $LIBXML_CFLAGS $LIBSOUP_CFLAGS $GLIB_CFLAGS $GTHREAD_CFLAGS $GIO_CFLAGS $JSONGLIB_CFLAGS -DG_DISABLE_DEPRECATED"

#CFLAGS="-g $CFLAGS -Wall -Werror"
CFLAGS="-g $CFLAGS -Wall -O0"
#CFLAGS="-g $CFLAGS -Wall -O0 -DSQLITE_MEMDEBUG"

AC_OUTPUT([
Makefile

src/Makefile

src/tbjson/Makefile
src/tbjsonpath/Makefile
src/lib/Makefile
src/httpd/Makefile
src/www/Makefile
src/tests/Makefile

support/Makefile
])
