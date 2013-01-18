#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

# Run this to set up the build system: configure, makefiles, etc.

warn() {
	echo "WARNING: $@" 1>&2
}

package="dupin"

DIE=0

(autoconf --version) < /dev/null > /dev/null 2>&1 || {
        echo
        echo "You must have autoconf installed to compile $package."
        echo "Download the appropriate package for your distribution,"
        echo "or get the source tarball at ftp://ftp.gnu.org/pub/gnu/"
        DIE=1
}

(automake --version) < /dev/null > /dev/null 2>&1 || {
        echo
        echo "You must have automake installed to compile $package."
	echo "Download the appropriate package for your system,"
	echo "or get the source from one of the GNU ftp sites"
	echo "listed in http://www.gnu.org/order/ftp.html"
        DIE=1
}

case `uname -s` in
Darwin)
	LIBTOOLIZE=glibtoolize
	;;
FreeBSD)
	LIBTOOLIZE=libtoolize
	;;
Linux)
	LIBTOOLIZE=libtoolize
	;;
SunOS)
	LIBTOOLIZE=libtoolize
	;;
*)
	warn "unrecognized platform:" `uname -s`
	LIBTOOLIZE=libtoolize
esac

($LIBTOOLIZE --version) < /dev/null > /dev/null 2>&1 || {
        echo
        echo "You must have $LIBTOOLIZE installed to compile $package."
	echo "Download the appropriate package for your system,"
	echo "or get the source from one of the GNU ftp sites"
	echo "listed in http://www.gnu.org/software/libtool/"
        DIE=1
}

if test "$DIE" -eq 1; then
        exit 1
fi

set -e

echo "Generating configuration files for $package, please wait...."

echo "  aclocal"
aclocal > /dev/null 2>&1

echo "  autoheader"
autoheader > /dev/null 2>&1

echo "  $LIBTOOLIZE"
$LIBTOOLIZE --force --copy > /dev/null 2>&1

touch NEWS README AUTHORS ChangeLog

echo "  automake --add-missing"
automake --copy --gnu --add-missing -c > /dev/null 2>&1

echo "  autoconf"
autoconf > /dev/null 2>&1

rm -rf autom4te.cache
