#!/bin/sh
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

if test "$DIE" -eq 1; then
        exit 1
fi

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
