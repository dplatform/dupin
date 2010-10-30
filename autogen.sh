#!/bin/bash

if [ `uname` == "Darwin" ]; then
  if [ ! -e /usr/share/aclocal/pkg.m4 ]; then
    echo ""
    echo "WARNING !"
    echo ""
    echo "There is a known problem with PKG_CHECK_MODULES macro on OSX/Darwin using Fink or Darwin Ports and pgk-config"
    echo "See details at http://playerstage.sourceforge.net/wiki/Basic_FAQ#I_have_a_syntax_error_involving_PKG_CHECK_MODULES._What.27s_the_fix.3F"
    echo ""
    echo "To fix it if you using Fink try the following command:"
    echo ""
    echo "  sudo ln -s /sw/share/aclocal/pkg.m4 /usr/share/aclocal/pkg.m4"
    echo ""
    echo "If you using Darwin Ports try the following command:"
    echo ""
    echo "  sudo ln -s /opt/local/share/aclocal/pkg.m4 /usr/share/aclocal/pkg.m4"
    echo ""
    echo "If you using Homebrew try the following command:"
    echo ""
    echo " sudo ln -s /usr/local/share/aclocal/pkg.m4 /usr/share/aclocal/pkg.m4"
    echo " (you might need to run the above command also for other libraries such as gtk-doc)"
    echo ""
    exit 1;
  fi
fi

aclocal --version &>/dev/null || {
  echo
  echo "**Error**: You must have \`aclocal' installed to compile this package."
  exit 1
}

autoheader --version &>/dev/null || {
  echo
  echo "**Error**: You must have \`autoconf' installed to compile this package."
  exit 1
}

if [ `uname` == "Darwin" ]; then
  glibtoolize --version &>/dev/null || {
    echo
    echo "**Error**: You must have \`glibtool' installed to compile this package."
    exit 1
  }
else
  libtoolize --version &>/dev/null || {
    echo
    echo "**Error**: You must have \`libtool' installed to compile this package."
    exit 1
  }
fi

automake --version &>/dev/null || {
  echo
  echo "**Error**: You must have \`automake' installed to compile this package."
  exit 1
}

autoconf --version &>/dev/null || {
  echo
  echo "**Error**: You must have \`autoconf' installed to compile this package."
  exit 1
}

echo "aclocal..."
aclocal &> /dev/null || exit 1

echo "autoheader..."
autoheader &> /dev/null || exit 1

if [ `uname` == "Darwin" ]; then
  echo "glibtoolize..."
  glibtoolize --force --copy &>/dev/null || exit 1
else
  echo "libtoolize..."
  libtoolize --force --copy &>/dev/null || exit 1
fi

touch NEWS README AUTHORS ChangeLog

echo "automake..."
automake --copy --gnu --add-missing &> /dev/null || exit 1

echo "autoconf..."
autoconf &> /dev/null || exit 1

rm -rf autom4te.cache

echo "Bye :)"
