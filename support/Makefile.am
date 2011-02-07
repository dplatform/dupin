bin_SCRIPTS = dupinctl

bin_PROGRAMS = dupin_loader

INCLUDES = \
	   -DDS_CONFIG_FILE=\"$(sysconfdir)/dupinserver.cfg\" \
	   -I../src/lib \
           -I../src/httpd \
           -I../src/tbjson

LDADD = ../src/lib/libdupin.la \
        ../src/httpd/dupin_server_common.o \
        ../src/httpd/configure.o

CLEANFILES = $(bin_SCRIPTS) $(bin_PROGRAMS)

DUPINSUPPORTSOURCES = dupinctl dupin_loader.c

EXTRA_DIST = $(DUPINSUPPORTSOURCES)

TARGETS  = $(DUPINSUPPORTSOURCES)

uninstall-hook:
	cd $(DESTDIR)$(bindir) && rm -f dupinctl \
	cd $(DESTDIR)$(bindir) && rm -f dupin_loader

## We can't use configure to do the substitution here; we must do it
## by hand.  We use a funny notation here to avoid configure
## substitutions in our text.
do_subst = sed \
  -e 's,[@]DUPINCTL_PORT[@],8088,g' \
  -e 's,[@]DUPINCTL_PIDFILE[@],/var/run/dupin.pid,g' \
  -e 's,[@]configure_input[@],Generated from $@.in; do not edit by hand.,g' \
  -e 's,[@]bindir[@],$(bindir),g' \
  -e 's,[@]datadir[@],$(datadir),g' \
  -e 's,[@]sysconfdir[@],$(sysconfdir),g'

dupinctl: dupinctl.in

dupinctl: Makefile
	rm -f $@ $@.tmp
	$(do_subst) $(srcdir)/$@.in >$@.tmp
	chmod +x $@.tmp
	chmod a-w $@.tmp
	mv -f $@.tmp $@

