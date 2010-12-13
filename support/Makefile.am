bin_SCRIPTS = dupinctl

CLEANFILES = $(bin_SCRIPTS)

DUPINSUPPORTSOURCES = dupinctl

EXTRA_DIST = $(DUPINSUPPORTSOURCES)

TARGETS  = $(DUPINSUPPORTSOURCES)

uninstall-hook:
	cd $(DESTDIR)$(bindir) && rm -f dupinctl

## We can't use configure to do the substitution here; we must do it
## by hand.  We use a funny notation here to avoid configure
## substitutions in our text.
do_subst = sed \
  -e 's,[@]DUPINCTL_PORT[@],8088,g' \
  -e 's,[@]DUPINCTL_CONFIGURATION[@],$(etcdir)/dupinserver.cfg,g' \
  -e 's,[@]DUPINCTL_PIDFILE[@],/var/run/dupin.pid,g' \
  -e 's,[@]configure_input[@],Generated from $@.in; do not edit by hand.,g' \
  -e 's,[@]bindir[@],$(bindir),g' \
  -e 's,[@]datadir[@],$(datadir),g'

dupinctl: dupinctl.in

dupinctl: Makefile
	rm -f $@ $@.tmp
	$(do_subst) $(srcdir)/$@.in >$@.tmp
	chmod +x $@.tmp
	chmod a-w $@.tmp
	mv -f $@.tmp $@

