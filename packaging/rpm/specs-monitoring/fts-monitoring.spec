Summary: FTS3 Web Application for monitoring
Name: fts-monitoring
Version: 3.1.72
Release: 1%{?dist}
URL: https://svnweb.cern.ch/trac/fts3
License: ASL 2.0
Group: Applications/Internet
BuildArch: noarch

#Requires: cx_Oracle
Requires: MySQL-python
Requires: Django >= 1.3.7
Requires: httpd
Requires: mod_wsgi
Requires: python
Requires: python-decorator
Requires: python-matplotlib

Source0: https://grid-deployment.web.cern.ch/grid-deployment/dms/fts3/tar/%{name}-%{version}.tar.gz

%description
FTS v3 web application for monitoring,
it gives a detailed view of the current state of FTS
including the queue with submitted transfer-jobs,
the active, failed and finished transfers, as well
as some statistics (e.g. success rate)

%post
service httpd condrestart

%package selinux
Summary:		SELinux support for fts-monitoring
Group:			Applications/Internet
Requires:       fts-monitoring = %{version}-%{release}

%description selinux
This package labels port 8449, used by fts-monitoring, as http_port_t,
so Apache can bind to it.
It also labels /var/log/fts3/ with httpd_sys_content_t, so the log files
can be served.

%post selinux
if [ $1 -gt 0 ] ; then # First install
    semanage port -a -t http_port_t -p tcp 8449
    setsebool -P httpd_can_network_connect=1 
    semanage fcontext -a -t httpd_sys_content_t "/var/log/fts3(/.*)?"
    restorecon -R /var/log/fts3/
    libnzz="/usr/lib64/oracle/11.2.0.3.0/client/lib64/libnnz11.so"
    if [ -f "$libnzz" ]; then
        execstack -c "$libnzz"
    fi
fi

%preun selinux
if [ $1 -eq 0 ] ; then # Final removal
    semanage port -d -t http_port_t -p tcp 8449
    setsebool -P httpd_can_network_connect=0
    semanage fcontext -d -t httpd_sys_content_t "/var/log/fts3(/.*)?"
    restorecon -R /var/log/fts3
    libnzz="/usr/lib64/oracle/11.2.0.3.0/client/lib64/libnnz11.so"
    if [ -f "$libnzz" ]; then
        execstack -s "$libnzz"
    fi
fi

%prep
%setup -qc

%build
mkdir build

%install
shopt -s extglob
mkdir -p %{buildroot}%{_datadir}/fts3web/
mkdir -p %{buildroot}%{_sysconfdir}/fts3web/
mkdir -p %{buildroot}%{_sysconfdir}/httpd/conf.d/ 
cp -r -p !(etc|httpd.conf.d) %{buildroot}%{_datadir}/fts3web/
cp -r -p etc/fts3web         %{buildroot}%{_sysconfdir}
install -m 644 httpd.conf.d/ftsmon.conf           %{buildroot}%{_sysconfdir}/httpd/conf.d/ 

%files
%{_datadir}/fts3web
%config(noreplace) %{_sysconfdir}/httpd/conf.d/ftsmon.conf
%config(noreplace) %dir %{_sysconfdir}/fts3web/
%config(noreplace) %attr(640, root, apache) %{_sysconfdir}/fts3web/fts3web.ini
%doc LICENSE

%files selinux

%changelog
 * Wed Nov 27 2013 Alejandro Alvarez <aalvarez@cern.ch> - 3.1.41-2
  - Using semanage instead of chcon to get permanent changes in
    the SELinux context for /var/log/fts3
  - Restart httpd on installation/upgrade

 * Mon Oct 28 2013 Alejandro Alvarez <aalvarez@cern.ch> - 3.1.33-3
  - Added versioned dependency fts-monitoring

 * Tue Oct 08 2013 Alejandro Alvarez <aalvarez@cern.ch> - 3.1.33-2
  - Added selinux rpm

 * Mon Sep 02 2013 Michal Simon <michal.simon@cern.ch> - 3.1.1-2
  - since it is a noarch package removing '%{?_isa}' sufix

 * Wed Aug 28 2013 Michal Simon <michal.simon@cern.ch> - 3.1.1-1
  - replacing '--no-preserve=ownership'
  - python macros have been removed
  - comments regarding svn have been removed
  - '%{_builddir}/%{name}-%{version}/' prefix is not used anymore
  - more detailed description

 * Tue Apr 30 2013 Michal Simon <michal.simon@cern.ch> - 3.1.0-1
  - First EPEL release
