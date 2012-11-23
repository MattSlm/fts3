%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib())")}
%{!?python_sitearch: %define python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}

Name: fts-mysql
Version: 0.0.1 
Release: 51%{?dist}
Summary: File Transfer Service V3 mysql plug-in
Group: Applications/Internet 
License: ASL 2.0
URL: https://svnweb.cern.ch/trac/fts3/wiki 
Source0: http://svnweb.cern.ch/trac/fts3/%{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  cmake
BuildRequires:  boost-devel%{?_isa}
BuildRequires:  glib2-devel%{?_isa}
BuildRequires:  python-devel%{?_isa}
BuildRequires:  soci-mysql-devel%{?_isa}
Requires(pre):  shadow-utils

%description
The File Transfer Service V3 mysql plug-in

%package mysql
Summary: File Transfer Service version 3 mysql plug-ins
Group: Applications/Internet
Requires: fts-libs = %{version}-%{release}
Requires:  soci-mysql%{?_isa}

%description mysql
FTS mysql plug-ins

%description mysql
FTS V3 mysql plug-ins

%prep
%setup -qc

%build
mkdir build
cd build
%cmake -DMYSQLBUILD=ON -D CMAKE_BUILD_TYPE=Release -D CMAKE_INSTALL_PREFIX='' ..
make %{?_smp_mflags}


%install
cd build
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT
mkdir -p %{buildroot}%{python_sitearch}/fts

%post mysql -p /sbin/ldconfig

%postun mysql -p /sbin/ldconfig

%clean
rm -rf $RPM_BUILD_ROOT

%files mysql
%defattr(-,root,root,-)
%{python_sitearch}/fts/ftsdb.so*
%{_libdir}/libfts_db_mysql.so.*

%changelog
 * Wed Aug 8 2012 Steve Traylen <steve.traylen@cern.ch> - 0.0.0-51
  - A bit like a fedora package
