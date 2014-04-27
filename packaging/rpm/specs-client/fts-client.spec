%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print (get_python_lib(1))")}

%global _hardened_build 1

%global __provides_exclude_from ^%{python_sitearch}/fts/.*\\.so$

Name: fts-client
Version: 3.2.23
Release: 5%{?dist}
Summary: File Transfer Service V3
Group: Applications/Internet
License: ASL 2.0
URL: https://svnweb.cern.ch/trac/fts3/wiki
# The source for this package was pulled from upstream's vcs.  Use the
# following commands to generate the tarball:
#  svn export https://svn.cern.ch/reps/fts3/trunk fts3
#  tar -czvf fts-3.2.23-2.tar.gz fts3
Source0:   https://grid-deployment.web.cern.ch/grid-deployment/dms/fts3/tar/%{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 6
BuildRequires:  boost-devel
BuildRequires:  cmake
BuildRequires:  libcurl-devel
BuildRequires:  python2-devel
%else
BuildRequires:  boost141-devel
BuildRequires:  cmake28
BuildRequires:  curl-devel
BuildRequires:  gcc44-c++
BuildRequires:  python-devel
%endif

BuildRequires:  CGSI-gSOAP-devel
BuildRequires:  doxygen
BuildRequires:  glib2-devel
BuildRequires:  globus-gsi-credential-devel
BuildRequires:  gridsite-devel
BuildRequires:  gsoap-devel
BuildRequires:  is-interface-devel
BuildRequires:  openldap-devel
Requires(pre):  shadow-utils

%description
A set of command line tools for submitting, querying
and canceling transfer-jobs to the FTS service. Additionally,
there is a CLI that can be used for configuration and
administering purposes.

%package -n fts-libs
Summary: File Transfer Service version 3 libraries
Group: System Environment/Libraries

%description -n fts-libs
FTS common libraries used across the client and
server. This includes among others: configuration
parsing, logging and error-handling utilities, as
well as, common definitions and interfaces

%package -n fts-python
Summary: File Transfer Service version 3 python bindings
Group: System Environment/Libraries
Requires: fts-libs%{?_isa} = %{version}-%{release}
Requires: python%{?_isa}

%description -n fts-python
FTS python bindings for client libraries and DB API

%clean
rm -rf %{buildroot}

%prep
%setup -qc

%build
# Make sure the version in the spec file and the version used
# for building matches
fts_cmake_ver=`sed -n 's/^set(VERSION_\(MAJOR\|MINOR\|PATCH\) \([0-9]\+\).*/\2/p' CMakeLists.txt | paste -sd '.'`
if [ "$fts_cmake_ver" != "%{version}" ]; then
    echo "The version in the spec file does not match the CMakeLists.txt version!"
    echo "$fts_cmake_ver != %{version}"
    exit 1
fi

# Build
mkdir build
cd build
%if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} >= 7
    %cmake -DCLIENTBUILD=ON -D CMAKE_BUILD_TYPE=RelWithDebInfo -D CMAKE_INSTALL_PREFIX='' ..
%elif %if %{?fedora}%{!?fedora:0} >= 10 || %{?rhel}%{!?rhel:0} == 6
    %cmake28 -DCLIENTBUILD=ON -D CMAKE_BUILD_TYPE=RelWithDebInfo -D CMAKE_INSTALL_PREFIX='' ..
%else
    %cmake28 -DCMAKE_C_COMPILER=/usr/bin/gcc44 -DCMAKE_CXX_COMPILER=/usr/bin/g++44 -DCLIENTBUILD=ON -D CMAKE_BUILD_TYPE=RelWithDebInfo -D CMAKE_INSTALL_PREFIX='' ..
%endif

make %{?_smp_mflags}

%install
cd build
make install DESTDIR=%{buildroot}

# Libs scriptlets
%post -n fts-libs -p /sbin/ldconfig

%postun -n fts-libs -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
%{_bindir}/fts-config-set
%{_bindir}/fts-config-get
%{_bindir}/fts-config-del
%{_bindir}/fts-set-debug
%{_bindir}/fts-set-blacklist
%{_bindir}/fts-set-priority
%{_bindir}/fts-transfer-list
%{_bindir}/fts-transfer-status
%{_bindir}/fts-transfer-submit
%{_bindir}/fts-transfer-cancel
%{_bindir}/fts-transfer-snapshot
%{_bindir}/fts-delegation-init

%{_mandir}/man1/fts*

%files -n fts-libs
%defattr(-,root,root,-)
%{_libdir}/libfts_cli_common.so*
%{_libdir}/libfts_ws_ifce_client.so*
%{_libdir}/libfts_ws_ifce_server.so*
%{_libdir}/libfts_delegation_api_simple.so*
%{_libdir}/libfts_delegation_api_cpp.so*
%doc README
%doc LICENSE

%files -n fts-python
%defattr(-,root,root,-)
%{python_sitearch}/fts

%changelog
* Tue Apr 08 2014 Alejandro Alvarez <aalvarez@cern.ch> - 3.2.23-5
  - Forked client-only spec file for SL5

