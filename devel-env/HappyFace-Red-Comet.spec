Summary: HappyFace-Red-Comet
Name: HappyFace-Red-Comet
Version: 3.0.0
Release: 4
License: Apache License Version 2.0
Group: System Environment/Daemons
URL: https://ekptrac.physik.uni-karlsruhe.de/trac/HappyFace
# svn co https://ekptrac.physik.uni-karlsruhe.de/public/HappyFace/branches/v3.0 HappyFace
Source0: HappyFace-Red-Comet/red-comet-devel.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-root
Requires: HappyFace = 3.0.0-1
Requires: python >= 2.6
Requires: httpd >= 2.0
Requires: python-cherrypy >= 3.0
Requires: python-sqlalchemy >= 0.5
Requires: python-migrate
Requires: python-mako
Requires: python-matplotlib
Requires: python-sqlite
Requires: python-psycopg2
Requires: python-lxml
Requires: numpy
Requires: mod_wsgi
Requires: sqlite

# For Red-Comet
Requires: openssl
Requires: cvmfs
Requires: eclipse-platform
Requires: eclipse-pde
Requires: yum-protectbase
Requires: fetch-crl
Requires: ca-policy-egi-core
Requires: umd-release >= 3.0.0
Requires: rpmforge-release
Requires: kdesdk


######################################################################
#
#
# Preamble
#
# Macro definitions
%define _devel_dir	/var/lib/HappyFace3-devel

%define happyface_uid	373
%define happyface_user	happyface3
%define happyface_gid	373
%define happyface_group	happyface3


%description
HappyFace is a powerful site specific monitoring system for data from multiple input sources. This system collects, processes, rates and presents all important monitoring information for the overall status and the services of a local or Grid computing site. 


The HappyFace-Red-Comet is a simple replacement of HappyFace Core System version 3.0.0-1. This package allows developers to use the Grid-enabled HappyFace system.

Code name: Red-Comet

First step: go to /var/lib/HappyFace3-devel and execute scripts


Second step: Install PyDEV in your Eclipse IDE environemnt
http://pydev.org/manual_101_install.html


Third step: Create your Python project (for example, HappyFaceDev project)
 In eclipse, open "File" -> "Properties", then open "PyDev - PYTHONPATH". check if "/var/lib/HappyFace3" is "Exernal Libraries".


Report Bugs and Opinions to <gen.kawamura@cern.ch>

%prep
%setup -b 0 -q -n red-comet-devel

%build
#make

%install
cd ..

[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT

# make directories
! [ -d $RPM_BUILD_ROOT/%{_devel_dir} ] && mkdir -p $RPM_BUILD_ROOT/%{_devel_dir}

# copy files
cp -v red-comet-devel/*.sh $RPM_BUILD_ROOT/%{_devel_dir}
cp -v red-comet-devel/README.txt $RPM_BUILD_ROOT/%{_devel_dir}

%clean
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT


%post
echo "Activating fetch-crl update daemons ..."
chkconfig fetch-crl-boot on
chkconfig fetch-crl-cron on
[ -e %{_devel_dir} ] && chmod 1777 %{_devel_dir}

%preun
service httpd stop

%postun
service httpd start


%files
%defattr(777,happyface3,happyface3)
%{_devel_dir}/*.sh
%defattr(644,happyface3,happyface3)
%{_devel_dir}/README.txt

%changelog
* Fri Mar 06 2015 Gen Kawamura <Gen.Kawamura@cern.ch> 3.0.0-4
- Sprint Zgok env
* Thu Aug 18 2014 Gen Kawamura <Gen.Kawamura@cern.ch> 3.0.0-3
- Sprint 3 env
- Code-Name: Black Tri Stars
https://www.youtube.com/watch?v=uGyrHynQe08
* Thu Jul 27 2014 Gen Kawamura <Gen.Kawamura@cern.ch> 3.0.0-2
- Very simplified
- Source codes will be given by GitHub
* Thu Jul 03 2014 Gen Kawamura <Gen.Kawamura@cern.ch> 3.0.0-1
- Fixed bugs of config reader, web page, performance of GridPopen
- Implemented further core functions
- Intermediate implementation of hf/gridtoolkit/AtlasDdmDatasetsViewer.py
- UML diagram is included
- Installation process is simplified
- Velocity of Developments is 3 times faster
http://www.youtube.com/watch?v=Hx1284IxBOY

* Fri Jun 27 2014 Gen Kawamura <Gen.Kawamura@cern.ch> 3.0.0-0
- made development directory (/var/lib/HappyFace3-devel)
- initial packaging
