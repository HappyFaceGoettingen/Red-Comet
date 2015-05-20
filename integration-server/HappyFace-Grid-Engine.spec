Summary: HappyFace-Grid-Engine
Name: HappyFace-Grid-Engine
Version: 3.0.0
Release: 3
License: Apache License Version 2.0
Group: System Environment/Daemons
URL: https://ekptrac.physik.uni-karlsruhe.de/trac/HappyFace
Source0: HappyFace-Red-Comet/red-comet.tar.gz
Source1: HappyFace-Red-Comet/happyface-red-comet.cfg
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
#Requires: eclipse-platform
Requires: yum-protectbase
Requires: fetch-crl
Requires: ca-policy-egi-core
Requires: umd-release >= 3.0.0
Requires: rpmforge-release
#Requires: kdesdk


######################################################################
#
#
# Preamble
#
# Macro definitions
%define _prefix         /var/lib/HappyFace3
%define _category_cfg   %{_prefix}/config/categories-enabled
%define _module_cfg     %{_prefix}/config/modules-enabled
%define _cert_dir	%{_prefix}/cert
%define _defaultconfig	%{_prefix}/defaultconfig


%define happyface_uid	373
%define happyface_user	happyface3
%define happyface_gid	373
%define happyface_group	happyface3


%description
HappyFace is a powerful site specific monitoring system for data from multiple input sources. This system collects, processes, rates and presents all important monitoring information for the overall status and the services of a local or Grid computing site. 


Note: How to generate grid certificate without passphrase

1. go to ~/.globus
 cd ~/.globus

2. generate user certificate
 openssl pkcs12 -clcerts -nokeys -in usercert.p12 -out usercert.pem

3. create a private certficate with passphrase
 openssl pkcs12 -nocerts -in usercert.p12 -out userkey.pem
 
4. create a private certificate without passphrase
 openssl rsa -in userkey.pem -out userkey.nopass.pem

5. set permissions
 chmod 400 userkey.pem
 chmod 400 userkey.nopass.pem
 chmod 644 usercert.pem

6. Please copy X.509 keys as follows. If you need to change names and locations, please fix properties (x509.user.key and x509.user.cert) in /var/lib/HappyFace3/defaultconfig/happyface.cfg

 cp userkey.nopass.pem /var/lib/HappyFace3/cert/userkey.pem
 cp usercert.pem /var/lib/HappyFace3/cert/usercert.pem
 chown happyface3:happyface3 /var/lib/HappyFace3/cert/userkey.pem /var/lib/HappyFace3/cert/usercert.pem

7. Test the system
 su %{happyface_user} -c "cd /var/lib/HappyFace3; python grid_enabled_acquire.py" 



Report Bugs and Opinions to <gen.kawamura@cern.ch>

%prep
%setup -b 0 -q -n red-comet

%build
#make

%install
cd ..

[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT


# make directories
! [ -d $RPM_BUILD_ROOT/%{_prefix} ] && mkdir -vp $RPM_BUILD_ROOT/%{_prefix}/hf
! [ -d $RPM_BUILD_ROOT/%{_cert_dir} ] && mkdir -vp $RPM_BUILD_ROOT/%{_cert_dir}
! [ -d $RPM_BUILD_ROOT/%{_module_cfg} ] && mkdir -vp $RPM_BUILD_ROOT/%{_module_cfg}
! [ -d $RPM_BUILD_ROOT/%{_category_cfg} ] && mkdir -vp $RPM_BUILD_ROOT/%{_category_cfg}
! [ -d $RPM_BUILD_ROOT/%{_defaultconfig} ] && mkdir -vp $RPM_BUILD_ROOT/%{_defaultconfig}
! [ -d $RPM_BUILD_ROOT/%{_sysconf_dir} ] && mkdir -p $RPM_BUILD_ROOT/%{_sysconf_dir}



# copy files
cp -vr red-comet/modules $RPM_BUILD_ROOT/%{_prefix}
cp -vr red-comet/config/modules-enabled/* $RPM_BUILD_ROOT/%{_module_cfg}
cp -vr red-comet/config/categories-enabled/* $RPM_BUILD_ROOT/%{_category_cfg}


# grid-related python codes
cp -vr red-comet/hf/gridengine $RPM_BUILD_ROOT/%{_prefix}/hf
cp -vr red-comet/hf/gridtoolkit $RPM_BUILD_ROOT/%{_prefix}/hf
cp -vr red-comet/grid_enabled_acquire.py $RPM_BUILD_ROOT/%{_prefix}




# defaultconfig
cp -v %{SOURCE1} $RPM_BUILD_ROOT/%{_defaultconfig}/


# rm .svn in devel dir
find $RPM_BUILD_ROOT/%{_prefix} -type f | grep .svn | xargs -I {} rm -vf {}
find $RPM_BUILD_ROOT/%{_prefix} -type d | grep .svn | sort -r | xargs -I {} rmdir -v {}




%clean
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT


%post
mv -v %{_defaultconfig}/happyface.cfg %{_defaultconfig}/happyface.cfg.org
mv -v %{_defaultconfig}/happyface-red-comet.cfg %{_defaultconfig}/happyface.cfg


echo "Activating fetch-crl update daemons ..."
chkconfig fetch-crl-boot on
chkconfig fetch-crl-cron on


if [ -e %{_cert_dir}/usercert.pem ] && [ -e %{_cert_dir}/usercert.pem ]; then 
   echo "------------------------------------"
   echo "Populating default Happy Face database ..."
   cd %{_prefix}
   su %{happyface_user} -c "python grid_enabled_acquire.py"
   echo "------------------------------------"
fi

service httpd restart

%preun
service httpd stop

%postun
mv -v %{_defaultconfig}/happyface.cfg.org %{_defaultconfig}/happyface.cfg
service httpd start


%files
%defattr(-,happyface3,happyface3)
%{_cert_dir}
%{_prefix}/modules
%{_prefix}/hf/gridengine
%{_prefix}/hf/gridtoolkit
%{_prefix}/grid_enabled_acquire.py*
%{_defaultconfig}/happyface-red-comet.cfg
%{_category_cfg}
%{_module_cfg}



%changelog
* Tue May 19 2015 Gen Kawamura <Gen.Kawamura@cern.ch> 3.0.0-3
- build sprint-4 Zgok
* Fri Mar 06 2015 Gen Kawamura <Gen.Kawamura@cern.ch> 3.0.0-2
- build sprint-3 Zanzibar
* Thu Aug 14 2014 Gen Kawamura <Gen.Kawamura@cern.ch> 3.0.0-1
- build sprint-2 blue-giant 
* Thu Jul 17 2014 Gen Kawamura <Gen.Kawamura@cern.ch> 3.0.0-0
- initial packaging
