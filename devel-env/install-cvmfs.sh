#!/bin/sh

#----------------------------------
# Check CVMFS
#----------------------------------
rpm -q cvmfs && exit 0



#----------------------------------
# make yum repo
#----------------------------------
repobase="http://cvmrepo.web.cern.ch/cvmrepo/yum/cvmfs/EL/6/x86_64"
repofile="cvmfs-release-2-4.el6.noarch.rpm"
HOME=/root

if ! [ -e $HOME/$repofile ]; then
    wget $repobase/$repofile -O $HOME/$repofile
    yum -y localinstall $HOME/$repofile
   
    #----------------------------------
    # install cvmfs
    #----------------------------------
    yum clean all
    yum -y install cvmfs cvmfs-auto-setup cvmfs-init-scripts
fi
#----------------------------------
# Configuration
#----------------------------------
cp -v default.local /etc/cvmfs/default.local
#----------------------------------
# Start
#----------------------------------
/etc/init.d/autofs restart

#----------------------------------
# Check
#----------------------------------
cvmfs_config chksetup
cvmfs_config probe

# details
cvmfs_config stat -v
cvmfs_talk cache list

# ls
ls /cvmfs/atlas.cern.ch
