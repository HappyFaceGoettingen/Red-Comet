EPEL_VERSION=epel-release-6-8.noarch.rpm
UMD_VERSION=umd-release-3.0.0-1.el6.noarch.rpm
HOME=/root

#------------------------------------
# install
#------------------------------------
[ -e /etc/yum.repos.d/dag.repo ] && rm -v /etc/yum.repos.d/dag.repo


# EPEL
if ! [ -e $HOME/$EPEL_VERSION ]; then
    rpm -e epel-release
    wget http://dl.fedoraproject.org/pub/epel/6/x86_64/$EPEL_VERSION -O $HOME/$EPEL_VERSION
    rm -v /etc/yum.repos.d/epel*
    yum -y install $HOME/$EPEL_VERSION

    # yum reset
    yum clean all
fi


# UMD
if ! [ -e $HOME/$UMD_VERSION ]; then
    rpm -e umd-release
    wget http://repository.egi.eu/sw/production/umd/3/sl6/x86_64/base/$UMD_VERSION -O $HOME/$UMD_VERSION
    yum -y install $HOME/$UMD_VERSION
    [ `arch` != "x86_64" ] && ls /etc/yum.repos.d/UMD* | xargs -I {} sed -e "s/\$basearch/x86_64/g" -i {}

    # yum reset
    yum clean all
fi


# RPM forge
RPMFORGE=http://pkgs.repoforge.org/rpmforge-release/rpmforge-release-0.5.3-1.el6.rf.x86_64.rpm
wget $RPMFORGE -O `basename $RPMFORGE`
yum --nogpgcheck -y install `basename $RPMFORGE`



