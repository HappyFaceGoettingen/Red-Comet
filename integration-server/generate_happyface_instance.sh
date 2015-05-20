#!/bin/bash

HF_PACKAGE_NAME="HappyFace-Grid-Engine"
HF_PACKAGE=/root/happyface/RPMS/x86_64/HappyFace-Grid-Engine-3.0.0-3.x86_64.rpm
LOG_DIR=/root/happyface/log
EMAIL="ph2-admin@gwdg.de"


check_git(){
    local git_dir=$1

    local_git=$(cd $git_dir; git log -1 HEAD)
    remote_git=$(cd $git_dir; git log -1 origin/HEAD)

    echo "$local_git" > /tmp/local.git
    echo "$remote_git" > /tmp/remote.git
    
    diff /tmp/local.git /tmp/remote.git && echo "Local Git repo [git_dir] is the latest." && exit 0
}


build_rpm(){
    ## create .rpmmacros
    echo "%_topdir        $PWD" > /root/.rpmmacros
    
    
    dist=`uname -r | perl -pe "s/^.*\.(el[0-9])\..*$/\1/g"`



    mkdir -pv BUILD BUILDROOT SOURCES SPECS RPMS log

    echo "------------------- Source packaging -----------------------"
    [ ! -e SOURCES/HappyFace-Red-Comet ] && mkdir -pv SOURCES/HappyFace-Red-Comet
    cd SOURCES/HappyFace-Red-Comet

    [ -e Red-Comet ] && rm -rf Red-Comet
    git clone https://github.com/HappyFaceGoettingen/Red-Comet.git -b Zgok
    cp -v Red-Comet/defaultconfig/happyface.cfg happyface-red-comet.cfg
    ln -sf $PWD/happyface-red-comet.cfg ..

    [ -e red-comet ] && rm -rf red-comet
    [ ! -e red-comet/hf/gridengine ] && mkdir -pv red-comet/hf/gridengine
    [ ! -e red-comet/hf/gridtoolkit ] && mkdir -pv red-comet/hf/gridtoolkit
    cp -v Red-Comet/hf/gridengine/*.py red-comet/hf/gridengine/
    cp -v Red-Comet/hf/gridtoolkit/*.py red-comet/hf/gridtoolkit/
    cp -v Red-Comet/grid_enabled_acquire.py red-comet/
    cp -rv Red-Comet/modules red-comet/
    cp -rv Red-Comet/config red-comet/
    cp -rv Red-Comet/defaultconfig red-comet/


    tar czvf red-comet.tar.gz red-comet

    cd ..
    mv -v HappyFace-Red-Comet/red-comet.tar.gz .
    cd ..

    echo "-------------------- RPM packaging -------------------------"
    rpmbuild --define 'dist .${dist}' --clean -ba SPECS/HappyFace-Grid-Engine.spec
    rm -rvf BUILD BUILDROOT

}


setup_HF_env(){
    KEY_HOME=/var/lib/gridkeys
    [ ! -e /var/lib/gridkeys ] && mkdir -v $KEY_HOME && chmod 1777 $KEY_HOME

    cp -v $KEY_HOME/userkey.nopass.pem /var/lib/HappyFace3/cert/userkey.pem
    cp -v $KEY_HOME/usercert.pem /var/lib/HappyFace3/cert/usercert.pem
    chown happyface3:happyface3 /var/lib/HappyFace3/cert/userkey.pem /var/lib/HappyFace3/cert/usercert.pem
}

run_HF(){
    [ -e /var/lib/HappyFace3/cert/userkey.pem ] && su happyface3 -c "cd /var/lib/HappyFace3; python grid_enabled_acquire.py"
}


#-------------------------------------------------------
# Check updates
#-------------------------------------------------------
[ -e SOURCES/HappyFace-Red-Comet/Red-Comet ] && check_git SOURCES/HappyFace-Red-Comet/Red-Comet
build_rpm 2>&1 | tee $LOG_DIR/build.log
echo "$(cat $LOG_DIR/build.log)
" | mail -s "Rebuilding HappyFace [$(hostname -s)]" $EMAIL



#-------------------------------------------------------
# Repo preparation
#-------------------------------------------------------
[ ! -e /etc/yum.repos.d/happyface.repo ] && wget http://physik2.uni-goettingen.de/~gen/happyface/repo/happyface-atlas.repo -O /etc/yum.repos.d/happyface.repo && yum clean all

#--------------------------------------------------------
# Remove HappyFace instance
#--------------------------------------------------------
yum -y remove $HF_PACKAGE_NAME HappyFace 2>&1 | tee $LOG_DIR/remove.log
rm -rvf /var/lib/HappyFace3  2>&1 | tee -a $LOG_DIR/remove.log

#--------------------------------------------------------
# Install HappyFace instance
#--------------------------------------------------------
yum -y --nogpgcheck install $HF_PACKAGE 2>&1 | tee $LOG_DIR/deploy.log


#--------------------------------------------------------
# Set up env
#--------------------------------------------------------
setup_HF_env


#--------------------------------------------------------
# Run
#--------------------------------------------------------
run_HF




echo "
=================================================================
 Removal
=================================================================
$(cat $LOG_DIR/remove.log)



=================================================================
 Installation
=================================================================
$(cat $LOG_DIR/deploy.log)



=================================================================
 Environment
=================================================================


=================================================================
 Run
=================================================================


" | mail -s "HappyFace Deployment Report [$(hostname -s)]" $EMAIL

