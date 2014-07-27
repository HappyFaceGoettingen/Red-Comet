#!/bin/bash -x

[ ! -e git.conf ] && echo "[git.conf] does not exist!" && exit -1 
. git.conf

cd $devel_dir

## cert
[ ! -e cert ] && mkdir -v cert
chmod 777 $devel_dir/cert
chmod +t $devel_dir/cert
sudo ln -sf $devel_dir/cert /var/lib/HappyFace3/

## hf.gridengine & toolkit
sudo ln -sf $devel_dir/$project_name/hf/gridengine /var/lib/HappyFace3/hf
sudo ln -sf $devel_dir/$project_name/hf/gridtoolkit /var/lib/HappyFace3/hf

## module
sudo ln -sf $devel_dir/$project_name/modules/* /var/lib/HappyFace3/modules/

## symlinks for module tests
ln -sf $devel_dir/$project_name/defaultconfig $devel_dir/$project_name/hf/gridengine/
ln -sf $devel_dir/$project_name/defaultconfig $devel_dir/$project_name/hf/gridtoolkit/
ln -sf $devel_dir/$project_name/config $devel_dir/$project_name/hf/gridengine/
ln -sf $devel_dir/$project_name/config $devel_dir/$project_name/hf/gridtoolkit/

## config
sudo ln -sf $devel_dir/$project_name/config/categories-enabled/*.cfg /var/lib/HappyFace3/config/categories-enabled/
sudo ln -sf $devel_dir/$project_name/config/modules-enabled/*.cfg /var/lib/HappyFace3/config/modules-enabled/

## defaultconfig
sudo rm -v /var/lib/HappyFace3/defaultconfig/happyface.cfg
sudo ln -s $devel_dir/$project_name/defaultconfig/happyface.cfg /var/lib/HappyFace3/defaultconfig/happyface.cfg

## acquire.py
sudo ln -s $devel_dir/$project_name/grid_enabled_acquire.py /var/lib/HappyFace3/grid_enabled_acquire.py



#----------------------------------------
# Generate certs
#----------------------------------------
[ ! -e $HOME/.globus/usercert.pem ] && echo "No [$HOME/.globus/usercert.pem] Usercert!!" && exit -1
[ ! -e $HOME/.globus/userkey.pem ] && echo "No [$HOME/.globus/userkey.pem] Userkey!!" && exit -1

## copying usercert
[ ! -e $devel_dir/cert/usercert.pem ] && cp -v $HOME/.globus/usercert.pem $devel_dir/cert/usercert.pem

## generating userkey witout passphrase
[ ! -e $devel_dir/cert/userkey.nopass.pem ] && openssl rsa -in $HOME/.globus/userkey.pem -out $devel_dir/cert/userkey.nopass.pem
[ ! -e $devel_dir/cert/userkey.pem ] && ln -sf $devel_dir/cert/userkey.nopass.pem $devel_dir/cert/userkey.pem

## giving proper permissions and ownerships
sudo chown $USER $devel_dir/cert/usercert.pem
sudo chown $USER $devel_dir/cert/userkey.nopass.pem
[ -e $devel_dir/cert/x509up_happyface ] && sudo chown $USER $devel_dir/cert/x509up_happyface
chmod 644 $devel_dir/cert/usercert.pem
chmod 400 $devel_dir/cert/userkey.nopass.pem
