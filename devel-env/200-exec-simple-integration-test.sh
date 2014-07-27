#!/bin/bash -x

[ ! -e git.conf ] && echo "[git.conf] does not exist!" && exit -1 
. git.conf

cd $devel_dir


#----------------------------------------
# Generate certs
#----------------------------------------
## giving proper permissions and ownerships
sudo chown happyface3 $devel_dir/cert/usercert.pem
sudo chown happyface3 $devel_dir/cert/userkey.nopass.pem
sudo chown happyface3 $devel_dir/cert/userkey.pem
[ -e $devel_dir/cert/x509up_happyface ] && sudo chown happyface3 $devel_dir/cert/x509up_happyface


#----------------------------------------
# Execute grid_enabled_acuire.py
#----------------------------------------
cd /var/lib/HappyFace3
sudo su happyface3 -c "export PYTHONPATH=/var/lib/HappyFace3; python grid_enabled_acquire.py"


#----------------------------------------
# Restart httpd
#----------------------------------------
sudo /etc/init.d/httpd restart
