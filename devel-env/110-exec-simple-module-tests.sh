#!/bin/bash -x


[ ! -e git.conf ] && echo "[git.conf] does not exist!" && exit -1 
. git.conf


#----------------------------------------
# Execute grid core functions
#----------------------------------------
cd $devel_dir/$project_name

export PYTHONPATH=/var/lib/HappyFace3
python hf/gridengine/envreader.py
python hf/gridengine/gridcertificate.py
python hf/gridengine/gridsubprocess.py


