#!/bin/bash

[ ! -e git.conf ] && echo "[git.conf] does not exist!" && exit -1 
. git.conf

## clone
git clone $git_repo -b $git_branch $project_name

## set github user
echo "Adding git user [$git_user] to [$project_name/.git/config]"
sed -e "s!://github.com!://$git_user@github.com!g" -i $project_name/.git/config

