#!/bin/bash

devel_dir=/var/lib/HappyFace3-devel
project_name=HappyFaceGridEngine
git_repo="https://github.com/HappyFaceGoettingen/Red-Comet.git"
git_branch="blue-giant"


echo -n "GitHub User Name = "
read git_user

#echo "changing permission of $devel_dir"
#sudo chown -R $USER $devel_dir

echo "devel_dir=$devel_dir
project_name=$project_name
git_repo=$git_repo
git_branch=$git_branch
git_user=$git_user" > $devel_dir/git.conf


echo "$devel_dir/git.conf
----------------------------
`cat $devel_dir/git.conf`
----------------------------
"