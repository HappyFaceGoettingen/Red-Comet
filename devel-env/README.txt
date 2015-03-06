#-------------------------------------------------
#
# How to make your development environment
#
#-------------------------------------------------

0. Go to https://github.com/HappyFaceGoettingen/Red-Comet/devel-env

1. Put "happyface-atlas.repo" in /etc/yum.repos.d

2. Install UMD, rpmforge and CVMFS

3. Download HappyFace-Red-Coment-3.0.0.....rpm

4. Install HappyFace-Red-Comment RPM
 yum install HappyFace-Red-Commet....rpm

5. Go to /var/lib/HappyFace3-devel

6. Execute scripts according to the number

7. Read explanation in 
 yum info HappyFace-Red-Comet

 
            : Second step: Install PyDEV in your Eclipse IDE environemnt
            : http://pydev.org/manual_101_install.html
            : 
            : 
            : Third step: Create your Python project (for example, HappyFaceDev project)
            :  In eclipse, open "File" -> "Properties", then open "PyDev - PYTHONPATH". check if "/var/lib/HappyFace3" is
            : "Exernal Libraries".
            : 
