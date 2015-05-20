#--------------------------------------------------------
# HappyFace integration and deployment test server
#--------------------------------------------------------

These scripts, 

      cron.HF.integration.sh
      generate_happyface_instance.sh


can generate an automatic HF integration and deployment server. 
Just apply the scripts to the server as follows,

    mkdir -v /root/happyface
    cp -v generate_happyface_instance.sh /root/happyface
    cp -v cron.HF.integration.sh /etc/cron.daily


Finally, copy a spec file into SPEC directory.
    mkdir -v /root/happyface/SPECS/
    cp -v HappyFace-Grid-Engine.spec /root/happyface/SPECS/


The scripts will try a sequencial procedure of checking GitHub, packaging RPM, 
removing old env, deplying new HF, and running acquire.py 

As for your initial test, just run the script as follows.

   cd /root/happyface
   ./generate_happyface_instance.sh
