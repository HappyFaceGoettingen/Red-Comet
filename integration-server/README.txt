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
   ./generate_happyface_instance.sh -r


A location of grid user key is "/var/lib/gridkeys". You must prepare a grid user key and cert pair.

  1. generate user certificate
  openssl pkcs12 -clcerts -nokeys -in usercert.p12 -out usercert.pem

  2. create a private certficate with passphrase
  openssl pkcs12 -nocerts -in usercert.p12 -out userkey.pem

  3. create a private certificate without passphrase
  openssl rsa -in userkey.pem -out userkey.nopass.pem

  4. copy grid user cert.
  cp -v userkey.nopass.pem /var/lib/gridkeys
  cp -v usercert.pem /var/lib/gridkeys

