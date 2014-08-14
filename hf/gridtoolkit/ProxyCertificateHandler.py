# -*- coding: utf-8 -*-
#
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
# Author: Gen Kawamura <gen.kawamura@cern.ch>, Date: 23/Jun/2014
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import logging

from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler
from hf.gridengine.envreader import GridEnv, CvmfsEnv
from hf.gridengine.gridcertificate import GridCertificate

        
class ProxyCertificateHandler(GridSubprocessBaseHandler):
    logger = logging.getLogger(__name__)

    """ Attributes for a grid certificate"""
    gridCertificate = GridCertificate()

    """ For delegation """
    __myproxy_server = None

    """ Actual Commands """
    __voms_proxy_generator = "voms-proxy-init"
    __grid_proxy_generator = "grid-proxy-init"

    def __init__(self):
        """ Attributes for proxy certificate"""
        self.__voms_enabled = self.gridEnv.get("voms.enabled")
        self.__vo = self.gridEnv.get("vo")

        self.__proxy_renew_enabled = self.gridEnv.get("proxy.renew.enabled")
        self.__proxy_lifetime_threshold_hours = self.gridEnv.get("proxy.lifetime.threshold.hours")
        self.__proxy_valid_hours = self.gridEnv.get("proxy.valid.hours")
        self.__proxy_voms_hours = self.gridEnv.get("proxy.voms.hours")
        
        self.cvmfsEnv.setEnabled('emi')


    def generateProxy(self):
        if not self.__proxy_renew_enabled: 
            self.logger.info("Proxy renewal function [proxy.renew.enabled] is not activated")
            return
            
        """ Checking certificate and proxy """
        """ Note: write try - catch here """
        if not self.checkCert():
            self.logger.error("CERT Error: Proxy certificate cannot be generated!")
            return
        
        if self.checkProxy():
            self.logger.info("Proxy certificate is still valid!")
            return

        """ execute grid command """
        subject = self.gridCertificate.getSubjectDN()
        self.logger.info("Generating Proxy Certificate for ["+subject+"] ...")

        """ run proxy certificate generator """        
        self.commandArgs = self.__proxyGenerator()
        self.execute()
        self.showGridProcess()


    def __proxyGenerator(self):
        if self.__voms_enabled:
            proxyGenerator = self.__voms_proxy_generator \
            + " --voms " + self.__vo \
            + " -cert $X509_USER_CERT -key $X509_USER_KEY" \
            + " --valid " + str(self.__proxy_valid_hours) + ":00" \
            + " --vomslife " + str(self.__proxy_voms_hours) + ":00" \
            + " -pwstdin" 
        else:
            proxyGenerator = self.__grid_proxy_generator \
            + " -cert $X509_USER_CERT -key $X509_USER_KEY" \
            + " -valid " + str(self.__proxy_valid_hours) + ":00" \
            + " -pwstdin" 
        return proxyGenerator


    def delegateProxy(self):
        if self.__myproxy_server is None: return


    def checkCert(self):
        if not self.gridCertificate.checkIfUserCertExists(): 
            self.logger.error("No UserCert! [" + self.gridCertificate.getUserCert() + "]")
            raise GridNoUserCertException(self.gridCertificate.getUserCert())
        if not self.gridCertificate.checkIfUserKeyExists():
            self.logger.error("No UserKey! [" + self.gridCertificate.getUserKey() + "]")
            raise GridNoUserKeyException(self.gridCertificate.getUserKey())
        if not self.gridCertificate.checkIfUserCertHasCorrectOwnership():
            self.logger.error("Ownership error! [" + self.gridCertificate.getUserCert() + "]")
            raise GridUserCertOwnershipException(self.gridCertificate.getUserCert())
        if not self.gridCertificate.checkIfUserKeyHasCorrectOwnership():
            self.logger.error("Ownership error! [" + self.gridCertificate.getUserKey() + "]")
            raise GridUserCertOwnershipException(self.gridCertificate.getUserKey())
        if not self.gridCertificate.checkIfValidUserKeyExists():
            self.logger.error("No Valid Userkey! [" + self.gridCertificate.getUserKey() + "]")
            raise GridNotValidUserKeyException(self.gridCertificate.getUserKey())
        if not self.gridCertificate.checkIfNoPassphraseUserKeyIs():
            self.logger.error("Passphrase for this UserKey is not proper or not empty! [" + self.gridCertificate.getUserKey() + "]")
            raise GridUserKeyPassphraseException(self.gridCertificate.getUserKey())

        return True
    

    def checkProxy(self):
        if not self.gridCertificate.checkIfGridProxyExists():
            self.logger.info("No X509 Proxy!")
            return False
        if not self.gridCertificate.checkIfUserProxyHasCorrectOwnership():
            self.logger.error("Ownership error! [" + self.gridCertificate.getUserProxy() + "]")
            raise GridUserCertOwnershipException(self.gridCertificate.getUserProxy())
        if not self.gridCertificate.checkIfGridProxyIsStillValid():
            self.logger.info("Proxy is not valid!")
            return False
        if not self.gridCertificate.checkIfGridProxyHasAcTimeleft(self.__proxy_lifetime_threshold_hours): 
            self.logger.info("No VOMS timeleft!")
            return False

        return True

# Exception classes used by this module.
class GridNoUserCertException(Exception):
    def __init__(self, userCert):
        self.userCert = userCert
    def __str__(self):
        return "User Certificate '%s' does not exist!" % (self.userCert)

# Exception classes used by this module.
class GridNoUserKeyException(Exception):
    def __init__(self, userKey):
        self.userKey = userKey
    def __str__(self):
        return "User Key '%s' does not exist!" % (self.userKey)


# Exception classes used by this module.
class GridNotValidUserKeyException(Exception):
    def __init__(self, userKey):
        self.userKey = userKey
    def __str__(self):
        return "User Key '%s' is not valid!" % (self.userKey)


# Exception classes used by this module.
class GridUserKeyPassphraseException(Exception):
    def __init__(self, userKey):
        self.userKey = userKey
    def __str__(self):
        return "Passphrase in UserKey '%s' is not valid or not empty!" % (self.userKey)


# Exception classes used by this module.
class GridUserCertOwnershipException(Exception):
    def __init__(self, userCert):
        self.userCert = userCert
    def __str__(self):
        return "Ownership in Grid Certificate '%s' is not valid!" % (self.userCert)



def main():
    
    print "Starting [ProxyCertificateHandler]"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)

    print "enabled? = " + str(ProxyCertificateHandler().cvmfsEnv.enabled)
    ProxyCertificateHandler().generateProxy()


if __name__ == '__main__':
    main()

