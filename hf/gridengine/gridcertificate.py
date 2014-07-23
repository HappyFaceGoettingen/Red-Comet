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

import os.path,logging
import subprocess
from subprocess import Popen, call

from hf.gridengine.envreader import GridEnvReader
from hf.gridengine.gridsubprocess import GridPopen, grid_call


class GridCertificate:
    logger = logging.getLogger(__name__)

    __voms_enabled = GridEnvReader().get("voms.enabled")
    __userKey = GridEnvReader().get("x509.user.key")
    __userCert = GridEnvReader().get("x509.user.cert")
    __userProxy = GridEnvReader().get("x509.user.proxy")
    
    def getUserKey(self):
        return self.__userKey

    def getUserCert(self):
        return self.__userCert

    def getUserProxy(self):
        return self.__userProxy

    """ Check existance of cert, key & proxy """
    def checkIfUserCertExists(self):
        return os.path.isfile(self.__userCert)

    def checkIfUserKeyExists(self):
        return os.path.isfile(self.__userKey)

    def checkIfGridProxyExists(self):
        return os.path.isfile(self.__userProxy)

    """ Basic Credential Checker for Passphrase, Key & Proxy """
    def checkIfNoPassphraseUserKey(self):
        check_passphrase="openssl rsa -passin pass: -in " + self.__userKey + " -noout"
        retcode = call(check_passphrase, shell=True)
        if retcode == 0: return True
        return False

    def checkIfValidUserKey(self):
        is_valid_key = "openssl verify -CApath $X509_CERT_DIR -purpose sslclient $X509_USER_CERT"
        retcode = grid_call(is_valid_key, shell=True, stdout=subprocess.PIPE)
        if retcode == 0: return True
        return False
        
    def checkIfGridProxyIsStillValid(self):
        check_proxy ="voms-proxy-info -e"
        retcode = grid_call(check_proxy, shell=True, stdout=subprocess.PIPE)
        self.logger.debug("result of voms-proxy-info -e = [" + str(retcode) + "]")
        if retcode == 0: return True
        return False

    def checkIfGridProxyAcTimeleft(self, proxy_lifetime_threshold_hours=0):
        if not self.__voms_enabled: return True

        check_actimeleft ="voms-proxy-info -actimeleft"
        p = GridPopen(check_actimeleft, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        actimeleft = p.communicate()[0].rstrip()
        proxy_lifetime_threshold_sec = int(proxy_lifetime_threshold_hours) * 3600 
        self.logger.debug("proxy_lifetime_thrashold_hours = ["+ str(proxy_lifetime_threshold_hours) +"]")
        self.logger.debug("proxy_lifetime_thrashold_sec = ["+ str(proxy_lifetime_threshold_sec) +"]")
        self.logger.debug("actimeleft = [" + actimeleft + "]")
        if p.returncode == 0:
            if int(actimeleft) > proxy_lifetime_threshold_sec: return True
        return False

    def getSubjectDN(self):
        check_subject = "openssl x509 -in " + self.__userCert + " -noout -subject"
        return Popen(check_subject, shell=True, stdout=subprocess.PIPE).communicate()[0].rstrip()


def main():
    
    print "Starting [gridcertificate]"
    logging.basicConfig(level=logging.INFO)
    logging.root.setLevel(logging.DEBUG)

    cert = GridCertificate()
    print cert.getUserCert()
    print "["+cert.getSubjectDN()+"]"
    print "No passphraase = " + str(cert.checkIfNoPassphraseUserKey())
    print "Is a valid Key?: " + str(cert.checkIfValidUserKey())
    print "Is a valid Proxy?: " + str(cert.checkIfGridProxyIsStillValid())
    print "Is AC timeleft?: " + str(cert.checkIfGridProxyAcTimeleft(1))

if __name__ == '__main__':
    main()
