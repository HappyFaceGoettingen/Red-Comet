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
import subprocess
from urlparse import urlparse
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler


class OpensslClientConnector(GridSubprocessBaseHandler):
    logger = logging.getLogger(__name__)

    __host = "www.google.com"
    __port = "443"

    commandArgs="openssl s_client -ssl3 -cert $X509_USER_CERT -key $X509_USER_KEY -CApath $X509_CERT_DIR -connect "

    def __init__(self, host, port, query):
        self.cvmfsEnv.setEnabled("emi")
        
        """ set parameters """
        self.__host = host
        self.__port = port
        self.commandArgs = self.commandArgs + self.__host + ":" + str(self.__port)
        self.standardInput = query

        """ logging """ 
        self.logger.debug(self.__host)
        self.logger.debug(self.__port)
        self.logger.debug(self.commandArgs)
        self.logger.debug(self.standardInput)



def main():
    print "OpensslClientConnector"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
    
    url = 'https://ngi-de-nagios.gridka.de:443/nagios/cgi-bin/status.cgi?se-goegrid.gwdg.de'
    URL = urlparse(url)
    
    #openssl_client = OpensslClientConnector(URL.hostname, URL.port, "GET " + URL.path + "?" + URL.query + "\n")
    #openssl_client = OpensslClientConnector(URL.hostname, URL.port, "GET /nagios/\n")
    openssl_client = OpensslClientConnector(URL.hostname, URL.port, "GET\n")
    
    openssl_client.execute(stdin=subprocess.PIPE)
    openssl_client.showGridProcess()


if __name__ == '__main__':
    main()
    

    