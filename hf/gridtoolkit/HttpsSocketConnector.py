# -*- coding: utf-8 -*-
#
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
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


import httplib, ssl, socket, logging
from urlparse import urlparse
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler
from hf.gridengine.envreader import GridEnvReader, GridEnv, CvmfsEnvReader, CvmfsEnv
from hf.gridengine.gridcertificate import GridCertificate

class HttpsSocketConnector(GridSubprocessBaseHandler):
    logger = logging.getLogger(__name__)

    __host = "www.google.com"
    __port = "443"

    __http_method = "GET"
    __ssl_version = ssl.PROTOCOL_SSLv3

    
    """ Attributes for a grid certificate"""
    gridCertificate = GridCertificate()


    def __init__(self, host, port, query):
        self.cvmfsEnv.setDisabled("emi")
        
        """ set parameters """
        self.__host = host
        self.__port = port
        self.standardInput = query

        """ logging """ 
        self.logger.debug(self.__host)
        self.logger.debug(self.__port)

    def execute(self):
        self.logger.debug("execute: host = " + self.__host)
        conn = httplib.HTTPSConnection(self.__host)
        sock = socket.create_connection((conn.host, conn.port), conn.timeout)
        conn.sock = ssl.wrap_socket(sock, self.gridCertificate.getUserKey(), self.gridCertificate.getUserCert(), ssl_version=self.__ssl_version)
        conn.request(self.__http_method, self.standardInput)
        self.gridProc = conn


    def showGridProcess(self):
        print self.gridProc.getresponse().read()
            

def main():
    print "HTTPSConnection"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)

    url = 'https://ngi-de-nagios.gridka.de/nagios/cgi-bin/status.cgi?se-goegrid.gwdg.de'
    URL = urlparse(url)
    https_socket_connector = HttpsSocketConnector(URL.hostname, URL.port, URL.path + "?" + URL.query)
    #https_socket_connector = HttpsSocketConnector(URL.hostname, URL.port, "/nagios/")
    https_socket_connector.execute()
    https_socket_connector.showGridProcess()


if __name__ == '__main__':
    main()
