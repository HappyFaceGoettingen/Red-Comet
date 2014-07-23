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

class SrmPingListner(GridSubprocessBaseHandler):
    logger = logging.getLogger(__name__)

    __srmhost = "srm://se-goegrid.gwdg.de"
    __srmport = 8443

    commandArgs="srmping "

    def __init__(self, srmhost, srmport=None):
        self.cvmfsEnv.setEnabled("emi")
        
        """ set parameters """
        self.__srmhost = srmhost
        if srmport is not None: self.__srmport = srmport
        self.commandArgs = self.commandArgs + self.__srmhost

        """ logging """ 
        self.logger.debug(self.__srmhost)
        self.logger.debug(self.__srmport)
        self.logger.debug(self.commandArgs)



def main():
    print "SrmPingListner"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
    
    hosts=["srm://se-goegrid.gwdg.de", 
           "srm://atlassrm-fzk.gridka.de:8443", 
           "srm://mgse1.physik.uni-mainz.de:8444",
           "srm://dcache-se-atlas.desy.de:8443"]
    
    for host in hosts:
        srmping = SrmPingListner(host)
        srmping.execute()
        print "srm host = " + host
        srmping.showGridProcess()

if __name__ == '__main__':
    main()
