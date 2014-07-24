# -*- coding: utf-8 -*-
#
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
# Author: Gen Kawamura <gen.kawamura@cern.ch>, Date: 23/Jul/2014
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
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler


class GangaRobotJobHandler(GridSubprocessBaseHandler):
    logger = logging.getLogger(__name__)

    commandArgs=""

    __jobTemplateFile = ""
    __executable = ""
    __inputSandbox = ""
    __numberOfSubjobs = ""
    __backend = ""
    __endpoint = ""
    __site = ""

    def __init__(self):
        self.cvmfsEnv.setEnabled("emi")
        self.cvmfsEnv.setEnabled("dq2.client")
        self.cvmfsEnv.setEnabled("ganga")
        
        """ set parameters """



        """ logging """ 
        self.logger.debug(self.commandArgs)


    def setJob(self, jobTemplateFile, executable, inputSandbox, numberOfSubjobs, backend, endpoint, site):
        self.__jobTemplateFile = jobTemplateFile
        self.__executable = executable
        self.__inputSandbox = inputSandbox
        self.__numberOfSubjobs = numberOfSubjobs
        self.__backend = backend
        self.__endpoint = endpoint
        self.__site = site


    def __generateEnvVariables(self):
        env = ""
        env += "export EXECUTABLE=" + self.__executable + ";"
        env += "export INPUT_SANDBOX=" + self.__inputSandbox + ";"
        env += "export NUMBER_OF_SUBJOBS=" + self.__numberOfSubjobs + ";"
        env += "export BACKEND=" + self.__backend + ";"
        env += "export ENDPOINT=" + self.__endpoint + ";"
        env += "export SITE=" + self.__site + ";"
        return env


    def jobSubmit():
        """ generate commandArgs """
        self.commandArgs = self.__generateEnvVariables()
        self.commandArgs += "ganga " + self.__jobTemplateFile
        
        """ logging """ 
        self.logger.info(self.commandArgs)
        self.execute()
        self.showGridProcess()


    def jobMonitor():
        """ logging """ 
        self.logger.debug(self.jobTemplateFile)



def main():
    print "GangaRobotJobHandler"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
    
    ganga = GangaRobotJobHandler()
    
    ganga.jobSubmit()
    # monitor = ganga.jobMonitor()


if __name__ == '__main__':
    main()
    

    
