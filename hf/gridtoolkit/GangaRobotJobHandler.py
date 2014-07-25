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

    __job_template_file = "ganga_job_template.py"

    __ganga_job_executable = "/bin/hostname"
    __ganga_input_sandbox = ""
    __ganga_number_of_subjobs = ""
    __ganga_grid_backend = "CREAM"
    __ganga_ce_endpoint = "cream-ge-2-kit.gridka.de:8443/cream-sge-sl6"
    __ganga_lcg_site = "FZK"


    def __init__(self):
        self.cvmfsEnv.setEnabled("emi")
        self.cvmfsEnv.setEnabled("dq2.client")
        self.cvmfsEnv.setEnabled("ganga")
        


    def setJob(self, job_template_file, job_executable, input_sandbox, number_of_subjobs, grid_backend, ce_endpoint, lcg_site):
        self.__job_template_file = job_template_file
        self.__ganga_job_executable = job_executable
        self.__ganga_input_sandbox = input_sandbox
        self.__ganga_number_of_subjobs = number_of_subjobs
        self.__ganga_grid_backend = grid_backend
        self.__ganga_ce_endpoint = ce_endpoint
        self.__ganga_lcg_site = lcg_site



    def __generateEnvVariables(self):
        env = ""
        env += "export GANGA_JOB_EXECUTABLE=" + self.__ganga_job_executable + ";"
        env += "export GANGA_INPUT_SANDBOX=" + self.__ganga_input_sandbox + ";"
        env += "export GANGA_NUMBER_OF_SUBJOBS=" + self.__ganga_number_of_subjobs + ";"
        env += "export GANGA_GRID_BACKEND=" + self.__ganga_grid_backend + ";"
        env += "export GANGA_CE_ENDPOINT=" + self.__ganga_ce_endpoint + ";"
        env += "export GANGA_LCG_SITE=" + self.__ganga_lcg_site + ";"
        return env


    def __checkIfGangaConfigExists(self):
        return True

    def __generateGangaConfig(self):
        """ prepare gangarc """
        if self.__checkIfGangaConfigExists(): return True

        self.commandArgs = "ganga -g"
        
        """ logging """ 
        self.logger.info(self.commandArgs)
        self.execute()
        self.showGridProcess()


    def jobSubmit():
        """ prepare gangarc """
        self.__generateGangaConfig()
        
        """ generate commandArgs """
        self.commandArgs = self.__generateEnvVariables()
        self.commandArgs += "ganga " + self.__job_template_file
        
        """ logging """ 
        self.logger.info(self.commandArgs)
        self.execute()
        self.showGridProcess()


    def daemonize(self):
        """ prepare gangarc """
        self.__generateGangaConfig()

        """ ganga daemon mode """
        self.commandArgs = "ganga --daemon"
        
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

    # ganga.daemonize()
    # monitor = ganga.jobMonitor()


if __name__ == '__main__':
    main()
    

    
