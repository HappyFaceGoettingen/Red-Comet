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

import os,logging,sys
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler
from hf.gridengine.gridcertificate import GridCertificate
from hf.gridengine.envreader import GridEnvReader


class GangaRobotJobHandler(GridSubprocessBaseHandler):
    logger = logging.getLogger(__name__)

    """ Attributes for a grid certificate"""
    gridCertificate = GridCertificate()

    """ default job attributes """
    __job_template_file = "ganga_job_template.py"

    __ganga_job_executable = "/bin/echo"
    __ganga_input_sandbox = ""
    __ganga_number_of_subjobs = 1
    __ganga_grid_backend = "CREAM"
    __ganga_ce_endpoint = "cream-ge-2-kit.gridka.de:8443/cream-sge-sl6"
    __ganga_lcg_site = "FZK"


    def __init__(self, verbose = False):
        self.cvmfsEnv.setEnabled("emi")
        self.cvmfsEnv.setEnabled("dq2.client")
        self.cvmfsEnv.setEnabled("ganga")
        if verbose: self.cvmfsEnv.verbose = True


    def setGangaPythonPath(self, path):
        self._gangaPythonPath = path


    def setJob(self, job_template_file, job_executable, input_sandbox, number_of_subjobs, grid_backend, ce_endpoint, lcg_site):
        self.__job_template_file = job_template_file
        self.__ganga_job_executable = job_executable
        self.__ganga_input_sandbox = input_sandbox
        self.__ganga_number_of_subjobs = number_of_subjobs
        self.__ganga_grid_backend = grid_backend
        self.__ganga_ce_endpoint = ce_endpoint
        self.__ganga_lcg_site = lcg_site


    def __generateEnvVariables(self):
        env = "export LANG=en_US;"
        env += "export GANGA_JOB_EXECUTABLE=" + self.__ganga_job_executable + ";"
        env += "export GANGA_INPUT_SANDBOX=" + self.__ganga_input_sandbox + ";"
        env += "export GANGA_NUMBER_OF_SUBJOBS=" + str(self.__ganga_number_of_subjobs) + ";"
        env += "export GANGA_GRID_BACKEND=" + self.__ganga_grid_backend + ";"
        env += "export GANGA_CE_ENDPOINT=" + self.__ganga_ce_endpoint + ";"
        env += "export GANGA_LCG_SITE=" + self.__ganga_lcg_site + ";"
        return env


    def __checkIfGangaConfigExists(self):
        return os.path.isfile(os.environ['HOME'] + "/.gangarc")
 

    def __generateGangaConfig(self):
        """ preparing gangarc if .gangarc does not exist """
        if self.__checkIfGangaConfigExists(): return True

        self.commandArgs = "ganga -g"
        self.__runGanga()


    def __runGanga(self):
        """ exec and show stdout & stderr """ 
        self.execute()
        self.showGridProcess(show_stderr=True)
        

    def jobSubmit(self):
        """ prepare gangarc """
        self.__generateGangaConfig()
        
        """ generate commandArgs """
        self.commandArgs = self.__generateEnvVariables()
        self.commandArgs += "ganga --daemon " + self.__job_template_file

        """ submit job """
        self.__runGanga()


    def jobMonitor(self):
        print " Ganga Monitoring: "



        
    def jobRemove(self, job_number):
        print "Removing " + job_number + " ..."




def main():
    print "GangaRobotJobHandler"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
    
    ganga = GangaRobotJobHandler()
    #ganga.jobSubmit()

    gangajobs = ganga.jobMonitor()


if __name__ == '__main__':
    main()
    

    
