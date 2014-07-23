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
from urlparse import urlparse
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler


class GangaRobotJobHandler(GridSubprocessBaseHandler):
    logger = logging.getLogger(__name__)

    jobTemplateFile = ""
    commandArgs="ganga "

    def __init__(self):
        self.cvmfsEnv.setEnabled("ganga")
        
        """ set parameters """



        """ logging """ 
        self.logger.debug(self.commandArgs)


    def jobSubmit():
        """ logging """ 
        self.logger.debug(self.jobTemplateFile)


    def jobMonitor():
        """ logging """ 
        self.logger.debug(self.jobTemplateFile)


def main():
    print "GangaRobotJobHandler"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
    
    ganga = GangaRobotJobHandler()
    
    ganga.jobSubmit()
    monitor = ganga.jobMonitor()


if __name__ == '__main__':
    main()
    

    
