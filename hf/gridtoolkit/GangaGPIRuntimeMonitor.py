# -*- coding: utf-8 -*-
#
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
# Author: Gen Kawamura <gen.kawamura@cern.ch>, Date: 01/Aug/2014
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

import logging, os, sys
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler
from hf.gridengine.envreader import GridEnvReader, CvmfsEnvReader


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Perform setup needed for using Ganga Public Interface (GPI)
# This is a Copy/Paste logic which must stay in THIS file

def standardSetup():
    """Function to perform standard setup for Ganga.
    """   

    # insert the path to Ganga itself
    exeDir = os.path.abspath(os.path.normpath(os.path.dirname(sys.argv[0])))
    gangaDir = os.path.join(os.path.dirname(exeDir), 'python' )
    sys.path.insert(0, gangaDir)

    import Ganga.PACKAGE
    Ganga.PACKAGE.standardSetup()


class GangaGPIRuntimeMonitor(GridSubprocessBaseHandler):

    """ ganga python configuration """
    _pythonPath = "/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/Ganga/current/install/6.0.34/python"


    """ Booting Runtime Env """
    def __init__(self):
        os.environ['X509_CERT_DIR'] = GridEnvReader().get("x509.cert.dir")
        os.environ['X509_USER_KEY'] = GridEnvReader().get("x509.user.key")
        os.environ['X509_USER_CERT'] = GridEnvReader().get("x509.user.cert")
        os.environ['X509_USER_PROXY'] = GridEnvReader().get("x509.user.proxy")
        sys.path.append(self.pythonPath) 


    def getJobTree(self):
        standardSetup()

        from Ganga.Core import GangaException

        try:
            # Process options given at command line and in configuration file(s)
            # Perform environment setup and bootstrap
            import Ganga.Runtime
            Ganga.Runtime._prog = Ganga.Runtime.GangaProgram()
            Ganga.Runtime._prog.parseOptions()
            Ganga.Runtime._prog.configure()
            Ganga.Runtime._prog.initEnvironment()
            Ganga.Runtime._prog.bootstrap()
            # Import GPI and run Ganga
            from Ganga.GPI import *
            Ganga.Runtime._prog.run()
        except GangaException,x:
            self.logger(x)

    def __bootstrap(self):
        print "test"

    
class GangaJobs():
    logger = logging.getLogger(__name__)


def main():
    print "GangaGPIRuntimeMonitor"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
    
    ganga = GangaGPIRuntimeMonitor()

    gangajobs = ganga.getJobTree()


if __name__ == '__main__':
    main()
