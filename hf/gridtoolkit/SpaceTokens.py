## -*- coding: utf-8 -*-
#
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
# Author: Haykuhi Musheghyan <haykuhi.musheghyan@cern.ch>, Date: 07/July/2014
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
## -*- coding: utf-8 -*-
#
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
# Author: Haykuhi Musheghyan <haykuhi.musheghyan@cern.ch>,  Date: 07/July/2014
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


import os, hf
import logging, subprocess
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler, GridPopen
import re
from hf.gridtoolkit.Transfers import Transfers

class SpaceTokens(object):
    
   
    logger = logging.getLogger(__name__)
    __src_disk = None
    __lg_disk = None
    __prod_disk = None
    __data_disk = None

    
    def setScratchDiskPath (self, path):
        self.__src_disk = path
    
    def getScratchDiskPath(self):
        return self.__src_disk
    
    def setLocalGroupDiskPath (self, path):
        self.__lg_disk = path
    
    def getLocalGroupDiskPath(self):
        return self.__lg_disk
    
    def setProdDiskPath (self, path):
        self.__prod_disk = path
    
    def getProdDiskPath(self):
        return self.__prod_disk
    
    def setDataDiskPath (self, path):
        self.__data_disk = path
    
    def getDataDiskPath(self):
        return self.__data_disk
    
  
        
def main():
    print "SpaceTokens"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
   
    Object = SpaceTokens()    
      
if __name__ == '__main__':
    main()
