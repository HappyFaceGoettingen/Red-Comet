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
    __spaceToken = None
    __spaceTokenPath = None    
    
    def setSpaceTokenPath (self, path):
        self.__spaceTokenPath = path
    
    def getSpaceTokenPath(self):
        return self.__spaceTokenPath
    
    def useSpaceToken(self, token):
        self.__spaceToken = token
        return  self.__spaceToken 


class ScratchDisk(SpaceTokens):
    __scratchDisk = None    
  
    def setScratchDisk (self, name):
        self.__scratchDisk = name
    
    def getScratchDisk(self):
        return self.__scratchDisk
    
    

class LocalGroupDisk(SpaceTokens):
    __localgroupDisk = None
       
    def setLocalGroupDisk (self, name):
        self.__localgroupDisk = name
    
    def getLocalGroupDisk(self):
        return self.__localgroupDisk



class ProdDisk(SpaceTokens):  
    __prodDisk = None
        
    def setProdDisk (self, name):
        self.__prodDisk = name
    
    def getProdDisk(self):
        return self.__prodDisk
    
class DataDisk(SpaceTokens):
    
    __dataDisk = None
            
    def setDataDisk (self, name):
        self.__dataDisk = name
    
    def getDataDisk(self):
        return self.__dataDisk
     
        
    
        
def main():
    print "SpaceTokens"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
   
    Object = SpaceTokens()
    
   
   
 
if __name__ == '__main__':
    main()
