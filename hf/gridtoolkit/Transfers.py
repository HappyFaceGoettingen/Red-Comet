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

class Transfers(GridSubprocessBaseHandler):
   
    logger = logging.getLogger(__name__)
    
    __srcHost = None
    __srcPort = None
        
    __dstHost = None
    __dstPort = None
    
    __fileName = None
       
    __transferType = None
    __spaceToken = None
    
    __commandArgs=None
  
   
    def __init__(self):        
        self.cvmfsEnv.setEnabled("emi")
        self.gridEnv.set('x509.user.key', None)    
         
      
    def setSrcHost (self, srcHost):
        self.__srcHost = srcHost
    
    def getSrcHost(self):
        return self.__srcHost
    
    def setSrcPort (self, srcPort):
        self.__srcPort = srcPort
    
    def getSrcPort(self):
        return self.__srcPort
    
    def setDstPort (self, dstPort):
        self.__dstPort = dstPort
    
    def getDstPort(self):
        return self.__dstPort
    
    def setDstHost (self, dstHost):
        self.__dstHost = dstHost
    
    def getDstHost(self):
        return self.__dstHost
    
    
    def setTransferType (self, type):
        self.__transferType = type
    
    def getTransferType(self):
        return self.__transferType    
    
    def setSpaceToken (self, token):
        self.__spaceToken = type
    
    def getSpaceToken(self):
        return self.__spaceToken
    
    
    def copyFileAndCheckExistance(self, srcPath, fileName, dstPath):
        data, error = self.copyFile(srcPath+fileName, dstPath, "gsiftp" )
        if not error:
           check_if_file_exists = self.checkFile(fileName, dstPath)
           if check_if_file_exists == 0:
               return "OK"                    
           else:
               return "Failed"                      
        else:
           return error
       
       
    def checkFile (self, host, fileName, dstPath):
        stdout, stderr = self.showFiles(host, dstPath)
        print stdout, stderr
        if stdout:
            for item in stdout:
                if str(item).find(fileName): 
                   return 0 # if file exists
                else:
                   return 1 # file doesnt exists              
        else:
            return 1   
        
        
    def showFiles (self, host, dstPath):               
        self.commandArgs = "uberftp -ls  gsiftp://" + host + dstPath
        self.logger.debug("Show files in destination path = " +  str(dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
            (data,error) = self.gridProcess.communicate()
            return data, error
        except Exception as e: 
            print "An exception has occurred: "
            print e   
       
    
        
    
        
def main():
    print "Transfers"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
   
    Object = Transfers()
    Object.setTransferType("Local")
   
   
 
if __name__ == '__main__':
    main()
