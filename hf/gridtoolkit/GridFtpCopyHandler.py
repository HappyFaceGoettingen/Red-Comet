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

class GridFtpCopyHandler(GridSubprocessBaseHandler):
   
    logger = logging.getLogger(__name__)

    __srcHost = None
    __srcPort = None
    __srcUrl = None
    __srcPath = None
    
    __dstHost = None
    __dstPort = None
    __dstUrl = None
    __dstPath = None
    
    commandArgs=None
    
   
    def __init__(self):
        self.cvmfsEnv.setEnabled("emi")
        self.gridEnv.set('x509.user.key', None)
    
    
    def setHostsAndPorts(self, srcHost=None, srcPort=None, dstHost=None, dstPort=None ):
        
        if srcHost is not None: self.__srcHost = srcHost
        if srcPort is not None: self.__srcPort = srcPort
        if dstHost is not None: self.__dstHost = dstHost
        if dstPort is not None: self.__dstPort = dstPort
          
        """ logging """ 
        self.logger.debug("Source host = " + str(self.__srcHost))
        self.logger.debug("Source port = " +  str(self.__srcPort))              
        self.logger.debug("Destination host = " + str(self.__dstHost))
        self.logger.debug("Destination port = " +  str(self.__dstPort))
        
    def executeAndShowResult(self):
        self.execute()
        std_out,std_err = self.showGridProcess(show_stdout = True, show_stderr = True)    
        return std_out, std_err
               
    def FtpConnect (self):
        self.commandArgs = "uberftp " + self.__dstHost
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.executeAndShowResult()
        
    def FtpShowFiles (self, dstPath):
        self.__dstPath = dstPath                
        self.commandArgs = "uberftp -ls  gsiftp://" + self.__dstHost + self.__dstPath
        self.logger.debug("Show files in destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        std_out, std_err = self.executeAndShowResult()        
        return std_out, std_err 
       
        
    def FtpCopy(self, srcPath, dstPath, protocol):
        self.__srcPath = srcPath
        self.__dstPath = dstPath
        check_if_file_exists = self.CheckFile(srcPath, dstPath) 
        #print   check_if_file_exists
        if check_if_file_exists == 1:
           self.commandArgs = 'uberftp ' + protocol + "://" + self.__srcHost + self.__srcPath +"  " + protocol + "://" + self.__dstHost + self.__dstPath
           self.logger.debug("File to copy from source path = " + str(self.__srcPath))
           self.logger.debug("Destination path = " +  str(self.__dstPath))
           self.logger.debug("Executed command = " +  str(self.commandArgs))
           self.executeAndShowResult()
        else:
           self.logger.debug("File already exists") 
        
        
    def CheckFile (self, srcPath, dstPath):
        self.__srcPath = srcPath
        self.__dstPath = dstPath
             
        stdout, stderr = self.FtpShowFiles(self.__dstPath)       
        file = os.path.basename(self.__srcPath)
        print file  
        if stdout:
            for item in stdout:
                if str(item).find(file): 
                   print item
                   return 0
                else:
                   return 1               
        else:
            print "Folder is empty"
            return 1
        
        
    def FtpMkDir (self, dstPath):
        self.__dstPath = dstPath
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "mkdir ' +  self.__dstPath + ' " '         
        self.logger.debug("Create file in destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.executeAndShowResult()
        
    def FtpRmDir (self, dstPath):
        self.__dstPath = dstPath
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "rmdir ' +  self.__dstPath + ' " '
        self.logger.debug("Remove directory from destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.executeAndShowResult()
        
    def FtpRm (self, dstPath):
        self.__dstPath = dstPath
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "rm ' +  self.__dstPath + ' " '
        self.logger.debug("Remove file from destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.executeAndShowResult()

    def FtpShowFileContent (self, dstPath):
        self.__dstPath = dstPath
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "cat ' +  self.__dstPath + ' " '
        self.logger.debug("Show file content = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.executeAndShowResult()
        
    def FtpSizeOfFile (self, dstPath):
        self.__dstPath = dstPath
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "size ' +  self.__dstPath + ' " '
        self.logger.debug("Show the size of the file in destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.executeAndShowResult() 
        
        
def main():
    print "GridFtpCopyHandler"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
   
    Object = GridFtpCopyHandler()
    Object.setHostsAndPorts("se-goegrid.gwdg.de", "", "se-goegrid.gwdg.de", "")
 
 #   Object.FtpConnect()
      
    Object.FtpCopy("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/bbb.txt ", "/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/test", "gsiftp")
    
#    Object.FtpCopy("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/test/bbb.txt", "/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/", "gsiftp")

     
#    Object.FtpMkDir("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/eclipse_garfinqyul")
   
#    Object.FtpRmDir("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/eclipse_garfinqyul")

#    Object.FtpRm("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/bbb.txt")

#    Object.FtpShowFileContent("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/bbb.txt") 

#    Object.FtpSizeOfFile("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/bbb.txt")

#    Object.FtpShowFiles("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/")

    Object.FtpShowFiles("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi")

if __name__ == '__main__':
    main()
