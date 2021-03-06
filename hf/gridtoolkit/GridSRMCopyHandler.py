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
from re import compile as compile_regex

class GridSRMCopyHandler(GridSubprocessBaseHandler):
   
    logger = logging.getLogger(__name__)

    __srcHost = None
    __srcPort = None
    __srcUrl = None
    __srcPath = None
    
    __dstHost = None
    __dstPort = None
    __dstUrl = None
    __dstPath = None
    __fileName = None
    
    __protocol = None
    commandArgs=None
    
    _MULTIPLE_PATHS = compile_regex(r"/{2,}")
   
    def __init__(self):
        self.cvmfsEnv.setEnabled("emi")
        self.gridEnv.set('x509.user.key', None)
    
    
    def setHostsAndPorts(self, srcHost=None, srcPort=None, dstHost=None, dstPort=None, protocol=None):
        
        if srcHost is not None: self.__srcHost = srcHost
        if srcPort is not None: self.__srcPort = srcPort
        if dstHost is not None: self.__dstHost = dstHost
        if dstPort is not None: self.__dstPort = dstPort
        if protocol is not None: self.__protocol = protocol
          
        """ logging """ 
        self.logger.debug("Source host = " + str(self.__srcHost))
        self.logger.debug("Source port = " +  str(self.__srcPort))              
        self.logger.debug("Destination host = " + str(self.__dstHost))
        self.logger.debug("Destination port = " +  str(self.__dstPort))
        self.logger.debug("Use protocol = " +  str(self.__protocol))
        
    def executeAndShowResult(self):
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True)            
               
    def ping (self):
        self.commandArgs = "srmping " + self.__protocol + "://" + self.__dstHost + ":" + self.__dstPort
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.executeAndShowResult()
        
    def showFiles (self, dstPath):
        self.__dstPath = dstPath                
        self.commandArgs = "srmls " + self.__protocol + "://" + self.__dstHost + ":" + self.__dstPort + self.__dstPath
        self.logger.debug("Show files in destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
            (data,error) = self.gridProcess.communicate()
            return data, error
        except Exception as e: 
            print "An exception has occurred: "
            print e

        
    def createFileInSrcPath (self, srcHost , srcPath):
        fileName, localPath = self.randomFileGenerator("random.txt")
        stdout, stderr = self.copyFormLocalToRemote(srcHost, localPath, srcPath)
        copyStatus = 0               
        if stderr:
           copyStatus = 1
        
        return copyStatus, stderr, srcPath, fileName 

    def copyFormLocalToRemote(self, srcHost, localPath, srcPath):     
        self.commandArgs = 'srmcp file:///' + localPath + '  ' + self.__protocol + "://" + self.__srcHost + ":" + self.__dstPort + srcPath 
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
           (data,error) = self.gridProcess.communicate()
           print data
           print error
           return data, error
        except Exception as e: 
           print "An exception has occurred: "
           print e
        
    
    def copyFile(self, srcPath, dstPath):
        self.__srcPath = srcPath
        self.__dstPath = dstPath              
        self.commandArgs = 'srmcp ' + self.__protocol + "://" + self.__srcHost + ":" + self.__srcPort + self.__srcPath + "  " +  self.__protocol + "://" + self.__dstHost + ":" + self.__dstPort + self.__dstPath 
        self.logger.debug("File to copy from source path = " + str(self.__srcPath))
        self.logger.debug("Destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
           (data,error) = self.gridProcess.communicate()
           print data
           print error
           return data, error
        except Exception as e: 
           print "An exception has occurred: "
           print e
           
   
    def copyFileAndCheckExistance(self, srcPath, fileName, dstPath):
        self.__srcPath = srcPath
        self.__dstPath = dstPath
        data, error = self.copyFile(srcPath+fileName, dstPath+ "srm_" +fileName)
        if not error:
           check_if_file_exists = self.checkFile(fileName, self.__dstPath)
           if check_if_file_exists == 0:
               return "OK"                    
           else:
               return "Failed"                      
        else:
           return error

  
    def randomFileGenerator(self, filename):
       from hashlib import md5
       from time import localtime
       fileName = "%s_%s" % (md5(str(localtime())).hexdigest(), filename)
       file = open(fileName, 'w+')
       file.write(str(localtime()))
       return fileName, os.path.abspath(fileName) 
        
       
    def checkFile (self, fileName, dstPath):
        self.__dstPath = dstPath 
        stdout, stderr = self.showFiles(self.__dstPath)
        print stdout, stderr
        if stdout:
            for item in stdout:
                if str(item).find(fileName): 
                   return 0 # if file exists
                else:
                   return 1 # file doesnt exists              
        else:
            return 1        
        
    def mkDir (self, dstPath):
        self.__dstPath = dstPath
        self.commandArgs = 'srmmkdir ' + self.__protocol + "://" + self.__dstHost + ":" + self.__dstPort + dstPath       
        self.logger.debug("Create file in destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
            (stdout,stderr) = self.gridProcess.communicate()
            return stdout, stderr 
        except Exception as e: 
            print "An exception has occurred: "
            print e
        
    def rmDir (self, dstPath):
        self.__dstPath = dstPath
        self.commandArgs = 'srmrmdir ' + self.__protocol + "://" + self.__dstHost + ":" + self.__dstPort + dstPath
        self.logger.debug("Remove directory from destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
            (data,error) = self.gridProcess.communicate()
            if data:
                print "Deleted \n"
            if error:
                print error 
        except Exception as e: 
            print "An exception has occurred: "
            print e
            
        
    def rmFile (self, fileName, dstPath):
        self.__fileName = fileName
        self.__dstPath = dstPath
        self.commandArgs = 'srmrm ' + self.__protocol + "://" + self.__dstHost + ":" + self.__dstPort + dstPath + self.__fileName
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
            (data,error) = self.gridProcess.communicate()
            if data:
                print "Deleted \n"
            if error:
                print error
        except Exception as e: 
            print "An exception has occurred: "
            print e

        
        
def main():
    print "GridSRMCopyHandler"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
   
    Object = GridSRMCopyHandler()
    Object.setHostsAndPorts("se-goegrid.gwdg.de", "", "se-goegrid.gwdg.de", "8443", "srm")
    Object.randomFileGenerator("random.txt")
  
if __name__ == '__main__':
    main()
