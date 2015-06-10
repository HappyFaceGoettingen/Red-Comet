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


import os, hf, glob
import logging, subprocess
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler, GridPopen
import re
from hf.gridtoolkit.Transfers import Transfers

class GridFtpCopyHandler(GridSubprocessBaseHandler, Transfers):
   
    logger = logging.getLogger(__name__)
    
    command= "uberftp "
    options = " -retry 2 -keepalive 10 "
    commandOptions = None
    protocol = "gsiftp"
    commandArgs = None
   
    def __init__(self):
        self.cvmfsEnv.setEnabled("emi")
        self.gridEnv.set('x509.user.key', None)
         
    
    def copying(self, srcHost, srcPath, dstHost, dstPath):
        self.commandArgs = self.command + self.options + self.protocol + "://" + srcHost + srcPath + "  " +  self.protocol + "://" + dstHost + dstPath
        self.logger.debug("File to copy from source path = " + str(srcPath))
        self.logger.debug("Destination path = " +  str(dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
           (data,error) = self.gridProcess.communicate()
           return data, error
        except Exception as e: 
           print "An exception has occurred: "
           print e    
          
    def showFiles (self, host, dstPath):  
        self.commandOptions = " -ls "             
        self.commandArgs = self.command + self.options + self.commandOptions +  self.protocol + "://" + host + dstPath
        self.logger.debug("Show files in destination path = " +  str(dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
            (data,error) = self.gridProcess.communicate()
            return data, error
        except Exception as e: 
            print "An exception has occurred: "
            print e 
            
    def mkDir (self, host, dstPath):
        self.commandOptions = "mkdir "
        self.commandArgs = self.command + self.options + host + ' "' + self.commandOptions + dstPath + ' " '         
        self.logger.debug("Create file in destination path = " +  str(dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
            (stdout,stderr) = self.gridProcess.communicate()
            return stdout, stderr 
        except Exception as e: 
            print "An exception has occurred: "
            print e
    
    #Remove files from destination path  
    def rmFile (self, host, dstPath):
        self.commandOptions = "cd "
        self.__dstPath = dstPath
        self.commandArgs = self.command + self.options + host + ' " ' + self.commandOptions + self.__dstPath + ' " ' + ' " rm *.txt " ' 
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
    
    #Remove directory from destination path
    def rmDir (self, host, dstPath):
        self.commandOptions = "rmdir "       
        self.__dstPath = dstPath
        self.commandArgs = self.command + self.options + host + ' " ' + self.commandOptions + self.__dstPath + ' " '
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
    
    #Remove files from local part
    def rmLocal(self):       
       filelist = glob.glob("tmp/*.txt")
       for txt_file in filelist: 
           os.remove(txt_file)
            
            
    def copyFromLocalToRemote(self, srcHost, localPath, srcPath):
        self.commandOptions = "put " 
        self.commandArgs = self.command + self.options + srcHost + '  " ' + self.commandOptions + localPath + "  " + srcPath + ' "'
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
           (data,error) = self.gridProcess.communicate()
           return data, error
        except Exception as e: 
           print "An exception has occurred: "
           print e
           
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
    
           
def main():
    print "GridcopyFileHandler"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
   
if __name__ == '__main__':
    main()
