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
    timeout =  None
   
    
    def __init__(self, _timeout=None):
        self.cvmfsEnv.setEnabled("emi")
        self.gridEnv.set('x509.user.key', None) 
        if _timeout is not None: 
            self.timeout = _timeout                         
    
          
    def copying(self, srcHost, srcPort, srcPath, dstHost, dstPath):                 
        self.commandArgs = self.command + self.options + self.protocol + "://" + srcHost + srcPath + "  " +  self.protocol + "://" + dstHost + dstPath
        self.logger.debug("File to copy from source path = " + str(srcPath))
        self.logger.debug("Destination path = " +  str(dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        retCode, error_msg, output_msg = self.execute()         
        print "Command executed return code: "
        print retCode
        if retCode == 0:  
            try: 
                (data,error) = self.gridProcess.communicate()
                return retCode, error, data
            except Exception as e: 
                self.logger.debug("An exception has occurred:")
                print e
        else:
            
            return retCode, error_msg, output_msg                     
        
          
          
    def showFiles (self, dstHost, dstPort, dstPath):  
        self.commandOptions = " -ls "             
        self.commandArgs = self.command + self.options + self.commandOptions +  self.protocol + "://" + dstHost + dstPath
        self.logger.debug("Show files in destination path = " +  str(dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        retCode, error_msg, output_msg = self.execute()         
        print "Command executed return code: "
        print retCode
        if retCode == 0:  
            try: 
                (data,error) = self.gridProcess.communicate()
                return retCode, error, data
            except Exception as e: 
                self.logger.debug("An exception has occurred:")
                print e
        else:
            
            return retCode, error_msg, output_msg                     
        
        
            
    def mkDir (self, dstHost, dstPort, dstPath):
        self.commandOptions = "mkdir "
        self.commandArgs = self.command + self.options + dstHost + ' "' + self.commandOptions + dstPath + ' " '         
        self.logger.debug("Create file in destination path = " +  str(dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        retCode, error_msg, output_msg = self.execute()         
        print "Command executed return code: "
        print retCode
        if retCode == 0:  
            try: 
                (data,error) = self.gridProcess.communicate()
                return retCode, error, data
            except Exception as e: 
                self.logger.debug("An exception has occurred:")
                print e
        else:
            
            return retCode, error_msg, output_msg                     
        
       
    #Remove files from destination path  
    def rmFile (self, dstHost, dstPort, dstPath):
        self.commandOptions = "cd "
        self.__dstPath = dstPath
        self.commandArgs = self.command + self.options + dstHost + ' " ' + self.commandOptions + self.__dstPath + ' " ' + ' " rm *.txt " ' 
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        retCode, error_msg, output_msg = self.execute()         
        print "Command executed return code: "
        print retCode
        if retCode == 0:  
            try: 
                (data,error) = self.gridProcess.communicate()
                return retCode, error, data
            except Exception as e: 
                self.logger.debug("An exception has occurred:")
                print e
        else:
            
            return retCode, error_msg, output_msg                     
        
       
    
    #Remove directory from destination path
    def rmDir (self, dstHost, dstPort, dstPath):
        self.commandOptions = "rmdir "       
        self.__dstPath = dstPath
        self.commandArgs = self.command + self.options + dstHost + ' " ' + self.commandOptions + self.__dstPath + ' " '
        self.logger.debug("Remove directory from destination path = " +  str(self.__dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        retCode, error_msg, output_msg = self.execute()         
        print "Command executed return code: "
        print retCode
        if retCode == 0:  
            try: 
                (data,error) = self.gridProcess.communicate()
                return retCode, error, data
            except Exception as e: 
                self.logger.debug("An exception has occurred:")
                print e
        else:
            
            return retCode, error_msg, output_msg                     
        
       
    #Remove files from local part
    def rmLocal(self):       
       filelist = glob.glob("/var/tmp/*_random.txt")
       for txt_file in filelist: 
           os.remove(txt_file)
            
    
    def copyFromLocalToRemote(self, dstHost, dstPort, localPath, dstPath, fileName):
        self.commandOptions = "put " 
        self.commandArgs = self.command + self.options + dstHost + '  " ' + self.commandOptions + localPath + "  " + dstPath + ' "'
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        retCode, error_msg, output_msg = self.execute()         
        print "Command executed return code: "
        print retCode
        if retCode == 0:  
            try: 
                (data,error) = self.gridProcess.communicate()
                return retCode, error, data
            except Exception as e: 
                self.logger.debug("An exception has occurred:")
                print e
        else:
            
            return retCode, error_msg, output_msg                     
        
       
           
    def checkFile (self, dstHost, dstPort, fileName, dstPath):
        stdout, stderr = self.showFiles(dstHost, dstPort, dstPath)
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
