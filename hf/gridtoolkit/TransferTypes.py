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


import os, glob, hf
import logging, subprocess
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler, GridPopen
from hf.gridtoolkit.Transfers import Transfers
import re

class TransferTypes(GridSubprocessBaseHandler, Transfers):
   
    logger = logging.getLogger(__name__)        

    __fileName = None
    
    def setGenaratedFileName (self, fileName):
        self.__fileName = fileName
    
    def getGenaratedFileName(self):
        return self.__fileName
    
    def copying(self):
        print "Copying..."                   
        if (self.getTransferType() == "Local"):
           print "LocalTransfers"           
        elif (self.getTransferType() == "ThirdParty"):
            print "ThirdParty"   
        else:
           print "No transfer Type set"        
        
        return self.getTransferType()        


class LocalToRemote(TransferTypes):
    logger = logging.getLogger(__name__)
    
    def mkDir (self, host, dstPath):
        self.commandArgs = 'uberftp ' + host + ' "mkdir ' +  dstPath + ' " '         
        self.logger.debug("Create file in destination path = " +  str(dstPath))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
            (stdout,stderr) = self.gridProcess.communicate()
            return stdout, stderr 
        except Exception as e: 
            print "An exception has occurred: "
            print e
            
                    
    def copying(self, host, path):
        copyStatus, stderr, srcPath, fileName = self.createFileInSrcPath(host, path)
        self.setGenaratedFileName(fileName)        
        print self.getGenaratedFileName()
    
   
    def createFileInSrcPath (self, host , path):
        random_file_gen_obj = GenerateFile()
        fileName, localPath = random_file_gen_obj.randomFileGenerator("random.txt")
        stdout, stderr = self.copyFormLocalToRemote(host, localPath, path)
        copyStatus = 0               
        if stderr:
           copyStatus = 1
        
        return copyStatus, stderr, path, fileName 
    
    def copyFormLocalToRemote(self, srcHost, localPath, srcPath):
        self.commandArgs = 'uberftp ' + srcHost + '  "put ' + localPath + "  " + srcPath + ' "'
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        try: 
           (data,error) = self.gridProcess.communicate()
           return data, error
        except Exception as e: 
           print "An exception has occurred: "
           print e            
           
              


class ThirdParty(TransferTypes):
   
    logger = logging.getLogger(__name__)
    
    def copying(self, srcHost, srcPath, dstHost, dstPath, protocol):
        self.commandArgs = 'uberftp ' + protocol + "://" + srcHost + srcPath + "  " + protocol + "://" + dstHost + dstPath
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
   

class GenerateFile(object):
   
    logger = logging.getLogger(__name__)
    
    def randomFileGenerator(self, filename):
       from hashlib import md5
       from time import localtime
       fileName = "%s_%s" % (md5(str(localtime())).hexdigest(), filename)
       file = open(fileName, 'w+')
       file.write(str(localtime()))
       return fileName, os.path.abspath(fileName)   
   


class CommandLine(GridSubprocessBaseHandler):
   
    logger = logging.getLogger(__name__)         
    #Create directory        
    def mkDir (self, host, dstPath):
        self.commandArgs = 'uberftp ' + host + ' "mkdir ' +  dstPath + ' " '         
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
        self.__dstPath = dstPath
        self.commandArgs = 'uberftp ' + host + ' "cd ' +  self.__dstPath + ' " ' + ' " rm *.txt " ' 
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
        self.__dstPath = dstPath
        self.commandArgs = 'uberftp ' + host + ' "rmdir ' +  self.__dstPath + ' " '
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
        filelist = glob.glob("*.txt")
        for txt_file in filelist:         
            os.remove(txt_file)
            
            
 
        
def main():
    print "TransferTypes"
    Object = TransferTypes()
    local_obj = LocalToRemote()
    third_party = ThirdParty()
    
    Object.setTransferType("Local")
    if Object.copying() == "Local":
        local_obj.copying()
    elif Object.copying() == "ThirdParty":
        third_party.copying()
   
    
    Object.setTransferType("ThirdParty")
    if Object.copying() == "Local":
        local_obj.copying()
    elif Object.copying() == "ThirdParty":
        third_party.setGenaratedFileName(local_obj.getGenaratedFileName())
        third_party.copying()
   
 
if __name__ == '__main__':
    main()
