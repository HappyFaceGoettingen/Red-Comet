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
from hf.gridtoolkit.GridFtpCopyHandler import GridFtpCopyHandler


class GenerateFile(object):
    
    __fileName = None 
    dir = "tmp/" 
    
    logger = logging.getLogger(__name__)
    
    def setGenaratedFileName (self, fileName):
        self.__fileName = fileName
    
    def getGenaratedFileName(self):
        return self.__fileName    
    
    def copying(self, host, path):
        copyStatus, stdout, stderr, fileName = self.createFileInSrcPath(host, path)
        self.setGenaratedFileName(fileName)        
        return copyStatus, stderr, path, fileName 
    
    def createFileInSrcPath (self, host , path):
        fileName, localPath = self.randomFileGenerator("random.txt")
        __grid_ftp = GridFtpCopyHandler()
        stdout, stderr = __grid_ftp.copyFromLocalToRemote(host, localPath, path)
        copyStatus = 0               
        if stderr:
           copyStatus = 1
        return copyStatus, stdout, stderr, fileName
    
    def randomFileGenerator(self, filename):
       from hashlib import md5
       from time import localtime      
       fileDir = self.dir + "%s_%s" % (md5(str(localtime())).hexdigest(), filename)
       file = open(fileDir, 'w+')
       file.write(str(localtime()))   
       return os.path.basename(fileDir), os.path.abspath(fileDir)   
   
      
def main():
    print "GenerateFile"    
    obj = GenerateFile()
    obj.randomFileGenerator("random.txt") 

    
if __name__ == '__main__':
    main()
