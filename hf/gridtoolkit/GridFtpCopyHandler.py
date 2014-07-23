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


import os, hf
import logging, subprocess
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler, GridPopen


class GridFtpCopyHandler(GridSubprocessBaseHandler):
   
    logger = logging.getLogger(__name__)

    __srcHost = None
    __srcPort = None
    __srcUrl = None
   
    __dstHost = None
    __dstPort = None
    __dstUrl = None
    
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
               
    def uberFtpConnect (self):
        self.commandArgs = "uberftp " + self.__dstHost
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True)
        
    def uberFtpShowFiles (self, dstUrl):
        self.__dstUrl = dstUrl                
        self.commandArgs = "uberftp -ls  gsiftp://" + self.__dstHost + self.__dstUrl
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True)
        

    def uberFtpCopyTo (self, srcUrl, dstUrl):
        self.__srcUrl = srcUrl
        self.__dstUrl = dstUrl                            
        self.commandArgs = 'uberftp ' + self.__dstHost + ' " put ' +  self.__srcUrl + " " + self.__dstUrl + '"'
        self.logger.debug("Source URL = " + str(self.__srcUrl))
        self.logger.debug("Destination URL = " +  str(self.__dstUrl))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True) 
        
               
    def uberFtpCopyFrom (self, srcUrl, dstUrl):
        self.__srcUrl = srcUrl
        self.__dstUrl = dstUrl                
        self.commandArgs = 'uberftp ' + self.__dstHost + ' " get ' +  self.__srcUrl + " " + self.__dstUrl + '"'
        self.logger.debug("Source URL = " + str(self.__srcUrl))
        self.logger.debug("Destination URL = " +  str(self.__dstUrl))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True)
        
    def uberFtpMkDir (self, dstUrl):
        self.__dstUrl = dstUrl
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "mkdir ' +  self.__dstUrl + ' " '         
        self.logger.debug("Destination URL = " +  str(self.__dstUrl))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True)
        
    def uberFtpRmDir (self, dstUrl):
        self.__dstUrl = dstUrl
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "rmdir ' +  self.__dstUrl + ' " '
        self.logger.debug("Destination URL = " +  str(self.__dstUrl))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True)
        
    def uberFtpRm (self, dstUrl):
        self.__dstUrl = dstUrl
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "rm ' +  self.__dstUrl + ' " '
        self.logger.debug("Destination URL = " +  str(self.__dstUrl))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True)

    def uberFtpShowFileContent (self, dstUrl):
        self.__dstUrl = dstUrl
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "cat ' +  self.__dstUrl + ' " '
        self.logger.debug("Destination URL = " +  str(self.__dstUrl))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True)
        
    def uberFtpSizeOfFile (self, dstUrl):
        self.__dstUrl = dstUrl
        self.commandArgs = 'uberftp ' + self.__dstHost + ' "size ' +  self.__dstUrl + ' " '
        self.logger.debug("Destination URL = " +  str(self.__dstUrl))
        self.logger.debug("Executed command = " +  str(self.commandArgs))
        self.execute()
        self.showGridProcess(show_stdout = True, show_stderr = True)
        
    
        
def main():
    print "GridFtpCopyHandler"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)
   
    Object = GridFtpCopyHandler()
    Object.setHostsAndPorts("", "", "se-goegrid.gwdg.de", "")

#    Object.uberFtpConnect()
      
#    Object.uberFtpCopyTo("/home/haykuhi/Desktop/bbb.txt" , "/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/")
    
#    Object.uberFtpCopyFrom("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/aaa.txt", "/home/haykuhi/Desktop/")
      
#    Object.uberFtpMkDir("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/eclipse_garfinqyul")
   
#    Object.uberFtpRmDir("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/eclipse_garfinqyul")

#    Object.uberFtpRm("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/bbb.txt")

#    Object.uberFtpShowFileContent("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/bbb.txt") 

#    Object.uberFtpSizeOfFile("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/bbb.txt")

    Object.uberFtpShowFiles("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/")
   

if __name__ == '__main__':
    main()

