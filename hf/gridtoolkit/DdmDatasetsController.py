# -*- coding: utf-8 -*-
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
# Author: Haykuhi Musheghyan <haykuhi.musheghyan@cern.ch>, Date: 13/August/2015
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

import logging, os, subprocess, re
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler
from hf.gridengine.gridsubprocess import GridPopen
import sqlite3
import datetime

class DdmDatasetsController(GridSubprocessBaseHandler):
 
    
    def __init__(self):
        self.cvmfsEnv.setEnabled("emi")
        self.cvmfsEnv.setEnabled("rucio.client")   
        self.gridEnv.set('x509.user.key', None)        
               
    
    def whoami(self):                
        self.commandArgs = "rucio whoami"
        retCode, error_msg, output_msg = self.execute()         
        print "Command executed return code: "
        print retCode          
        try: 
                (data,error) = self.gridProcess.communicate()
                print output_msg                
        except Exception as e: 
                self.logger.debug("An exception has occurred:")
                print e
                
    
    def listDatasets(self, token, start, end):            
        command = "rucio list-datasets-rse "        
        self.commandArgs = command + token + " | sed -n " + start + "," + end + "p" 
        retCode, error_msg, output_msg = self.execute()         
        print "Command executed return code: "
        print retCode          
        try: 
                (data,error) = self.gridProcess.communicate()
                print retCode, error_msg, output_msg
                return retCode, error_msg, output_msg
                
        except Exception as e: 
                self.logger.debug("An exception has occurred:")
                print e       
                 
    
    def getMetaData(self, dataset):        
        self.commandArgs = "rucio get-metadata " + dataset        
        retCode, error_msg, output_msg = self.execute()         
        print "Command executed return code: "
        print retCode          
        try: 
                (data,error) = self.gridProcess.communicate()
                #print retCode, error_msg, output_msg
                
        except Exception as e: 
                self.logger.debug("An exception has occurred:")
                print e
       
              
        '''write output of the executed command to dictionary'''
        # string massage
        tmp = output_msg.replace(': ',',')
        csv = tmp.replace(" ","")
        strings = re.split(',|\n',csv)
        words = [x for x in strings if x != '']
        
        info = {}        
        keys = map(lambda i: words[i],filter(lambda i: i%2 == 0,range(len(words))))
        values = map(lambda i: words[i],filter(lambda i: i%2 == 1,range(len(words))))
        
        info = dict(zip(keys, values))
        return info    
        

def main():
       
    datasetViewer = DdmDatasetsController()     

if __name__ == '__main__':
    main()






        






