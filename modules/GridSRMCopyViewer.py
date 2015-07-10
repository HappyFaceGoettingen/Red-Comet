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
from sqlalchemy import *
from hf.gridtoolkit.GridSRMCopyHandler import GridSRMCopyHandler
from hf.gridtoolkit.Transfers import Transfers
from hf.gridtoolkit.SpaceTokens import SpaceTokens
from hf.gridtoolkit.GenerateFile import *

class GridSRMCopyViewer(hf.module.ModuleBase):
    
    config_keys = {
        'test': ('GridSRMCopyViewer', '100')
    }
    
    config_hint = ''
    
    table_columns = [
        Column('status', TEXT),
    ], []

    subtable_columns = {
        'site_transfers': ([
        Column('siteName', TEXT),                   
        Column('scratchdiskToScratchdisk', TEXT),
        Column('scratchdiskToLocalgroupdisk', TEXT),
        Column('scratchdiskToProddisk', TEXT),
        Column('scratchdiskToDatadisk', TEXT),
        Column('localgroupdiskToScratchdisk', TEXT),
        Column('localgroupdiskToLocalgroupdisk', TEXT),
        Column('localgroupdiskToProddisk', TEXT),
        Column('localgroupdiskToDatadisk', TEXT),
    ], [])                
    }
         
    __transfer_obj = None
    __spacetoken_obj = None
    __third_party_obj = None
    __genFile = None
    
    __transfer_obj_1_2 = None
    __spacetoken_obj_1_2 = None
    __third_party_obj_1_2 = None
    __genFile_1_2 = None
    
    __transfer_obj_2_1 = None
    __spacetoken_obj_2_1 = None
    __third_party_obj_2_1 = None
    __genFile_2_1 = None
     
     
    def performTransfers(self, _grid_srm_handler, __transfer_obj, __genFile, diskOneFilePath, diskTwo):        
        ## Transfer from  one disk to another
        fileName = __genFile.getGenaratedFileName()
        retCode, error, data = _grid_srm_handler.copying(__transfer_obj.getSrcHost(), __transfer_obj.getSrcPort(), diskOneFilePath, __transfer_obj.getDstHost(),__transfer_obj.getDstPort(),diskTwo, fileName)
        ## show files in destination part       
        status = ""        
        if retCode == 0:
            status = "OK"
            _grid_srm_handler.showFiles(__transfer_obj.getDstHost(),__transfer_obj.getDstPort(), diskTwo)    
                 
        else: 
            status = error
        
        return status    
        
        
    def fillTable(self, site_name, src_src_status, src_lg_status, src_prod_status, src_data_status, lg_src_status,lg_lg_status, lg_prod_status, lg_data_status):
        details = {}        
        details['siteName'] = site_name                
        details['scratchdiskToScratchdisk'] = src_src_status        
        details['scratchdiskToLocalgroupdisk'] = src_lg_status
        details['scratchdiskToProddisk'] = src_prod_status   
        details['scratchdiskToDatadisk'] = src_data_status                       
        details['localgroupdiskToScratchdisk'] = lg_src_status        
        details['localgroupdiskToLocalgroupdisk'] = lg_lg_status            
        details['localgroupdiskToProddisk'] = lg_prod_status            
        details['localgroupdiskToDatadisk'] = lg_data_status                 
        return details
       
        
    def siteTransfers(self, __transfer_obj, __spacetoken_obj, _grid_srm_handler, __genFile):
                       
        __genFile.copying(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(), __transfer_obj.getSrcPath(), _grid_srm_handler)        
        
        diskOneFilePath = __transfer_obj.getSrcPath() +__genFile.getGenaratedFileName()        
        
        src_src_status = self.performTransfers(_grid_srm_handler, __transfer_obj, __genFile, diskOneFilePath, __spacetoken_obj.getScratchDiskPath()+ "test/")
        src_local_status = self.performTransfers(_grid_srm_handler, __transfer_obj, __genFile, diskOneFilePath, __spacetoken_obj.getLocalGroupDiskPath()+ "test/")
        src_prod_status = self.performTransfers(_grid_srm_handler, __transfer_obj, __genFile, diskOneFilePath, __spacetoken_obj.getProdDiskPath())
        src_data_status = self.performTransfers(_grid_srm_handler, __transfer_obj, __genFile, diskOneFilePath, __spacetoken_obj.getDataDiskPath())
        
        return  src_src_status,  src_local_status, src_prod_status, src_data_status
        
    def extractData(self):        
        data = {
            'dataset': "Hi",
            'status': 1
        }   
        
        logging.basicConfig(level=logging.INFO)
        logging.root.setLevel(logging.INFO) 
        
       # Site 1 to Site 1 transfers
        __transfer_obj = Transfers()
        __spacetoken_obj = SpaceTokens()               
        __grid_srm_handler = GridSRMCopyHandler(self.config['timeout'])      
        __genFile = GenerateFile()     
         
        # Set Sites name 
        __transfer_obj.setSiteName(self.config['site1_name'])
        # Set source and destination hosts and ports
        __transfer_obj.setSrcHost(self.config['site1_host'])
        __transfer_obj.setSrcPort(self.config['site1_port'])
        __transfer_obj.setDstHost(self.config['site1_host'])
        __transfer_obj.setDstPort(self.config['site1_port'])            
        
        # Set all space tokens 
        __spacetoken_obj.setScratchDiskPath(self.config['site1_scratchdisk_path'])        
        __spacetoken_obj.setLocalGroupDiskPath(self.config['site1_localgroupdisk_path'])
        __spacetoken_obj.setProdDiskPath(self.config['site1_proddisk_path']) 
        __spacetoken_obj.setDataDiskPath(self.config['site1_datadisk_path'])
        
        # Copy a file from local to remote
        retCode_token1, error_token1, output_msg1 = __grid_srm_handler.mkDir(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(),__spacetoken_obj.getScratchDiskPath())
        retCode_token2, error_token2, output_msg2 = __grid_srm_handler.mkDir(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(),__spacetoken_obj.getLocalGroupDiskPath())
                      
        src_src_status, src_lg_status, src_prod_status, src_data_status  = ("",)*4
        lg_src_status, lg_lg_status, lg_prod_status, lg_data_status = ("",)*4
        
        mkdir_status1 = False
        if retCode_token1 == 0 or "exists" in output_msg1 or  "exists" in error_token1:
            __grid_srm_handler.mkDir(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(),__spacetoken_obj.getScratchDiskPath()+ "test/")         
            mkdir_status1 = True
        
        print "Scratchdisk:"
        print mkdir_status1
        
        mkdir_status2 = False        
        if retCode_token2 == 0 or "exists" in output_msg2 or  "exists" in error_token2: 
            __grid_srm_handler.mkDir(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(),__spacetoken_obj.getLocalGroupDiskPath()+ "test/")         
            mkdir_status2 = True
        
        print "Localgroupdisk:"
        print mkdir_status2   
            
        if mkdir_status1:
            __transfer_obj.setSrcPath(self.config['site1_scratchdisk_path'])  
            src_src_status,  src_lg_status, src_prod_status, src_data_status = self.siteTransfers( __transfer_obj, __spacetoken_obj, __grid_srm_handler, __genFile)
               
        else: 
            print "Failed to create a directory"
            src_src_status = "Cannot create a directory" 
            src_lg_status = "Cannot create a directory"
            src_prod_status = "Cannot create a directory"
            src_data_status = "Cannot create a directory"
        
                    
        if mkdir_status2:
            __transfer_obj.setSrcPath(self.config['site1_localgroupdisk_path']) 
            lg_src_status,   lg_lg_status,  lg_prod_status, lg_data_status = self.siteTransfers(__transfer_obj, __spacetoken_obj, __grid_srm_handler, __genFile)
       
        else: 
            print "Failed to create a directory"
            lg_src_status = "Cannot create a directory" 
            lg_lg_status = "Cannot create a directory"
            lg_prod_status = "Cannot create a directory"
            lg_data_status = "Cannot create a directory"
          
        
        details = self.fillTable(__transfer_obj.getSiteName(), src_src_status, src_lg_status, src_prod_status, src_data_status, 
                                       lg_src_status,lg_lg_status, lg_prod_status, lg_data_status)
                
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[0] = details
        
        
        # Site 1 to Site 2 transfers
        __transfer_obj_1_2 = Transfers()
        __spacetoken_obj_1_2 = SpaceTokens()               
        __grid_srm_handler_1_2 = GridSRMCopyHandler(self.config['timeout'])
        __genFile_1_2 = GenerateFile() 
        
        __transfer_obj_1_2.setSiteName(self.config['site2_name1'])
        # Set source and destination hosts and ports
        __transfer_obj_1_2.setSrcHost(self.config['site1_host'])
        __transfer_obj_1_2.setSrcPort(self.config['site1_port'])
        __transfer_obj_1_2.setDstHost(self.config['site2_host'])
        __transfer_obj_1_2.setDstPort(self.config['site2_port'])            
        
        # Set all space tokens 
        __spacetoken_obj_1_2.setScratchDiskPath(self.config['site2_scratchdisk_path'])        
        __spacetoken_obj_1_2.setLocalGroupDiskPath(self.config['site2_localgroupdisk_path'])
        __spacetoken_obj_1_2.setProdDiskPath(self.config['site2_proddisk_path']) 
        __spacetoken_obj_1_2.setDataDiskPath(self.config['site2_datadisk_path'])
               
        # Copy a file from local to remote
        retCode_token3, error_token3, output_msg3 = __grid_srm_handler_1_2.mkDir(__transfer_obj_1_2.getDstHost(),__transfer_obj_1_2.getDstPort(),__spacetoken_obj_1_2.getScratchDiskPath())
        retCode_token4, error_token4, output_msg4 = __grid_srm_handler_1_2.mkDir(__transfer_obj_1_2.getDstHost(),__transfer_obj_1_2.getDstPort(),__spacetoken_obj_1_2.getLocalGroupDiskPath())
        
        src_src_status_1_2, src_lg_status_1_2, src_prod_status_1_2, src_data_status_1_2  = ("",)*4
        lg_src_status_1_2, lg_lg_status_1_2, lg_prod_status_1_2, lg_data_status_1_2 = ("",)*4

        mkdir_status3 = False
        if retCode_token3 == 0 or "exists" in output_msg3 or "exists" in error_token3:
            __grid_srm_handler_1_2.mkDir(__transfer_obj_1_2.getDstHost(),__transfer_obj_1_2.getDstPort(),__spacetoken_obj_1_2.getScratchDiskPath()+ "test/")         
            mkdir_status3 = True
        
        print "Scratchdisk:"
        print mkdir_status3
            
        if mkdir_status3:
            __transfer_obj_1_2.setSrcPath(self.config['site1_scratchdisk_path'])  
            src_src_status_1_2,  src_lg_status_1_2, src_prod_status_1_2, src_data_status_1_2 = self.siteTransfers( __transfer_obj_1_2, __spacetoken_obj_1_2, __grid_srm_handler_1_2, __genFile_1_2)
          
        else: 
            print "Failed to create a directory"
            src_src_status_1_2 = "Cannot create a directory" 
            src_lg_status_1_2 = "Cannot create a directory"
            src_prod_status_1_2 = "Cannot create a directory"
            src_data_status_1_2 = "Cannot create a directory"
        
        mkdir_status4 = False
        if retCode_token4 == 0 or "exists" in output_msg4 or "exists" in error_token4: 
            __grid_srm_handler_1_2.mkDir(__transfer_obj_1_2.getDstHost(),__transfer_obj_1_2.getDstPort(),__spacetoken_obj_1_2.getLocalGroupDiskPath()+ "test/")         
            mkdir_status4 = True
        
        print "Localgroupdisk:"
        print mkdir_status4  
            
        if mkdir_status4:         
            __transfer_obj_1_2.setSrcPath(self.config['site1_localgroupdisk_path']) 
            lg_src_status_1_2,   lg_lg_status_1_2,  lg_prod_status_1_2, lg_data_status_1_2 = self.siteTransfers(__transfer_obj_1_2, __spacetoken_obj_1_2, __grid_srm_handler_1_2, __genFile_1_2)
       
        else: 
            print "Failed to create a directory"
            lg_src_status_1_2 = "Cannot create a directory" 
            lg_lg_status_1_2 = "Cannot create a directory"
            lg_prod_status_1_2 = "Cannot create a directory"
            lg_data_status_1_2 = "Cannot create a directory"
          
        
        details_1_2 = self.fillTable(__transfer_obj_1_2.getSiteName(), src_src_status_1_2, src_lg_status_1_2, src_prod_status_1_2, src_data_status_1_2, 
                                       lg_src_status_1_2,lg_lg_status_1_2, lg_prod_status_1_2, lg_data_status_1_2)
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[1] = details_1_2          
       
    
        # Site 2 to Site 1 transfers
        __transfer_obj_2_1 = Transfers()
        __spacetoken_obj_2_1 = SpaceTokens()               
        __grid_srm_handler_2_1 = GridSRMCopyHandler(self.config['timeout'])      
        __genFile_2_1 = GenerateFile()     
        
        __transfer_obj_2_1.setSiteName(self.config['site2_name2'])
        # Set source and destination hosts and ports
        __transfer_obj_2_1.setSrcHost(self.config['site2_host'])
        __transfer_obj_2_1.setSrcPort(self.config['site2_port'])
        __transfer_obj_2_1.setDstHost(self.config['site1_host'])
        __transfer_obj_2_1.setDstPort(self.config['site1_port'])            
        
        # Set all space tokens 
        __spacetoken_obj_2_1.setScratchDiskPath(self.config['site1_scratchdisk_path'])        
        __spacetoken_obj_2_1.setLocalGroupDiskPath(self.config['site1_localgroupdisk_path'])
        __spacetoken_obj_2_1.setProdDiskPath(self.config['site1_proddisk_path']) 
        __spacetoken_obj_2_1.setDataDiskPath(self.config['site1_datadisk_path'])
        
        # Result of the transfers from Site1 to Site1      
        __transfer_obj_2_1.setSrcPath(self.config['site2_scratchdisk_path'])  
        src_src_status_2_1,  src_lg_status_2_1, src_prod_status_2_1, src_data_status_2_1 = self.siteTransfers( __transfer_obj_2_1, __spacetoken_obj_2_1, __grid_srm_handler_2_1, __genFile_2_1)
               
        __transfer_obj_2_1.setSrcPath(self.config['site2_localgroupdisk_path']) 
        lg_src_status_2_1, lg_lg_status_2_1, lg_prod_status_2_1, lg_data_status_2_1 = self.siteTransfers(__transfer_obj_2_1, __spacetoken_obj_2_1, __grid_srm_handler_2_1, __genFile_2_1)
       
      
        details_2_1 = self.fillTable(__transfer_obj_2_1.getSiteName(), src_src_status_2_1, src_lg_status_2_1, src_prod_status_2_1, src_data_status_2_1, 
                                       lg_src_status_2_1, lg_lg_status_2_1, lg_prod_status_2_1, lg_data_status_2_1)
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[2] = details_2_1    
       
        
        obj = GridSRMCopyHandler()
        #Remove files from local path
        obj.rmLocal() 

        #self.removeTmpDir(obj, __transfer_obj.getDstHost(), __transfer_obj.getDstPort(), __spacetoken_obj.getScratchDiskPath(),__spacetoken_obj.getLocalGroupDiskPath())
                           
        return data
    
    def removeTmpDir(self, obj, host, port, dst_scr_path, dst_lg_path):
        #Remove folders from scratchdisk
        obj.rmDir(host, port, dst_scr_path + "test/")
        obj.rmDir(host, port, dst_scr_path)        
        #Remove folders from localgroupdisk
        obj.rmDir(host, port, dst_lg_path + "test/")
        obj.rmDir(host, port, dst_lg_path)    
     
    def removeTmpFiles(self, obj, host, port, dst_scr_path, dst_lg_path, dst_prod_path, dst_data_path, fileName):
        #Remove files from space tokens
        obj.rmFile(host, port, dst_scr_path + fileName)
        obj.rmFile(host, port, dst_scr_path + "test/srm_" + fileName)
        obj.rmFile(host, port, dst_lg_path + fileName)
        obj.rmFile(host, port, dst_lg_path + "test/srm_" + fileName)        
        obj.rmFile(host, port, dst_prod_path + fileName)
        obj.rmFile(host, port, dst_data_path + fileName)       
        
    def prepareAcquisition(self):        
        self.details_table_db_value_list = []
             
    def fillSubtables(self, parent_id):
        self.subtables['site_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])
           
    def SQLQuery(self, siteName):
        max_id_number1_uberftp = func.max(self.subtables['site_transfers'].c.id).select().where(self.subtables['site_transfers'].c.siteName == str(siteName)).execute().scalar()        
        details1 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number1_uberftp).execute().fetchall()
        return map(dict, details1)        
    
                       
    def getTemplateData(self):        
        data = hf.module.ModuleBase.getTemplateData(self)        
        self.dataset['site1Name'] = self.config['site1_name']                
        self.dataset['site1_space_token_scratchdisk'] = self.config['site1_space_token_scratchdisk']
        self.dataset['site1_space_token_localgroupdisk'] = self.config['site1_space_token_localgroupdisk']
        self.dataset['site1_space_token_proddisk'] = self.config['site1_space_token_proddisk']
        self.dataset['site1_space_token_datadisk'] = self.config['site1_space_token_datadisk']
                      
        self.dataset['site2Name1'] = self.config['site2_name1']   
        self.dataset['site2Name2'] = self.config['site2_name2']            
        self.dataset['site2_space_token_scratchdisk'] = self.config['site2_space_token_scratchdisk']
        self.dataset['site2_space_token_localgroupdisk'] = self.config['site2_space_token_localgroupdisk']
        self.dataset['site2_space_token_proddisk'] = self.config['site2_space_token_proddisk']
        self.dataset['site2_space_token_datadisk'] = self.config['site2_space_token_datadisk']
        
        self.dataset['ok_status'] = ['OK', 'SpaceException', 'FileExists', 'copy', 'completed', 'SUCCESS' ]
        self.dataset['ok_status_prod_data'] = ['Permission denied', 'SRM_AUTHORIZATION_FAILURE', 'No permission', 'failed']
        self.dataset['error_status'] = ['Failed', 'Error', 'No match', 'timeout', 'Cannot create a directory']
        self.dataset['error_status_prod_data'] = ['OK', 'SpaceException', 'FileExists', 'SUCCESS', 'Cannot create a directory']         
                               
        data['site1_site1_transfers'] = self.SQLQuery(self.config['site1_name'])  
        data['site1_site2_transfers'] = self.SQLQuery(self.config['site2_name1'])     
        data['site2_site1_transfers'] = self.SQLQuery(self.config['site2_name2'])
                
        
        return data
