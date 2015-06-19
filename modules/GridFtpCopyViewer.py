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
from hf.gridtoolkit.GridFtpCopyHandler import GridFtpCopyHandler
from hf.gridtoolkit.Transfers import Transfers
from hf.gridtoolkit.SpaceTokens import SpaceTokens
from hf.gridtoolkit.GenerateFile import *

class GridFtpCopyViewer(hf.module.ModuleBase):
    
    config_keys = {
        'test': ('GridFTPCopyViewer', '100')
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
   
     
    def performTransfers(self, __grid_ftp_handler, __transfer_obj, __genFile, diskOneFilePath, diskTwo):         
        ## Transfer from  one disk to another   
        data, error = __grid_ftp_handler.copying(__transfer_obj.getSrcHost(), __transfer_obj.getSrcPort(), diskOneFilePath, __transfer_obj.getDstHost(), diskTwo)
        ## Check the status of copy
        status = ""
        if not error:
           check_if_file_exists = __grid_ftp_handler.checkFile(__transfer_obj.getDstHost(), __transfer_obj.getDstPort(),__genFile.getGenaratedFileName(), diskTwo)
           if check_if_file_exists == 0:
               status = "OK"                    
           else:
               status = "Failed"                      
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
       
        
    def siteTransfers(self, __transfer_obj, __spacetoken_obj, __grid_ftp_handler, __genFile):               
        
        __genFile.copying(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(), __transfer_obj.getSrcPath(), __grid_ftp_handler)        
        
        diskOneFilePath = __transfer_obj.getSrcPath() +__genFile.getGenaratedFileName()        
        src_src_status = self.performTransfers(__grid_ftp_handler, __transfer_obj, __genFile, diskOneFilePath, __spacetoken_obj.getScratchDiskPath()+ "test/")
        src_local_status = self.performTransfers(__grid_ftp_handler, __transfer_obj, __genFile, diskOneFilePath, __spacetoken_obj.getLocalGroupDiskPath()+ "test/")
        src_prod_status = self.performTransfers(__grid_ftp_handler, __transfer_obj, __genFile, diskOneFilePath, __spacetoken_obj.getProdDiskPath())
        src_data_status = self.performTransfers(__grid_ftp_handler, __transfer_obj, __genFile, diskOneFilePath, __spacetoken_obj.getDataDiskPath())
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
        __grid_ftp_handler = GridFtpCopyHandler()      
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
        __grid_ftp_handler.setTimeout(int(self.config['timeout']))
        __grid_ftp_handler.mkDir(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(),__spacetoken_obj.getScratchDiskPath())
        __grid_ftp_handler.mkDir(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(),__spacetoken_obj.getScratchDiskPath()+ "test/")         
       
        __grid_ftp_handler.mkDir(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(),__spacetoken_obj.getLocalGroupDiskPath())
        __grid_ftp_handler.mkDir(__transfer_obj.getSrcHost(),__transfer_obj.getSrcPort(),__spacetoken_obj.getLocalGroupDiskPath()+ "test/")         
       
        # Result of the transfers from Site1 to Site1      
        __transfer_obj.setSrcPath(self.config['site1_scratchdisk_path'])  
        src_src_status,  src_lg_status, src_prod_status, src_data_status = self.siteTransfers( __transfer_obj, __spacetoken_obj, __grid_ftp_handler, __genFile)
               
        __transfer_obj.setSrcPath(self.config['site1_localgroupdisk_path']) 
        lg_src_status,   lg_lg_status,  lg_prod_status, lg_data_status = self.siteTransfers(__transfer_obj, __spacetoken_obj, __grid_ftp_handler, __genFile)
       
      
        details = self.fillTable(__transfer_obj.getSiteName(), src_src_status, src_lg_status, src_prod_status, src_data_status, 
                                       lg_src_status,lg_lg_status, lg_prod_status, lg_data_status)
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[0] = details
        
        
        # Site 1 to Site 2 transfers
        __transfer_obj_1_2 = Transfers()
        __spacetoken_obj_1_2 = SpaceTokens()               
        __grid_ftp_handler_1_2 = GridFtpCopyHandler()
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
        __grid_ftp_handler_1_2.setTimeout(int(self.config['timeout']))
        __grid_ftp_handler_1_2.mkDir(__transfer_obj_1_2.getDstHost(),__transfer_obj.getSrcPort(),__spacetoken_obj_1_2.getScratchDiskPath())
        __grid_ftp_handler_1_2.mkDir(__transfer_obj_1_2.getDstHost(),__transfer_obj.getSrcPort(),__spacetoken_obj_1_2.getScratchDiskPath()+ "test/")         
       
        __grid_ftp_handler_1_2.mkDir(__transfer_obj_1_2.getDstHost(),__transfer_obj.getSrcPort(),__spacetoken_obj_1_2.getLocalGroupDiskPath())
        __grid_ftp_handler_1_2.mkDir(__transfer_obj_1_2.getDstHost(),__transfer_obj.getSrcPort(),__spacetoken_obj_1_2.getLocalGroupDiskPath()+ "test/")         
       
        # Result of the transfers from Site1 to Site1      
        __transfer_obj_1_2.setSrcPath(self.config['site1_scratchdisk_path'])  
        src_src_status_1_2,  src_lg_status_1_2, src_prod_status_1_2, src_data_status_1_2 = self.siteTransfers( __transfer_obj_1_2, __spacetoken_obj_1_2, __grid_ftp_handler_1_2, __genFile_1_2)
               
        __transfer_obj_1_2.setSrcPath(self.config['site1_localgroupdisk_path']) 
        lg_src_status_1_2,   lg_lg_status_1_2,  lg_prod_status_1_2, lg_data_status_1_2 = self.siteTransfers(__transfer_obj_1_2, __spacetoken_obj_1_2, __grid_ftp_handler_1_2, __genFile_1_2)
       
      
        details_1_2 = self.fillTable(__transfer_obj_1_2.getSiteName(), src_src_status_1_2, src_lg_status_1_2, src_prod_status_1_2, src_data_status_1_2, 
                                       lg_src_status_1_2,lg_lg_status_1_2, lg_prod_status_1_2, lg_data_status_1_2)
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[1] = details_1_2          
       
        
        # Site 2 to Site 1 transfers
        __transfer_obj_2_1 = Transfers()
        __spacetoken_obj_2_1 = SpaceTokens()               
        __grid_ftp_handler_2_1 = GridFtpCopyHandler()      
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
        
        __grid_ftp_handler_2_1.setTimeout(int(self.config['timeout']))
        
        # Result of the transfers from Site1 to Site1      
        __transfer_obj_2_1.setSrcPath(self.config['site2_scratchdisk_path'])  
        src_src_status_2_1,  src_lg_status_2_1, src_prod_status_2_1, src_data_status_2_1 = self.siteTransfers( __transfer_obj_2_1, __spacetoken_obj_2_1, __grid_ftp_handler_2_1, __genFile_2_1)
               
        __transfer_obj_2_1.setSrcPath(self.config['site2_localgroupdisk_path']) 
        lg_src_status_2_1, lg_lg_status_2_1, lg_prod_status_2_1, lg_data_status_2_1 = self.siteTransfers(__transfer_obj_2_1, __spacetoken_obj_2_1, __grid_ftp_handler_2_1, __genFile_2_1)
       
      
        details_2_1 = self.fillTable(__transfer_obj_2_1.getSiteName(), src_src_status_2_1, src_lg_status_2_1, src_prod_status_2_1, src_data_status_2_1, 
                                       lg_src_status_2_1, lg_lg_status_2_1, lg_prod_status_2_1, lg_data_status_2_1)
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[2] = details_2_1    
       
        
        obj = GridFtpCopyHandler()
        #Remove files from local path
        obj.rmLocal()  
        #Remove files from Site1 space tokens
        self.removeTmpFiles(obj, __transfer_obj.getDstHost(), __transfer_obj.getDstPort(), __spacetoken_obj.getScratchDiskPath(),__spacetoken_obj.getLocalGroupDiskPath(), __spacetoken_obj.getProdDiskPath(), __spacetoken_obj.getDataDiskPath())
        #Remove files from Site2 space tokens
        self.removeTmpFiles(obj, __transfer_obj_1_2.getDstHost(), __transfer_obj.getDstPort(), __spacetoken_obj_1_2.getScratchDiskPath(),__spacetoken_obj_1_2.getLocalGroupDiskPath(), __spacetoken_obj_1_2.getProdDiskPath(), __spacetoken_obj_1_2.getDataDiskPath())
                                 
        return data
    
     
    def removeTmpFiles(self, obj, host, port,  dst_scr_path, dst_lg_path, dst_prod_path, dst_data_path):
        #Remove files from space tokens
        obj.rmFile(host, port, dst_scr_path)
        obj.rmFile(host, port, dst_scr_path + "test/")
        obj.rmFile(host, port, dst_lg_path)
        obj.rmFile(host, port, dst_lg_path + "test/")        
        obj.rmFile(host, port, dst_prod_path)
        obj.rmFile(host, port, dst_data_path)        
        #Remove folders from scratchdisk
        obj.rmDir(host, port, dst_scr_path + "test/")
        obj.rmDir(host,port,  dst_scr_path)        
        #Remove folders from localgroupdisk
        obj.rmDir(host, port, dst_lg_path + "test/")
        obj.rmDir(host, port, dst_lg_path)    
        

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
        
        self.dataset['ok_status'] = ['OK', 'SpaceException', 'FileExists']
        self.dataset['ok_status_prod_data'] = ['Permission denied', 'Authorization', 'No permission']
        self.dataset['error_status'] = ['Failed', 'Error', 'No match']
        self.dataset['error_status_prod_data'] = ['OK', 'SpaceException', 'FileExists']
                        
        data['site1_site1_transfers'] = self.SQLQuery(self.config['site1_name'])  
        data['site1_site2_transfers'] = self.SQLQuery(self.config['site2_name1'])     
        data['site2_site1_transfers'] = self.SQLQuery(self.config['site2_name2'])
                
        
        return data
