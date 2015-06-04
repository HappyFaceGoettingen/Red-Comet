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
from hf.gridtoolkit.SpaceTokens import ScratchDisk, LocalGroupDisk, ProdDisk, DataDisk
from hf.gridtoolkit.TransferTypes import *

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

  
    def performTransfers(self, obj, transfer_obj, localToRemote_obj, diskOneFilePath, diskTwo, protocol):
        
        ## Transfer from  one disk to another   
        data, error = obj.copying(transfer_obj.getSrcHost(), 
                                 diskOneFilePath, 
                                 transfer_obj.getDstHost(), 
                                 diskTwo, 
                                 "gsiftp")
        ## Check the status of copy
        status = ""
        if not error:
           check_if_file_exists = transfer_obj.checkFile(transfer_obj.getDstHost(), localToRemote_obj.getGenaratedFileName(), diskTwo)
           if check_if_file_exists == 0:
               status = "OK"                    
           else:
               status = "Failed"                      
        else:
           status = error
        
        return status
    
    def fillTable(self,site_name, src_src_status, src_local_status, src_prod_status, src_data_status, 
                   local_src_status,local_local_status, local_prod_status, local_data_status):
        
        details = {}        
        details['siteName'] = site_name        
        details['scratchdiskToScratchdisk'] = src_src_status        
        details['scratchdiskToLocalgroupdisk'] = src_local_status
        details['scratchdiskToProddisk'] = src_prod_status   
        details['scratchdiskToDatadisk'] = src_data_status                       
        details['localgroupdiskToScratchdisk'] = local_src_status        
        details['localgroupdiskToLocalgroupdisk'] = local_local_status            
        details['localgroupdiskToProddisk'] = local_prod_status            
        details['localgroupdiskToDatadisk'] = local_data_status        
        return details
    
    def setAllParams(self, srcHost, srcPort, dstHost, dstPort, src_scr_path, dst_scr_path,
                       src_lg_path, dst_lg_path, dst_prod_path, dst_data_path):
        
        transfer_obj = Transfers()
                            
        #Set hosts and ports for the uberftp objects 
        transfer_obj.setSrcHost(srcHost)
        transfer_obj.setSrcPort(srcPort)        
        transfer_obj.setDstHost(dstHost)
        transfer_obj.setDstPort(dstPort)
        
        
        #Set space tokens
        src_scratchDisk_obj = ScratchDisk()
        src_scratchDisk_obj.setScratchDisk("ScratchDisk")
        src_scratchDisk_obj.setSpaceTokenPath(src_scr_path)
        
        dst_scratchDisk_obj = ScratchDisk()
        dst_scratchDisk_obj.setScratchDisk("ScratchDisk")
        dst_scratchDisk_obj.setSpaceTokenPath(dst_scr_path)
        
        src_localGroup_obj = LocalGroupDisk()
        src_localGroup_obj.setLocalGroupDisk("LocalGroupDisk")
        src_localGroup_obj.setSpaceTokenPath(src_lg_path)
        
        dst_localGroup_obj = LocalGroupDisk()
        dst_localGroup_obj.setLocalGroupDisk("LocalGroupDisk")
        dst_localGroup_obj.setSpaceTokenPath(dst_lg_path)
                
        dst_prodDisk_obj = ProdDisk()
        dst_prodDisk_obj.setProdDisk("ProdDisk")
        dst_prodDisk_obj.setSpaceTokenPath(dst_prod_path)        
        
        dst_dataDisk_obj = DataDisk()
        dst_dataDisk_obj.setDataDisk("DataDisk")
        dst_dataDisk_obj.setSpaceTokenPath(dst_data_path)
        
        #Third-party transfers        
        transfer_obj.setTransferType("ThirdParty")
        third_party_obj = ThirdParty()
        
        #Local to remote transfers
        transfer_obj.setTransferType("Local")
        localToRemote_obj = LocalToRemote()
        
        return (transfer_obj, src_scratchDisk_obj, dst_scratchDisk_obj, src_localGroup_obj, dst_localGroup_obj, 
                dst_prodDisk_obj, dst_dataDisk_obj, third_party_obj, localToRemote_obj) 
    
    
    def siteTransfers(self, site_name, transfer_obj, src_scratchDisk_obj, dst_scratchDisk_obj, src_localGroup_obj, dst_localGroup_obj, dst_prodDisk_obj, dst_dataDisk_obj, third_party_obj, localToRemote_obj):
        
        #Copy from local to remote               
        localToRemote_obj.copying(transfer_obj.getSrcHost(),src_scratchDisk_obj.getSpaceTokenPath())
    
        #ScratchDisk
        diskOneFilePath = src_scratchDisk_obj.getSpaceTokenPath()+localToRemote_obj.getGenaratedFileName()        
        
        src_src_status = self.performTransfers(third_party_obj, transfer_obj, localToRemote_obj, diskOneFilePath, dst_scratchDisk_obj.getSpaceTokenPath()+ "test/", "gsiftp")
        src_local_status = self.performTransfers(third_party_obj, transfer_obj, localToRemote_obj, diskOneFilePath, dst_localGroup_obj.getSpaceTokenPath()+ "test/", "gsiftp")
        src_prod_status = self.performTransfers(third_party_obj, transfer_obj, localToRemote_obj, diskOneFilePath, dst_prodDisk_obj.getSpaceTokenPath(), "gsiftp")
        src_data_status = self.performTransfers(third_party_obj, transfer_obj, localToRemote_obj, diskOneFilePath, dst_dataDisk_obj.getSpaceTokenPath(), "gsiftp")
               
        #LocalGroupDisk               
        localToRemote_obj.copying(transfer_obj.getSrcHost(),src_localGroup_obj.getSpaceTokenPath())
            
        diskOneFilePath = src_localGroup_obj.getSpaceTokenPath()+localToRemote_obj.getGenaratedFileName()
        local_src_status = self.performTransfers(third_party_obj, transfer_obj, localToRemote_obj, diskOneFilePath, dst_scratchDisk_obj.getSpaceTokenPath()+ "test/", "gsiftp")
        local_local_status = self.performTransfers(third_party_obj, transfer_obj, localToRemote_obj, diskOneFilePath, dst_localGroup_obj.getSpaceTokenPath()+ "test/", "gsiftp")
        local_prod_status = self.performTransfers(third_party_obj, transfer_obj, localToRemote_obj, diskOneFilePath, dst_prodDisk_obj.getSpaceTokenPath(), "gsiftp")
        local_data_status = self.performTransfers(third_party_obj, transfer_obj, localToRemote_obj, diskOneFilePath, dst_dataDisk_obj.getSpaceTokenPath(), "gsiftp")
       
        #Fill the details dictionary
        site1_details = self.fillTable(site_name, src_src_status, src_local_status, src_prod_status, src_data_status, 
                                       local_src_status,local_local_status, local_prod_status, local_data_status)
        
        
        return site1_details   
        
    def extractData(self):
        
        data = {
            'dataset': "Hi",
            'status': 1
        }   
        
        logging.basicConfig(level=logging.INFO)
        logging.root.setLevel(logging.INFO)             
        
        #From Site1 to Site1 transfers
        srcHost_1_1 = self.config['site1_host']
        srcPort_1_1 = self.config['site1_port']
        dstHost_1_1 = self.config['site1_host']
        dstPort_1_1 = self.config['site1_port']
        
        src_scr_path_1_1 = self.config['site1_scratchdisk_path']
        dst_scr_path_1_1 = self.config['site1_scratchdisk_path']
        
        src_lg_path_1_1 = self.config['site1_localgroupdisk_path']   
        dst_lg_path_1_1 = self.config['site1_localgroupdisk_path']
           
        dst_prod_path_1_1 = self.config['site1_proddisk_path']        
        dst_data_path_1_1 = self.config['site1_datadisk_path']
        
        site_name_1_1 = self.config['site1_name']
                
        #Set all necessary parameters
        transfer_obj_1_1, src_scratchDisk_obj_1_1, dst_scratchDisk_obj_1_1, src_localGroup_obj_1_1, dst_localGroup_obj_1_1, dst_prodDisk_obj_1_1, dst_dataDisk_obj_1_1, third_party_obj_1_1, localToRemote_obj_1_1 =  self.setAllParams(srcHost_1_1, srcPort_1_1, dstHost_1_1, dstPort_1_1, src_scr_path_1_1, dst_scr_path_1_1, src_lg_path_1_1, dst_lg_path_1_1, dst_prod_path_1_1, dst_data_path_1_1)
        
        #Create directory in space token
        localToRemote_obj_1_1.mkDir(transfer_obj_1_1.getSrcHost(),src_scratchDisk_obj_1_1.getSpaceTokenPath())
        localToRemote_obj_1_1.mkDir(transfer_obj_1_1.getSrcHost(),src_scratchDisk_obj_1_1.getSpaceTokenPath()+ "test/")         
        localToRemote_obj_1_1.mkDir(transfer_obj_1_1.getSrcHost(),src_localGroup_obj_1_1.getSpaceTokenPath())
        localToRemote_obj_1_1.mkDir(transfer_obj_1_1.getSrcHost(),src_localGroup_obj_1_1.getSpaceTokenPath()+ "test/")   
             
        #Result of the transfers in dictionary         
        details_1_1 = self.siteTransfers(site_name_1_1, transfer_obj_1_1, src_scratchDisk_obj_1_1, dst_scratchDisk_obj_1_1, 
                                         src_localGroup_obj_1_1, dst_localGroup_obj_1_1, 
                                         dst_prodDisk_obj_1_1, dst_dataDisk_obj_1_1, 
                                         third_party_obj_1_1, localToRemote_obj_1_1)
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[0] = details_1_1    
            
                
        
        #From Site1 to Site2 transfers
        srcHost_1_2 = self.config['site1_host']
        srcPort_1_2 = self.config['site1_port']
        dstHost_1_2 = self.config['site2_host']
        dstPort_1_2 = self.config['site2_port']
        
        src_scr_path_1_2 = self.config['site1_scratchdisk_path']
        dst_scr_path_1_2 = self.config['site2_scratchdisk_path']
        
        src_lg_path_1_2 = self.config['site1_localgroupdisk_path']   
        dst_lg_path_1_2 = self.config['site2_localgroupdisk_path']
                   
        dst_prod_path_1_2 = self.config['site2_proddisk_path']        
        dst_data_path_1_2 = self.config['site2_datadisk_path']
        
        site_name_1_2 = self.config['site2_name1']
        
        #Set all necessary parameters
        transfer_obj_1_2, src_scratchDisk_obj_1_2, dst_scratchDisk_obj_1_2, src_localGroup_obj_1_2, dst_localGroup_obj_1_2, dst_prodDisk_obj_1_2, dst_dataDisk_obj_1_2, third_party_obj_1_2, localToRemote_obj_1_2 =  self.setAllParams(srcHost_1_2, srcPort_1_2, dstHost_1_2, dstPort_1_2, src_scr_path_1_2, dst_scr_path_1_2, src_lg_path_1_2, dst_lg_path_1_2, dst_prod_path_1_2, dst_data_path_1_2)
        
        #Create directory in space token        
        localToRemote_obj_1_2.mkDir(transfer_obj_1_2.getDstHost(),dst_scratchDisk_obj_1_2.getSpaceTokenPath())
        localToRemote_obj_1_2.mkDir(transfer_obj_1_2.getDstHost(),dst_scratchDisk_obj_1_2.getSpaceTokenPath()+ "test/")         
        localToRemote_obj_1_2.mkDir(transfer_obj_1_2.getDstHost(),dst_localGroup_obj_1_2.getSpaceTokenPath())
        localToRemote_obj_1_2.mkDir(transfer_obj_1_2.getDstHost(),dst_localGroup_obj_1_2.getSpaceTokenPath()+ "test/")   
        
        #Result of the transfers in dictionary      
        details_1_2 = self.siteTransfers(site_name_1_2, transfer_obj_1_2, src_scratchDisk_obj_1_2, dst_scratchDisk_obj_1_2, 
                                         src_localGroup_obj_1_2, dst_localGroup_obj_1_2, 
                                         dst_prodDisk_obj_1_2, dst_dataDisk_obj_1_2, 
                                         third_party_obj_1_2, localToRemote_obj_1_2)
        
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[1] = details_1_2
        
        
        
        #For Site2 to Site1
        srcHost_2_1 = self.config['site2_host']
        srcPort_2_1 = self.config['site2_port']
        dstHost_2_1 = self.config['site1_host']
        dstPort_2_1 = self.config['site1_port']
        
        src_scr_path_2_1 = self.config['site2_scratchdisk_path']
        dst_scr_path_2_1 = self.config['site1_scratchdisk_path']
        
        src_lg_path_2_1 = self.config['site2_localgroupdisk_path']   
        dst_lg_path_2_1 = self.config['site1_localgroupdisk_path']
                   
        dst_prod_path_2_1 = self.config['site1_proddisk_path']        
        dst_data_path_2_1 = self.config['site1_datadisk_path']
        
        site_name_2_1 = self.config['site2_name2']       
        
        #Set all necessary parameters
        transfer_obj_2_1, src_scratchDisk_obj_2_1, dst_scratchDisk_obj_2_1, src_localGroup_obj_2_1, dst_localGroup_obj_2_1, dst_prodDisk_obj_2_1, dst_dataDisk_obj_2_1, third_party_obj_2_1, localToRemote_obj_2_1 =  self.setAllParams(srcHost_2_1, srcPort_2_1, dstHost_2_1, dstPort_2_1, src_scr_path_2_1, dst_scr_path_2_1, src_lg_path_2_1, dst_lg_path_2_1, dst_prod_path_2_1, dst_data_path_2_1)
        
        #Result of the transfers in dictionary 
        details_2_1 = self.siteTransfers(site_name_2_1, transfer_obj_2_1, src_scratchDisk_obj_2_1, dst_scratchDisk_obj_2_1, 
                                         src_localGroup_obj_2_1, dst_localGroup_obj_2_1, 
                                         dst_prodDisk_obj_2_1, dst_dataDisk_obj_2_1, 
                                         third_party_obj_2_1, localToRemote_obj_2_1)
        
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[2] = details_2_1 
        
        
        #For Site1 to Site3
        srcHost_1_3 = self.config['site1_host']
        srcPort_1_3 = self.config['site1_port']
        dstHost_1_3 = self.config['site3_host']
        dstPort_1_3 = self.config['site3_port']
        
        src_scr_path_1_3 = self.config['site1_scratchdisk_path']
        dst_scr_path_1_3 = self.config['site3_scratchdisk_path']
        
        src_lg_path_1_3 = self.config['site1_localgroupdisk_path']   
        dst_lg_path_1_3 = self.config['site3_localgroupdisk_path']
                   
        dst_prod_path_1_3 = self.config['site3_proddisk_path']        
        dst_data_path_1_3 = self.config['site3_datadisk_path']
        
        site_name_1_3 = self.config['site3_name1']       
        
        #Set all necessary parameters
        transfer_obj_1_3, src_scratchDisk_obj_1_3, dst_scratchDisk_obj_1_3, src_localGroup_obj_1_3, dst_localGroup_obj_1_3, dst_prodDisk_obj_1_3, dst_dataDisk_obj_1_3, third_party_obj_1_3, localToRemote_obj_1_3 =  self.setAllParams(srcHost_1_3, srcPort_1_3, dstHost_1_3, dstPort_1_3, src_scr_path_1_3, dst_scr_path_1_3, src_lg_path_1_3, dst_lg_path_1_3, dst_prod_path_1_3, dst_data_path_1_3)
        
        #Create directory in space token        
        localToRemote_obj_1_3.mkDir(transfer_obj_1_3.getDstHost(),dst_scratchDisk_obj_1_3.getSpaceTokenPath())
        localToRemote_obj_1_3.mkDir(transfer_obj_1_3.getDstHost(),dst_scratchDisk_obj_1_3.getSpaceTokenPath()+ "test/")         
        localToRemote_obj_1_3.mkDir(transfer_obj_1_3.getDstHost(),dst_localGroup_obj_1_3.getSpaceTokenPath())
        localToRemote_obj_1_3.mkDir(transfer_obj_1_3.getDstHost(),dst_localGroup_obj_1_3.getSpaceTokenPath()+ "test/")  
        
        #Result of the transfers in dictionary 
        details_1_3 = self.siteTransfers(site_name_1_3, transfer_obj_1_3, src_scratchDisk_obj_1_3, dst_scratchDisk_obj_1_3, 
                                         src_localGroup_obj_1_3, dst_localGroup_obj_1_3, 
                                         dst_prodDisk_obj_1_3, dst_dataDisk_obj_1_3, 
                                         third_party_obj_1_3, localToRemote_obj_1_3)
        
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[3] = details_1_3 
                
        
        
        obj = CommandLine()
        #Remove files from local path
        obj.rmLocal()  
        #Remove files from Site1 space tokens
        self.removeTmpFiles(obj, dstHost_1_1, dst_scr_path_1_1, dst_lg_path_1_1, dst_prod_path_1_1, dst_data_path_1_1)
        #Remove files from Site2 space tokens
        self.removeTmpFiles(obj, dstHost_1_2, dst_scr_path_1_2, dst_lg_path_1_2, dst_prod_path_1_2, dst_data_path_1_2)
        #Remove files from Site3 space tokens
        self.removeTmpFiles(obj, dstHost_1_3, dst_scr_path_1_3, dst_lg_path_1_3, dst_prod_path_1_3, dst_data_path_1_3)      

                           
        return data
    
    def removeTmpFiles(self, obj, host, dst_scr_path, dst_lg_path, dst_prod_path, dst_data_path):
        #Remove files from space tokens
        obj.rmFile(host, dst_scr_path)
        obj.rmFile(host, dst_scr_path + "test/")
        obj.rmFile(host, dst_lg_path)
        obj.rmFile(host, dst_lg_path + "test/")        
        obj.rmFile(host, dst_prod_path)
        obj.rmFile(host, dst_data_path)
        
        #Remove folders from scratchdisk
        obj.rmDir(host, dst_scr_path + "test/")
        obj.rmDir(host, dst_scr_path)
        
        #Remove folders from localgroupdisk
        obj.rmDir(host, dst_lg_path + "test/")
        obj.rmDir(host, dst_lg_path)    
        

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
        
        
        self.dataset['site3Name1'] = self.config['site3_name1']   
        self.dataset['site3Name2'] = self.config['site3_name2']            
        self.dataset['site3_space_token_scratchdisk'] = self.config['site3_space_token_scratchdisk']
        self.dataset['site3_space_token_localgroupdisk'] = self.config['site3_space_token_localgroupdisk']
        self.dataset['site3_space_token_proddisk'] = self.config['site3_space_token_proddisk']
        self.dataset['site3_space_token_datadisk'] = self.config['site3_space_token_datadisk']
        
                     
        data['site1_site1_transfers'] = self.SQLQuery(self.config['site1_name'])  
        data['site1_site2_transfers'] = self.SQLQuery(self.config['site2_name1'])     
        data['site2_site1_transfers'] = self.SQLQuery(self.config['site2_name2']) 
        data['site1_site3_transfers'] = self.SQLQuery(self.config['site3_name1'])                   
       
        return data
