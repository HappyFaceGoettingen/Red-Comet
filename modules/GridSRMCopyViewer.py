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

  
    def transfers(self, Obj, srcPath, fileToCopy, dstPath):        
        transfer_status = Obj.copyFileAndCheckExistance(srcPath, fileToCopy, dstPath)              
        return transfer_status                
                                                
    
    def spaceTokenStatus(self, siteName, Object, srcHost, dstHost, scr_scrDiskPath, src_lgDiskPath, dst_scrDiskPath, dst_lgDiskPath, dst_prodDiskPath, dst_dataDiskPath):
        
        copyStatus_scr, stderr_scr, srcPath_scr, fileName_scr = Object.createFileInSrcPath (srcHost, scr_scrDiskPath)
        copyStatus_lg, stderr_lg, srcPath_lg, fileName_lg = Object.createFileInSrcPath (srcHost, src_lgDiskPath) 
               
        detail = {}
        
        detail['siteName'] = siteName 
         
        if copyStatus_scr == 0:               
            detail['scratchdiskToScratchdisk'] = self.transfers(Object, srcPath_scr, fileName_scr, dst_scrDiskPath )
            detail['scratchdiskToLocalgroupdisk'] = self.transfers(Object, srcPath_scr, fileName_scr, dst_lgDiskPath) 
            detail['scratchdiskToProddisk'] = self.transfers(Object, srcPath_scr, fileName_scr, dst_prodDiskPath )
            detail['scratchdiskToDatadisk'] = self.transfers(Object, srcPath_scr, fileName_scr, dst_dataDiskPath )            
                        
            print "MSG: " + str(copyStatus_scr)
            
            ##Removes files from source and destination part##
            Object.rmFile(srcHost , fileName_scr, srcPath_scr)
            Object.rmFile(dstHost, fileName_scr, dst_scrDiskPath) 
            Object.rmFile(dstHost, fileName_scr, dst_lgDiskPath)     
            Object.rmFile(dstHost, fileName_scr, dst_prodDiskPath) 
            Object.rmFile(dstHost, fileName_scr, dst_dataDiskPath)     
            
        else:
            detail['scratchdiskToScratchdisk'] = stderr_scr
            detail['scratchdiskToLocalgroupdisk'] = stderr_scr
            detail['scratchdiskToProddisk'] = stderr_scr
            detail['scratchdiskToDatadisk'] = stderr_scr       
        
            print "Error msg: " + str(stderr_scr)
        
        if copyStatus_lg == 0:        
            detail['localgroupdiskToScratchdisk'] = self.transfers(Object, srcPath_lg, fileName_lg, dst_scrDiskPath ) 
            detail['localgroupdiskToLocalgroupdisk'] = self.transfers(Object, srcPath_lg, fileName_lg, dst_lgDiskPath )
            detail['localgroupdiskToProddisk'] = self.transfers(Object, srcPath_lg, fileName_lg, dst_prodDiskPath)
            detail['localgroupdiskToDatadisk'] = self.transfers(Object, srcPath_lg, fileName_lg, dst_dataDiskPath)
            
            print "MSG: " + str(copyStatus_lg)
            
            ##Removes files from source and destination part##
            Object.rmFile(srcHost , fileName_lg, srcPath_lg)
            Object.rmFile(dstHost, fileName_lg, dst_scrDiskPath) 
            Object.rmFile(dstHost, fileName_lg, dst_lgDiskPath)
            Object.rmFile(dstHost, fileName_lg, dst_prodDiskPath) 
            Object.rmFile(dstHost, fileName_lg, dst_dataDiskPath)    
        
        else:
            detail['localgroupdiskToScratchdisk'] = stderr_lg
            detail['localgroupdiskToLocalgroupdisk'] = stderr_lg
            detail['localgroupdiskToProddisk'] = stderr_lg
            detail['localgroupdiskToDatadisk'] = stderr_lg
            print "Error msg: " + str(stderr_lg)
        
        os.remove(os.path.abspath(fileName_scr))       
        os.remove(os.path.abspath(fileName_lg)) 
        
        return detail
        
         
    def extractData(self):
        
        data = {
            'dataset': "Hi",
            'status': 1
        }        
        
        Object = GridSRMCopyHandler()  
                
        #GOEGRID->GOEGRID transfers      
        srcHostSite1 = self.config['site1_host']
        dstHostSite1 = self.config['site1_host']
        
        Object.setHostsAndPorts(srcHostSite1, "8443", dstHostSite1, "8443", "srm")
        Object.ping()
       # Object.showFiles("/srm/managerv2?SFN=/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_srm_haykuhi/")
       # Object.mkDir("/srm/managerv2?SFN=/pnfs/gwdg.de/data/atlas/atlasscratchdisk/aaaaaa")
       # Object.rmDir("/srm/managerv2?SFN=/pnfs/gwdg.de/data/atlas/atlasscratchdisk/aaaaaa")
       # Object.copyFromLocalToRemote("/home/haykuhi/Desktop/a.txt", "/srm/managerv2?SFN=/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_srm_haykuhi/dd.txt")
       # Object.copyFile("/srm/managerv2?SFN=/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_srm_haykuhi/dd.txt", "/srm/managerv2?SFN=/pnfs/gwdg.de/data/atlas/atlaslocalgroupdisk/aaa/kkk.txt")
       # Object.rmFile("dd.txt","/srm/managerv2?SFN=/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_srm_haykuhi/" )
        
        """
        ##Crate a folder with a subfolder in GOEGRID-SCRATCHDISK##
        scrtDiskPathSite1 = self.config['site1_scratchdisk_path']
        stdout_mkdir_scratchdisk, stderr_mkdir_scratchdisk = Object.mkDir(scrtDiskPathSite1)
        if stdout_mkdir_scratchdisk:
           Object.mkDir(scrtDiskPathSite1+ "test/")
        
        if stderr_mkdir_scratchdisk:
           print stderr_mkdir_scratchdisk
           
        ##Crate a folder with a subfolder in GOEGRID-LOCALGROUPDISK##
        lGrDiskPathSite1 = self.config['site1_localgroupdisk_path']
        stdout_mkdir_localgroupdisk, stderr_mkdir_localgroupdisk = Object.mkDir(lGrDiskPathSite1)
        if stdout_mkdir_localgroupdisk:
           Object.mkDir(lGrDiskPathSite1 + "test/")
        
        if stderr_mkdir_localgroupdisk:
           print stderr_mkdir_localgroupdisk 
           
        prodDiskPathSite1 = self.config['site1_proddisk_path'] 
        dataDiskPathSite1 = self.config['site1_datadisk_path'] 
        
        siteName  = self.config['site1_name']
        
        detail1 = self.spaceTokenStatus(siteName,
                                        Object,
                                        srcHostSite1, 
                                        dstHostSite1, 
                                        scrtDiskPathSite1, 
                                        lGrDiskPathSite1, 
                                        scrtDiskPathSite1 + "test/", 
                                        lGrDiskPathSite1 + "test/", 
                                        prodDiskPathSite1, 
                                        dataDiskPathSite1)               
                
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[0] = detail1
        
        #GOEGRID->WUPPERTAL transfers                       
        scrtDiskPathSite3 = self.config['site3_scratchdisk_path']
        lGrDiskPathSite3 = self.config['site3_localgroupdisk_path']
        prodDiskPathSite3 = self.config['site3_proddisk_path']
        dataDiskPathSite3 = self.config['site3_datadisk_path']
        
        srcHostSite1 = self.config['site1_host']
        dstHostSite3 = self.config['site3_host']
        
        Object.setHostsAndPorts(srcHostSite1, "", dstHostSite3, "")               
        Object.mkDir(scrtDiskPathSite3)
        Object.mkDir(lGrDiskPathSite3)  
        
        siteName  = self.config['site3_name1']
        detail3 = self.spaceTokenStatus(siteName,
                                        Object,
                                        srcHostSite1, 
                                        dstHostSite3, 
                                        scrtDiskPathSite1, 
                                        lGrDiskPathSite1, 
                                        scrtDiskPathSite3, 
                                        lGrDiskPathSite3, 
                                        prodDiskPathSite3, 
                                        dataDiskPathSite3)               
                
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[1] = detail3  
        
        
        #WUPPERTAL->GOEGRID transfers
        srcHostSite3 = self.config['site3_host']
        dstHostSite1 = self.config['site1_host']     
        siteName  = self.config['site3_name2']    
       
        detail3 = self.spaceTokenStatus(siteName,
                                        Object,
                                        srcHostSite3, 
                                        dstHostSite1, 
                                        scrtDiskPathSite3, 
                                        lGrDiskPathSite3,
                                        scrtDiskPathSite1 + "test/", 
                                        lGrDiskPathSite1 + "test/", 
                                        prodDiskPathSite1, 
                                        dataDiskPathSite1)
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[2] = detail3 
        
        ##Removes created folders and subfolders##
        Object.rmDir(srcHostSite1, scrtDiskPathSite1 + "test/")
        Object.rmDir(srcHostSite1, lGrDiskPathSite1 + "test/")
        Object.rmDir(srcHostSite1, scrtDiskPathSite1)
        Object.rmDir(srcHostSite1, lGrDiskPathSite1)
        
        Object.rmDir(dstHostSite3, scrtDiskPathSite3)
        Object.rmDir(dstHostSite3, lGrDiskPathSite3)   
        """                         
        return data
    
    def prepareAcquisition(self):
        
        self.details_table_db_value_list = []
             
    def fillSubtables(self, parent_id):
        
        self.subtables['site_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])
              
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        """
        self.dataset['site1Name'] = self.config['site1_name']                
        self.dataset['site1_space_token_scratchdisk'] = self.config['site1_space_token_scratchdisk']
        self.dataset['site1_space_token_localgroupdisk'] = self.config['site1_space_token_localgroupdisk']
        self.dataset['site1_space_token_proddisk'] = self.config['site1_space_token_proddisk']
        self.dataset['site1_space_token_datadisk'] = self.config['site1_space_token_datadisk']
        
        self.dataset['site3Name1'] = self.config['site3_name1']   
        self.dataset['site3Name2'] = self.config['site3_name2']            
        self.dataset['site3_space_token_scratchdisk'] = self.config['site3_space_token_scratchdisk']
        self.dataset['site3_space_token_localgroupdisk'] = self.config['site3_space_token_localgroupdisk']
        self.dataset['site3_space_token_proddisk'] = self.config['site3_space_token_proddisk']
        self.dataset['site3_space_token_datadisk'] = self.config['site3_space_token_datadisk']
            
        max_id_number1 = func.max(self.subtables['site_transfers'].c.id).select().where(self.subtables['site_transfers'].c.siteName == str(self.config['site1_name'])).execute().scalar()        
        #self.dataset['aaa'] = max_id_number1                
        details1 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number1).execute().fetchall()
        data['site1Tosite1'] = map(dict, details1)
      
        max_id_number2 = func.max(self.subtables['site_transfers'].c.id).select().where(self.subtables['site_transfers'].c.siteName ==str(self.config['site2_name1'])).execute().scalar()        
        #self.dataset['bbb'] = max_id_number2               
        details2 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number2).execute().fetchall()
        data['site1Tosite2'] = map(dict, details2)
        
        max_id_number3 = func.max(self.subtables['site_transfers'].c.id).select().where(self.subtables['site_transfers'].c.siteName ==str(self.config['site2_name2'])).execute().scalar()        
        #self.dataset['ccc'] = max_id_number3               
        details3 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number3).execute().fetchall()
        data['site2Tosite1'] = map(dict, details3)
        """ 
        return data
    

