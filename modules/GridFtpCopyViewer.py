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
        
        Object = GridFtpCopyHandler()  
         
        #GOEGRID->GOEGRID transfers      
        srcHostSite1 = self.config['goegrid_host']
        dstHostSite1 = self.config['goegrid_host']
        
        Object.setHostsAndPorts(srcHostSite1, "", dstHostSite1, "")
        
        ##Crate a folder with a subfolder in GOEGRID-SCRATCHDISK##
        scrtDiskPathSite1 = self.config['goegrid_scratchdisk_path']
        stdout_mkdir_scratchdisk, stderr_mkdir_scratchdisk = Object.mkDir(scrtDiskPathSite1)
        if stdout_mkdir_scratchdisk:
           Object.mkDir(scrtDiskPathSite1+ "test/")
        
        if stderr_mkdir_scratchdisk:
           print stderr_mkdir_scratchdisk
           
        ##Crate a folder with a subfolder in GOEGRID-LOCALGROUPDISK##
        lGrDiskPathSite1 = self.config['goegrid_localgroupdisk_path']
        stdout_mkdir_localgroupdisk, stderr_mkdir_localgroupdisk = Object.mkDir(lGrDiskPathSite1)
        if stdout_mkdir_localgroupdisk:
           Object.mkDir(lGrDiskPathSite1 + "test/")
        
        if stderr_mkdir_localgroupdisk:
           print stderr_mkdir_localgroupdisk 
           
        prodDiskPathSite1 = self.config['goegrid_proddisk_path'] 
        dataDiskPathSite1 = self.config['goegrid_datadisk_path'] 
        
        
        detail1 = self.spaceTokenStatus("GoeGrid-GoeGrid",
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
        scrtDiskPathSite2 = self.config['wuppertal_scratchdisk_path']
        lGrDiskPathSite2 = self.config['wuppertal_localgroupdisk_path']
        prodDiskPathSite2 = self.config['wuppertal_proddisk_path']
        dataDiskPathSite2 = self.config['wuppertal_datadisk_path']
        
        srcHostSite1 = self.config['goegrid_host']
        dstHostSite2 = self.config['wupperal_host']
        
        Object.setHostsAndPorts(srcHostSite1, "", dstHostSite2, "")               
        Object.mkDir(scrtDiskPathSite2)
        Object.mkDir(lGrDiskPathSite2)  
        
        detail2 = self.spaceTokenStatus("GoeGrid-Wuppertal",
                                               Object,
                                               srcHostSite1, 
                                               dstHostSite2, 
                                               scrtDiskPathSite1, 
                                               lGrDiskPathSite1, 
                                               scrtDiskPathSite2, 
                                               lGrDiskPathSite2, 
                                               prodDiskPathSite2, 
                                               dataDiskPathSite2)               
                
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[1] = detail2  
        
        
        #WUPPERTAL->GOEGRID transfers
        srcHostSite2 = self.config['wupperal_host']
        dstHostSite1 = self.config['goegrid_host']         
       
        detail3 = self.spaceTokenStatus('Wuppertal-GoeGrid',
                                                Object,
                                                srcHostSite2, 
                                                dstHostSite1, 
                                                scrtDiskPathSite2, 
                                                lGrDiskPathSite2,
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
        
        Object.rmDir(dstHostSite2, scrtDiskPathSite2)
        Object.rmDir(dstHostSite2, lGrDiskPathSite2)
        
                                 
        return data
    
    def prepareAcquisition(self):
        
        self.details_table_db_value_list = []
             
    def fillSubtables(self, parent_id):
        
        self.subtables['site_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])
              
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
            
        max_id_number1 = func.max(self.subtables['site_transfers'].c.id).select().where(self.subtables['site_transfers'].c.siteName == 'GoeGrid-GoeGrid').execute().scalar()        
        #self.dataset['aaa'] = max_id_number1                
        details1 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number1).execute().fetchall()
        data['goegrid_goegrid'] = map(dict, details1)
      
        max_id_number2 = func.max(self.subtables['site_transfers'].c.id).select().where(self.subtables['site_transfers'].c.siteName == 'GoeGrid-Wuppertal').execute().scalar()        
        #self.dataset['bbb'] = max_id_number2               
        details2 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number2).execute().fetchall()
        data['goegrid_wuppertal'] = map(dict, details2)
        
        max_id_number3 = func.max(self.subtables['site_transfers'].c.id).select().where(self.subtables['site_transfers'].c.siteName == 'Wuppertal-GoeGrid').execute().scalar()        
        #self.dataset['ccc'] = max_id_number3               
        details3 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number3).execute().fetchall()
        data['wuppertal_goegrid'] = map(dict, details3)

        return data
    
