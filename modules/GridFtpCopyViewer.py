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
        'goegrid_goegrid_transfers': ([
        Column('scratchdiskToScratchdisk', TEXT),
        Column('scratchdiskToLocalgroupdisk', TEXT),
        Column('scratchdiskToProddisk', TEXT),
        Column('scratchdiskToDatadisk', TEXT),
        Column('localgroupdiskToScratchdisk', TEXT),
        Column('localgroupdiskToLocalgroupdisk', TEXT),
        Column('localgroupdiskToProddisk', TEXT),
        Column('localgroupdiskToDatadisk', TEXT),
    ], []),
        'goegrid_wuppertal_transfers':([
        Column('scratchdiskToScratchdisk', TEXT),
        Column('scratchdiskToLocalgroupdisk', TEXT),
        Column('scratchdiskToProddisk', TEXT),
        Column('scratchdiskToDatadisk', TEXT),
        Column('localgroupdiskToScratchdisk', TEXT),
        Column('localgroupdiskToLocalgroupdisk', TEXT),
        Column('localgroupdiskToProddisk', TEXT),
        Column('localgroupdiskToDatadisk', TEXT),
    ], []),
        'wuppertal_goegrid_transfers':([
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
                                                
    
    def spaceTokenStatus(self, Object, srcHost, dstHost, scr_scrDiskPath, src_lgDiskPath, dst_scrDiskPath, dst_lgDiskPath, dst_prodDiskPath, dst_dataDiskPath):
        
        copyStatus_scr, stderr_scr, srcPath_scr, fileName_scr = Object.createFileInSrcPath (srcHost, scr_scrDiskPath)
        copyStatus_lg, stderr_lg, srcPath_lg, fileName_lg = Object.createFileInSrcPath (srcHost, src_lgDiskPath) 
               
        detail = {}
         
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
            
        return detail
        
         
    def extractData(self):
        data = {
            'dataset': "Hi",
            'status': 1
        }
        
        
        Object = GridFtpCopyHandler()      
        
        #GOEGRID->GOEGRID transfers 
        goegridSrcHost = self.config['goegrid_host']
        goegridDstHost = self.config['goegrid_host']
        
        Object.setHostsAndPorts(goegridSrcHost, "", goegridDstHost, "")
        
        ##Crate a folder with a subfolder in GOEGRID-SCRATCHDISK##
        goegridScrtDiskPath = self.config['goegrid_scratchdisk_path']
        stdout_mkdir_scratchdisk, stderr_mkdir_scratchdisk = Object.mkDir(goegridScrtDiskPath)
        if stdout_mkdir_scratchdisk:
           Object.mkDir(goegridScrtDiskPath+ "test/")
        
        if stderr_mkdir_scratchdisk:
           print stderr_mkdir_scratchdisk
           
        ##Crate a folder with a subfolder in GOEGRID-LOCALGROUPDISK##
        goegridLGrDiskPath = self.config['goegrid_localgroupdisk_path']
        stdout_mkdir_localgroupdisk, stderr_mkdir_localgroupdisk = Object.mkDir(goegridLGrDiskPath)
        if stdout_mkdir_localgroupdisk:
           Object.mkDir(goegridLGrDiskPath + "test/")
        
        if stderr_mkdir_localgroupdisk:
           print stderr_mkdir_localgroupdisk 
           
        goegridProdDiskPath = self.config['goegrid_proddisk_path'] 
        goegridDataDiskPath = self.config['goegrid_datadisk_path'] 
        
        
        detail_goe_goe = self.spaceTokenStatus(Object,
                                               goegridSrcHost, 
                                               goegridDstHost, 
                                               goegridScrtDiskPath, 
                                               goegridLGrDiskPath, 
                                               goegridScrtDiskPath + "test/", 
                                               goegridLGrDiskPath + "test/", 
                                               goegridProdDiskPath, 
                                               goegridDataDiskPath)
               
                
        self.goegrid_details_table_db_value_list.append({})
        self.goegrid_details_table_db_value_list[0] = detail_goe_goe
        
        
        #GOEGRID->WUPPERTAL transfers                       
        wuppertalScrtDiskPath = self.config['wuppertal_scratchdisk_path']
        wuppertalLGrDiskPath = self.config['wuppertal_localgroupdisk_path']
        wuppertalProdDiskPath = self.config['wuppertal_proddisk_path']
        wuppertalDataDiskPath = self.config['wuppertal_datadisk_path']
        
        goegridSrcHost = self.config['goegrid_host']
        wuppertalDstHost = self.config['wupperal_host']
        
        Object.setHostsAndPorts(goegridSrcHost, "", wuppertalDstHost, "")               
        Object.mkDir(wuppertalScrtDiskPath)
        Object.mkDir(wuppertalLGrDiskPath)  
        
        detail_goe_wupp = self.spaceTokenStatus(Object,
                                               goegridSrcHost, 
                                               wuppertalDstHost, 
                                               goegridScrtDiskPath, 
                                               goegridLGrDiskPath, 
                                               wuppertalScrtDiskPath, 
                                               wuppertalLGrDiskPath, 
                                               wuppertalProdDiskPath, 
                                               wuppertalDataDiskPath)
               
                
        self.goegrid_wuppertal_details_table_db_value_list.append({})
        self.goegrid_wuppertal_details_table_db_value_list[0] = detail_goe_wupp  
        
        #WUPPERTAL->GOEGRID transfers
        wuppertalSrcHost = self.config['wupperal_host']
        goegridDstHost = self.config['goegrid_host']         
       
        detail_wupp_goe = self.spaceTokenStatus(Object,
                                               wuppertalSrcHost, 
                                               goegridDstHost, 
                                               wuppertalScrtDiskPath, 
                                               wuppertalLGrDiskPath,
                                               goegridScrtDiskPath + "test/", 
                                               goegridLGrDiskPath + "test/", 
                                               goegridProdDiskPath, 
                                               goegridDataDiskPath)
        
        self.wuppertal_goegrid_details_table_db_value_list.append({})
        self.wuppertal_goegrid_details_table_db_value_list[0] = detail_wupp_goe 
 
        ##Removes created folders and subfolders##
        Object.rmDir(goegridSrcHost, goegridScrtDiskPath + "test/")
        Object.rmDir(goegridSrcHost, goegridLGrDiskPath + "test/")
        Object.rmDir(goegridSrcHost, goegridScrtDiskPath)
        Object.rmDir(goegridSrcHost, goegridLGrDiskPath)
        
        Object.rmDir(wuppertalDstHost, wuppertalScrtDiskPath)
        Object.rmDir(wuppertalDstHost, wuppertalLGrDiskPath)
                          
        return data
    
    def prepareAcquisition(self):
        
         self.goegrid_details_table_db_value_list = []
         self.goegrid_wuppertal_details_table_db_value_list = []
         self.wuppertal_goegrid_details_table_db_value_list = []
             
    def fillSubtables(self, parent_id):
        self.subtables['goegrid_goegrid_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.goegrid_details_table_db_value_list])
        self.subtables['goegrid_wuppertal_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.goegrid_wuppertal_details_table_db_value_list])
        self.subtables['wuppertal_goegrid_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.wuppertal_goegrid_details_table_db_value_list])
                
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details = self.subtables['goegrid_goegrid_transfers'].select().where(self.subtables['goegrid_goegrid_transfers'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['goegrid_goegrid'] = map(dict, details)
        
        details2 = self.subtables['goegrid_wuppertal_transfers'].select().where(self.subtables['goegrid_wuppertal_transfers'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['goegrid_wuppertal'] = map(dict, details2)

        details3 = self.subtables['wuppertal_goegrid_transfers'].select().where(self.subtables['wuppertal_goegrid_transfers'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['wuppertal_goegrid'] = map(dict, details3)        
        
        return data
    
