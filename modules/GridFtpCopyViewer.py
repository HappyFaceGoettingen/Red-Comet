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

  
  
    def transfers(self, Obj, srcFilePathOnScr, srcFilePathOnLG, dstScratchDiskPath, dstLocalGroupDiskPath, dstProdDiskPath, dstDataDiskPath):
        
        ##Create file in GOEGRID-SCRATCHDISK##
        scratchdiskStdout, scratchdiskStderr, scratchdiskSrcPath, scratchdiskGeneratedFile = Obj.createFileInSrcPath(srcFilePathOnScr)

        if scratchdiskStdout: 
           #SCRATCHDISK->SCRATCHDISK
           scratchdiskToScratchdisk = Obj.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, dstScratchDiskPath)
           print scratchdiskToScratchdisk
           
           
           #SCRATCHDISK->LOCALGROUPDISK
           scratchdiskToLocalgroupdisk = Obj.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, dstLocalGroupDiskPath)
           print scratchdiskToLocalgroupdisk
           
           
           #SCRATCHDISK->PRODDISK
           scratchdiskToProddisk = Obj.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, dstProdDiskPath)
           print scratchdiskToProddisk
           
           #SCRATCHDISK->DATADISK
           scratchdiskToDatadisk = Obj.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, dstDataDiskPath)
           print scratchdiskToDatadisk
                    
                    
        if scratchdiskStderr:
           print scratchdiskStderr  
           
        
        ##Create file in GOEGRID-LOCALGROUPDDISK##
        localgroupdiskStdout, localgroupdiskStderr, localgroupdiskSrcPath, localgroupdiskGeneratedFile = Obj.createFileInSrcPath(srcFilePathOnLG)
        if localgroupdiskStdout: 
            
           #LOCALGROUPDDISK->SCRATCHDISK
           localgroupdiskToScratchdisk = Obj.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile,  dstScratchDiskPath)
           print localgroupdiskToScratchdisk
           
           #LOCALGROUPDDISK->LOCALGROUPDISK
           localgroupdiskToLocalgroupdisk = Obj.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, dstLocalGroupDiskPath)
           print localgroupdiskToLocalgroupdisk
                      
           #LOCALGROUPDDISK->PRODDISK
           localgroupdiskToProddisk = Obj.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, dstProdDiskPath)
           print localgroupdiskToProddisk
           
           #LOCALGROUPDDISK->DATADISK
           localgroupdiskToDatadisk = Obj.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, dstDataDiskPath)
           print localgroupdiskToDatadisk
          
        if localgroupdiskStderr:
           print localgroupdiskStderr    
           
           
        detail = {}
          
        detail['scratchdiskToScratchdisk'] = scratchdiskToScratchdisk
        detail['scratchdiskToLocalgroupdisk'] = scratchdiskToLocalgroupdisk 
        detail['scratchdiskToProddisk'] = scratchdiskToProddisk
        detail['scratchdiskToDatadisk'] = scratchdiskToDatadisk
        
        detail['localgroupdiskToScratchdisk'] = localgroupdiskToScratchdisk 
        detail['localgroupdiskToLocalgroupdisk'] = localgroupdiskToLocalgroupdisk
        detail['localgroupdiskToProddisk'] = localgroupdiskToProddisk
        detail['localgroupdiskToDatadisk'] = localgroupdiskToDatadisk
                        
        return detail        
    
    def removeFilesFromSpaceToken (self, Obj, scratchdisk, localgroupdisk):
   
        ##Remove created folder from GOEGRID/WUPPERTAL SCRATCHDISK/LOCALGROUPDISK##
        Obj.rmDirAndSubfolders(scratchdisk)
        Obj.rmDirAndSubfolders(localgroupdisk)    
  
  
    def extractData(self):
        data = {
            'dataset': "Hi",
            'status': 1
        }
        
        #GOEGRID->GOEGRID transfers 
        Object = GridFtpCopyHandler()
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
        
        goegridToGoegridTransfers = self.transfers(
                                                   Object, 
                                                   goegridScrtDiskPath, 
                                                   goegridLGrDiskPath,
                                                   goegridScrtDiskPath+ "test/",
                                                   goegridLGrDiskPath + "test/", 
                                                   goegridProdDiskPath,
                                                   goegridDataDiskPath)
        
        self.goegrid_details_table_db_value_list.append({})
        
        self.goegrid_details_table_db_value_list[0] = goegridToGoegridTransfers

              
        
        #GOEGRID->WUPPERTAL transfers
        goegridSrcHost = self.config['goegrid_host']
        wuppertalDstHost = self.config['wupperal_host']
       
        Object.setHostsAndPorts(goegridSrcHost, "", wuppertalDstHost, "")
        
        wuppertalScrtDiskPath = self.config['wuppertal_scratchdisk_path']
        wuppertalLGrDiskPath = self.config['wuppertal_localgroupdisk_path']
        wuppertalProdDiskPath = self.config['wuppertal_proddisk_path']
        wuppertalDataDiskPath = self.config['wuppertal_datadisk_path']
        
        Object.mkDir(wuppertalScrtDiskPath)
        Object.mkDir(wuppertalLGrDiskPath) 
        
        goegridToWuppertalTransfers = self.transfers(
                                                     Object,
                                                     goegridScrtDiskPath, 
                                                     goegridLGrDiskPath,
                                                     wuppertalScrtDiskPath,
                                                     wuppertalLGrDiskPath,
                                                     wuppertalProdDiskPath,
                                                     wuppertalDataDiskPath )            
                               
        self.goegrid_wuppertal_details_table_db_value_list.append({})
        self.goegrid_wuppertal_details_table_db_value_list[0] = goegridToWuppertalTransfers  
        
        
        #WUPPERTAL->GOEGRID transfers
        wuppertalSrcHost = self.config['wupperal_host']
        goegridDstHost = self.config['goegrid_host']
       
        Object.setHostsAndPorts(wuppertalSrcHost, "", goegridDstHost, "")
        
        wuppertalToGoegridTransfers = self.transfers(
                                                     Object,
                                                     wuppertalScrtDiskPath, 
                                                     wuppertalLGrDiskPath,
                                                     goegridScrtDiskPath + "test/",
                                                     goegridLGrDiskPath + "test/", 
                                                     goegridProdDiskPath,
                                                     goegridDataDiskPath )            
                               
        self.wuppertal_goegrid_details_table_db_value_list.append({})
        self.wuppertal_goegrid_details_table_db_value_list[0] = wuppertalToGoegridTransfers
        
        Object.setHostsAndPorts(goegridSrcHost, "", wuppertalDstHost, "")
        self.removeFilesFromSpaceToken(
                                       Object,
                                       wuppertalScrtDiskPath,
                                       wuppertalLGrDiskPath)
        
        Object.setHostsAndPorts(goegridSrcHost, "", goegridDstHost, "")
        
        self.removeFilesFromSpaceToken(
                                       Object, 
                                       goegridScrtDiskPath,
                                       goegridLGrDiskPath)
                     
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
    
