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
        Column('goegridScratchdiskToScratchdisk', TEXT),
        Column('goegridScratchdiskToLocalgroupdisk', TEXT),
        Column('goegridScratchdiskToProddisk', TEXT),
        Column('goegridScratchdiskToDatadisk', TEXT),
        Column('goegridLocalgroupdiskToScratchdisk', TEXT),
        Column('goegridLocalgroupdiskToLocalgroupdisk', TEXT),
        Column('goegridLocalgroupdiskToProddisk', TEXT),
        Column('goegridLocalgroupdiskToDatadisk', TEXT),
    ], []),
        'goegrid_wuppertal_transfers':([
        Column('goegridScratchdiskToWuppertalScratchdisk', TEXT),
        Column('goegridScratchdiskToWuppertalLocalgroupdisk', TEXT),
        Column('goegridScratchdiskToWuppertalProddisk', TEXT),
        Column('goegridScratchdiskToWuppertalDatadisk', TEXT),
        Column('goegridLocalgroupdiskToWuppertalScratchdisk', TEXT),
        Column('goegridLocalgroupdiskToWuppertalLocalgroupdisk', TEXT),
        Column('goegridLocalgroupdiskToWuppertalProddisk', TEXT),
        Column('goegridLocalgroupdiskToWuppertalDatadisk', TEXT),
    ], [])}

              
  
    def extractData(self):
        data = {
            'dataset': "Hi",
            'status': 1
        }
        
        detail = {}
        #GOEGRID host
        goegridSrcHost = self.config['goegrid_host']
        goegridDstHost = self.config['goegrid_host']
        
        Object = GridFtpCopyHandler()
        Object.setHostsAndPorts(goegridSrcHost, "", goegridDstHost, "")
         
        ##Crate folder in GOEGRID-SCRATCHDISK##
        stdout_mkdir_goe_scratchdisk, stderr_mkdir_goe_scratchdisk = Object.mkDir("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/")
        if stdout_mkdir_goe_scratchdisk:
           Object.mkDir("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/test/")
        
        if stderr_mkdir_goe_scratchdisk:
           print stderr_mkdir_goe_scratchdisk
           
        ##Crate folder in GOEGRID-LOCALGROUPDISK##
        stdout_mkdir_goe_localgroupdisk, stderr_mkdir_goe_localgroupdisk = Object.mkDir("/pnfs/gwdg.de/data/atlas/atlaslocalgroupdisk/test_haykuhi/")
        if stdout_mkdir_goe_localgroupdisk:
           Object.mkDir("/pnfs/gwdg.de/data/atlas/atlaslocalgroupdisk/test_haykuhi/test/")
        
        if stderr_mkdir_goe_localgroupdisk:
           print stderr_mkdir_goe_localgroupdisk           
        
        ##Create file in GOEGRID-SCRATCHDISK##
        scratchdiskStdout, scratchdiskStderr, scratchdiskSrcPath, scratchdiskGeneratedFile = Object.createFileInSrcPath("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/")
        #print "output" + scratchdiskStdout + "\n"
        #print "std_error" + scratchdiskStderr + "\n"
        if scratchdiskStdout: 
           #GOEGRID-SCRATCHDISK->GOEGRID-SCRATCHDISK
           scratchdiskToScratchdisk = Object.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, "/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/test/")
           print scratchdiskToScratchdisk
           
           
           #GOEGRID-SCRATCHDISK->GOEGRID-LOCALGROUPDISK
           scratchdiskToLocalgroupdisk = Object.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, "/pnfs/gwdg.de/data/atlas/atlaslocalgroupdisk/test_haykuhi/test/")
           print scratchdiskToLocalgroupdisk
           
           
           #GOEGRID-SCRATCHDISK->GOEGRID-PRODDISK
           scratchdiskToProddisk = Object.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, "/pnfs/gwdg.de/data/atlas/atlasproddisk/")
           print scratchdiskToProddisk
           
           #GOEGRID-SCRATCHDISK->GOEGRID-DATADISK
           scratchdiskToDatadisk = Object.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, "/pnfs/gwdg.de/data/atlas/atlasdatadisk/")
           print scratchdiskToDatadisk
           
           #GOEGRID-SCRATCHDISK->WUPPERTAL-SCRATCHDISK
           wuppertalDstHost = self.config['wupperal_host']
           Object.setHostsAndPorts(goegridSrcHost, "", wuppertalDstHost, "")
                      
           ##Crate folder in WUPPERTAL-SCRATCHDISK and LOCALGROUPDISK##
           Object.mkDir("/pnfs/physik.uni-wuppertal.de/data/atlas/atlasscratchdisk/user.haykuhi/")
           Object.mkDir("/pnfs/physik.uni-wuppertal.de/data/atlas/atlaslocalgroupdisk/user/test_haykuhi/") 
           
           goegridScratchdiskToWuppertalScratchdisk = Object.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, "/pnfs/physik.uni-wuppertal.de/data/atlas/atlasscratchdisk/user.haykuhi/")
           print goegridScratchdiskToWuppertalScratchdisk
           
           #GOEGRID-SCRATCHDISK->WUPPERTAL-LOCALGROUPDISK
           goegridScratchdiskToWuppertalLocalgroupdisk = Object.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, "/pnfs/physik.uni-wuppertal.de/data/atlas/atlaslocalgroupdisk/user/test_haykuhi/")
           print goegridScratchdiskToWuppertalLocalgroupdisk
           
           #GOEGRID-SCRATCHDISK->WUPPERTAL-PRODDISK
           goegridScratchdiskToWuppertalProddisk = Object.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, "/pnfs/physik.uni-wuppertal.de/data/atlas/atlasproddisk/")
           print goegridScratchdiskToWuppertalProddisk
           
           #GOEGRID-SCRATCHDISK->WUPPERTAL-DATADISK
           goegridScratchdiskToWuppertalDatadisk = Object.copyFileAndCheckExistance(scratchdiskSrcPath, scratchdiskGeneratedFile, "/pnfs/physik.uni-wuppertal.de/data/atlas/atlasdatadisk/")
           print goegridScratchdiskToWuppertalDatadisk
           
        if scratchdiskStderr:
           print scratchdiskStderr    
       
        
        Object.setHostsAndPorts(goegridSrcHost, "", goegridDstHost, "")
        ##Create file in GOEGRID-LOCALGROUPDDISK##
        localgroupdiskStdout, localgroupdiskStderr, localgroupdiskSrcPath, localgroupdiskGeneratedFile = Object.createFileInSrcPath("/pnfs/gwdg.de/data/atlas/atlaslocalgroupdisk/test_haykuhi/")
        #print "output" + localgroupdiskStdout + "\n"
        #print "std_error" + localgroupdiskStderr + "\n"
        if localgroupdiskStdout: 
            
           #GOEGRID-LOCALGROUPDDISK->GOEGRID-SCRATCHDISK
           localgroupdiskToScratchdisk = Object.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, "/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/test/")
           print localgroupdiskToScratchdisk
           
           #GOEGRID-LOCALGROUPDDISK->GOEGRID-LOCALGROUPDISK
           localgroupdiskToLocalgroupdisk = Object.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, "/pnfs/gwdg.de/data/atlas/atlaslocalgroupdisk/test_haykuhi/test/")
           print localgroupdiskToLocalgroupdisk
                      
           #GOEGRID-LOCALGROUPDDISK->GOEGRID-PRODDISK
           localgroupdiskToProddisk = Object.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, "/pnfs/gwdg.de/data/atlas/atlasproddisk/")
           print localgroupdiskToProddisk
           
           #GOEGRID-LOCALGROUPDDISK->GOEGRID-DATADISK
           localgroupdiskToDatadisk = Object.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, "/pnfs/gwdg.de/data/atlas/atlasdatadisk/")
           print localgroupdiskToDatadisk
           
           #GOEGRID-LOCALGROUPDDISK->WUPPERTAL-SCRATCHDISK
           wuppertalDstHost = self.config['wupperal_host']
           Object.setHostsAndPorts(goegridSrcHost, "", wuppertalDstHost, "")
           
           ##Crate folder in WUPPERTAL-SCRATCHDISK and LOCALGROUPDISK##
           Object.mkDir("/pnfs/physik.uni-wuppertal.de/data/atlas/atlasscratchdisk/user.haykuhi/")
           Object.mkDir("/pnfs/physik.uni-wuppertal.de/data/atlas/atlaslocalgroupdisk/user/test_haykuhi/") 

           goegridLocalgroupdiskToWuppertalScratchdisk = Object.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, "/pnfs/physik.uni-wuppertal.de/data/atlas/atlasscratchdisk/user.haykuhi/")
           print goegridLocalgroupdiskToWuppertalScratchdisk           
   
           #GOEGRID-LOCALGROUPDDISK->WUPPERTAL-LOCALGROUPDISK
           goegridLocalgroupdiskToWuppertalLocalgroupdisk = Object.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, "/pnfs/physik.uni-wuppertal.de/data/atlas/atlaslocalgroupdisk/user/test_haykuhi/")
           print goegridLocalgroupdiskToWuppertalLocalgroupdisk
           
           #GOEGRID-LOCALGROUPDDISK->WUPPERTAL-PRODDISK
           goegridLocalgroupdiskToWuppertalProddisk = Object.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, "/pnfs/physik.uni-wuppertal.de/data/atlas/atlasproddisk/")
           print goegridLocalgroupdiskToWuppertalProddisk
           
           
           #GOEGRID-LOCALGROUPDDISK->WUPPERTAL-DATADISK
           goegridLocalgroupdiskToWuppertalProddisk = Object.copyFileAndCheckExistance(localgroupdiskSrcPath, localgroupdiskGeneratedFile, "/pnfs/physik.uni-wuppertal.de/data/atlas/atlasdatadisk/")
           print goegridLocalgroupdiskToWuppertalProddisk
           
          
        if localgroupdiskStderr:
           print localgroupdiskStderr 

        
        
        detail = {}
     
        detail['goegridScratchdiskToScratchdisk'] = scratchdiskToScratchdisk
        detail['goegridScratchdiskToLocalgroupdisk'] = scratchdiskToLocalgroupdisk 
        detail['goegridScratchdiskToProddisk'] = scratchdiskToProddisk
        detail['goegridScratchdiskToDatadisk'] = scratchdiskToDatadisk
        
        detail['goegridLocalgroupdiskToScratchdisk'] = localgroupdiskToScratchdisk 
        detail['goegridLocalgroupdiskToLocalgroupdisk'] = localgroupdiskToLocalgroupdisk
        detail['goegridLocalgroupdiskToProddisk'] = localgroupdiskToProddisk
        detail['goegridLocalgroupdiskToDatadisk'] = localgroupdiskToDatadisk
        
        self.goegrid_details_table_db_value_list.append({})
        self.goegrid_details_table_db_value_list[0] = detail
        
        
        detail2 = {}
     
        detail2['goegridScratchdiskToWuppertalScratchdisk'] = goegridScratchdiskToWuppertalScratchdisk
        detail2['goegridScratchdiskToWuppertalLocalgroupdisk'] = goegridScratchdiskToWuppertalLocalgroupdisk
        detail2['goegridScratchdiskToWuppertalProddisk'] = goegridScratchdiskToWuppertalProddisk
        detail2['goegridScratchdiskToWuppertalDatadisk'] = goegridScratchdiskToWuppertalDatadisk
        
        detail2['goegridLocalgroupdiskToWuppertalScratchdisk'] = goegridLocalgroupdiskToWuppertalScratchdisk 
        detail2['goegridLocalgroupdiskToWuppertalLocalgroupdisk'] = goegridLocalgroupdiskToWuppertalLocalgroupdisk
        detail2['goegridLocalgroupdiskToWuppertalProddisk'] = goegridLocalgroupdiskToWuppertalProddisk
        detail2['goegridLocalgroupdiskToWuppertalDatadisk'] = goegridLocalgroupdiskToWuppertalProddisk
        
        self.wuppertal_details_table_db_value_list.append({})
        self.wuppertal_details_table_db_value_list[0] = detail2
        
        ##Remove created folder from GOEGRID SCRATCHDISK##
        Object.setHostsAndPorts(goegridSrcHost, "", goegridDstHost, "")
        Object.rmDirAndSubfolders("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/")
        
        ##Remove created folder from GOEGRID LOCALGROUPDISK##
        Object.rmDirAndSubfolders("/pnfs/gwdg.de/data/atlas/atlaslocalgroupdisk/test_haykuhi/")     
        
        ##Remove created folder from WUPPERTAL SCRATCHDISK##
        wuppertalDstHost = self.config['wupperal_host']
        Object.setHostsAndPorts(goegridSrcHost, "", wuppertalDstHost, "")
        Object.rmDirAndSubfolders("/pnfs/physik.uni-wuppertal.de/data/atlas/atlasscratchdisk/user.haykuhi/") 
        
        ##Remove created folder from WUPPERTAL LOCALGROUPDISK##
        Object.rmDirAndSubfolders("/pnfs/physik.uni-wuppertal.de/data/atlas/atlaslocalgroupdisk/user/test_haykuhi/")
              
        return data
    
    def prepareAcquisition(self):
        
         self.goegrid_details_table_db_value_list = []
         self.wuppertal_details_table_db_value_list = []
             
    def fillSubtables(self, parent_id):
        self.subtables['goegrid_goegrid_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.goegrid_details_table_db_value_list])
        self.subtables['goegrid_wuppertal_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.wuppertal_details_table_db_value_list])
                
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details = self.subtables['goegrid_goegrid_transfers'].select().where(self.subtables['goegrid_goegrid_transfers'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['goegridToGoegridTransfers'] = map(dict, details)
        
        details2 = self.subtables['goegrid_wuppertal_transfers'].select().where(self.subtables['goegrid_wuppertal_transfers'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['goegridToWuppertalTransfers'] = map(dict, details2)        
        
        return data
    
