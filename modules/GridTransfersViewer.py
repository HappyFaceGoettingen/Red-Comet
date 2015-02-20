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
from hf.gridtoolkit.GridSRMCopyHandler import GridSRMCopyHandler
from hf.gridtoolkit.GridLCGCopyHandler import GridLCGCopyHandler

class GridTransfersViewer(hf.module.ModuleBase):
    
    config_keys = {
        'test': ('GridTransfersViewer', '100')
    }
    
    config_hint = ''
    
    table_columns = [
        Column('status', TEXT),
    ], []

    subtable_columns = {
        'site_transfers': ([
        Column('siteName', TEXT),   
        Column('transferType', TEXT),                  
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

  
    def transfers_uberftp(self, Obj, srcPath, fileToCopy, dstPath):        
        transfer_status = Obj.copyFileAndCheckExistance(srcPath, fileToCopy, dstPath)              
        return transfer_status
    
    def transfers_srm(self, Obj, srcPath, fileToCopy, dstPath):        
        transfer_status = Obj.copyFileAndCheckExistance(srcPath, fileToCopy, dstPath)              
        return transfer_status                 
                                                
    
    def spaceTokenStatus(self, siteName, Object_uberftp, Object_srm, srcHost, dstHost, srcPort, dstPort, scr_scrDiskPath, src_lgDiskPath, dst_scrDiskPath, dst_lgDiskPath, dst_prodDiskPath, dst_dataDiskPath):
        
        copyStatus_scr, stderr_scr, srcPath_scr, fileName_scr = Object_uberftp.createFileInSrcPath (srcHost, scr_scrDiskPath)
        copyStatus_lg, stderr_lg, srcPath_lg, fileName_lg = Object_uberftp.createFileInSrcPath (srcHost, src_lgDiskPath) 
               
        detail_uberftp = {}
        detail_srm = {}
        
        detail_uberftp['siteName'] = siteName 
        detail_uberftp['transferType'] = "UberFTP" 
        
        detail_srm['siteName'] = siteName 
        detail_srm['transferType'] = "SRM"
        
        if copyStatus_scr == 0:               
            detail_uberftp['scratchdiskToScratchdisk'] = self.transfers_uberftp(Object_uberftp, srcPath_scr, fileName_scr, dst_scrDiskPath )
            detail_srm['scratchdiskToScratchdisk'] = self.transfers_srm(Object_srm, srcPath_scr, fileName_scr, dst_scrDiskPath )
            
            detail_uberftp['scratchdiskToLocalgroupdisk'] = self.transfers_uberftp(Object_uberftp, srcPath_scr, fileName_scr, dst_lgDiskPath) 
            detail_srm['scratchdiskToLocalgroupdisk'] = self.transfers_srm(Object_srm, srcPath_scr, fileName_scr, dst_lgDiskPath )
            
            detail_uberftp['scratchdiskToProddisk'] = self.transfers_uberftp(Object_uberftp, srcPath_scr, fileName_scr, dst_prodDiskPath )
            detail_srm['scratchdiskToProddisk'] = self.transfers_srm(Object_srm, srcPath_scr, fileName_scr, dst_prodDiskPath )
            
            detail_uberftp['scratchdiskToDatadisk'] = self.transfers_uberftp(Object_uberftp, srcPath_scr, fileName_scr, dst_dataDiskPath )            
            detail_srm['scratchdiskToDatadisk'] = self.transfers_srm(Object_srm, srcPath_scr, fileName_scr, dst_dataDiskPath )
                        
            print "MSG: " + str(copyStatus_scr)
            
            """
            ##Removes files from source and destination part##
            Object_uberftp.rmFile(srcHost , fileName_scr, srcPath_scr)
            Object_uberftp.rmFile(dstHost, fileName_scr, dst_scrDiskPath) 
            Object_uberftp.rmFile(dstHost, fileName_scr, dst_lgDiskPath)     
            Object_uberftp.rmFile(dstHost, fileName_scr, dst_prodDiskPath) 
            Object_uberftp.rmFile(dstHost, fileName_scr, dst_dataDiskPath)   
            
            Object_srm.rmFile("srm_" + fileName_scr, dst_scrDiskPath) 
            Object_srm.rmFile("srm_" + fileName_scr, dst_lgDiskPath)     
            Object_srm.rmFile("srm_" + fileName_scr, dst_prodDiskPath) 
            Object_srm.rmFile("srm_" + fileName_scr, dst_dataDiskPath) 
            """
        
            
        else:
            detail_uberftp['scratchdiskToScratchdisk'] = stderr_scr
            detail_srm['scratchdiskToScratchdisk'] = stderr_scr
            
            detail_uberftp['scratchdiskToLocalgroupdisk'] = stderr_scr
            detail_srm['scratchdiskToLocalgroupdisk'] = stderr_scr
                                
            detail_uberftp['scratchdiskToProddisk'] = stderr_scr
            detail_srm['scratchdiskToProddisk'] = stderr_scr
            
            detail_uberftp['scratchdiskToDatadisk'] = stderr_scr
            detail_srm['scratchdiskToDatadisk'] = stderr_scr       
        
        
            print "Error msg: " + str(stderr_scr)
        
        if copyStatus_lg == 0:        
            detail_uberftp['localgroupdiskToScratchdisk'] = self.transfers_uberftp(Object_uberftp, srcPath_lg, fileName_lg, dst_scrDiskPath ) 
            detail_srm['localgroupdiskToScratchdisk'] = self.transfers_srm(Object_srm, srcPath_scr, fileName_scr, dst_scrDiskPath )
            
            detail_uberftp['localgroupdiskToLocalgroupdisk'] = self.transfers_uberftp(Object_uberftp, srcPath_lg, fileName_lg, dst_lgDiskPath )
            detail_srm['localgroupdiskToLocalgroupdisk'] = self.transfers_srm(Object_srm, srcPath_scr, fileName_scr, dst_lgDiskPath )
            
            detail_uberftp['localgroupdiskToProddisk'] = self.transfers_uberftp(Object_uberftp, srcPath_lg, fileName_lg, dst_prodDiskPath)
            detail_srm['localgroupdiskToProddisk'] = self.transfers_srm(Object_srm, srcPath_scr, fileName_scr, dst_prodDiskPath )
            
            detail_uberftp['localgroupdiskToDatadisk'] = self.transfers_uberftp(Object_uberftp, srcPath_lg, fileName_lg, dst_dataDiskPath)
            detail_srm['localgroupdiskToDatadisk'] = self.transfers_srm(Object_srm, srcPath_scr, fileName_scr, dst_dataDiskPath )
            
            print "MSG: " + str(copyStatus_lg)
            
            ##Removes files from source and destination part##
            """
            Object_uberftp.rmFile(srcHost , fileName_lg, srcPath_lg)
            Object_uberftp.rmFile(dstHost, fileName_lg, dst_scrDiskPath) 
            Object_uberftp.rmFile(dstHost, fileName_lg, dst_lgDiskPath)
            Object_uberftp.rmFile(dstHost, fileName_lg, dst_prodDiskPath) 
            Object_uberftp.rmFile(dstHost, fileName_lg, dst_dataDiskPath) 
            
            Object_srm.rmFile("srm_" + fileName_lg, dst_scrDiskPath) 
            Object_srm.rmFile("srm_" + fileName_lg, dst_lgDiskPath)     
            Object_srm.rmFile("srm_" + fileName_lg, dst_prodDiskPath) 
            Object_srm.rmFile("srm_" + fileName_lg, dst_dataDiskPath)   
            """ 
        
        else:
            detail_uberftp['localgroupdiskToScratchdisk'] = stderr_lg
            detail_srm['localgroupdiskToScratchdisk'] = stderr_lg
            
            detail_uberftp['localgroupdiskToLocalgroupdisk'] = stderr_lg
            detail_srm['localgroupdiskToLocalgroupdisk'] = stderr_lg
            
            detail_uberftp['localgroupdiskToProddisk'] = stderr_lg
            detail_srm['localgroupdiskToProddisk'] = stderr_lg
            
            detail_uberftp['localgroupdiskToDatadisk'] = stderr_lg
            detail_srm['localgroupdiskToDatadisk'] = stderr_lg
            
            print "Error msg: " + str(stderr_lg)
        
        #os.remove(os.path.abspath(fileName_scr))       
        #os.remove(os.path.abspath(fileName_lg)) 
        
        return detail_uberftp, detail_srm
        
         
    def extractData(self):
        
        data = {
            'dataset': "Hi",
            'status': 1
        }        
        
        Object_uberftp = GridFtpCopyHandler()
        Object_srm = GridSRMCopyHandler()  
       
        #GOEGRID->GOEGRID transfers   
        srcHostSite1 = self.config['site1_host']
        dstHostSite1 = self.config['site1_host']
        
        srcPortSite1 = self.config['site1_port']
        dstPortSite1 = self.config['site1_port']
        
        Object_uberftp.setHostsAndPorts(srcHostSite1, "", dstHostSite1, "")
        Object_srm.setHostsAndPorts(srcHostSite1, srcPortSite1, dstHostSite1, dstPortSite1, "srm")
        
        Object_lcg = GridLCGCopyHandler()
        Object_lcg.setHostsAndPorts(srcHostSite1, srcPortSite1, dstHostSite1, dstPortSite1, "srm")
        Object_lcg.showFiles("/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi/")
        
        
        """
        ##Crate a folder with a subfolder in GOEGRID-SCRATCHDISK##
        scrtDiskPathSite1 = self.config['site1_scratchdisk_path']
        stdout_mkdir_scratchdisk, stderr_mkdir_scratchdisk = Object_uberftp.mkDir(scrtDiskPathSite1)
        if stdout_mkdir_scratchdisk:
           Object_uberftp.mkDir(scrtDiskPathSite1+ "test/")
        
        if stderr_mkdir_scratchdisk:
           print stderr_mkdir_scratchdisk
           
        ##Crate a folder with a subfolder in GOEGRID-LOCALGROUPDISK##
        lGrDiskPathSite1 = self.config['site1_localgroupdisk_path']
        stdout_mkdir_localgroupdisk, stderr_mkdir_localgroupdisk = Object_uberftp.mkDir(lGrDiskPathSite1)
        if stdout_mkdir_localgroupdisk:
           Object_uberftp.mkDir(lGrDiskPathSite1 + "test/")
        
        if stderr_mkdir_localgroupdisk:
           print stderr_mkdir_localgroupdisk 
           
        prodDiskPathSite1 = self.config['site1_proddisk_path'] 
        dataDiskPathSite1 = self.config['site1_datadisk_path'] 
        
        siteName  = self.config['site1_name']        
        
        detail1_uberftp, detail1_srm = self.spaceTokenStatus(siteName,
                                        Object_uberftp,
                                        Object_srm,
                                        srcHostSite1, 
                                        dstHostSite1,
                                        srcPortSite1,
                                        dstPortSite1, 
                                        scrtDiskPathSite1, 
                                        lGrDiskPathSite1, 
                                        scrtDiskPathSite1 + "test/", 
                                        lGrDiskPathSite1 + "test/", 
                                        prodDiskPathSite1, 
                                        dataDiskPathSite1)               
                
        self.details_table_db_value_list.append({})        
        self.details_table_db_value_list[0] = detail1_uberftp
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[1] = detail1_srm
        

        
        
        
        
        #GOEGRID->Wuppertal transfers                       
        scrtDiskPathSite2 = self.config['site2_scratchdisk_path']
        lGrDiskPathSite2 = self.config['site2_localgroupdisk_path']
        prodDiskPathSite2 = self.config['site2_proddisk_path']
        dataDiskPathSite2 = self.config['site2_datadisk_path']
        
        srcHostSite1 = self.config['site1_host']
        dstHostSite2 = self.config['site2_host']
        
        srcPortSite1 = self.config['site1_port']
        dstPortSite2 = self.config['site2_port']
        
        
        Object_uberftp.setHostsAndPorts(srcHostSite1, "", dstHostSite2, "")
        Object_srm.setHostsAndPorts(srcHostSite1, srcPortSite1, dstHostSite2, dstPortSite2, "srm")
                    
        Object_uberftp.mkDir(scrtDiskPathSite2)
        Object_uberftp.mkDir(lGrDiskPathSite2)  
        
        siteName  = self.config['site2_name1']     
           
        
        detail2_uberftp, detail2_srm = self.spaceTokenStatus(siteName,
                                        Object_uberftp,
                                        Object_srm,
                                        srcHostSite1, 
                                        dstHostSite2,
                                        srcPortSite1,
                                        dstPortSite2, 
                                        scrtDiskPathSite1, 
                                        lGrDiskPathSite1, 
                                        scrtDiskPathSite2, 
                                        lGrDiskPathSite2, 
                                        prodDiskPathSite2, 
                                        dataDiskPathSite2)               
                
        self.details_table_db_value_list.append({})        
        self.details_table_db_value_list[2] = detail2_uberftp
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[3] = detail2_srm  
        
        
        
        """
        #Wuppertal->GOEGRID transfers
        """
        srcHostSite2 = self.config['site2_host']    
        dstHostSite2 = self.config['site2_host'] 
        
        dstHostSite1 = self.config['site1_host']     
        
        siteName  = self.config['site2_name2'] 
        
        srcPortSite2 = self.config['site2_host']
        dstPortSite1 = self.config['site1_port']  
        
        Object_uberftp.setHostsAndPorts(srcHostSite2, "", dstHostSite2, "")
        Object_srm.setHostsAndPorts(srcHostSite2, srcPortSite2, dstHostSite1, dstPortSite1, "srm") 
        
        scrtDiskPathSite1 = self.config['site1_scratchdisk_path']
        lGrDiskPathSite1 = self.config['site1_localgroupdisk_path']
        prodDiskPathSite1 = self.config['site1_proddisk_path']
        dataDiskPathSite1 = self.config['site1_datadisk_path']
        
        
        scrtDiskPathSite2 = self.config['site2_scratchdisk_path']
        stdout_mkdir_scratchdisk, stderr_mkdir_scratchdisk = Object_uberftp.mkDir(scrtDiskPathSite2)
        if stdout_mkdir_scratchdisk:
           Object_uberftp.mkDir(scrtDiskPathSite2+ "test/")
        
        if stderr_mkdir_scratchdisk:
           print stderr_mkdir_scratchdisk
           
        ##Crate a folder with a subfolder in GOEGRID-LOCALGROUPDISK##
        lGrDiskPathSite2 = self.config['site2_localgroupdisk_path']
        stdout_mkdir_localgroupdisk, stderr_mkdir_localgroupdisk = Object_uberftp.mkDir(lGrDiskPathSite2)
        if stdout_mkdir_localgroupdisk:
           Object_uberftp.mkDir(lGrDiskPathSite2 + "test/")
        
        if stderr_mkdir_localgroupdisk:
           print stderr_mkdir_localgroupdisk 
           
        Object_uberftp.setHostsAndPorts(srcHostSite2, "", dstHostSite1, "")
        
        print "\n"
        print siteName
        print "\n"
        print srcHostSite2
        print "\n" 
        print dstHostSite1
        print "\n" 
        print srcPortSite2
        print "\n" 
        print dstPortSite1
        print "\n" 
        print scrtDiskPathSite2 
        print "\n"
        print lGrDiskPathSite2
        print "\n" 
        print scrtDiskPathSite1 + "test/"
        print "\n"
        print lGrDiskPathSite1 + "test/"
        print "\n"
        print prodDiskPathSite1
        print "\n"
        print dataDiskPathSite1
        print "\n"
        
        
        detail3_uberftp, detail3_srm = self.spaceTokenStatus(siteName,
                                        Object_uberftp,
                                        Object_srm,
                                        srcHostSite2, 
                                        dstHostSite1,
                                        srcPortSite2,
                                        dstPortSite1, 
                                        scrtDiskPathSite2, 
                                        lGrDiskPathSite2, 
                                        scrtDiskPathSite1 + "test/", 
                                        lGrDiskPathSite1 + "test/", 
                                        prodDiskPathSite1, 
                                        dataDiskPathSite1)  
        
        print "\n"             
        print   detail3_uberftp 
        print "\n"
        print "\n"
        print   detail3_srm   
        
              
        self.details_table_db_value_list.append({})       
        self.details_table_db_value_list[0] = detail3_uberftp
        
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[1] = detail3_srm 
        
        
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
        
        #self.details_uberftp_table_db_value_list = []
        #self.details_srm_table_db_value_list = []
        self.details_table_db_value_list = []
             
    def fillSubtables(self, parent_id):
        self.subtables['site_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])
                   
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
        
            
        max_id_number1_uberftp = func.max(self.subtables['site_transfers'].c.id).select().where(and_(self.subtables['site_transfers'].c.siteName == str(self.config['site1_name']), self.subtables['site_transfers'].c.transferType == "UberFTP")).execute().scalar()        
        details1 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number1_uberftp).execute().fetchall()
        data['site1Tosite1_uberftp'] = map(dict, details1)
        
        max_id_number1_srm = func.max(self.subtables['site_transfers'].c.id).select().where(and_(self.subtables['site_transfers'].c.siteName == str(self.config['site1_name']), self.subtables['site_transfers'].c.transferType.like ("%SRM%"))).execute().scalar()        
        #self.dataset['bbb'] = max_id_number1_srm                 
        details2 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number1_srm).execute().fetchall()
        data['site1Tosite1_srm'] = map(dict, details2)
        
        
        max_id_number2_uberftp = func.max(self.subtables['site_transfers'].c.id).select().where(and_(self.subtables['site_transfers'].c.siteName == str(self.config['site2_name1']), self.subtables['site_transfers'].c.transferType == "UberFTP")).execute().scalar()        
        #self.dataset['ccc'] = max_id_number2_uberftp                
        details3 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number2_uberftp).execute().fetchall()
        data['site1Tosite2_uberftp'] = map(dict, details3)
        
        max_id_number2_srm = func.max(self.subtables['site_transfers'].c.id).select().where(and_(self.subtables['site_transfers'].c.siteName == str(self.config['site2_name1']), self.subtables['site_transfers'].c.transferType.like ("%SRM%"))).execute().scalar()        
        #self.dataset['ddd'] = max_id_number2_srm                 
        details4 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number2_srm).execute().fetchall()
        data['site1Tosite2_srm'] = map(dict, details4)
        
        
        
        max_id_number3_uberftp = func.max(self.subtables['site_transfers'].c.id).select().where(and_(self.subtables['site_transfers'].c.siteName == str(self.config['site2_name2']), self.subtables['site_transfers'].c.transferType == "UberFTP")).execute().scalar()        
        details5 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number3_uberftp).execute().fetchall()
        data['site2Tosite1_uberftp'] = map(dict, details5)
        
        max_id_number3_srm = func.max(self.subtables['site_transfers'].c.id).select().where(and_(self.subtables['site_transfers'].c.siteName == str(self.config['site2_name2']), self.subtables['site_transfers'].c.transferType.like ("%SRM%"))).execute().scalar()        
        details6 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number3_srm).execute().fetchall()
        data['site2Tosite1_srm'] = map(dict, details6)
        
       
        return data
    
