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
from hf.module.database import hf_runs
import time
import datetime

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
    ], []) ,
        'site_transfers_plot': ([
        Column('efficiency_plot', TEXT),
    ], [])}
         
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
        
        #Ploting
        
        ok_sp_token_eff1, failed_sp_token_eff1 = self.sqlEfficiency('scratchdiskToScratchdisk', 'OK', self.config['plot_start_date'])
        ok_sp_token_eff2, failed_sp_token_eff2 = self.sqlEfficiency('localgroupdiskToLocalgroupdisk', 'OK', self.config['plot_start_date'])              
        
        ok_sp_token_eff3, failed_sp_token_eff3 = self.sqlEfficiency('scratchdiskToProddisk', 'Error', self.config['plot_start_date'])              
        ok_sp_token_eff4, failed_sp_token_eff4 = self.sqlEfficiency('scratchdiskToDatadisk', 'Error', self.config['plot_start_date'])
                
        
        ok_status_plot = (ok_sp_token_eff1, ok_sp_token_eff2, ok_sp_token_eff3, ok_sp_token_eff4)
        failed_status_plot = (failed_sp_token_eff1, failed_sp_token_eff2, failed_sp_token_eff3, failed_sp_token_eff4)
        
        details_plot = {}        
        details_plot['efficiency_plot'] = self.efficiencyPlot(ok_status_plot, failed_status_plot)
                                       
        self.details_table_db_value_list_plot.append({})
        self.details_table_db_value_list_plot[0] = details_plot  
                                  
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
        self.details_table_db_value_list_plot = []
             
    def fillSubtables(self, parent_id):
        self.subtables['site_transfers'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])
        self.subtables['site_transfers_plot'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list_plot])
           
    def SQLQuery(self, siteName):
        max_id_number1_uberftp = func.max(self.subtables['site_transfers'].c.id).select().where(self.subtables['site_transfers'].c.siteName == str(siteName)).execute().scalar()        
        details1 = self.subtables['site_transfers'].select().where(self.subtables['site_transfers'].c.id == max_id_number1_uberftp).execute().fetchall()
        return map(dict, details1)   
    
    def sqlQuery_plot(self):        
        plot = self.subtables['site_transfers_plot'].select().where(self.subtables['site_transfers_plot'].c.parent_id==self.dataset['id']).execute().fetchall()
        return map(dict, plot)      
    
    
    def showSqlErroMsg(self, db_space_token1, db_space_token2, msg, date_str):
        """
        SELECT DISTINCT sub_grid_ftp_copy_viewer_site_transfers."scratchdiskToScratchdisk" 
        FROM sub_grid_ftp_copy_viewer_site_transfers INNER JOIN hf_runs 
        ON hf_runs.id = sub_grid_ftp_copy_viewer_site_transfers.id 
        WHERE hf_runs.time >= '2015-06-05' AND scratchdiskToScratchdisk NOT LIKE '%OK%'  
        
        UNION 
        
        SELECT DISTINCT sub_grid_ftp_copy_viewer_site_transfers."localgroupdiskToScratchdisk" 
        FROM sub_grid_ftp_copy_viewer_site_transfers INNER JOIN hf_runs 
        ON hf_runs.id = sub_grid_ftp_copy_viewer_site_transfers.id 
        WHERE hf_runs.time >= '2015-06-05' AND localgroupdiskToScratchdisk NOT LIKE '%OK%';
        """
        
        range = "hf_runs.time >='" + date_str + "'"       
        failed_sp_token_str1 = db_space_token1 + " NOT LIKE '%" + msg + "%'"
        failed_sp_token_str2 = db_space_token2 + " NOT LIKE '%" + msg + "%'"
        
        query1 =  self.subtables['site_transfers'].join(hf_runs, hf_runs.c.id== self.subtables['site_transfers'].c.id)
        failed_status1 = select([getattr(self.subtables['site_transfers'].c, db_space_token1)]).select_from(query1).distinct().where(and_(range, failed_sp_token_str1))#.execute().fetchall()
        
        query2 =  self.subtables['site_transfers'].join(hf_runs, hf_runs.c.id== self.subtables['site_transfers'].c.id)
        failed_status2 = select([getattr(self.subtables['site_transfers'].c, db_space_token2)]).select_from(query2).distinct().where(and_(range, failed_sp_token_str2))#.execute().fetchall()
               
        result = failed_status1.union(failed_status2).execute().fetchall()
               
        return result    
        
        
        
    def sqlEfficiency(self, db_space_token, msg, date_str):
      
        """
        SELECT sub_grid_ftp_copy_viewer_site_transfers.id, sub_grid_ftp_copy_viewer_site_transfers.parent_id, sub_grid_ftp_copy_viewer_site_transfers."siteName", sub_grid_ftp_copy_viewer_site_transfers."scratchdiskToScratchdisk", sub_grid_ftp_copy_viewer_site_transfers."scratchdiskToLocalgroupdisk", sub_grid_ftp_copy_viewer_site_transfers."scratchdiskToProddisk", sub_grid_ftp_copy_viewer_site_transfers."scratchdiskToDatadisk", sub_grid_ftp_copy_viewer_site_transfers."localgroupdiskToScratchdisk", sub_grid_ftp_copy_viewer_site_transfers."localgroupdiskToLocalgroupdisk", sub_grid_ftp_copy_viewer_site_transfers."localgroupdiskToProddisk", sub_grid_ftp_copy_viewer_site_transfers."localgroupdiskToDatadisk" 
        FROM sub_grid_ftp_copy_viewer_site_transfers JOIN hf_runs ON hf_runs.id = sub_grid_ftp_copy_viewer_site_transfers.id 
        WHERE hf_runs.time >= '2015-06-01' AND scratchdiskToScratchdisk LIKE '%OK%'; 
                
        SELECT count(sub_grid_ftp_copy_viewer_site_transfers.id) AS count_1 FROM hf_runs INNER JOIN sub_grid_ftp_copy_viewer_site_transfers ON   hf_runs.id =sub_grid_ftp_copy_viewer_site_transfers.id WHERE sub_grid_ftp_copy_viewer_site_transfers.scratchdiskToScratchdisk LIKE '%OK%' AND hf_runs.time >=  '2015-08-01';
                
        SELECT sub_grid_ftp_copy_viewer_site_transfers.id 
        FROM sub_grid_ftp_copy_viewer_site_transfers JOIN hf_runs ON hf_runs.id = sub_grid_ftp_copy_viewer_site_transfers.id 
        WHERE hf_runs.time >= '2015-06-01' AND scratchdiskToScratchdisk LIKE '%OK%';

        """
        range = "hf_runs.time >='" + date_str + "'"
        ok_compr = db_space_token + " LIKE '%" + msg + "%'"
               
        query1 =  self.subtables['site_transfers'].join(hf_runs, hf_runs.c.id== self.subtables['site_transfers'].c.id)
        ok_status = select([self.subtables['site_transfers'].c.id]).select_from(query1).where(and_(range, ok_compr)).execute().fetchall()
 
        failed_compr = db_space_token + " NOT LIKE '%" + msg + "%'"
        query2 =  self.subtables['site_transfers'].join(hf_runs, hf_runs.c.id== self.subtables['site_transfers'].c.id)
        failed_status = select([self.subtables['site_transfers'].c.id]).select_from(query2).where(and_(range, failed_compr)).execute().fetchall()
   
        return len(ok_status), len(failed_status)
    
    def autolabel(self, ax, rects):
            for rect in rects:
                h = rect.get_height()
                ax.text(rect.get_x()+rect.get_width()/2., 1.05*h, '%d'%int(h),
                        ha='center', va='bottom')
    
    
    def efficiencyPlot(self, arry1, arry2):
        import numpy as np
        import matplotlib.pyplot as plt
                
        N = len(arry1)
        ind = np.arange(1, N+1)
        width = 0.30

        fig = plt.figure()
        ax = fig.add_subplot(111)  
        
        bar1 = ax.bar(ind, arry1, width, color='#4FD516' , yerr = 0.001)    
        bar2 = ax.bar(ind+width, arry2, width, color='#FF6464', yerr = 0.001) 
       
        ax.set_xlabel('Space tokens')
        ax.set_ylabel('DB records count')
        ax.set_title('GoeGrid space tokens efficiency by SRM \n starting from '  + self.config['plot_start_date'])
        ax.set_xticks(ind + width)        
        ax.set_xticklabels(('SrcDisk', 'LgDisk', 'ProdDisk', 'DataDisk'))
        
        if max(arry1) >= max(arry2):            
            plt.yticks(np.arange(0,max(arry1)*2, max(arry1)//3))
        else:
            plt.yticks(np.arange(0,max(arry2)*2, max(arry2)//3))
        
        ax.legend((bar1[0], bar2[0]), ('Succeed', 'Failed'))
        
        self.autolabel(ax, bar1)
        self.autolabel(ax, bar2)
        
        plt.show()

        fig.savefig(hf.downloadService.getArchivePath(self.run, self.instance_name + "db_eff.png"), dpi=60)
        plt.close()       
        dest = "/"+hf.downloadService.getArchivePath(self.run, self.instance_name + "db_eff.png")
        return dest
    
                       
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
        
        self.dataset['plot_start_date'] = self.config['plot_start_date']
              
        self.dataset['ok_status'] = ['OK', 'SpaceException', 'FileExists', 'copy', 'completed', 'SUCCESS' ]
        self.dataset['ok_status_prod_data'] = ['Permission denied', 'SRM_AUTHORIZATION_FAILURE', 'No permission', 'failed']
        self.dataset['error_status'] = ['Failed', 'Error', 'No match', 'timeout', 'Cannot create a directory', 'Remote server has disconnected', 'Transfer in progress', 'is not a directory']
        self.dataset['error_status_prod_data'] = ['OK', 'SpaceException', 'FileExists', 'SUCCESS', 'Cannot create a directory', 'Remote server has disconnected', 'Transfer in progress']         
                               
        data['site1_site1_transfers'] = self.SQLQuery(self.config['site1_name'])  
        data['site1_site2_transfers'] = self.SQLQuery(self.config['site2_name1'])     
        data['site2_site1_transfers'] = self.SQLQuery(self.config['site2_name2'])
        
        data['site_transfers_plot'] = self.sqlQuery_plot()
        
        data['show_err1'] = self.showSqlErroMsg('scratchdiskToScratchdisk', 'localgroupdiskToScratchdisk', 'OK', self.config['plot_start_date'])
        data['show_err2'] = self.showSqlErroMsg('scratchdiskToLocalgroupdisk', 'localgroupdiskToLocalgroupdisk', 'OK', self.config['plot_start_date']) 
        data['show_err3'] = self.showSqlErroMsg('scratchdiskToProddisk', 'localgroupdiskToProddisk', 'Error', self.config['plot_start_date']) 
        data['show_err4'] = self.showSqlErroMsg('scratchdiskToDatadisk', 'localgroupdiskToDatadisk', 'Error', self.config['plot_start_date'])  
                       
        
        return data
