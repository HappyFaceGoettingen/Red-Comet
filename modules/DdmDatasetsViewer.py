# -*- coding: utf-8 -*-
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
# Author: Haykuhi Musheghyan <haykuhi.musheghyan@cern.ch>, Date: 13/August/2015
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

import hf
from hf.gridtoolkit.DdmDatasetsController import DdmDatasetsController
from sqlalchemy import *
from lxml import etree
import re
from datetime import datetime
from datetime import timedelta
import random 


class DdmDatasetsViewer(hf.module.ModuleBase):
    config_keys = {
        'dataset': ('Dataset names', ''),
    }

    table_columns = [
        Column('dataset', TEXT),
    ], []

    subtable_columns = {
        'details': ([
        Column('datasetName', TEXT),
        Column('datasetSize', INT),
        Column('datasetOwner', TEXT),
        Column('datasetCreatedDate', TEXT),
        Column('datasetUpdatedDate', TEXT),
    ], [])}
    
    
    def prepareAcquisition(self):
                
        self.details_table_db_value_list = []    

    def extractData(self):
        data = {
            'dataset': "Hi",
            'status': 1
            }
        
        DataObject = DdmDatasetsController()
       
        DataObject.whoami()
        
        dataset_start = int(self.config['datasets_range_start'])
        dataset_end = int(self.config['datasets_range_end'])
         
        start = random.randrange(dataset_start, dataset_end)
        end = int(start) + int(self.config['dataset_size'])         
                       
        #print "Start:::" 
        #print randStart
        #print "End:::"
        #print randEnd
                      
        space_token = self.config['space_token']         
        retCode, error_msg, datasetName = DataObject.listDatasets(space_token, start, end)
       
        if not error_msg:
            filtered_datasetName = str.replace(datasetName, 'SCOPE:NAME', '')
            filtered_datasetName = str.replace(filtered_datasetName, '-', '')
               
            datasetName_list = filtered_datasetName.split( )
        
            k = -1
            for dataset in datasetName_list:            
                print "List element:::::  "  + dataset
                info = DataObject.getMetaData(dataset)
                if info:
                    k = k + 1
                    print info.get('name')
                    #print info.get('length')
                    print int(info.get('bytes'))/1048576 #convert to MB
                    #print info.get('scope')  
                    print info.get('account')      
                    print info.get('created_at')
                    print info.get('updated_at')
                    details = {}
                    details['datasetName'] = info.get('name')
                    details['datasetSize'] = int(info.get('bytes'))/1048576  #convert to MB length
                    details['datasetOwner'] = info.get('account') #scope
                    details['datasetCreatedDate'] = info.get('created_at')
                    details['datasetUpdatedDate'] = info.get('updated_at')
                     
                    self.details_table_db_value_list.append({})
                    self.details_table_db_value_list[k] = details              
        
        self.removeDuplicatedRowsFromDB()
        
        return data


    def fillSubtables(self, parent_id):
        self.subtables['details'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])
   
    def removeDuplicatedRowsFromDB(self):        
        #DELETE FROM sub_ddm_datasets_viewer_details WHERE id NOT IN (SELECT MAX(id) FROM sub_ddm_datasets_viewer_details GROUP BY datasetName);
        self.subtables['details'].delete().where(~self.subtables['details'].c.id.in_(func.max(self.subtables['details'].c.id).select().group_by(self.subtables['details'].c.datasetName))).execute()
                
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)              
        table_size = self.config['limit_table_size']
        details = self.subtables['details'].select().where(self.subtables['details'].c.parent_id==self.dataset['id']).order_by(desc(self.subtables['details'].c.datasetSize)).limit(table_size).execute().fetchall()
        data['details_ddm'] = map(dict, details)

        return data

