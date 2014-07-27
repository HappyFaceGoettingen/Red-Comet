# Copyright 2014 II. Physikalisches Institut - Georg-August-Universitaet Goettingen
# Author: Max Robinson (mrobinson31415@gmail.com)
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
#
#    Last edited: July 17th, 2014 by Max Robinson

import hf
from hf.gridtoolkit.DdmDatasetsController import DdmDatasetsController
from sqlalchemy import *
from lxml import etree
import re
from datetime import datetime
from datetime import timedelta

class DdmDatasetsViewer(hf.module.ModuleBase):
    config_keys = {
        'dataset': ('Dataset names', ''),
    }

    #config_hint = ''

    table_columns = [
        Column('dataset', TEXT),
    ], []

    subtable_columns = {
        'details_table': ([
        Column('datasetname', TEXT),
        Column('datasetsize', INT),
        Column('datasetowner', TEXT),
        Column('datasetdate', TEXT),
    ], [])}

    def prepareAcquisition(self):
                
        self.details_table_db_value_list = []

    def extractData(self):
        data = {
            'dataset': "Hi",
            'status': 1
            }
        
        DataObject = DdmDatasetsController()
        #DataObject.run('GOEGRID_LOCALGROUPDISK')
        #datasetnames = DataObject.getFromDatabase('datasetname')
        #stuff = DataObject.listDatasets("GOEGRID_LOCALGROUPDISK")
        ## 
        print "Extracting Data..........."


        ## Get Dataset info and what not. 
        detail = {}
        #detail['datasetname'] = DataObject.getFromDatabase("datasetname")
        #detail['datasetsize'] = DataObject.getFromDatabase("datasetsize")
        #detail['datasetowner'] = DataObject.getFromDatabase("datasetowner")
        #detail['datasetdate'] = DataObject.getFromDatabase("datasetdate")
        
        detail['datasetname'] = "hiworld"
        detail['datasetsize'] = 1
        detail['datasetowner'] = "Max Robinson"
        detail['datasetdate'] = "7/7/2014"
        
        ## set database data
        index_row = 0
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[index_row] = detail

        return data


    def fillSubtables(self, parent_id):
        self.subtables['details_table'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])
   
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details = self.subtables['details_table'].select().where(self.subtables['details_table'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['details_ddm'] = map(dict, details)

        return data
