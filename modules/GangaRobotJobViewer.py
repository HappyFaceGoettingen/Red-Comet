# Copyright 2014 II. Physikalisches Institut - Georg-August-Universitaet Goettingen
# Author: Gen Kawamura (gen.kawamura@cern.ch)
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
from sqlalchemy import *
from datetime import datetime
from hf.gridtoolkit.GangaRobotJobHandler import GangaRobotJobHandler


class GangaRobotJobViewer(hf.module.ModuleBase):
    config_keys = {
        'job_template_file': ('Job Template File', 'hf/gridtoolkit/ganga_job_template.py'),
        'ganga_job_executable': ('Job Executable', '/bin/echo'),
        'ganga_input_sandbox': ('Input Sandbox', ''),
        'ganga_number_of_subjobs': ('Number of Subjobs', '1'),
        'ganga_grid_backend': ('Grid Backend', 'CREAM'),
        'ganga_ce_endpoint': ('CE Endpoint', 'cream-ge-2-kit.gridka.de:8443/cream-sge-sl6'),
        'ganga_lcg_site': ('A LCG site', 'FZK'),
    }

    #config_hint = ''


    table_columns = [
        Column('job_template_file', TEXT),
        Column('ganga_job_executable', TEXT),
        Column('ganga_input_sandbox', TEXT),
        Column('ganga_number_of_subjobs', TEXT),
        Column('ganga_grid_backend', TEXT),
        Column('ganga_ce_endpoint', TEXT),
        Column('ganga_lcg_site', TEXT),
        Column('ganga_job_id', TEXT),
    ], []

    subtable_columns = {
        'details_table': ([
        Column('ganga_job_id', TEXT),
        Column('ganga_subjob_id', TEXT),
        Column('job_status', TEXT),
        Column('information', TEXT),
        Column('datetime', DATETIME),
        Column('stdout', TEXT),
        Column('stderr', TEXT),
    ], [])}

    def prepareAcquisition(self):
        # Set job submission interface
        self.job_template_file = self.config['job_template_file']
        self.ganga_job_executable = self.config['ganga_job_executable']
        self.ganga_input_sandbox = self.config['ganga_input_sandbox']
        self.ganga_number_of_subjobs = self.config['ganga_number_of_subjobs']
        self.ganga_grid_backend = self.config['ganga_grid_backend']
        self.ganga_ce_endpoint = self.config['ganga_ce_endpoint']
        self.ganga_lcg_site = self.config['ganga_lcg_site']
        self.ganga_job_id = -9999
                
        self.details_table_db_value_list = []

    def extractData(self):
        data = {
            'job_template_file': self.config['job_template_file'],
            'ganga_job_executable': self.config['ganga_job_executable'],
            'ganga_input_sandbox': self.config['ganga_input_sandbox'],
            'ganga_number_of_subjobs': self.config['ganga_number_of_subjobs'],
            'ganga_grid_backend': self.config['ganga_grid_backend'],
            'ganga_ce_endpoint': self.config['ganga_ce_endpoint'],
            'ganga_lcg_site': self.config['ganga_lcg_site'],
            'ganga_job_id': -9999,
            'status': 1
            }
     

        ## Submit Ganga Jobs
        ganga = GangaRobotJobHandler()
        ganga.setJob(self.job_template_file, self.ganga_job_executable, self.ganga_input_sandbox, 
                     self.ganga_number_of_subjobs, self.ganga_grid_backend, self.ganga_ce_endpoint, 
                     self.ganga_lcg_site)

        ganga.jobSubmit()

        ## Get Ganga Status
        ganga.jobMonitor()

        detail = {}
        detail['ganga_job_id'] = "1"
        detail['ganga_subjob_id'] = "1"
        detail['job_status'] = "Running"
        detail['information'] = "Information dayo"
        detail['datetime'] = datetime.now()
        detail['stdout'] = "Stdout"
        detail['stderr'] = "Stderr"


        ## set database data
        index_row = 0
        self.details_table_db_value_list.append({})
        self.details_table_db_value_list[index_row] = detail

        
        #print self.details_table_db_value_list
        for detail in self.details_table_db_value_list:
            if detail['job_status'].lower() == 'Oh No!':
                data['status'] = min(data['status'],0)
            elif detail['job_status'].lower() == 'Bad!':
                data['status'] = min(data['status'],0.5)
            elif detail['job_status'].lower() == 'running':
                data['status'] = min(data['status'],1)
            else:
                data['status'] = min(data['status'],0)

        return data


    def fillSubtables(self, parent_id):
        self.subtables['details_table'].insert().execute([dict(parent_id=parent_id, **row) for row in self.details_table_db_value_list])
   
    def getTemplateData(self):
        data = hf.module.ModuleBase.getTemplateData(self)
        details = self.subtables['details_table'].select().where(self.subtables['details_table'].c.parent_id==self.dataset['id']).execute().fetchall()
        data['details_job'] = map(dict, details)

        return data

