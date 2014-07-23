# -*- coding: utf-8 -*-
#
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
# Author: Gen Kawamura <gen.kawamura@cern.ch>, Date: 23/Jul/2014
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

import re,os
from urlparse import urlparse
from Ganga.Lib.LCG.LCG import grids


# input variables
input_exec = os.getenv("input_exec")
input_jnum = int(os.getenv("input_jnum"))
input_sandbox = os.getenv("input_sandbox")
backend = os.getenv("backend")
endpoint = os.getenv("endpoint")


#----------------------------------
# make argument
#----------------------------------
def makeArgs(max):
    arg_list = []
    for i in range(0,max):
        y = []
        y.append(str(i))
        arg_list.append(y)
        
        splitter = ArgSplitter(args=arg_list)
    return splitter


#----------------------------------
# cream job split and submit
#----------------------------------
def splitJobSubmit(job_num):

    s=makeArgs(job_num)
    j=Job(splitter=s)
    j.application = Executable()
    j.application.exe = File(input_exec)
    j.outputsandbox=["stdout.gz","stderr.gz", "output_sandbox.tgz"]
    j.name = backend

    if (input_sandbox != ""):
        j.inputsandbox=[input_sandbox]


    if (backend == "LCG"):
        j.backend = LCG()
        j.backend.requirements = AtlasLCGRequirements()
        j.backend.requirements.sites = ["FZK"]
        j.backend.requirements.ipconnectivity = True
    elif (backend == "CREAM"):
        j.backend = CREAM()
        j.backend.CE = endpoint
    elif (backend == "Panda"):
        j.backend = Panda()
        j.outputdata = DQ2OutputDataset()
    else:
        j.backend = Local()
    
    merger=TextMerger()
    merger.files=['stdout.gz', "stderr.gz"]
    j.merger=merger    
    j.submit()


splitJobSubmit(input_jnum)

