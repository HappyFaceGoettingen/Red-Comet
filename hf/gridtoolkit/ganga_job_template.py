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
GANGA_JOB_EXECUTABLE = os.getenv("GANGA_JOB_EXECUTABLE")
GANGA_NUMBER_OF_SUBJOBS = int(os.getenv("GANGA_NUMBER_OF_SUBJOBS"))
GANGA_INPUT_SANDBOX = os.getenv("GANGA_INPUT_SANDBOX")
GANGA_GRID_BACKEND = os.getenv("GANGA_GRID_BACKEND")
GANGA_CE_ENDPOINT = os.getenv("GANGA_CE_ENDPOINT")
GANGA_LCG_SITE = os.getenv("GANGA_LCG_SITE")

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
# split and submit jobs
#----------------------------------
def splitJobSubmit(job_num):

    s=makeArgs(job_num)
    j=Job(splitter=s)
    j.application = Executable()
    j.application.exe = File(GANGA_JOB_EXECUTABLE)
    j.outputsandbox=["stdout.gz","stderr.gz", "output_sandbox.tgz"]
    j.name = GANGA_GRID_BACKEND

    if (GANGA_INPUT_SANDBOX != ""):
        j.inputsandbox=[GANGA_INPUT_SANDBOX]


    if (GANGA_GRID_BACKEND == "LCG"):
        j.backend = LCG()
        j.backend.requirements = AtlasLCGRequirements()
        j.backend.requirements.sites = [GANGA_LCG_SITE]
        j.backend.requirements.ipconnectivity = True
    elif (GANGA_GRID_BACKEND == "CREAM"):
        j.backend = CREAM()
        j.backend.CE = GANGA_CE_ENDPOINT
    elif (GANGA_GRID_BACKEND == "PANDA"):
        j.backend = Panda()
        j.outputdata = DQ2OutputDataset()
    else:
        j.backend = Local()
    
    merger=TextMerger()
    merger.files=['stdout.gz', "stderr.gz"]
    j.merger=merger    
    j.submit()


splitJobSubmit(GANGA_NUMBER_OF_SUBJOBS)

