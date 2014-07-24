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
EXECUTABLE = os.getenv("EXECUTABLE")
NUMBER_OF_SUBJOBS = int(os.getenv("NUMBER_OF_SUBJOBS"))
INPUT_SANDBOX = os.getenv("INPUT_SANDBOX")
BACKEND = os.getenv("BACKEND")
ENDPOINT = os.getenv("ENDPOINT")
SITE = os.getenv("SITE")

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
    j.application.exe = File(EXECUTABLE)
    j.outputsandbox=["stdout.gz","stderr.gz", "output_sandbox.tgz"]
    j.name = BACKEND

    if (INPUT_SANDBOX != ""):
        j.inputsandbox=[INPUT_SANDBOX]


    if (BACKEND == "LCG"):
        j.backend = LCG()
        j.backend.requirements = AtlasLCGRequirements()
        j.backend.requirements.sites = [SITE]
        j.backend.requirements.ipconnectivity = True
    elif (BACKEND == "CREAM"):
        j.backend = CREAM()
        j.backend.CE = ENDPOINT
    elif (BACKEND == "Panda"):
        j.backend = Panda()
        j.outputdata = DQ2OutputDataset()
    else:
        j.backend = Local()
    
    merger=TextMerger()
    merger.files=['stdout.gz', "stderr.gz"]
    j.merger=merger    
    j.submit()


splitJobSubmit(NUMBER_OF_SUBJOBS)

