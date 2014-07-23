# -*- coding: utf-8 -*-
#
# Copyright 2012 Institut f腴� Experimentelle Kernphysik - Karlsruher Institut f腴� Technologie
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

import hf, logging
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler, GridPopen
from hf.gridtoolkit.ProxyCertificateHandler import ProxyCertificateHandler


logger = logging.getLogger(__name__)

def init():
    """ According to hf configuration, Configure initial grid environment by CVMFS. Call before any grid-commands """
    try:
        ProxyCertificateHandler().generateProxy()
    except Exception, e:
        logger.error("Cannot generate grid proxy certificate %s" % str(e))

def main():
    print "Starting [init()]"
    logging.basicConfig(level=logging.INFO)
    init()

if __name__ == '__main__':
    main()

