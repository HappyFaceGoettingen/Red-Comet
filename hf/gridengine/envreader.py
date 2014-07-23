# -*- coding: utf-8 -*-
#
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
# Author: Gen Kawamura <gen.kawamura@cern.ch>, Date: 23/Jun/2014
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

import hf, os, sys, traceback, logging
import subprocess
from ConfigParser import NoOptionError

def getuid():
    return subprocess.Popen("echo -n $UID", shell=True, stdout=subprocess.PIPE).stdout.read()

def load_happyface_config_reader():
    if hf.config is None:
        try:
            print "hf.config is None. So, this routine is making a test instance"
            hf.hf_dir = os.path.dirname(os.path.abspath(__file__))
            hf.configtools.readConfigurationAndEnv()
        except Exception:
            print "Setting up HappyFace failed"
            traceback.print_exc()
            sys.exit(-1)


def check_if_happyface_config_key_exist(section, option):
    try:
        hf.config.get(section, option)
        return True
    except NoOptionError:
        return False


def read_happyface_config(section, option, datatype=None):
    if datatype == "string":
        return hf.config.get(section, option)
    if datatype == "boolean":
        return hf.config.getboolean(section, option)
    if datatype == "int":
        return hf.config.getint(section, option)
    if datatype == "float":
        return hf.config.getfloat(section, option)

    return hf.config.get(section, option)

    

class BaseEnvReader(object):
    logger = logging.getLogger(__name__)

    __envObj = None
    enabled = False

    def enableEnv(self):
        self.enabled = True
        self.__envObj.enabled = True

    def disableEnv(self):
        self.enabled = False
        self.__envObj.enabled = False

    def getEnv(self):
        return self.__envObj

    def setEnv(self, envObj):
        self.__envObj = envObj

    def get(self, key):
        return self.__envObj.get(key)
        
    def _readHappyFaceConf(self):

        """ Please make sure, happyface.cfg has the following structure, and "ConfigParser" module is used
            -------------------------------------------
            [section]
            option1 = value1
            option2 = value2
            -------------------------------------------

        """
        
        """ set standalone mode """
        load_happyface_config_reader()


        """ start reading a section and options in happyface.cfg """
        envObj = self.__envObj
        if hf.config.getboolean(envObj.section, 'enabled'): self.enableEnv()
        if self.enabled:
            print envObj.sectionName + " is enabled!"
            self.logger.info(envObj.sectionName + " is enabled!")
            for option in envObj.keys(): 
                if not check_if_happyface_config_key_exist(envObj.section, option): continue
                datatype = envObj.getDatatype(option)
                value = read_happyface_config(self.__envObj.section, option, datatype)
                envObj.set(option, value)
                
            """ set final result """
            self.__envObj = envObj



class GridEnvReader(BaseEnvReader):
    logger = logging.getLogger(__name__)
    __instance = None


    """ Signleton class , only one obejct of this type can be created """
    def __new__(self, *args, **kwargs):
        if self.__instance == None:
            obj = object.__new__(self, *args, **kwargs)
            self.__instance = obj

            """ initializing a derived class of BaseEnvReader """
            obj.__init__()

            """ reading happyface.cfg """ 
            if not obj.getEnv().DEBUG_MODE:
                obj._readHappyFaceConf()
                obj.getEnv().showConf()

        return self.__instance

        
    def __init__(self):
        if self.getEnv() is None: self.setEnv(GridEnv())
        if self.getEnv().DEBUG_MODE:
            print "Debug mode: Initialization - Ingnoring HappyFace configuration file."
            self.enableEnv()
            self.getEnv().set('vo', 'atlas')
            #self.__gridEnv.set['x509.user.key'] = 'Ha Ha Ha, KEY KEY KEY, USER KEY, Grie User Key!'
            #self.__gridEnv.set['x509.user.cert'] = 'Hi Hi, CERT CERT USER CERT, Grid USER CERT!'
            #self.__gridEnv.set['x509.user.proxy'] = 'Hi, Proxy, USER Proxy, Grie User Proxy!'



class CvmfsEnvReader(BaseEnvReader):
    logger = logging.getLogger(__name__)
    __instance = None


    """ Signleton class , only one obejct of this type can be created """
    def __new__(self, *args, **kwargs):
        if self.__instance == None:
            obj = object.__new__(self, *args, **kwargs)
            self.__instance = obj

            """ initializing a derived class of BaseEnvReader """
            obj.__init__()

            """ reading happyface.cfg """ 
            if not obj.getEnv().DEBUG_MODE:
                obj._readHappyFaceConf()
                obj.getEnv().showConf()

        return self.__instance


    def __init__(self):
        if self.getEnv() is None: self.setEnv(CvmfsEnv())
        if self.getEnv().DEBUG_MODE:
            self.logger.info("Debug mode: Initialization - Ingnoring HappyFace configuration file.")
            self.enableEnv()
                  

class BaseEnv:
    logger = logging.getLogger(__name__)

    section = "" 
    sectionName = ""
    DEBUG_MODE = False
    enabled = False
    conf = {}
    confDatatype = {}


    def showConf(self):
        if self.enabled:
            for confKey in self.conf.keys(): 
                self.logger.info(self.section + " config: " + confKey + " = [" + str(self.conf[confKey]) + "]")
    
    def set(self, confKey, value):
        self.conf[confKey] = value
        
    def get(self, confKey):
        return self.conf[confKey]

    def setDebugValue(self, confKey, value):
        self.conf[confKey] = value
    
    def keys(self):
        return self.conf.keys()

    def getDatatype(self, confKey):
        return self.confDatatype[confKey]


    
class GridEnv(BaseEnv):
    logger = logging.getLogger(__name__)
    
    ## configuration name in happyface.cfg
    section = "grid"
    sectionName = "Grid Engine"

    ## default configuraiton (minimum configuration for CMS)
    conf = {'vo':'cms',
                'voms.enabled':True,
                'proxy.renew.enabled':True,
                'proxy.lifetime.threshold.hours':1,
                'proxy.valid.hours':100,
                'proxy.voms.hours':100,
                'x509.cert.dir':None,
                'x509.user.cert':os.environ['HOME']+"/.globus/usercert.pem",
                'x509.user.key':os.environ['HOME']+"/.globus/userkey.nopass.pem",
                'x509.user.proxy':"/tmp/x509up_u"+getuid(),
                }

    confDatatype = {'vo':None,
                'voms.enabled':'boolean',
                'proxy.renew.enabled':'boolean',
                'proxy.lifetime.threshold.hours':'int',
                'proxy.valid.hours':'int',
                'proxy.voms.hours':'int',
                'x509.cert.dir':None,
                'x509.user.cert':None,
                'x509.user.key':None,
                'x509.user.proxy':None,
                    }
    
    def generateLoader(self):
        if not self.enabled: return ""
        
        env = ""
        if self.conf['x509.cert.dir'] is not None: env += "export X509_CERT_DIR=" + self.conf['x509.cert.dir'] + ";"
        if self.conf['x509.user.cert'] is not None: env += "export X509_USER_CERT=" + self.conf['x509.user.cert'] + ";"
        if self.conf['x509.user.key'] is not None: env += "export X509_USER_KEY=" + self.conf['x509.user.key'] + ";"
        if self.conf['x509.user.proxy'] is not None: env += "export X509_USER_PROXY=" + self.conf['x509.user.proxy'] + ";"

        self.logger.debug(env)
        return env




    
class CvmfsEnv(BaseEnv):
    logger = logging.getLogger(__name__)

    ## configuration name in happyface.cfg
    section = "cvmfs"
    sectionName = "CVMFS"
    
    """ configuration """
    conf = {'rucio.account':None,
            'agis':False, 
            'atlantis':False, 
            'dq2.client':False,
            'emi':True, 
            'fax':False,
            'ganga':False,
            'gcc':False,
            'pacman':False,
            'panda.client':False,
            'pyami':False,
            'pod':False,
            'root':False,
            'dq2wrappers':False,
            'sft':False,
            'xrootd':False,
            }

    confDatatype = {'rucio.account':'string',
            'agis':'boolean', 
            'atlantis':'boolean', 
            'dq2.client':'boolean',
            'emi':'boolean', 
            'fax':'boolean',
            'ganga':'boolean',
            'gcc':'boolean',
            'pacman':'boolean',
            'panda.client':'boolean',
            'pyami':'boolean',
            'pod':'boolean',
            'root':'boolean',
            'dq2wrappers':'boolean',
            'sft':'boolean',
            'xrootd':'boolean',
            }


    """ Loader Commands """
    __cvmfsPackageOrder = ['emi', 'dq2.client', 'agis', 'panda.client', 'gcc']

    
    __cvmfsEnvPackageLoaderCommon = 'export LCG_LOCATION=;export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source $ATLAS_LOCAL_ROOT_BASE/user/atlasLocalSetup.sh "" &> /dev/null;'
    __cvmfsEnvPackageLoaders = {'agis':'source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalAGISSetup.sh --agisVersion ${agisVersionVal} &> /dev/null;', 
                    'atlantis':'source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalAtlantisSetup.sh --atlantisVersion ${atlantisVersionVal} &> /dev/null;', 
                    'dq2.client':'source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalDQ2ClientSetup.sh --skipConfirm --dq2ClientVersion ${dq2ClientVersionVal} &> /dev/null;', 
                    'emi':'source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalEmiSetup.sh --emiVersion ${emiVersionVal} &> /dev/null;', 
                    'fax':'',
                    'ganga':'source ${ATLAS_LOCAL_ROOT_BASE}/packageSetups/atlasLocalGangaSetup.sh --gangaVersion ${gangaVersionVal} &> /dev/null;',
                    'gcc':'',
                    'pacman':'',
                    'panda.client':'',
                    'pyami':'',
                    'pod':'',
                    'root':'',
                    'dq2wrappers':'',
                    'sft':'',
                    'xrootd':'',
                        }

    def __generateRucioAccountEnv(self):
        if self.conf['rucio.account'] is None: return ""
        if not self.conf['dq2.client']: return ""
        return "export RUCIO_ACCOUNT=" + self.conf['rucio.account']+";"
        
    def localSetup(self, package):
        return self.__cvmfsEnvPackageLoaderCommon + self.__cvmfsEnvPackageLoaders[package]

    def generateLoader(self):
        if not self.enabled: return ""
            
        setuploader = self.__generateRucioAccountEnv() + self.__cvmfsEnvPackageLoaderCommon
        for package in self.__cvmfsPackageOrder:
            if self.conf[package]:
                self.logger.debug("CVMFS package = " + package)
                setuploader += self.__cvmfsEnvPackageLoaders[package]
        return setuploader

    def setEnabled(self, package):
        self.conf[package] = True

    def setDisabled(self, package):
        self.conf[package] = False


    
def main():

    print "Starting [gridenvreader]"
    logging.basicConfig(level=logging.DEBUG)
    logging.root.setLevel(logging.DEBUG)

    gridEnv = GridEnv()
    gridEnv.enabled = True
    gridEnv.showConf()
    print gridEnv.generateLoader()


    print "*** first creation"
    gridEnvReader = GridEnvReader()
    print "vo = " + gridEnvReader.get('vo')
    
    """ directly editing gridEnv data object """
    gridEnv = gridEnvReader.getEnv()
    gridEnv.setDebugValue('x509.user.key', 'test test test location. key location')
    gridEnvReader.setEnv(gridEnv)

    print "*** second creation"
    print "vo = " + gridEnvReader.get('vo')
    print "X509_USER_KEY = " + GridEnvReader().get('x509.user.key')
    print "Grid Env enabled = " + str(GridEnvReader().getEnv().enabled)

    cvmfsEnv = CvmfsEnvReader().getEnv()
    cvmfsEnv.enabled = True
    cvmfsEnv.showConf()
    print cvmfsEnv.generateLoader()
    if not cvmfsEnv.get('dq2wrappers'): print "dq2wrappers = " + str(cvmfsEnv.get('dq2wrappers'))



if __name__ == '__main__':
    main()
