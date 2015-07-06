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

import sys, os, subprocess,logging, time, threading, signal
from hf.gridengine.envreader import GridEnv, GridEnvReader, CvmfsEnv, CvmfsEnvReader
from ipalib.errors import SubprocessError
import datetime, time
from subprocess import TimeoutExpired
from time import sleep


# Exception classes used by this module.
class GridCalledProcessError(Exception):
    """This exception is raised when a process run by check_call() returns
    a non-zero exit status.  The exit status will be stored in the
    returncode attribute."""
    def __init__(self, returncode, cmd):
        self.returncode = returncode
        self.cmd = cmd
    def __str__(self):
        return "Grid Command '%s' returned non-zero exit status %d" % (self.cmd, self.returncode)


class GridTimeoutExpired(SubprocessError):
    """This exception is raised when the timeout expires while waiting for a
    child process.
    """
    def __init__(self, cmd, timeout, output=None, stderr=None):
        self.cmd = cmd
        self.timeout = timeout
        self.output = output
        self.stderr = stderr

    def __str__(self):
        return ("Command '%s' timed out after %s seconds" %
                (self.cmd, self.timeout))

    @property
    def stdout(self):
        return self.output

    @stdout.setter
    def stdout(self, value):
        # There's no obvious reason to set this, but allow it anyway so
        # .stdout is a transparent alias for .output
        self.output = value




def grid_call(*popenargs, **kwargs):
    """Run command with arguments.  Wait for command to complete or
    timeout, then return the returncode attribute.

    The arguments are the same as for the Popen constructor.  Example:

    retcode = call(["ls", "-l"])
    """
    timeout = kwargs.pop('timeout', None)
    p = GridPopen(*popenargs, **kwargs)
    try:
        return p.wait(timeout=timeout)
    except GridTimeoutExpired:
        p.kill()
        p.wait()
        raise


def check_grid_call(*popenargs, **kwargs):
    """Run command with arguments.  Wait for command to complete.  If
    the exit code was zero then return, otherwise raise
    CalledProcessError.  The CalledProcessError object will have the
    return code in the returncode attribute.

    The arguments are the same as for the Popen constructor.  Example:

    check_call(["ls", "-l"])
    """
    retcode = grid_call(*popenargs, **kwargs)
    cmd = kwargs.get("args")
    if cmd is None:
        cmd = popenargs[0]
    if retcode:
        raise GridCalledProcessError(retcode, cmd)
    return retcode


""" Grid Subprocess Opener """
class GridPopen(subprocess.Popen):
    logger = logging.getLogger(__name__)
    
    def __init__(self, args, bufsize=0, executable=None,
                 stdin=None, stdout=None, stderr=None,
                 preexec_fn=None, close_fds=False, shell=False,
                 cwd=None, env=None, universal_newlines=False,
                 startupinfo=None, creationflags=0, cvmfs_env=None):

        
        """ set Grid environment & CVMFS environment """
        gridSetupLoader=""
        cvmfsSetupLoader=""
        if GridEnvReader().isEnvEnabled():
            gridSetupLoader = GridEnvReader().getEnv().generateLoader()
            
        if CvmfsEnvReader().isEnvEnabled():
            if cvmfs_env is None: cvmfs_env = CvmfsEnvReader().getEnv()
            cvmfsSetupLoader = cvmfs_env.generateLoader()
            self.logger.debug("CVMFS SETUP LOADER: " + cvmfsSetupLoader)

        args = cvmfsSetupLoader + gridSetupLoader + args
        self.logger.debug(args)

        
        """ Override constructor __init__() of subprocess.Popen """
        super(GridPopen, self).__init__(args, bufsize=bufsize, executable=executable,
                 stdin=stdin, stdout=stdout, stderr=stderr,
                 preexec_fn=preexec_fn, close_fds=close_fds, shell=shell,
                 cwd=cwd, env=env, universal_newlines=universal_newlines,
                 startupinfo=startupinfo, creationflags=creationflags)


""" 
    Grid Subprocess Handler Base class 

"""
class GridSubprocessBaseHandler:
    logger = logging.getLogger(__name__)

    """ Attributes for environments """
    gridEnv = GridEnvReader().getEnv()
    cvmfsEnv = CvmfsEnvReader().getEnv()


    """ Attributes for process """
    gridProcess = None
    commandArgs = ""
    waitBeforeStdInput = 1
    standardInput = None
    timeout = None


    """ Override this method when handler calls original grid environments and commands """
    def execute(self, args=None, bufsize=10, executable=None,
                stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                 preexec_fn=None, close_fds=False, shell=True,
                 cwd=None, env=os.environ, universal_newlines=False,
                 startupinfo=None, creationflags=0):

                """ set command line """         
                if args is None: args = self.commandArgs

                """ show command args """
                self.logger.info("Command Args = [" + args + "]")
                
                """ call Grid Subprocess Opener """         
                self.gridProcess = GridPopen(args=args, bufsize=bufsize, executable=executable,
                 stdin=stdin, stdout=stdout, stderr=stderr,
                 preexec_fn=preexec_fn, close_fds=close_fds, shell=shell,
                 cwd=cwd, env=env, universal_newlines=universal_newlines,
                 startupinfo=startupinfo, creationflags=creationflags, cvmfs_env=self.cvmfsEnv)

                """ give strings to stdin """
                if stdin is not None and self.standardInput is not None:
                    self.logger.debug("stdin = ["+self.standardInput + "]")
                    time.sleep(self.waitBeforeStdInput)
                    self.gridProcess.stdin.write(self.standardInput)                   
                    #self.gridProcess.stdin.close()                   
                
                                 
                """ wait until timeout expires """ 
                
                try:  
                    if self.timeout is not None: 
                        exitcode = self.gridProcess.wait(int(self.timeout))
                        print exitcode
                                        
                except GridTimeoutExpired as e:                        
                        self.gridProcess.kill()                        
                        return 1, "Command execution time is expired. Raised timeout problem. Transfer failed.", self.gridProcess.stdout.read()  #Failed                                        
                                                
                except TimeoutExpired:
                    print "Command execution time is expired. Raised timeout problem. Transfer failed."                        
                    os.waitpid(-1, os.WNOHANG) 
                    time.sleep(5)
                    os.kill(self.gridProcess.pid, signal.SIGKILL)
                    print 'The process killed'  
                    return 1, "Command execution time is expired. Raised timeout problem. Transfer failed.", self.gridProcess.stdout.read() #Failed   
                           
                
                return self.gridProcess.returncode,  self.gridProcess.stderr.read(), self.gridProcess.stdout.read()                                               


    """ Show GridPopen Process """    
    def showGridProcess(self, show_stdout=True, show_stderr=False):
        while self.gridProcess.returncode is None:
            (stdout, stderr) = self.gridProcess.communicate()
            if show_stdout:
                for stdout_line in stdout.splitlines():
                    print stdout_line

            if show_stderr:
                for stderr_line in stderr.splitlines():
                    print stderr_line
        
        
def main():
    
    print "Starting [gridsubprocess]"
    logging.basicConfig(level=logging.DEBUG)
    
    p1 = GridPopen("echo -n X509_USER_KEY = $X509_USER_KEY", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)
    p2 = GridPopen("echo -n X509_USER_CERT = $X509_USER_CERT", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)
    p3 = GridPopen("echo -n X509_USER_PROXY = $X509_USER_PROXY", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=None)
    p4 = GridPopen("echo -n X509_CERT_DIR = $X509_CERT_DIR", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=None)
    
    cvmfsEnv = CvmfsEnv()
    cvmfsEnv.setEnabled('dq2.client')
    cvmfsEnv.setEnabled('emi')
    
    p5 = GridPopen("dq2-ls", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=None, cvmfs_env=cvmfsEnv)
    
    string = "uberftp -retry 2 -keepalive 10 gsiftp://grid-se.physik.uni-wuppertal.de/pnfs/physik.uni-wuppertal.de/data/atlas/atlaslocalgroupdisk/test_haykuhi/100_7573.MOV  gsiftp://se-goegrid.gwdg.de/pnfs/gwdg.de/data/atlas/atlasscratchdisk/test_haykuhi"
    
    p1.wait()
    p2.wait()
    p3.wait()
    p4.wait()

    print p1.stdout.read()
    print p2.stdout.read()
    print p3.stdout.read()
    print p4.stdout.read()
    p5.wait()
    
    gridSubprocess = GridSubprocessBaseHandler()
    gridSubprocess.timeout = 60
    gridSubprocess.execute(string)
    
    
    print p5.stdout.read()
    print p5.stderr.read()
    
if __name__ == '__main__':
    main()
