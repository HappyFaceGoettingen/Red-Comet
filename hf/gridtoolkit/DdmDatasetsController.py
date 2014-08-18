# -*- coding: utf-8 -*-
#
# Copyright 2014 II. Physikalisches Institut - Georg-August-Universität Göttingen
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
'''
Created on June 30, 2014

@author: Max
'''

import logging, os, subprocess, re
from hf.gridengine.gridsubprocess import GridSubprocessBaseHandler
from hf.gridengine.gridsubprocess import GridPopen
import sqlite3
import datetime

class DdmDatasetsController(GridSubprocessBaseHandler):
    #Class variables
    __command = None
    __spaceToken = None
    __TIMEFORMAT = "%Y-%m-%d %H:%M:%S.%f"
    __LOGFILEPATH = "/var/lib/HappyFace3/log/DdmLastRun.log"
    __TIMEDELTA = .01
    __TESTNUMBEROFDATASETS = 50
    __RUNMODE = "test"
    
    #Constructor
    def __init__(self, spaceToken=None, logFilePath=None, timeDelta=None, testNumberOfDatasets=None, runMode=None):
        """
        ___init__(self, spaceToken=None): Initializes the class by setting up the 
            needed environment and setting default values. 
            args:
                spaceToken: The name of the space token that the class will use
            return: void (constructor)
        """        
        print "init"
        self.cvmfsEnv.setEnabled('dq2.client')
        
        #set space token
        if spaceToken == None: 
            self.__spaceToken = "GOEGRID_LOCALGROUPDISK"
        else: 
            self.__spaceToken = spaceToken
        
        #set log file path
        if logFilePath != None:
            self.__LOGFILEPATH = logFilePath
        
        #set time Delta
        if timeDelta != None: 
            self.__TIMEDELTA = float(timeDelta)
        
        #set test Number of Datasets    
        if testNumberOfDatasets != None:
            self.__TESTNUMBEROFDATASETS = int(testNumberOfDatasets)
            
        #set run Mode, must be either test or production
        if runMode == "test" or runMode == "production":
            self.__RUNMODE = runMode
        
        
        #set command   
        self.__command = "dq2-ls -s "
    
    def listDatasets(self, token):
        """
        listDatasets(self, token): Makes a call to get names of all datasets 
            using a dq2 command and then returns the names of those datasets. 
            args: 
                token: Name of the space token to get datasets from.
            returns: Array of dataset names (Array of Strings) 
        """
        
        self.__spaceToken = token
        self.commandArgs = self.__command + self.__spaceToken
        self.execute()
        
        
        try: 
            (data,error) = self.gridProcess.communicate()
        except Exception as e: 
            print "An exception has occurred: "
            print e
        
        #Print the command used for Debugging
        print "__command: " + self.__command
        
        #turn the string into an array of strings
        listOfDatasets = data.split('\n')
        
        # Split up the result, and remove the empty line(s):
        result = []
        for dataset in listOfDatasets:
            if not len(dataset): continue
            result += [dataset]
            pass
        
        
        return result
          
    def getMetaData(self, dataset):
        """
        getMetaData(self, dataset): Given the name of a dataset it gathers the 
            metadata using a dq2 command, and then formats it into a returnable
            dictionary. 
            args:
                dataset: the name of a given dataset
            return: a dictionary of values containing the metadata for a dataset
        """
        
        print 'getting metadata'
        self.commandArgs = "dq2-get-metadata " + dataset
        self.execute()
        
        try:
            (meta, error) = self.gridProcess.communicate()
        except Exception as e:
            print "An exception has occured: "
            print e

        '''From Jordi'''
        # string massage
        tmp = meta.replace(' :',',')
        csv = tmp.replace(" ","")
        strings = re.split(',|\n',csv)
        words = [x for x in strings if x != '']
        
        info = {}
        keys = map(lambda i: words[i],filter(lambda i: i%2 == 0,range(len(words))))
        values = map(lambda i: words[i],filter(lambda i: i%2 == 1,range(len(words))))
        
        info = dict(zip(keys, values))
        return info
    
    def getFromDatabase(self, column):
        """
        getFromDatabase(self, column): takes the name of a column the a user
            wishes to receive all information in that column from the database.
            args:
                column: The name of the column a user wishes to receive
                    all values in the table from.
            return: Array of values from the given column in the table 
                (Array of Strings).
        """
        
        database = DdmDatabaseHandler('mydq2db')
        data = database.getFromDatabase(column)
        database.close()
        return data
    
    def checkIfExists(self, dataset):
        """
        checkIfExists(self, dataset): Checks if the given dataset already exists in the database.
            args: 
                dataset: The name of a given dataset
            return: Boolean, True if it exists, False if it does not exist
        """
        database = DdmDatabaseHandler('mydq2db')
        value = database.checkIfExists(dataset)
        database.close()
        return value
    
    def run(self, spaceToken=None):
        """
        run(self, spaceToken=None): Main run method for the class. This method takes a space token and
            then collects information about the datasets in the given space token, and stores the 
            information in a database.
            args: 
                spaceToken: The name of a space token
            return: void
        """
        
        if spaceToken == None:
            token = self.__spaceToken
        else:
            token = spaceToken
            
        if(self.needToRun()):
            #write the new runtime to log file
            self.writeRunTime()
            
            # Querying token
            listOfDatasets = []
            
            #check the run Mode and use the correct method
            #test mode = limited, production = listDatasets
            if self.__RUNMODE == "production":
                listOfDatasets = self.listDatasets(token)
            else:
                listOfDatasets = self.limited(token)
            
            database = DdmDatabaseHandler('mydq2db')
            database.updateDatabase(listOfDatasets)
            database.close()
    
    
    ############### HELPER FUNCTIONS OR TEST FUNCTIONS ################################
    
    def limited(self, token):
        """
        limited(self, token): This method shortens the number of datasets returned from the entire
                list of datasets. This is useful for testing to save time. 
                    ex: instead of returning the entire list of datasets, you return only 10
            args:
                token: the name of the space token a user wants to query
            return: Array of dataset names (as strings)
        """
        
        print "Called limited......."
        datasets = self.listDatasets(token)
        shortlistOfDatasets = []
                
        
        for x in range(0,self.__TESTNUMBEROFDATASETS):
            # make sure no duplicates 
            shortlistOfDatasets.append(datasets[x])
        
        return shortlistOfDatasets

    
    ######### For Checking last run time ########
     
    def checkLastRun(self):
        """
        checkLastRun(self): reads a logfile which contains the last known run time 
        of the run method and returns that time. 
            args: None
            Return: a Date Time object with the last run time or the string "never"
        """
        lastrun = None
        #check for log file with last run time
        try: 
            LogFile = open(self.__LOGFILEPATH,"r")
            lastrun = LogFile.read()
        except Exception as e:
            print e
            lastrun = "never"
            print "last run: ", lastrun
            return lastrun
        
        lastrunDatetimeObject = datetime.datetime.strptime(lastrun, self.__TIMEFORMAT)
        
        LogFile.close()
        return lastrunDatetimeObject
    
    def writeRunTime(self):
        """
        writeRunTime(self): writes the current datetime out to a logfile
            args: None
            return: void
        """
        runtime = datetime.datetime.now()
        
        runtimeString = str(runtime)
        
        try:
            logFile = open(self.__LOGFILEPATH,"w")
            logFile.write(runtimeString)
        except Exception as e:
            print e
            return
            
        logFile.close()
        return
    
    def needToRun(self):
        """
        needToRun(self): checks if the last run time of the run method was called was a certain
        interval of time ago. If it was run in less than that interval, return False
        if more than that interval, return True because the main run method needs to
        be executed. 
            args: None
            return: Boolean
        """
        timeDelta = self.__TIMEDELTA
        #get last run time
        time = self.checkLastRun()
        #check if module has never been run
        if(time == "never"):
            return True
        
        #if it has been run check delta time
        currentTime = datetime.datetime.now()
        dif = currentTime - time
        
        delta = datetime.timedelta(hours=timeDelta)
        
        print "last run time: ", time
        print "current time : ", currentTime
        
        if(dif > delta):
            print "needs to be run"
            return True
        else:
            print "doesn't need to be run"
            return False
            
    

class DdmDatabaseHandler(DdmDatasetsController):
    #Class Variables
    #__db = sqlite3.connect('mydq2db')
    __db = None
    __cursor = None
    
    def __init__(self, databaseName='mydq2db'):
        # Creates or opens a file called whatever the user specifies with the default being 'mydq2db' with a SQLite3 DB
        self.__db = sqlite3.connect(databaseName)
        
        # Get a cursor object
        self.__cursor = self.__db.cursor()
        
        '''Need to check if Table has already been created. If so, don't recreate the table'''
        try:
            self.__cursor.execute('''CREATE TABLE dq2(id INTEGER PRIMARY KEY, datasetname TEXT, datasetsize INT, datasetowner TEXT, datasetdate TEXT)''')
            self.__db.commit()
        except Exception as e:
            print e
            print "moving on"

    def getFromDatabase(self, column):
        """
        getFromDatabase(self, column): takes the name of a column the a user
            wishes to receive all information in that column from the database.
            args:
                column: The name of the column a user wishes to receive
                    all values in the table from.
            return: Array of values from the given column in the table 
                (Array of Strings).
        """

        data = []
        returnData = []
        try: 
            # This may be unsafe... Look for a different way for this to work? 
            self.__cursor.execute('SELECT %s FROM dq2' %column)
            data = self.__cursor.fetchall() ## returns tuples
        except Exception as e:
            print e

        #Change the returned value from list of tuples to list of string
        for x in data:
            adata = x[0]
            returnData.append(adata)

        return returnData

    def checkIfExists(self, dataset):
        """
        checkIfExists(self, dataset): Checks if the given dataset already exists in the database.
            args: 
                dataset: The name of a given dataset
            return: Boolean, True if it exists, False if it does not exist
        """
        
        data = []
        resultData = []
        try: 
            self.__cursor.execute('SELECT datasetname FROM dq2 WHERE datasetname=?',(dataset,))
            data = self.__cursor.fetchall()     #returns tuples
        except Exception as e: 
            print e
            
        
        #transform list of tuples (data) to list of strings(resultData)
        for x in data:
            adata = x[0]
            resultData.append(adata)
        
        #check if dataset is in is in the list
        for x in resultData:
            if(x == dataset):
                return True
            
        return False

    def updateDatabase(self, listOfDatasets):
        """
        updateDatabase(self, listToDelete, db, cursor):
            args: 
                listToDelete: a list of dataset names that need to be deleted from the database
                db: a database connection. 
                cursor: a cursor object that is a cursor to the provided db connection. 
            return: void
        """
        
        databaseSet = set(self.getFromDatabase("datasetname"))
        gridSet = set(listOfDatasets)
        
        toDeleteFromDatabase = databaseSet - gridSet
        toAddToDatabase = gridSet - databaseSet
        
        #turn result sets into a list
        toDeleteFromDatabase = list(toDeleteFromDatabase)
        toAddToDatabase = list(toAddToDatabase)
        
        #remove any datasets that are no longer in Existing from Database 
        self.deleteFromDatabase(toDeleteFromDatabase) 
        #add any datasets that are not in the Database
        self.addToDatabase(toAddToDatabase)

    def addToDatabase(self, listOfDatasets):
        """
        addToDatabase(self, listToDelete, db, cursor): Takes a list of dataset names and adds
            those datasets to the database.
            args: 
                listToDelete: a list of dataset names that need to be added to the database.
                db: a database connection. 
                cursor: a cursor object that is a cursor to the provided db connection. 
            return: void
        """
        #to get the number of changes prior to this method call
        priorChanges = self.__db.total_changes
        
        #to get metadata
        #DdmController = DdmDatasetsController()
        
        #update database with any new datasets that it doesn't have. 
        for dataset in listOfDatasets:
            #Before getting metadata we should check to see if it already exists
            #alreadyExists = self.checkIfExists(dataset)
            #if not alreadyExists: 
                #datasetInfo = DdmController.getMetaData(dataset) # query the attribute
                datasetInfo = self.getMetaData(dataset)
                if datasetInfo == 'None': continue
                else: 
                    if 'length' in datasetInfo.keys():
                        datasetsize  = float(datasetInfo['length'])
                    else:
                        continue
                    user = (datasetInfo['owner'])
                    datasetowner = re.sub("/(.*)/CN=","", user)
                    datasetdate  = (datasetInfo['creationdate'])
                    self.__cursor.execute('''INSERT INTO dq2(datasetname, datasetsize, datasetowner, datasetdate) VALUES(?,?,?,?)''', (dataset, datasetsize,datasetowner,datasetdate))
                    self.__db.commit()
        print "Total Added: " , self.__db.total_changes - priorChanges
        
    def deleteFromDatabase(self, listToDelete):
        """
        deleteFromDatabase(self, listToDelete, db, cursor): Takes a list of dataset names and deletes
            those datasets from the database.
            args: 
                listToDelete: a list of dataset names that need to be deleted from the database.
                db: a database connection. 
                cursor: a cursor object that is a cursor to the provided db connection. 
            return: void
        """
        #to get the number of changes prior to this method call
        priorChanges = self.__db.total_changes
        
        #remove datasets that no longer exist. 
        for item in listToDelete: 
            try:
                self.__cursor.execute('DELETE FROM dq2 WHERE datasetname=?',(item,))
                self.__db.commit()
            except Exception as e:
                print "Cannot delete"
                print e
        
        print "Total Deleted: " , self.__db.total_changes - priorChanges

    def close(self):
        """
        close(self): Closes the connection to the database
        args: None
        return: void
        """
        
        self.__cursor.close()
        self.__db.close()

def main():
    print "Starting [AtlasDdmDatasetsViewer]"
    logging.basicConfig(level=logging.INFO)
    logging.root.setLevel(logging.DEBUG)
    
    # Make DatasetViewer
    datasetViewer = DdmDatasetsController()
     
    #call run
    datasetViewer.run("GOEGRID_LOCALGROUPDISK")
     
    ''' check that the info is actually in the Data base '''
    data = datasetViewer.getFromDatabase('datasetname')
    '''
    thing = None
    for x in data: 
        print x
        thing = x
         
    print datasetViewer.checkIfExists(thing)
    '''

    
    ##################### Testing Database Class ###################################
    #database = DdmDatabaseHandler('mydq2db')
    #data = database.getFromDatabase('datasetname')
    #for x in data:
    #    print x
    #database.close()
    
    #datasetViewer.writeRunTime()
    #print datasetViewer.needToRun()
    
    
    

if __name__ == '__main__':
    main()






        






