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
    
    #Constructor
    def __init__(self, spaceToken=None):
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
        
        # Open connection to database
        db = sqlite3.connect('mydq2db')
        
        # Set the Select command
        # itemName = (column,)
        
        # Get a cursor object
        cursor = db.cursor()
        data = []
        returnData = []
        try: 
            # This may be unsafe... Look for a different way for this to work? 
            cursor.execute('SELECT %s FROM dq2' %column)
            data = cursor.fetchall()   ## returns tuples
        except Exception as e:
            print e

        #Change the returned value from list of tuples to list of string
        for x in data:
            adata = x[0]
            returnData.append(adata)
        
        #Close cursor and connection
        cursor.close()
        db.close()

        return returnData
    
    def checkIfExists(self, dataset):
        """
        checkIfExists(self, dataset): Checks if the given dataset already exists in the database.
            args: 
                dataset: The name of a given dataset
            return: Boolean, True if it exists, False if it does not exist
        """
        
        db = sqlite3.connect('mydq2db')
        cursor = db.cursor()
        data = []
        resultData = []
        try: 
            cursor.execute('SELECT datasetname FROM dq2 WHERE datasetname=?',(dataset,))
            data = cursor.fetchall() 
        except Exception as e: 
            print e
            
        #Close cursor and connection
        cursor.close()
        db.close()
        
        for x in data:
            adata = x[0]
            resultData.append(adata)
        
        for x in resultData:
            if(x == dataset):
                return True
            
        return False
        
    def updateDatabase(self, ListOfDatasets, db, cursor):
        """
        updateDatabase(self, listToDelete, db, cursor):
            args: 
                listToDelete: a list of dataset names that need to be deleted from the database
                db: a database connection. 
                cursor: a cursor object that is a cursor to the provided db connection. 
            return: void
        """
        
        databaseSet = set(self.getFromDatabase("datasetname"))
        gridSet = set(ListOfDatasets)
        
        toDeleteFromDatabase = databaseSet - gridSet
        toAddToDatabase = gridSet - databaseSet
        
        #turn result sets into a list
        toDeleteFromDatabase = list(toDeleteFromDatabase)
        toAddToDatabase = list(toAddToDatabase)
        
        #remove any datasets that are no longer in Existing from Database 
        self.deleteFromDatabase(toDeleteFromDatabase, db, cursor) 
        #add any datasets that are not in the Database
        self.addToDatabase(toAddToDatabase, db, cursor)     
        
    def addToDatabase(self, listOfDatasets, db, cursor):
        """
        addToDatabase(self, listToDelete, db, cursor): Takes a list of dataset names and adds
            those datasets to the database.
            args: 
                listToDelete: a list of dataset names that need to be added to the database.
                db: a database connection. 
                cursor: a cursor object that is a cursor to the provided db connection. 
            return: void
        """
        
        #update database with any new datasets that it doesn't have. 
        for dataset in listOfDatasets:
            #Before getting metadata we should check to see if it already exists
            #alreadyExists = self.checkIfExists(dataset)
            #if not alreadyExists: 
                datasetInfo = self.getMetaData(dataset) # query the attribute
                if datasetInfo == 'None': continue
                else: 
                    if 'length' in datasetInfo.keys():
                        datasetsize  = float(datasetInfo['length'])
                    else:
                        continue
                    user = (datasetInfo['owner'])
                    datasetowner = re.sub("/(.*)/CN=","", user)
                    datasetdate  = (datasetInfo['creationdate'])
                    cursor.execute('''INSERT INTO dq2(datasetname, datasetsize, datasetowner, datasetdate) VALUES(?,?,?,?)''', (dataset, datasetsize,datasetowner,datasetdate))
                    db.commit()
        print "Total Added: " , db.total_changes
    
    def deleteFromDatabase(self, listToDelete, db, cursor):
        """
        deleteFromDatabase(self, listToDelete, db, cursor): Takes a list of dataset names and deletes
            those datasets from the database.
            args: 
                listToDelete: a list of dataset names that need to be deleted from the database.
                db: a database connection. 
                cursor: a cursor object that is a cursor to the provided db connection. 
            return: void
        """
        
        #remove datasets that no longer exist. 
        for item in listToDelete: 
            try:
                cursor.execute('DELETE FROM dq2 WHERE datasetname=?',(item,))
                db.commit()
            except Exception as e:
                print "Cannot delete"
                print e
        
        print "Total Deleted: " , db.total_changes

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
        ### SQLITE3
    
        # Creates or opens a file called mydb with a SQLite3 DB
        db = sqlite3.connect('mydq2db')
        
        # Get a cursor object
        cursor = db.cursor()
        
        '''Need to check if Table has already been created. If so, don't recreate the table'''
        try:
            cursor.execute('''CREATE TABLE dq2(id INTEGER PRIMARY KEY, datasetname TEXT, datasetsize INT, datasetowner TEXT, datasetdate TEXT)''')
            db.commit()
        except Exception as e:
            print e
            print "moving on"

        # Querying token
        ListOfDatasets = []
        ListOfDatasets = self.limited(token)
        ##ListOfDatasets = self.listDatasets(token)
        
        #update the database
        self.updateDatabase(ListOfDatasets, db, cursor)

        #Close cursor and connection
        cursor.close()
        db.close()
    
    
    
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
                
        
        for x in range(0,10):
            # make sure no duplicates 
            shortlistOfDatasets.append(datasets[x])
        
        return shortlistOfDatasets
        

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
    #'''
    thing = None
    for x in data: 
        print x
        thing = x
        
    print datasetViewer.checkIfExists(thing)
    #'''
    '''
    db = sqlite3.connect('mydq2db')
    cursor = db.cursor()
    
    datasetViewer.deleteFromDatabase(data, db, cursor)
    data2 = datasetViewer.getFromDatabase('datasetname')
    print data2
    
    cursor.close()
    db.close()
    '''

if __name__ == '__main__':
    main()
