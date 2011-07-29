#!/usr/bin/python

'''
MongoLogger.py - python script to allow storing data in MongoDB

Tutorials Used: 
  -http://api.mongodb.org/python/1.8.1/index.html

'''

__author__ = ('jasonrdsouza (Jason Dsouza)')

import sys
import getopt
import datetime
import time
#import pymongo
from pymongo import Connection


class mongoCRUD:
  '''Simple class for persistant storage using MongoDB.
     Requires that MongoDB is installed to work.
     Defaults to connecting to a local MongoDB instance.'''
  def __init__(self, host, port):
    self.connection = Connection(host, port)
  
  def _createDB(self, db_name):
    '''Creates a new database with the given name'''
    return self.connection[db_name]
    
  def _createCollection(self, db, coll_name):
    '''Creates a new collection with coll_name as its name, in the supplied 
       database.'''
    return db[coll_name]
  
  def initBenchmarkDB(self, db_name):
    '''Creates a new database in MongoDB, that has the necessary structure
       to function as a database for benchmarking the DB, and store all its
       data'''
    self.database = self._createDB(db_name)
    self.hd_usage = self._createCollection(self.database, 'hd_usage')
    self.insertion_speed = self._createCollection(self.database, 'insertion_speed')
    self.query_speed = self._createCollection(self.database, 'query_speed')
    self.db_status = self._createCollection(self.database, 'db_status')
    #self.full_log = self._createCollection(self.database, 'full_log') --> add more collections here, if necessary
  
  def initDataDB(self, db_name, coll_name):
    '''Creates a database for inserting "application data" into, to benchmark 
       querying speeds, insertion speed, etc'''
    self.dataDB = self._createDB(db_name)
    self.dataColl = self._createCollection(self.dataDB, coll_name)
    self.initBenchmarkDB('benchmarks')
  
  def killBenchmarkDB(self):
    '''Drops the benchmarking database. Akin to deleting all the data associated with
       the benchmarking. Useful if the user wants to start over with a fresh 
       database with nothing in it. Once executed, call 'initBenchmarkDB' to 
       get a clean Benchmark database.'''
    self.hd_usage.drop()
    self.insertion_speed.drop()
    self.query_speed.drop()
    self.db_status.drop()
    #self.full_log.drop() --> add more collections here, if necessary, as well
  
  def killDataDB(self):
    '''Drops the data datebase. Similar to the killBenchmarkDB function'''
    self.dataColl.drop()
  
  def addHdUsageEntry(self, entry):
    '''Inserts a harddrive usage log entry into the database. The entry must be a valid
       JSON string. Function accepts multiple entries as a list. 
       This function returns the entry ID(s)'''
    return self.hd_usage.insert(entry)
    
  def addInsertionSpeedEntry(self, entry):
    '''Inserts an insertion speed log entry into the database. The entry must be a valid
       JSON string. Function accepts multiple entries as a list. 
       This function returns the entry ID(s)'''
    return self.insertion_speed.insert(entry)
    
  def addQuerySpeedEntry(self, entry):
    '''Inserts a query speed log entry into the database. The entry must be a valid
       JSON string. Function accepts multiple entries as a list. 
       This function returns the entry ID(s)'''
    return self.query_speed.insert(entry)
    
  def addDbStatusEntry(self, entry):
    '''Inserts a DB status log entry into the database. The entry must be a valid
       JSON string. Function accepts multiple entries as a list. 
       This function returns the entry ID(s)'''
    return self.db_status.insert(entry)
  
  def insertData(self, entry):
    '''Inserts a data entry into the data database. The entry must be a valid JSON
       string. Function accepts multiple entries as a list.
       This function also logs how long the insertion takes.'''
    logEntry = {"EntryType" : "Insertion Speed",
           "Timestamp" : datetime.datetime.now(),
           "InsertAmount" : int(len(entry)),
           "InsertType" : str(type(entry))}
    start = time.time()
    self.dataColl.insert(entry)
    end = time.time()
    logEntry["Start"] = float(start)
    logEntry["End"] = float(end)
    logEntry["SecondsToInsert"] = float(end - start)
    self.addInsertionSpeedEntry(logEntry)
  
  def Test(self):
    '''Method that gets called when executing MongoLogger.py from the
       command line. Currently tests BenchmarkDB creation'''
    # If database not present, prompt user for input
    db = None
    while not db:
      db = raw_input('Please enter the database name to create: ')
      if not db:
        print 'Must enter a valid database name to proceed'
    self.initBenchmarkDB(db)
    print 'Successfully created database', db

def main():
  '''Simple Python program to communicate with a MongoDB database, 
     to store and keep track of benchmark data'''
  # Parse command line options (if present)
  try:
    opts, args = getopt.getopt(sys.argv[1:], '', ['host=', 'port='])
  except getopt.error, msg:
    print 'python MongoLogger.py --host [hostname] --port [port number]'
    sys.exit(2)
  host = 'localhost'
  port = 27017
  # Process options
  for option, arg in opts:
    if option == '--host':
      host = arg
    elif option == '--port':
      port = arg
  
  # Test the program
  mdb = mongoCRUD(host, port)
  mdb.Test()

# Boilerplate code to get the program to run from the command line
if __name__ == '__main__':
  main()