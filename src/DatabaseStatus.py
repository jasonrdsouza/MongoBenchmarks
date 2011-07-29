#!/usr/bin/python

'''
DatabaseStatus.py - Python script to get MongoDB server information for logging.

Resources used:
-http://www.mongodb.org/display/DOCS/serverStatus+Command
-http://www.mongodb.org/display/DOCS/Monitoring+and+Diagnostics
-http://www.mongodb.org/display/DOCS/Database+Profiler
-http://www.mongodb.org/display/DOCS/mongostat
-http://www.mongodb.org/display/DOCS/mongosniff
-Erez Zarum (erezz at icinga.org.il)

'''


__author__ = ('jasonrdsouza (Jason Dsouza)')

import pymongo
from pymongo import Connection
import datetime
import re


class databaseStatus:
  '''Class to facilitate connecting to a mongo database, and pulling
     useful statistics from it by utilizing mongo's build in monitoring
     and profiling functions.'''
  def __init__(self, host, port, db):
    connection = Connection(host, port)
    self.host = host
    self.port = port
    self.database = pymongo.database.Database(connection, db)
    self.configdb = pymongo.database.Database(connection, "config")
  
  def getServerStatusLogEntry(self):
    '''Function to return a log entry for the db.serverStatus() mongo command'''
    server_status = self.database.command("serverStatus")
    server_status["EntryType"] = "Server Status"
    server_status["Timestamp"] = datetime.datetime.now()
    server_status["Host"] = self.host
    server_status["Port"] = self.port
    return server_status
  
  def getDatabaseStatsLogEntry(self):
    '''Function to return a log entry for the db.stats() mongo command'''
    database_stats = self.database.command("dbstats")
    database_stats["EntryType"] = "Database Stats"
    database_stats["Timestamp"] = datetime.datetime.now()
    '''hack to fix a stupid ip address dict problem'''
    database_stats["raw"]["10,0,100,38:10000"] = database_stats["raw"]["10.0.100.38:10000"]
    del database_stats["raw"]["10.0.100.38:10000"]
    database_stats["raw"]["10,0,100,40:10000"] = database_stats["raw"]["10.0.100.40:10000"]
    del database_stats["raw"]["10.0.100.40:10000"]
    database_stats["raw"]["10,0,100,41:10000"] = database_stats["raw"]["10.0.100.41:10000"]
    del database_stats["raw"]["10.0.100.41:10000"]
    database_stats["raw"]["10,0,100,42:10000"] = database_stats["raw"]["10.0.100.42:10000"]
    del database_stats["raw"]["10.0.100.42:10000"]
    database_stats["raw"]["10,0,100,43:10000"] = database_stats["raw"]["10.0.100.43:10000"]
    del database_stats["raw"]["10.0.100.43:10000"]
    '''end hack'''
    return database_stats
    
  def getAvailableShardsLogEntry(self):
    '''Function to get the shards of a particular db'''
    avail_shards = {"EntryType" : "Available Shards",
                    "Timestamp" : datetime.datetime.now()}
    i = 1 #needed to put the shards in the dictionary
    for shard in self.configdb.shards.find():
      avail_shards[str(i)] = shard
      i = i+1
    return avail_shards
  
  def getCollectionStatsLogEntry(self, collection_name):
    '''Function to get a specific collections stats'''
    coll_stats = self.database.command("collstats", collection_name)
    coll_stats["EntryType"] = "Collection Stats"
    coll_stats["Timestamp"] = datetime.datetime.now()
    return coll_stats
  
  def getChunkDistributionLogEntry(self):
    '''Function to return the current distribution of the chunks across shards'''
    chunkLogEntry = {"EntryType" : "Chunk Distribution",
                     "Timestamp" : datetime.datetime.now()}
    databases = self.configdb.databases.find( {"partitioned" : True} )
    for db in databases:
      collections = self.configdb.collections.find( { "_id" : { "$regex" : re.compile("^" + db['_id'] + ".") } }, { "dropped" : False } )
      for colls in collections:
        result = self.configdb.chunks.group( 
                                  key=['shard'],
                                  condition={ 'ns' : colls['_id'] },
                                  initial={ "nChunks" : 0 },
                                  reduce="function (doc, out) { out.nChunks++; }"
                                            )
        results_list = []
        for value in result:
          results_list.append({value['shard'] : str(value['nChunks'])})
        chunkLogEntry[colls['_id'].replace(".",",")] = results_list
    return chunkLogEntry


def main():
  '''Program to log various statistics of a mongo database'''

if __name__ == '__main__':
  main()