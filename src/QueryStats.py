#!/usr/bin/python

'''
QueryStats.py - Python script to benchmark the query speed of 
a MongoDB server, and allow for arbitrary queries and map-reduce 
operations to be run against the database.

'''


__author__ = ('jasonrdsouza (Jason Dsouza)')

from pymongo import Connection
from bson.code import Code
import time
import datetime
import sys
import getopt
#import ast
import json


class MongoQuerier:
  '''Simple class to query against the connected MongoDB.
     Requires an active mongod instance to work.
     Defaults to connecting to a local MongoDB instance.'''
  def __init__(self, host, port, db_name, collection_name):
    self.connection = Connection(host, port)
    self.database = self.connection[db_name]
    self.collection = self.database[collection_name]
    # Set profiling level to log slow events
    #self.database.set_profiling_level(pymongo.SLOW_ONLY)
    
  def switchCollection(self, new_db_name, new_collection_name):
    '''Function to allow switching which collection queries are performed on'''
    self.database = self.connection[new_db_name]
    self.collection = self.database[new_collection_name]
    
  def queryCollection(self, query = None):
    '''Function to allow for querying the currently active collection.
       If no query is supplied, all the documents in the active collection
       are returned. Otherwise, documents matching the query are returned.
       The return type is a list of result documents'''
    resultsList = []
    for doc in self.collection.find(query):
      resultsList.append(doc)
    return resultsList
  
  def timeQuery(self, query = None):
    '''Function to time a specific query sent to the database.'''
    x = time.time()
    self.queryCollection(query)
    return (time.time() - x)
  
  def getQueryExplainLogEntry(self, query = None):
    '''Function to get the "explain plan" information for a specific query'''
    queryCursor = self.collection.find(query)
    entry = queryCursor.explain()
    entry["EntryType"] = "Query Explain"
    entry["Timestamp"] = datetime.datetime.now()
    entry["Query"] = str(query)
    '''hack to fix a stupid ip address dict problem
    entry["shards"]["10,0,100,38:10000"] = entry["shards"]["10.0.100.38:10000"]
    del entry["shards"]["10.0.100.38:10000"]
    entry["shards"]["10,0,100,40:10000"] = entry["shards"]["10.0.100.40:10000"]
    del entry["shards"]["10.0.100.40:10000"]
    entry["shards"]["10,0,100,41:10000"] = entry["shards"]["10.0.100.41:10000"]
    del entry["shards"]["10.0.100.41:10000"]
    entry["shards"]["10,0,100,42:10000"] = entry["shards"]["10.0.100.42:10000"]
    del entry["shards"]["10.0.100.42:10000"]
    entry["shards"]["10,0,100,43:10000"] = entry["shards"]["10.0.100.43:10000"]
    del entry["shards"]["10.0.100.43:10000"]
    end hack'''
    '''Even larger hack'''
    entry["shards"] = "removed"
    return entry
  
  def getQuerySize(self, query = None):
    '''Returns the number of documents in the collection that match the 
       specified query. If no query is supplied, then the returned number
       is the number of documents in the whole collection.'''
    return self.collection.find(query).count()
  
  def getDistinct(self, key):
    '''Function to wrap the MongoDB distinct command, which returns a list
       of distinct values for the given key across a collection'''
    return self.collection.distinct(key)
  
  def collectionMapReduce(self, map_fun, reduce_fun, result_collection_name, mr_query = None):
    '''Function to perform a map-reduce operation on the current collection.
       The optional query parameter allows for limiting the documents that
       will be mapped over to the ones that match the query.'''
    result = self.collection.map_reduce(map_fun, reduce_fun, result_collection_name, query=mr_query)
    results_list = []
    for doc in result.find():
      results_list.append(doc)
    return results_list
  
  def getCollectionMapReduceLogEntry(self, map_fun, reduce_fun, result_collection_name, mr_query=None):
    '''Performs the same task as the collectionMapReduce function, but
       only returns the useful log details of the map_reduce, as opposed
       to the actual results. Use for benchmarking purposes.'''
    entry = self.collection.map_reduce(map_fun, reduce_fun, result_collection_name, query=mr_query, full_response=True)
    entry["EntryType"] = "Map Reduce Info"
    entry["Timestamp"] = datetime.datetime.now()
    entry["Mapper"] = str(map_fun)
    entry["Reducer"] = str(reduce_fun)
    entry["OptionalQuery"] = str(mr_query)
    '''hack to fix a stupid ip address dict problem'''
    entry["shardCounts"]["10,0,100,38:10000"] = entry["shardCounts"]["10.0.100.38:10000"]
    del entry["shardCounts"]["10.0.100.38:10000"]
    entry["shardCounts"]["10,0,100,40:10000"] = entry["shardCounts"]["10.0.100.40:10000"]
    del entry["shardCounts"]["10.0.100.40:10000"]
    entry["shardCounts"]["10,0,100,41:10000"] = entry["shardCounts"]["10.0.100.41:10000"]
    del entry["shardCounts"]["10.0.100.41:10000"]
    entry["shardCounts"]["10,0,100,42:10000"] = entry["shardCounts"]["10.0.100.42:10000"]
    del entry["shardCounts"]["10.0.100.42:10000"]
    entry["shardCounts"]["10,0,100,43:10000"] = entry["shardCounts"]["10.0.100.43:10000"]
    del entry["shardCounts"]["10.0.100.43:10000"]
    '''end hack'''
    return entry
  
  def timeMapReduce(self, map_fun, reduce_fun, query = None):
    '''Function to time a specific map-reduce operation done on the database'''
    x = time.time()
    self.collectionMapReduce(map_fun, reduce_fun, query)
    return (time.time() - x)
  
  def getProfilingLogEntry(self):
    '''Function to get profiling information of the database'''
    return self.database.profiling_info()
  
  def interactiveMode(self):
    '''Function to allow interactive querying of the mongo db from the 
       command line, using this class'''
    start = datetime.datetime(2010, 4, 1)
    end = datetime.datetime(2010, 4, 2)
    timeRangeQuery = {"HistoricalDate" : {"$gte" : start, "$lt" : end}}
    print self.queryCollection(timeRangeQuery)
    while True:
      query_str = raw_input('Enter query in JSON form (or type exit): ')
      if str(query_str) == "exit":
        print "goodbye"
        break
      elif str(query_str) == "distinct":
        key = raw_input('Enter a keyword to perform distinct operation on: ')
        print self.getDistinct(key)
        break
      query = json.loads(query_str)
      print '======= Begin Query Results ======='
      print self.queryCollection(query)
      print '======= End Query Results ========='


class QueryBuilder:
  '''Class to build queries for use with the MongoQuerier class'''
  def __init__(self):
    self.timeStart = datetime.datetime(2011, 1, 3, 9, 52)
    self.timeEnd = datetime.datetime(2011, 1, 3, 9, 55)
    self.largeSetQuery = {"Symbol" : "MSFT"}
    self.mediumSetQuery = {"HistoricalTimestamp" : {"$gte" : self.timeStart, "$lt" : self.timeEnd}}
    self.smallSetQuery = {"Symbol" : "TRI", "BidSize" : 100, "AskPrice" : 37.96}
    self.javascriptQuery = "this.AskSize > 100000" #arbitrary javascript querys can be executed!
    self.regexQuery = {"Symbol" : {"$regex" : "B.S", "$options" : "i"}}
    self.map_totalvolume = Code("function () {"
                "if(this.TickType == \"Trade\")"
                "  {emit(this.Symbol, this.Volume);}"
                "}")
    self.reduce_totalvolume = Code("function (key, values) {"
                   "  var totalVolume = 0;"
                   "  for(var i = 0; i < values.length; i++) {"
                   "    totalVolume += values[i];"
                   "  }"
                   "  return totalVolume;"
                   "}")
    self.map_averageAsk = Code("function () {"
                "if(this.TickType == \"Quote\")"
                "  {emit(this.Symbol, {AskPrice: this.AskPrice, AskSize: this.AskSize});}"
                "}")
    self.reduce_averageAsk = Code("function (key, values) {"
                   "  var totalPrice = 0;"
                   "  var totalSize = 0;"
                   "  for(var i = 0; i < values.length; i++) {"
                   "    totalPrice += (values[i].AskPrice * values[i].AskSize);"
                   "    totalSize += values[i].AskSize"
                   "  }"
                   "  var newPrice = totalPrice/totalSize;"
                   "  return {AskPrice: newPrice, AskSize: totalSize};"
                   "}")
    self.finalize_averageAsk = Code("function (key, value) {"
                                    "  return value.AskPrice"
                                    "}")
    
    
    # self.uniqueKeyQuery = {"Ticker" : "GOOG", "High" : {"$gt":747}}
    
    
    

def main():
  '''Function to test the MongoQuerier class from the command line, to
     make allow for testing of the module and queries'''
  # Parse command line options (if present)
  try:
    opts, args = getopt.getopt(sys.argv[1:], '', ['host=', 'port=', 'dbname=', 'cname='])
  except getopt.error, msg:
    print 'Error:', str(msg)
    sys.exit(2)
  host = ''
  port = 0
  database = ''
  collection = ''
  # Process options
  for option, arg in opts:
    if option == '--host':
      host = arg
    elif option == '--port':
      port = int(arg)
    elif option == '--dbname':
      database = arg
    elif option == '--cname':
      collection = arg
    else:
      assert False, "unhandled option"
  # Get parameters not passed in as command line args
  while not host:
    host = raw_input('Please enter the hostname of the Mongo database: ')
  while not port:
    port_str = raw_input('Please enter the port to connect Mongo with: ')
    port = int(port_str)
  while not database:
    database = raw_input('Please enter the database to be queried: ')
  while not collection:
    collection = raw_input('Please enter the collection to be queried: ')

  # Instantiate the MongoQuerier class
  querier = MongoQuerier(host, port, database, collection)
  # Run it in interactive mode
  querier.interactiveMode()
  '''
  # Use the query builder to test querying
  builder = QueryBuilder(querier)
  builder.sampleHistoricalMR()
  '''
  

# Boilerplate code to get the program to run from the command line
if __name__ == '__main__':
  main()