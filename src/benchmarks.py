#!/usr/bin/python

'''
benchmarks.py - Python script to carry out the actual benchmarking of MongoDB.

This is the file that would be set to run every so often, to allow
for data collection, and should be the only thing that the user has
to set to run. This script will call all the necessary helper modules
to log all the appropriate data.

'''


__author__ = ('jasonrdsouza (Jason Dsouza)')

import sys
import getopt
import pprint
import MongoLogger
import DriveStats
import DatabaseStatus
import QueryStats


def usage():
  '''Prints command line usage help of the script'''
  print 'Sample Usage:'
  print '\tpython benchmarks.py --host [mongodb hostname] --port [mongodb port #] --db [mongodb name]'
  print 'Optional Args:'
  print '\t--hd (turn hd logging on)'
  print '\t--dbstats (turn database stats logging on)'
  print '\t--query (turn query time logging on)'
  print

def printLogEntry(entry):
  pprint.pprint(entry)

def main():
  '''Program to run all the desired benchmarks against the db
     and store the results for later processing.
     CONFIGURE THE COMMANDS BELOW, ONCE I KNOW WHAT THEY SHOULD BE!!!'''
  # Parse command line options (if present)
  try:
    opts, args = getopt.getopt(sys.argv[1:], '', ['host=', 'port=', 'db=', 'hd', 'dbstats', 'query', 'help'])
  except getopt.error, msg:
    usage()
    print 'Error:', str(msg)
    sys.exit(2)
  host = '10.0.100.40'
  port = 27017
  db = 'benchmarks'
  hd_logging = False
  dbstats_logging = False
  query_logging = False
  # Process options
  for option, arg in opts:
    if option == '--host':
      host = arg
    elif option == '--port':
      port = arg
    elif option == '--db':
      db = arg
    elif option == '--hd':
      hd_logging = True
    elif option == '--dbstats':
      dbstats_logging = True
    elif option == '--query':
      query_logging = True
    elif option == '--help':
      usage()
    else:
      assert False, "unhandled option"
  
  '''Carry out the benchmarks, given the command line options'''
  # Get a logger instance to write benchmark data
  benchmarkDB = MongoLogger.mongoCRUD(host, port)
  benchmarkDB.initBenchmarkDB(db)
  
  if hd_logging: # Do harddrive stats benchmarking
    driveStatsEntry = DriveStats.getDriveStatsLogEntry()
    benchmarkDB.addHdUsageEntry(driveStatsEntry);
  
  if dbstats_logging: # Do database status benchmarking
    status_db = DatabaseStatus.databaseStatus(host, port, "data")
    db_entries_list = []
    db_entries_list.append(status_db.getServerStatusLogEntry())
    db_entries_list.append(status_db.getDatabaseStatsLogEntry())
    db_entries_list.append(status_db.getAvailableShardsLogEntry())
    db_entries_list.append(status_db.getCollectionStatsLogEntry("historical"))
    db_entries_list.append(status_db.getChunkDistributionLogEntry())
    # Use printLogEntry(test_entry) to debug a test entry
    benchmarkDB.addDbStatusEntry(db_entries_list)
  
  if query_logging: # Do query speed benchmarking
    db_querier = QueryStats.MongoQuerier(host, port, "data", "historical")
    '''do a bunch of queries, and log them all, using the above dbstats 
       list logging technique'''
    query_builder = QueryStats.QueryBuilder()
    query_entries_list = []
    query_entries_list.append(db_querier.getQueryExplainLogEntry(query_builder.smallSetQuery))
    query_entries_list.append(db_querier.getQueryExplainLogEntry(query_builder.mediumSetQuery))
    query_entries_list.append(db_querier.getQueryExplainLogEntry(query_builder.largeSetQuery))
    query_entries_list.append(db_querier.getCollectionMapReduceLogEntry(query_builder.map_totalvolume, query_builder.reduce_totalvolume, "mr_totalvolume"))
    query_entries_list.append(db_querier.getCollectionMapReduceLogEntry(query_builder.map_averageAsk, query_builder.reduce_averageAsk, "mr_averageask"))
    # Log all the query log entries
    benchmarkDB.addQuerySpeedEntry(query_entries_list)
  
  '''INSERTION LOGGING OCCURS AUTOMATICALLY'''
  
  # Add more benchmarks here...
  

# Boilerplate code to get the program to run from the command line
if __name__ == '__main__':
  main()