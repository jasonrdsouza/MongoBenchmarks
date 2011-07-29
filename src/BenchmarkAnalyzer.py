#!/usr/bin/python

'''
BenchmarkAnalyzer.py - Python script to extract benchmark data from 
the MongoDB benchmarks database, and analyze/ visualize it.

Uses the Google Charts API, and the pygooglecharts module.
-http://pygooglechart.slowchop.com

'''


__author__ = ('jasonrdsouza (Jason Dsouza)')

import getopt
import sys
from QueryStats import MongoQuerier
import pygooglechart
import time
import csv


class BenchmarkDataGrabber:
  '''Class to pull the benchmark data from the benchmarks db where it is logged, 
     and put it into the correct format for a grapher to interpret and generate 
     graphs with. All data is formatted into a list of (x,y) coordinates, or a
     dictionary of such lists, depending on the particular dataset.'''
  def __init__(self, host, port, db, coll):
    self.db = db
    self.querier = MongoQuerier(host, port, db, coll)
  
  def getHdUsageGraphData(self):
    '''Function to get graph data for the HD usage of all the different 
       servers. Returns a dictionary of server-data_values key-value pairs, 
       where the data_values is a list of (x,y) values (timestamp, usedSpace).'''
    self.querier.switchCollection(self.db, 'hd_usage')
    servers = self.querier.getDistinct('Hostname')
    graphData = {}
    for server in servers:
      rawData = self.querier.queryCollection({"Hostname" : server})
      singleGraphData = []
      for entry in rawData:
        time = entry['Timestamp']
        usedSpaceKB = entry['UsedSpace'][2]
        singleGraphData.append((time,usedSpaceKB))
      graphData[server] = singleGraphData
    return graphData
  
  def getInsertSpeedGraphData(self):
    '''Function to get graph data for the insertion speed benchmarks. 
       Returns a list of (x,y) values (timestamp, secondsToInsert)'''
    self.querier.switchCollection(self.db, 'insertion_speed')
    rawData = self.querier.queryCollection() #gets all the elements in the collection
    graphData = []
    for entry in rawData:
      time = entry['Timestamp']
      speedSecs = entry['SecondsToInsert']
      graphData.append((time, speedSecs))
    return graphData
  
  def getQuerySpeedGraphData(self):
    '''Function to get graph data for the querying speed benchmarks.
       Returns a dictionary of query-data_values key-value pairs,
       where the data_values is a list of (x,y) values (total#scanned, querytime)'''
    self.querier.switchCollection(self.db, 'query_speed')
    queries = self.querier.getDistinct('Query')
    graphData = {}
    for qry in queries:
      rawData = self.querier.queryCollection({"EntryType" : "Query Explain", "Query" : qry})
      singleGraphData = []
      for entry in rawData:
        totalNumScanned = entry['nscanned']
        queryTime = entry['millisTotal']
        singleGraphData.append((totalNumScanned, queryTime))
      graphData[qry] = singleGraphData
    return graphData
  
  def getMapReduceSpeedGraphData(self):
    '''Function to get graph data for the map-reduce speed benchmarks.
       Returns a dictionary of mr_query-data_values key-value pairs,
       where the mr_query is the mapper function as a string, and the
       data_values is a list of (x,y) values (total#scanned, querytime)'''
    self.querier.switchCollection(self.db, 'query_speed')
    queries = self.querier.getDistinct('Mapper')
    graphData = {}
    for qry in queries:
      rawData = self.querier.queryCollection({"EntryType" : "Map Reduce Info", "Mapper" : qry})
      singleGraphData = []
      for entry in rawData:
        totalNumScanned = entry['counts']['input']
        queryTime = entry['timeMillis']
        singleGraphData.append((totalNumScanned, queryTime))
      graphData[qry] = singleGraphData
    return graphData
  
  def getChunkDistributionGraphData(self):
    '''Function to get graph data for the distribution of chunks among the servers.
       Returns a dictionary of shard#-data_values key-value pairs, where the
       data_values is a list of (x,y) values (timestamp, #ofChunks)
       
       This method is dependant on there being 5 shards'''
    self.querier.switchCollection(self.db, 'db_status')
    rawData = self.querier.queryCollection({"EntryType" : "Chunk Distribution"})
    graphData = {}
    shard1data = []
    shard2data = []
    shard3data = []
    shard4data = []
    shard5data = []
    for entry in rawData:
      time = entry['Timestamp']
      chunkNum1 = entry['data,historical'][0]
      chunkNum2 = entry['data,historical'][1]
      chunkNum3 = entry['data,historical'][2]
      chunkNum4 = entry['data,historical'][3]
      chunkNum5 = entry['data,historical'][4]
      '''if (chunkNum1.keys() != ['shard0002']):
        print chunkNum1.keys()'''
      shard1data.append((time,chunkNum1['shard0002']))
      shard2data.append((time,chunkNum2['shard0003']))
      shard3data.append((time,chunkNum3['shard0001']))
      shard4data.append((time,chunkNum4['shard0000']))
      shard5data.append((time,chunkNum5['shard0004']))
    graphData['Shard1'] = shard1data
    graphData['Shard2'] = shard2data
    graphData['Shard3'] = shard3data
    graphData['Shard4'] = shard4data
    graphData['Shard5'] = shard5data
    return graphData

class BenchmarkDataGrapher:
  '''Class to allow for visualizing the benchmark data generated by the
     'benchmarks' module, using Google Charts, and the pygooglechart python
     wrapper for the Google Charts API. Handles data formatted as specified
     by the BenchmarkDataGrabber.'''
  def __init__(self):
    print 'Plotting benchmarks now...'
  
  def graphInsertSpeed(self, data):
    '''Function to take insertion speed data, specified as a list of (x,y)
       values, and generate a line chart with the specified data.'''
    x_data = [self.convertDatetimeToInt(el[0]) for el in data]
    y_data = [el[1] for el in data]
    chart = pygooglechart.XYLineChart(250,100)
    chart.add_data(x_data) #XYLineChart treats alternating add_data calls
    chart.add_data(y_data) #as X and then Y data respectively.
    return chart
  
  def convertDatetimeToInt(self, element):
    '''Helper function to convert a datetime.datetime object into an integer
       since Google Charts requires them.'''
    int_time = int(time.mktime(element.timetuple()) + element.microsecond/1000000.0)
    return int_time
  
  def getChartURL(self, chart):
    '''Function to get the url used to generate a specific chart using the
       Google Charts API. Useful if the actual png file is not required, 
       or if downloading is not working.'''
    return chart.get_url()

class BenchmarkCSVGenerator:
  '''Class to generate a csv file out of the benchmark data, to be loaded 
     into excel and plotted.'''
  def __init__(self):
    print 'CSV generator initialized'
    
  def makeInsertSpeedCSVFile(self, data, filename='insert_speed_data'):
    '''Function to generate a CSV file for insert speed data.'''
    with open(filename+'.csv', 'wb') as f:
      writer = csv.writer(f)
      writer.writerow(['Timestamp', 'Insert Speed'])
      for entry in data:
        writer.writerow([entry[0], entry[1]])
    assert f.closed
    print 'Insert Speed CSV file written'
  
  def makeHDUsageCSVFile(self, data, filename='hd_usage_data'):
    '''Function to generate a CSV file for the HD usage data.'''
    with open(filename+'.csv', 'wb') as f:
      writer = csv.writer(f)
      writer.writerow(['Server', 'Timestamp', 'Used Space'])
      for server in data.keys():
        usage_list = data[server]
        for entry in usage_list:
          writer.writerow([server, entry[0], entry[1]])
    assert f.closed
    print 'HD Usage CSV file written'
  
  def makeQuerySpeedCSVFile(self, data, filename='query_speed_data'):
    '''Function to generate a CSV file for the Query Speed data'''
    with open(filename+'.csv', 'wb') as f:
      writer = csv.writer(f)
      writer.writerow(['Query', 'Total # Scanned', 'Query Time'])
      for query in data.keys():
        querySpeeds = data[query]
        for entry in querySpeeds:
          writer.writerow([query, entry[0], entry[1]])
    assert f.closed
    print 'Query Speed CSV file written'
  
  def makeMapReduceSpeedCSVFile(self, data, filename='map-reduce_speed_data'):
    '''Function to generate a CSV file for the Map Reduce Speed data'''
    with open(filename+'.csv', 'wb') as f:
      writer = csv.writer(f)
      writer.writerow(['MR_Query', 'Total # Scanned', 'Query Time'])
      for query in data.keys():
        mr_speeds = data[query]
        for entry in mr_speeds:
          writer.writerow([query, entry[0], entry[1]])
    assert f.closed
    print 'Map Reduce Speed CSV file written'
    
  def makeChunkDistributionCSVFile(self, data, filename='chunk_distribution_data'):
    '''Function to generate a CSV file for the Chunk Distribution data'''
    with open(filename+'.csv', 'wb') as f:
      writer = csv.writer(f)
      writer.writerow(['Shard #', 'Timestamp', '# of Chunks'])
      for shard in data.keys():
        chunkData = data[shard]
        for entry in chunkData:
          writer.writerow([shard, entry[0], entry[1]])
    assert f.closed
    print 'Chunk Distribution CSV file written'
  

def usage():
  '''Prints command line usage help of the script'''
  print 'Sample Usage:'
  print '\tpython BenchmarkAnalyzer.py --host [mongodb hostname] --port [mongodb port #] --db [mongodb name] --coll [collection name]'
  print

def main():
  '''Function to run the BenchmarkAnalyzer module from the command line'''
  try:
    opts, args = getopt.getopt(sys.argv[1:], '', ['host=', 'port=', 'db=', 'coll='])
  except getopt.error, msg:
    usage()
    print 'Error:', str(msg)
    sys.exit(2)
  host = '10.0.100.40'
  port = 27017
  db = 'benchmarks'
  coll = 'hd_usage'
  # Parse command line options
  for option, arg in opts:
    if option == '--host':
      host = arg
    elif option == '--port':
      port = arg
    elif option == '--db':
      db = arg
    elif option == '--coll':
      coll = arg
  # Get remaining necessary arguments
  '''while not coll:
    coll = raw_input('Please enter the collection name to import data to: ')'''
  
  # Run analyzer
  dataGrabber = BenchmarkDataGrabber(host, port, db, coll)
  csv_generator = BenchmarkCSVGenerator()
  # Generate Insert Data CSV
  insert_data = dataGrabber.getInsertSpeedGraphData()
  csv_generator.makeInsertSpeedCSVFile(insert_data)
  # Generate HD Usage CSV
  hd_usage_data = dataGrabber.getHdUsageGraphData()
  csv_generator.makeHDUsageCSVFile(hd_usage_data)
  # Generate Query Speed CSV
  query_data = dataGrabber.getQuerySpeedGraphData()
  csv_generator.makeQuerySpeedCSVFile(query_data)
  # Generate Map Reduce Speed CSV
  mr_data = dataGrabber.getMapReduceSpeedGraphData()
  csv_generator.makeMapReduceSpeedCSVFile(mr_data)
  # Generate Chunk Distribution CSV
  #chunk_data = dataGrabber.getChunkDistributionGraphData()
  #csv_generator.makeChunkDistributionCSVFile(chunk_data)

# Boilerplate code to get the program to run from the command line
if __name__ == '__main__':
  main()
