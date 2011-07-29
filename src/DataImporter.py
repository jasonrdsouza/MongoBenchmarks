#!/usr/bin/python

'''
DataImporter.py - Python script to import data from a file into 
the MongoDB instance

'''


__author__ = ('jasonrdsouza (Jason Dsouza)')

import getopt
import sys
import os
import csv
import gzip
import datetime
import MongoLogger
import MySQLLogger


class MongoDataImporter:
  '''Class to handle reading arbitrary csv files, and make the data they 
     contain into usable JSON data for storage in MongoDB'''
  def __init__(self, host, port, db_name, coll_name):
    self.logger = MongoLogger.mongoCRUD(host, port)
    self.logger.initDataDB(db_name, coll_name)
  
  def parseHistoricalStockDataToDB(self, filepath):
    '''Function to handle reading historical stock data from a file, and
       generate storable JSON objects which are then stored in MongoDB.
       Uses data from: http://pages.swcp.com/stocks/'''
    if os.path.exists(filepath) and os.path.isfile(filepath):
      print 'Historical stock data import initiated'
      with open(filepath, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
          # Generate the data entry to insert
          entry = {"EntryType" : "Historical Stock Data",
                   "Timestamp" : datetime.datetime.now(),
                   "HistoricalDate" : datetime.datetime.strptime(row[0], '%Y%m%d'),
                   "Ticker" : row[1],
                   "Open" : float(row[2]),
                   "High" : float(row[3]),
                   "Low" : float(row[4]),
                   "Close" : float(row[5]),
                   "Volume" : int(row[6])}
          self.logger.insertData(entry)
      assert f.closed
      print 'Historical stock data import completed'
    else:
      print 'Invalid filepath', filepath
  
  def _parseSingleStockFile(self, folderpath, filepath):
    '''Helper function to parse an individual historical stock price data
       file, named with the ticker symbol, and return a list of valid dicts
       for writing to the database.'''
    print 'Parsing file:', filepath
    entry_list = []
    with open(os.path.join(folderpath, filepath), 'r') as f:
      reader = csv.reader(f, delimiter=' ')
      ticker = filepath[:-4]
      for row in reader:
        # Generate the data entry to insert
        entry = {"EntryType" : "Historical Stock Data",
                 "Timestamp" : datetime.datetime.now(),
                 "HistoricalDate" : datetime.datetime.strptime(row[0], '%Y%m%d'),
                 "Ticker" : ticker,
                 "Open" : float(row[1]),
                 "High" : float(row[2]),
                 "Low" : float(row[3]),
                 "Close" : float(row[4]),
                 "Volume" : int(row[5])}
        entry_list.append(entry)
    assert f.closed
    return entry_list

  def parseHistoricalStockFolderToDB(self, folderpath):
    '''Function to handle going through a folder containing stock data in files,
       named by the ticker symbol of the stock, and converting the information
       into JSON documents to be stored in MongoDB.
       Uses data from: http://www.optiontradingtips.com/resources/historical-data/sp500.html
       --> ASCII format "sp500-ascii.zip" '''
    if os.path.exists(folderpath) and os.path.isdir(folderpath):
      print 'Historical stock folder import initiated'
      listing = os.listdir(folderpath)
      for f in listing:
        entries = self._parseSingleStockFile(folderpath, f)
        self.logger.insertData(entries)
      print 'Historical stock folder import completed'
    else:
      print 'Invalid directory', folderpath

  def _blankToNum(self, input):
    '''Helper function to allow for conversion of a blank column value to an
       integer or float in the parsing functions'''
    if input == '':
      return 0
    else:
      return input

  def _parseSingleRawTickFile(self, folderpath, filepath):
    '''Helper function to parse a single, raw tick file (not gzipped), 
       and return a list of valid dicts to be added to the database.'''
    print 'Parsing file:', filepath
    entry_list = []
    with open(os.path.join(folderpath, filepath), 'r') as f:
      reader = csv.reader(f, delimiter='|')
      region = filepath[0:3]
      for line in reader:
        # Do blank value catching to avoid parsing errors
        row = [self._blankToNum(i) for i in line]
        # Generate the data entry to insert
        if row[0] == 'T': # Tick Data
          entry = {"EntryType" : "Historical Tick Data",
                   "TickType" : "Trade",
                   "Timestamp" : datetime.datetime.now(),
                   "HistoricalTimestamp" : datetime.datetime.strptime(filepath[4:12] + ':' + row[1][0:8], '%Y%m%d:%H:%M:%S'),
                   "Region" : region,
                   "Symbol" : str(row[2]),
                   "Price" : float(row[3]),
                   "Volume" : int(row[4]),
                   "ExchangeID" : int(row[5])}
          entry_list.append(entry)
        elif row[0] == 'Q': # Quote Data
          entry = {"EntryType" : "Historical Tick Data",
                   "TickType" : "Quote",
                   "Timestamp" : datetime.datetime.now(),
                   "HistoricalTimestamp" : datetime.datetime.strptime(filepath[4:12] + ':' + row[1][0:8], '%Y%m%d:%H:%M:%S'),
                   "Region" : region,
                   "Symbol" : str(row[2]),
                   "AskPrice" : float(row[3]),
                   "AskSize" : int(row[4]),
                   "AskExchangeID" : int(row[5]),
                   "BidPrice" : float(row[6]),
                   "BidSize" : int(row[7]),
                   "BidExchangeID" : int(row[8])}
          entry_list.append(entry)
        elif row[0] == 's': # Start Record
          print 'Starting new record:', row[2]
        elif row[0] == 'e': # End Record
          print 'Ending record:', row[2]
        elif row[0] == 'z': # EOF Continuation Record
          print 'End of file', filepath
        else:
          print 'Invalid entry:', row
        '''Begin hack to deal with memory overflow problem'''
        if len(entry_list) > 10000:
          self.logger.insertData(entry_list)
          entry_list = []
        '''End hack'''
    assert f.closed
    return entry_list

  def parseRawTickDataFolderToDB(self, folderpath):
    '''Function to handle going through a folder containing historical tick data, 
       located in raw text files, separated by day, and converting the 
       information into JSON documents to be stored in MongoDB'''
    if os.path.exists(folderpath) and os.path.isdir(folderpath):
      print 'Historical tick data folder import initiated'
      listing = os.listdir(folderpath)
      for f in listing:
        entries = self._parseSingleRawTickFile(folderpath, f)
        self.logger.insertData(entries)
      print 'Historical tick data folder import completed'
    else:
      print 'Invalid directory', folderpath

  def _parseSingleGzTickFile(self, folderpath, filepath):
    '''Helper function to parse a single tick file gzip container, and return
       a list of valid dicts to be added to the database. Essentially does 
       the same thing as the _parseSingleRawTickFle function, but allows for
       decompression on the fly, with the drawback of slower performance.'''
    print 'Parsing file:', filepath
    entry_list = []
    with gzip.open(os.path.join(folderpath, filepath), 'rb') as f:
      reader = csv.reader(f, delimiter='|')
      region = filepath[0:3]
      for line in reader:
        # Do blank value catching to avoid parsing errors
        row = [self._blankToNum(i) for i in line]
        # Generate the data entry to insert
        if row[0] == 'T': # Tick Data
          entry = {"EntryType" : "Historical Tick Data",
                   "TickType" : "Trade",
                   "Timestamp" : datetime.datetime.now(),
                   "HistoricalTimestamp" : datetime.datetime.strptime(filepath[4:12] + ':' + row[1][0:8], '%Y%m%d:%H:%M:%S'),
                   "Region" : region,
                   "Symbol" : str(row[2]),
                   "Price" : float(row[3]),
                   "Volume" : int(row[4]),
                   "ExchangeID" : int(row[5])}
          entry_list.append(entry)
        elif row[0] == 'Q': # Quote Data
          entry = {"EntryType" : "Historical Tick Data",
                   "TickType" : "Quote",
                   "Timestamp" : datetime.datetime.now(),
                   "HistoricalTimestamp" : datetime.datetime.strptime(filepath[4:12] + ':' + row[1][0:8], '%Y%m%d:%H:%M:%S'),
                   "Region" : region,
                   "Symbol" : str(row[2]),
                   "AskPrice" : float(row[3]),
                   "AskSize" : int(row[4]),
                   "AskExchangeID" : int(row[5]),
                   "BidPrice" : float(row[6]),
                   "BidSize" : int(row[7]),
                   "BidExchangeID" : int(row[8])}
          entry_list.append(entry)
        elif row[0] == 's': # Start Record
          print 'Starting new record:', row[2]
        elif row[0] == 'e': # End Record
          print 'Ending record:', row[2]
        elif row[0] == 'z': # EOF Continuation Record
          print 'End of file', filepath
        else:
          print 'Invalid entry:', row
        '''Begin hack to deal with memory overflow problem'''
        if len(entry_list) > 100000:
          self.logger.insertData(entry_list)
          entry_list = []
        '''End hack'''
    assert f.closed
    return entry_list

  def parseGzTickDataFolderToDB(self, folderpath):
    '''Function to handle going through a folder containing historical tick data, 
       located in gzipped containers, separated by day, and converting the 
       information into JSON documents to be stored in MongoDB. Essentially
       the same thing as parseRawTickDataFolderToDB function, but allows for
       dealing with compressed files, with the drawback of slower performance.'''
    if os.path.exists(folderpath) and os.path.isdir(folderpath):
      print 'Historical tick data folder import initiated'
      listing = os.listdir(folderpath)
      for f in listing:
        entries = self._parseSingleGzTickFile(folderpath, f)
        self.logger.insertData(entries)
      print 'Historical tick data folder import completed'
    else:
      print 'Invalid directory', folderpath

class SQLDataImporter:
  '''Class to handle reading arbitrary csv files, and sending them to MySQL 
     to be stored'''
  def __init__(self, host, dbName, userID, password):
    self.logger = MySQLLogger.MySQLCRUD(host, dbName, userID, password)
    #self.logger.initDataDB()
  
  def _parseSingleRawTickFile(self, folderpath, filepath):
    '''Helper function to parse a single, raw tick file (not gzipped), 
       and return a list of valid dicts to be added to the database.'''
    print 'Parsing file:', filepath
    entry_list = []
    with open(os.path.join(folderpath, filepath), 'r') as f:
      reader = csv.reader(f, delimiter='|')
      region = filepath[0:3]
      for line in reader:
        # Do blank value catching to avoid parsing errors
        row = [self._blankToNum(i) for i in line]
        # Generate the data entry to insert
        if row[0] == 'T': # Tick Data
          entry = {"EntryType" : "Historical Tick Data",
                   "TickType" : "Trade",
                   "Timestamp" : datetime.datetime.now(),
                   "HistoricalTimestamp" : datetime.datetime.strptime(filepath[4:12] + ':' + row[1][0:8], '%Y%m%d:%H:%M:%S'),
                   "Region" : region,
                   "Symbol" : str(row[2]),
                   "Price" : float(row[3]),
                   "Volume" : int(row[4]),
                   "ExchangeID" : int(row[5])}
          entry_list.append(entry)
        elif row[0] == 'Q': # Quote Data
          entry = {"EntryType" : "Historical Tick Data",
                   "TickType" : "Quote",
                   "Timestamp" : datetime.datetime.now(),
                   "HistoricalTimestamp" : datetime.datetime.strptime(filepath[4:12] + ':' + row[1][0:8], '%Y%m%d:%H:%M:%S'),
                   "Region" : region,
                   "Symbol" : str(row[2]),
                   "AskPrice" : float(row[3]),
                   "AskSize" : int(row[4]),
                   "AskExchangeID" : int(row[5]),
                   "BidPrice" : float(row[6]),
                   "BidSize" : int(row[7]),
                   "BidExchangeID" : int(row[8])}
          entry_list.append(entry)
        elif row[0] == 's': # Start Record
          print 'Starting new record:', row[2]
        elif row[0] == 'e': # End Record
          print 'Ending record:', row[2]
        elif row[0] == 'z': # EOF Continuation Record
          print 'End of file', filepath
        else:
          print 'Invalid entry:', row
        '''Begin hack to deal with memory overflow problem'''
        if len(entry_list) > 10000:
          self.logger.insertData(entry_list)
          entry_list = []
        '''End hack'''
    assert f.closed
    return entry_list

  def parseRawTickDataFolderToDB(self, folderpath):
    '''Function to handle going through a folder containing historical tick data, 
       located in raw text files, separated by day, and converting the 
       information into JSON documents to be stored in MongoDB'''
    if os.path.exists(folderpath) and os.path.isdir(folderpath):
      print 'Historical tick data folder import initiated'
      listing = os.listdir(folderpath)
      for f in listing:
        entries = self._parseSingleRawTickFile(folderpath, f)
        self.logger.insertData(entries)
      print 'Historical tick data folder import completed'
    else:
      print 'Invalid directory', folderpath
  
  def _blankToNum(self, input):
    '''Helper function to allow for conversion of a blank column value to an
       integer or float in the parsing functions'''
    if input == '':
      return 0
    else:
      return input
  

def usage():
  '''Prints command line usage help of the script'''
  print 'Sample Usage:'
  print '\tpython DataImporter.py --host [mongodb hostname] --port [mongodb port #] --db [mongodb name] --coll [collection name] --path [file|folder path]'
  print

def main():
  '''Function to test the DataImporter module from the command line'''
  try:
    opts, args = getopt.getopt(sys.argv[1:], '', ['host=', 'port=', 'db=', 'coll=', 'userID=', 'pwd=', 'path=', 'sql'])
  except getopt.error, msg:
    usage()
    print 'Error:', str(msg)
    sys.exit(2)
  host = '10.0.100.40'
  port = 27017
  db = 'data'
  coll = 'historical'
  userID = 'root'
  pwd = 'root'
  path = ''
  sql_option = False
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
    elif option == '--path':
      path = arg
    elif option == '--userID':
      userID = arg
    elif option == '--pass':
      pwd = arg
    elif option == '--sql':
      sql_option = True
      host = 'VM-Atlas01-SDNY'
      db = 'test'
    else:
      assert False, "unhandled option"
  # Get remaining necessary arguments
  while not path:
    path = raw_input('Please enter the path of the folder to extract data from: ')
  # Instantiate the importer, and import the data
  if sql_option: #Import to SQL database
    print 'Importing to SQL database'
    importer = SQLDataImporter(host, db, userID, pwd)
    importer.parseRawTickDataFolderToDB(path)
  else: #Do the default Mongo importing
    print 'If db has been dropped recently, make sure to reenable sharding, and ensure indexes'
    importer = MongoDataImporter(host, port, db, coll)
    importer.parseRawTickDataFolderToDB(path)

# Boilerplate code to get the program to run from the command line
if __name__ == '__main__':
  main()
