#!/usr/bin/env python

'''
MongoLogger.py - python script to allow storing data in MySQL

'''

__author__ = ('josephweitekamp (Joe Weitekamp)')

import sys
import getopt
import datetime
import time
import types
import MySQLdb


class MySQLCRUD:
    def __init__(self, host, dbname, user, password):
        self.conn=MySQLdb.connect(host=host, user=user, passwd=password, db=dbname)
        
    def initDataDB(self):
        cursor = self.conn.cursor()
        cursor.execute('create table Ticks (EntryType varchar(30), TickType char(6), InsertTime timestamp, HistoricalTimestamp timestamp, Region varchar(10), Symbol varchar(20), QuotePrice dec(10,2), Volume integer, QuoteExchangeId varchar(20), AskPrice dec(10,2), AskSize integer, AskExchangeId varchar(20), BidPrice dec(10,2), BidSize integer, BidExchangeId varchar(20), Open dec(10,2), High dec(10,2), Low dec(10,2), Close dec(10,2))')
        print 'Created table Ticks'
        
        cursor = self.conn.cursor()
        cursor.execute('create Index SymbolIndex on Ticks (Symbol)')
        print 'Created index Ticks'
        
        cursor = self.conn.cursor()
        cursor.execute('create Index HistTime on Ticks (HistoricalTimeStamp)')
        print 'Created index HistTime'


    def killDataDB(self):
        cursor = self.conn.cursor()
        cursor.execute('drop table Ticks')
        print 'Dropped table Ticks'
        


    def insertData(self, entry):
        if type(entry) == types.ListType:
            for thisEntry in entry:
                self.insertSingleEntry(thisEntry)
            self.conn.commit()
        elif type(entry) == types.DictType:
            self.insertSingleEntry(entry)
            self.conn.commit()
        else:
            print 'Unexpected type passed to insertData: '
            print type(entry)

    def insertSingleEntry(self, entry):
        sqlStringBase = 'insert into Ticks ('
        sqlStringVals = ''
        firstTimeThrough = True

        for key in entry.keys():
            value = entry[key]
            if firstTimeThrough == True:
                firstTimeThrough = False;
            else:
                sqlStringBase = sqlStringBase + ', '
                sqlStringVals = sqlStringVals + ', '
            if key == 'Ticker':
                sqlStringBase = sqlStringBase + 'Symbol'
            elif key == 'Timestamp':
                sqlStringBase = sqlStringBase + 'InsertTime'
            elif key == 'HistoricalDate':
                sqlStringBase = sqlStringBase + 'HistoricalTimestamp'
            elif key == 'ExchangeID':
                sqlStringBase = sqlStringBase + 'QuoteExchangeID'
            elif key == 'Price':
                sqlStringBase = sqlStringBase + 'QuotePrice'
            else:
                sqlStringBase = sqlStringBase + key

            if type(value) == types.StringType:
                sqlStringVals = sqlStringVals + "'" + str(value) + "'"
            elif type(value) == datetime.datetime:
                sqlStringVals = sqlStringVals + "'" + str(value) + "'"
            else:
                sqlStringVals = sqlStringVals + str(value)
        
        sqlStatement = sqlStringBase +  ') values (' + sqlStringVals + ')'
        print sqlStatement
        
        cursor = self.conn.cursor()
        cursor.execute(sqlStatement)


        
    def Test(self):
        cursor = self.conn.cursor()
        cursor.execute('create table testtable (theName varchar(20), theNum integer)')
        print 'Created table'
        
        cursor = self.conn.cursor()
        cursor.execute('insert into testtable values ("Joe", 51)')
        print 'Inserted into table'
        
        cursor = self.conn.cursor()
        cursor.execute('select * from testtable')
        print 'Queried table: results:'
        
        result = cursor.fetchall();
        for row in result:
            print "%s, %d" % (row[0], row[1])

        cursor = self.conn.cursor()
        cursor.execute('drop table testtable')
        print 'Dropped table'
        

    def Test2(self):
        
        entry_list = []
        
        entry = {"EntryType" : "Historical Stock Data",
         "Timestamp" : datetime.datetime.now(),
         "HistoricalDate" : datetime.datetime.strptime('20110719', '%Y%m%d'),
         "Ticker" : 'MSFT',
         "Open" : 22.1,
         "High" : 23.9,
         "Low" : 21.8,
         "Close" : 22.4,
         "Volume" : 100000}
        entry_list.append(entry)
        
        entry = {"EntryType" : "Historical Stock Data",
         "Timestamp" : datetime.datetime.now(),
         "HistoricalDate" : datetime.datetime.strptime('20110719', '%Y%m%d'),
         "Ticker" : 'ITG',
         "Open" : 12.1,
         "High" : 13.9,
         "Low" : 11.8,
         "Close" : 12.4,
         "Volume" : 200000}
        entry_list.append(entry)
        
        
        self.insertData(entry_list)
        

    def Test3(self):
        cursor = self.conn.cursor()
        cursor.execute('select Symbol, High from Ticks')
        print 'Queried table: results:'
        
        result = cursor.fetchall();
        for row in result:
            print "%s, %f" % (row[0], row[1])




def main():
  '''Simple Python program to communicate with a MySQL database, 
     to store and keep track of benchmark data'''
  # Parse command line options (if present)
  try:
    opts, args = getopt.getopt(sys.argv[1:], '', ['host=', 'dbName=', 'user=', 'pwd='])
  except getopt.error, msg:
    print 'python MySQLLogger.py --host [hostname] --dbName [database name] --user [MySQL userid] --pwd [MySQL password]'
    sys.exit(2)
  
  '''Connection String: "DSN=algoLoggingDb;DATABASE=test;UID=root;PWD=root"'''
  host='VM-Atlas01-SDNY'
  dbName='test'
  userId='root'
  password='root'
  # Process options
  for option, arg in opts:
    if option == '--host':
      host = arg
    elif option == '--dbName':
      dbName = arg
    elif option == '--user':
      userId = arg
    elif option == '--pwd':
      password = arg
  
  # Test the program
  mdb = MySQLCRUD(host, dbName, userId, password)
  mdb.initDataDB()
  mdb.Test2()
  mdb.Test3()
  
  

# Boilerplate code to get the program to run from the command line
if __name__ == '__main__':
  main()