#!/usr/bin/env python
#
# This code is based on printShardingStatus function from mongodb engine.
#
# Erez Zarum, <erezz at icinga.org.il>, 2011
#

import re
import sys 
import json
from pymongo import Connection, errors

host = '10.0.100.40' # mongos host
port = 27017 # mongos port

try:
  session = Connection(host=host, port=port)
except errors.ConnectionFailure:
  print 'Failed to connect to: %s:%d' % (host, port)
  sys.exit(1)

config = session['config']
databases = config.databases.find( { "partitioned" : True } ) 

for db in databases:
  print db['_id']
  collections = config.collections.find( { "_id" : { "$regex" : re.compile("^" + db['_id'] + ".") } }, { "dropped" : False } ) 
  for colls in collections:
    print '\t' + colls['_id'] + ' chunks:'
    result = config.chunks.group( key=['shard'],
                                  condition={ 'ns' : colls['_id'] },
                                  initial={ "nChunks" : 0 },
                                  reduce="function (doc, out) { out.nChunks++; }"
                                )   
    for value in result:
      print '\t\t' + value['shard'] + ' ' + str(value['nChunks'])
  print