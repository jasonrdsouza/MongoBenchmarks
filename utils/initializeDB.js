//Use this script to initialize the MongoDB schema
//Also initialize sharding and replication in here

//Drop any old data
use "algo_log_db";
db.algo_log_collection.drop();

//ensure indexes
db.algo_log_collection.ensureIndex({DocumentType:1});
db.algo_log_collection.ensureIndex({TradeDateID:1});
db.algo_log_collection.ensureIndex({GateExecID:1});


//Setting up sharding (all on 1 server):
//  -make sure the paths /data/shard1, /data/shard2, /data/config
//  -CMD1: mongod --shardsvr --dbpath /data/shard1 --port 10000
//  -CMD2: mongod --shardsvr --dbpath /data/shard2 --port 10001
//  -CMD3: mongod --configsvr --dbpath /data/config --port 20000
//  -CMD4: mongos --configdb localhost:20000 [--chunkSize (#forChunkSizeInMb)]
//  -from mongo shell
//      -use admin (to use the admin db)
//      -db.runCommand( { addshard : "localhost:10000" } )  --> its localhost since we are on the server in the shell
//      -db.runCommand( { addshard : "localhost:10001" } )
//      -db.runCommand( { enablesharding : "algo_log_db" } )
//      -db.runCommand( { shardcollection : "algo_log_db.algo_log_collection", key : {[keyname] : 1} } )  --> proper choice of the key (keyname) is important for good performance
//          -possible shard key value: {GateExecID:1,TradeDateID:1,Timestamp:1} --> order is important


//Setting up sharding (on multiple servers)
//  -3 of the servers contain a shard and a config db
//      -CMD1: mongod --shardsvr --dbpath /data/shard1 --port 10000
//      -CMD2: mongod --configsvr --dbpath /data/config --port 20000
//  -1 of the servers contains a shard and a mongos process (router)
//      -CMD1: mongod --shardsvr --dbpath /data/shard1 --port 10000
//      -CMD2: mongos --configdb [3 addresses of the config servers (ex. 10.0.100.41:20000,10.0.100.42:20000,10.0.100.43:20000)] [--chunkSize (#forChunkSizeInMb)]
//  -remaining servers contain a shard and optionally, a mongos process
//      -CMD1: mongod --shardsvr --dbpath /data/shard1 --port 10000
//      -optionally, the mongos command above
//  -from mongo shell
//      -use admin (to use the admin db)
//      -db.runCommand( { addshard : "10.0.100.43:10000" } )
//      -db.runCommand( { addshard : "10.0.100.42:10000" } )
//      -db.runCommand( { addshard : "10.0.100.41:10000" } )
//      -db.runCommand( { addshard : "10.0.100.40:10000" } )
//      -db.runCommand( { addshard : "10.0.100.38:10000" } )
//      -db.runCommand( { enablesharding : "algo_log_db" } )
//      -db.runCommand( { shardcollection : "algo_log_db.algo_log_collection", key : {[keyname] : 1} } )  --> proper choice of the key (keyname) is important for good performance
//          -possible shard key value: {GateExecID:1,TradeDateID:1,Timestamp:1} --> order is important
//			-another possible value: {TickType:1,Symbol:1,_id:1}



//Useful commands
// -"mongo 10.0.100.38" to connect to the server's mongo instance from local computer
//      -can connect to any server that is running a mongos instance (router)
//      -10.0.100.38, and 10.0.100.40 currently
// -Ensure an Index:
//		-db.collectionName.ensureIndex({key:1});
// -AFTER DROPPING A DATABASE
//		-make sure to re-enable sharding, and shard the necessary collections
//		-make sure to ensure the appropriate indexes

//Useful URLs
//http://www.mongodb.org/display/DOCS/A+Sample+Configuration+Session
