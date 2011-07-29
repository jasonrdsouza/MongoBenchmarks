function printShardingInfo(configDB, verbose) {
    if (configDB === undefined) {
        configDB = db.getSisterDB("config");
    }
    var version = configDB.getCollection("version").findOne();
    if (version == null) {
        print("not a shard db!");
        return;
    }
    var raw = "";
    var output = function (s) {
            raw += s + "\n";
        };
    //output("--- Sharding Status --- ");
    output("  sharding version: " + tojson(configDB.getCollection("version").findOne()));
    output("  shards:");
    //configDB.shards.find().forEach(function (z) {
    //    output("      " + tojson(z));
    //});
    //output("  databases:");
    configDB.databases.find().sort({
        name: 1
    }).forEach(function (db) {
        //output("\t" + tojson(db, "", true));
        if (db.partitioned) {
            configDB.collections.find({
                _id: new RegExp("^" + db._id + ".")
            }).sort({
                _id: 1
            }).forEach(function (coll) {
                if (coll.dropped == false) {
                    output("\t\t" + coll._id + " chunks:");
                    res = configDB.chunks.group({
                        cond: {
                            ns: coll._id
                        },
                        key: {
                            shard: 1
                        },
                        reduce: function (doc, out) {
                            out.nChunks++;
                        },
                        initial: {
                            nChunks: 0
                        }
                    });
                    var
                    totalChunks = 0;
                    res.forEach(function (z) {
                        totalChunks += z.nChunks;
                        output("\t\t\t\t" + z.shard + "\t" + z.nChunks);
                    });
                    //if (totalChunks < 10 || verbose) {
                    //    configDB.chunks.find({
                    //        ns: coll._id
                    //    }).sort({
                    //        min: 1
                    //    }).forEach(function (chunk) {
                    //        output("\t\t\t" + tojson(chunk.min) + " -->> " + tojson(chunk.max) + " on : " + chunk.shard + " " + tojson(chunk.lastmod));
                    //    });
                    //} else {
                    //    output("\t\t\ttoo many chunksn to print, use verbose if you want to force print");
                    //}
                }
            });
        }
    });
    print(raw);
}