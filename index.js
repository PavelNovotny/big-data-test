/**
 * 
 * Created by pavelnovotny on 07.09.16.
 */
var Client = require('node-rest-client').Client;
var randomizer = require("data-randomizer");
var async = require("async");
var nconf = require("nconf");
var client = new Client();

nconf.argv()
    .env()
    .defaults({ env : 'production' })
    .file({ file: 'config-'+nconf.get('env')+'.json' });


var textID = 0;
var stringArrayID = 0;
var objectID = 0;
var objectArrayID = 0;

insertRandomBigData();

function insertRandomBigData() {
    insertBig(function (callback) {
        var inserted = randomizer.randomText({wordMinLen:5, wordMaxLen:15, wordCountMin: 400000, wordCountMax : 500000 });
        insertData({id : textID++, "text": inserted }, "/big-data-test/bigText", callback);
    });
    insertBig(function (callback) {
        var inserted = randomizer.randomStringArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 40000, wordCountMax : 50000 });
        insertData({id : stringArrayID++, "stringArray": inserted }, "/big-data-test/bigStringArray", callback);
    });
    // insertBig(function (callback) {
    //     var inserted = randomizer.randomObject({wordMinLen:5, wordMaxLen:15, wordCountMin: 100000, wordCountMax : 500000 });
    //     insertData({id : objectID++, "object": inserted }, "/big-data-test/bigObject", callback);
    // });

    //(10*15000)*100
    insertBig(function (callback) {
        //var inserted = randomizer.randomObjectArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 10000, wordCountMax : 20000, objectCountMin: 50, objectCountMax: 150   });
        //var inserted = randomizer.randomObjectArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 10000, wordCountMax : 20000, objectCountMin: 5, objectCountMax: 15   });
        var inserted = randomizer.randomObjectArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 1000, wordCountMax : 2000, objectCountMin: 500, objectCountMax: 500   });
        insertData({id : objectArrayID++, "objectArray": inserted }, "/big-data-test/bigObjectArray", callback);
    });

}

function insertBig(func) {
    var bigFunc = [];
    for (var i = 0; i < 5000; i++) {
        bigFunc.push(func);
    }
    async.series(bigFunc,
        function (err, results) {
            console.log("Finished insertBig");
        }
    );
}

function insertData(data, path, callback) {
    var args = {
        data: data,
        headers: { "Content-Type": "application/json" }
    };
    client.put(nconf.get('listen-address')+":"+nconf.get('listen-port')+path+"/"+data.id, args, function (data, response) {
        console.log(data);
        callback();
    });
}
