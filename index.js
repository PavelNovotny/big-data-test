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


var id1 = 0;
var id2 = 0;
var id3 = 0;
var id4 = 0;
var id5 = 0;
var stringArrayID = 0;
var objectID = 0;
var objectArrayID = 0;
var documentCount = 10;

//insertRandomBigData();
insertOtecSyn40Po1000();
insertOtecSyn40000Po1();
insertOtecPole();

function insertOtecSyn40Po1000() {
    var func = function(callback) {
        var otec_index = {"index":{"_index":"big-data-test","_type":"otec40po1000","_id":++id1}};
        var otec = { "text" : randomizer.randomText({wordMinLen:5, wordMaxLen:15, wordCountMin: 1, wordCountMax : 1 }) };
        var postText = JSON.stringify(otec_index)+"\n"
            +JSON.stringify(otec)+"\n";
        for (var i=0; i<40; i++) {
            var syn_index = {"index":{"_index":"big-data-test","_type":"syn40po1000","_id":++id2,"_parent":id1}};
            var syn = { "login" : randomizer.randomStringArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 1000, wordCountMax : 1000 }) };
            postText += JSON.stringify(syn_index)+"\n"
                +JSON.stringify(syn)+"\n";
        }
        var args = {
            data: postText,
            headers: { "Content-Type": "application/json" }
        };
        client.post(nconf.get('listen-address')+":"+nconf.get('listen-port')+"/_bulk", args, function (data, response) {
            console.log(JSON.stringify(data));
            callback();
        });
    }
    var funcArray = [];
    for (var i = 0; i < documentCount; i++) {
        funcArray.push(func);
    }
    async.series(funcArray,
        function (err, results) {
            console.log("Finished insert otec a syn");
        }
    );
}

function insertOtecSyn40000Po1() {
    var func = function(callback) {
        var otec_index = {"index":{"_index":"big-data-test","_type":"otec40000","_id":++id3}};
        var otec = { "text" : randomizer.randomText({wordMinLen:5, wordMaxLen:15, wordCountMin: 1, wordCountMax : 1 }) };
        var postText = JSON.stringify(otec_index)+"\n"
            +JSON.stringify(otec)+"\n";
        for (var i=0; i<40000; i++) {
            var syn_index = {"index":{"_index":"big-data-test","_type":"syn40000","_id":++id4,"_parent":id3}};
            var syn = { "login" : randomizer.randomText({wordMinLen:5, wordMaxLen:15, wordCountMin: 1, wordCountMax : 1 }) };
            postText += JSON.stringify(syn_index)+"\n"
                +JSON.stringify(syn)+"\n";
        }
        var args = {
            data: postText,
            headers: { "Content-Type": "application/json" }
        };
        client.post(nconf.get('listen-address')+":"+nconf.get('listen-port')+"/_bulk", args, function (data, response) {
            console.log(JSON.stringify(data));
            callback();
        });
    }
    var funcArray = [];
    for (var i = 0; i < documentCount; i++) {
        funcArray.push(func);
    }
    async.series(funcArray,
        function (err, results) {
            console.log("Finished insert otec a syn");
        }
    );
}

function insertOtecPole() {
    var func = function(callback) {
        var otec_index = {"index":{"_index":"big-data-test","_type":"otecpole","_id":++id5}};
        var otec = { "text" : randomizer.randomStringArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 40000, wordCountMax : 40000 }) };
        var postText = JSON.stringify(otec_index)+"\n"
            +JSON.stringify(otec)+"\n";
        var args = {
            data: postText,
            headers: { "Content-Type": "application/json" }
        };
        client.post(nconf.get('listen-address')+":"+nconf.get('listen-port')+"/_bulk", args, function (data, response) {
            console.log(JSON.stringify(data));
            callback();
        });
    }
    var funcArray = [];
    for (var i = 0; i < documentCount; i++) {
        funcArray.push(func);
    }
    async.series(funcArray,
        function (err, results) {
            console.log("Finished insert otec a syn");
        }
    );
}

function insertRandomBigData() {
    // insertBig(function (callback) {
    //     var inserted = randomizer.randomText({wordMinLen:5, wordMaxLen:15, wordCountMin: 1, wordCountMax : 1 });
    //     insertData({id : objectID++,"text": inserted }, "/big-data-test/otec", callback);
    // });
    insertBig(function (callback) {
        var inserted = randomizer.randomStringArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 40000, wordCountMax : 50000 });
        insertData({id : stringArrayID++, "stringArray": inserted }, "/big-data-test/syn", callback);
    });
    // insertBig(function (callback) {
    //     var inserted = randomizer.randomObject({wordMinLen:5, wordMaxLen:15, wordCountMin: 100000, wordCountMax : 500000 });
    //     insertData({id : objectID++, "object": inserted }, "/big-data-test/bigObject", callback);
    // });

    //(10*15000)*100
    // insertBig(function (callback) {
    //     //var inserted = randomizer.randomObjectArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 10000, wordCountMax : 20000, objectCountMin: 50, objectCountMax: 150   });
    //     //var inserted = randomizer.randomObjectArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 10000, wordCountMax : 20000, objectCountMin: 5, objectCountMax: 15   });
    //     var inserted = randomizer.randomObjectArray({wordMinLen:5, wordMaxLen:15, wordCountMin: 1000, wordCountMax : 2000, objectCountMin: 500, objectCountMax: 500   });
    //     insertData({id : objectArrayID++, "objectArray": inserted }, "/big-data-test/bigObjectArray", callback);
    // });

}

function insertBig(func) {
    var bigFunc = [];
    for (var i = 0; i < documentCount; i++) {
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
