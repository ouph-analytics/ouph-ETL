'use strict';
var
    path = require('path'),
    mongo = require('mongodb'),
    MongoClient = mongo.MongoClient,
    ObjectId = require('mongodb').ObjectID,
    config = require(path.join(__dirname, '../config')),
    os = require("os"),
    moment = require('moment'),
    queue_pool,
    db_pool;
module.exports = {
    init_queue: function (callback) {
        MongoClient.connect(config.conn.queue.str, function (err, database) {
            if (err) throw err;
            queue_pool = database;
            callback();
        });

    },
    ondata: null,
    init_db: function (callback) {
        MongoClient.connect(config.conn.db.str, function (err, database) {
            if (err) throw err;
            db_pool = database;
            callback();
        });
    },
    start: function () {
        queue_pool.collection('web_queue').findAndModify(
            {},
            [['_id', +1]],
            { "$set": {} },
            { 'remove': true },
            function (err, doc) {
                // work here
                if (err) {
                    console.log(err);
                    return;
                }
                if (doc && doc.value) {
                    process.nextTick(module.exports.start);
                    var json = doc.value.query;
                    json.__id = doc.value._id.toString();
                    json.ip = doc.value.ip;
                    json.ua = doc.value.ua;
                    json.visittime = doc.value.visittime;
                    doc.value.error = "-";
                    queue_pool.collection('web_queue_log').insertOne(doc.value);
                    module.exports.ondata(json);
                } else {
                    console.log(moment() + " sleep");
                    setTimeout(function () {
                        process.nextTick(module.exports.start);
                    }, 10000);

                }

            }
        );

    },

    save: function (data) {
        var table = data.t;
        delete data.t;
        if ('pageview' == table) {
            table = "web_pageview";
        } else if ('event' == table) {
            table = "web_event";
        } else {
            return;
        }
        data.ts = moment.utc().format('YYYY-MM-DD HH:mm:ss');
        db_pool.collection(table).insertOne(data);
    },
    error: function (data, error) {
        if (error.length > 10) {
            error = error.substring(0, 10);
        }

        if (data.__id) {
            db_pool.collection('web_queue_log').update({ _id: ObjectId(data.__id) }, { $set: { error: error } });
        }

    },
    check: function (result) {
        return true;
    },
    install: function (callback) {
        db_pool.createCollection("web_queue_log", function (err, collection) {
            if (err) {
                console.log(err);
                process.exit();
            } else {
                db_pool.createCollection("web_pageview", function (err, collection) {
                    if (err) {
                        console.log(err);
                        process.exit();
                    } else {
                        db_pool.createCollection("web_event", function (err, collection) {
                            if (err) {
                                console.log(err);
                                process.exit();
                            } else {
                                callback();
                            }
                        });
                    }
                });
            }
        });
    }
};



function isFunction(fn) {
    return Object.prototype.toString.call(fn) === '[object Function]';
}

