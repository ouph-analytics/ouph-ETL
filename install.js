'use strict';
var
    path = require('path'),
    config = require(path.join(__dirname, 'config')),
    os = require('os');
var
    db = require(path.join(__dirname, '/db/' + config.etl.db + '.js'));
db.init_db(function () {
    db.install(function () {
        console.log("install finished");
        console.log("please download  geoip database and copy to geoip dir");
        process.exit();

    });
});
