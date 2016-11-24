
/*
ct： cookies的最后时间 2-1
vid：唯一用户标识 0
sid：session 标识 0，1
aid： 广告ID
uid： 用户ID，如果网站登录的话
 */


'use strict';
var
    path = require('path'),
    url = require('url'),
    config = require(path.join(__dirname, 'config')),
    os = require('os'),
    moment = require('moment'),
    uaparser = require('ua-parser-js'),
    MMDBReader = require('mmdb-reader'),
    sync = require('synchronize');
var queue = require(path.join(__dirname, '/db/' + config.etl.queue + '.js'));
var db = require(path.join(__dirname, '/db/' + config.etl.db + '.js'));
var geoip = new MMDBReader('./geoip/GeoLite2-City.mmdb');

queue.ondata = function (result) {
    if (result && db.check(result)) {
        normalize(result,
            function (data) {
                db.save(data);
            },
            function (result, err) {
                db.error(result, err);
            });
    } else {
        db.error(result, 'check');
    }
};

queue.init_queue(function () {
    db.init_db(function () {
        queue.start();
    });

});


function normalize(result, success, error) {
    var data = {};
    var ip = geoip.lookup(result.ip);
    var ua = uaparser(result.ua);
    var cookieArr = result["cid"].split('.'), h;
    if (cookieArr.length != 4 || (h = cookieArr.pop(), h != gahash(cookieArr.join('.')))) {
        error(result, 'checksum');
        return;
    }
    var f = [];
    if (result['f']) {
        f = result['f'].split("|");
        if (f.length == 1) {
            f = result['f'].split("_");
        }
        if (f.length == 1) {
            f = result['f'].split("-");
        }

    }
    data["t"] = result["t"];
    data["v"] = result["v"];
    data["_v"] = result["_v"];
    data["ct"] = cookieArr[2] - cookieArr[1];
    data["vid"] = cookieArr[0];
    data["sid"] = cookieArr[0] + "." + cookieArr[1];
    data["uid"] = result["uid"] || "";
    data["tid"] = result["tid"];
    data["je"] = result["je"];
    data["vp"] = result["vp"];
    data["cs"] = result["cs"];
    data["fv"] = result["fv"];
    data["la"] = result["la"];
    data["sc"] = result["sc"];
    data["sr"] = result["sr"];
    data["re"] = result["re"];
    data["pt"] = result["pt"];
    data["ot"] = result["ot"];
    data["lp"] = result["lp"];
    data["aid"] = result["aid"] || "";
    data["ip"] = result["ip"];
    data["queueid"] = result["__id"];
    data["country"] = (ip && ip["country"] && ip["country"]["iso_code"]) ? ip["country"]["iso_code"] : "";
    data["city"] = (ip && ip["city"] && ip["city"]["names"] && ip["city"]["names"]["en"]) ? ip["city"]["names"]["en"] : "";[0];
    data["region"] = (ip && ip["subdivisions"] && ip["subdivisions"][0] && ip["subdivisions"][0]["names"] && ip["subdivisions"][0]["names"]["en"]) ? ip["subdivisions"][0]["names"]["en"] : "";
    data["browser"] = ua.browser.name || "";
    data["device"] = ua.device.model || "";
    data["os"] = ua.os.name || "";
    data["engine"] = ua.engine.name || "";
    data["browserversion"] = ua.browser.version || "";
    data["deviceversion"] = ua.device.vendor || "";
    data["osversion"] = ua.os.version || "";
    data["engineversion"] = ua.engine.version || "";
    data["ab"] = result["ab"] || "";
    data["p1"] = f[0] || "";
    data["p2"] = f[1] || "";
    data["p3"] = f[2] || "";
    data["p4"] = f[3] || "";
    data["p5"] = f[4] || "";
    data["p6"] = f[5] || "";
    data["p7"] = f[6] || "";
    data["p8"] = f[7] || "";
    data["p9"] = f[8] || "";
    data["p10"] = f[9] || "";
    data["utctime"] = result["visittime"];
    var lp = url.parse(data["lp"]);
    if (!lp.hostname || config.domain.indexOf(lp.hostname) < 0) {
        error(result, 'domain');
        return;
    }
    if (data["t"] == 'event') {
        data["en"] = result["en"] || "";
        data["ea"] = result["ea"] || "";
        data["el"] = result["el"] || "";
        data["ev"] = result["ev"] || "";

    } else if (data["t"] == 'pageview') {

        var ref = url.parse(data["re"]);
        data["rd"] = ref.hostname || '';
        data["param"] = "";
    } else {
        error(result, 't-name');
        return;
    }
    data["usage"] = result["usage"];
    success(data);

}
function gahash(str) {
    var hash = 1,
        charCode = 0,
        idx;
    if (str) {
        hash = 0;
        for (idx = str.length - 1; idx >= 0; idx--) {
            charCode = str.charCodeAt(idx);
            hash = (hash << 6 & 268435455) + charCode + (charCode << 14);
            charCode = hash & 266338304;
            hash = charCode != 0 ? hash ^ charCode >> 21 : hash;
        }
    }
    return hash;
};