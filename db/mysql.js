'use strict';
var
    path = require('path'),
    mysql = require('mysql'),
    config = require(path.join(__dirname, '../config')),
    os = require("os"),
    moment = require('moment'),
    queue_pool, db_pool,
    arr = ['v', '_v', 'cid', 'tid', 'je', 'vp', 'cs', 'fv', 'la', 'sc', 'sr', 're', 'pt', 'ot', 'lp', 't', 'usage'];
module.exports = {
    init_queue: function (callback) {
        queue_pool = mysql.createPool(config.conn.queue);
        callback();

    },
    ondata: null,
    init_db: function (callback) {
        db_pool = mysql.createPool(config.conn.db);
        callback();
    },
    start: function () {
        //select get_lock('etlv1',30);\ SELECT RELEASE_LOCK('etlv1');"
        var m_date=moment();
        var __id=m_date.format("x");
        var sql = `select @id:=id from web_queue order by id asc limit 1;
INSERT ignore INTO web_queue_log (qid,\`query\`,ua,ip,visittime,error)  SELECT concat(id,'-',?),\`query\`,ua,ip,visittime,'-' FROM web_queue where id=@id;
select * from web_queue where id=@id;
delete from web_queue where id=@id;`;
        queue_pool.query(sql,[__id], function (err, result) {
            if (err) {
                console.log(err);
                return;
            }

            var raw = result[2];
            if (raw && raw.length) {
                process.nextTick(module.exports.start);
                var json = JSON.parse(raw[0].query);
                json.__id =__id;
                json.ip = raw[0].ip;
                json.ua = raw[0].ua;
                json.visittime = raw[0].visittime;
                module.exports.ondata(json);

            } else {
                console.log(m_date + " sleep");
                setTimeout(function () {
                    process.nextTick(module.exports.start);
                }, 10000);
            }


        });

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
        var sql = 'insert ignore into ' + table + ' set ?';
        db_pool.query(sql, data, function (err, result) {
            if (err) {
                console.log(err);
                return;
            }

        });

    },
    error: function (data, error) {
        if (error.length > 10) {
            error = error.substring(0, 10);
        }
        if (data.__id) {
            var sql = "update web_queue_log set error=? where qid=?";
            db_pool.query(sql, [error, data.__id], function (err, result) {

                if (err) {
                    console.log(err);
                    return;
                }
            });


        }

    },
    check: function (result) {

        var len = arr.length,
            i = 0;
        for (; i < len; ++i) {
            if (!(arr[i] in result)) {
                return false;
            }

        }
        /* 
        for (var key in result) {
            if (result.hasOwnProperty(key)) {
                var element = result[key];
                if (key == '')
    
                    console.log(key);
                console.log(element);
    
            }
        }
        */
        return true;
    },
    install: function (callback) {

        var sql = [];
        sql.push("CREATE TABLE IF NOT EXISTS `web_queue_log` (\
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT,\
    `qid` VARCHAR(100) NOT NULL,\
	`query` VARCHAR(3000) NOT NULL,\
	`ua` VARCHAR(1000) NOT NULL,\
	`ip` VARCHAR(128) NOT NULL,\
	`visittime` DATETIME NOT NULL,\
	`error` VARCHAR(10) NOT NULL,\
	PRIMARY KEY (`id`)\
)\
COLLATE='utf8_general_ci' ;\
");
        sql.push("CREATE TABLE IF NOT EXISTS `web_pageview` ( \
	`id` INT(11) NOT NULL AUTO_INCREMENT,\
	`v` VARCHAR(10) NOT NULL,\
	`_v` VARCHAR(10) NOT NULL,\
	`ct` INT(11) NOT NULL ,\
	`sid` VARCHAR(50) NOT NULL ,\
	`vid` VARCHAR(50) NOT NULL ,\
	`uid` VARCHAR(50) NOT NULL ,\
	`tid` VARCHAR(50) NOT NULL ,\
	`je` VARCHAR(1) NOT NULL ,\
	`vp` VARCHAR(20) NOT NULL ,\
	`cs` VARCHAR(10) NOT NULL ,\
	`fv` VARCHAR(10) NOT NULL ,\
	`la` VARCHAR(10) NOT NULL ,\
	`sc` VARCHAR(20) NOT NULL ,\
	`sr` VARCHAR(10) NOT NULL ,\
	`re` VARCHAR(2000) NOT NULL ,\
	`pt` VARCHAR(500) NOT NULL ,\
	`ot` VARCHAR(5) NOT NULL ,\
	`lp` VARCHAR(2000) NOT NULL ,\
	`aid` VARCHAR(64) NOT NULL ,\
	`ip` VARCHAR(128) NOT NULL,\
	`queueid` VARCHAR(100) NOT NULL,\
	`utctime` DATETIME NOT NULL,\
	`country` VARCHAR(2) NOT NULL,\
	`region` VARCHAR(10) NOT NULL,\
	`city` VARCHAR(20) NOT NULL,\
	`browser` VARCHAR(50) NOT NULL,\
	`device` VARCHAR(50) NOT NULL,\
	`os` VARCHAR(50) NOT NULL,\
	`engine` VARCHAR(50) NOT NULL,\
	`browserversion` VARCHAR(20) NOT NULL,\
	`deviceversion` VARCHAR(20) NOT NULL,\
	`osversion` VARCHAR(20) NOT NULL,\
	`engineversion` VARCHAR(20) NOT NULL,\
	`ab` VARCHAR(10) NOT NULL,\
	`p1` VARCHAR(20) NOT NULL,\
	`p2` VARCHAR(20) NOT NULL,\
	`p3` VARCHAR(20) NOT NULL,\
	`p4` VARCHAR(20) NOT NULL,\
	`p5` VARCHAR(20) NOT NULL,\
	`p6` VARCHAR(20) NOT NULL,\
	`p7` VARCHAR(20) NOT NULL,\
	`p8` VARCHAR(20) NOT NULL,\
	`p9` VARCHAR(20) NOT NULL,\
	`p10` VARCHAR(20) NOT NULL,\
	`ts` DATETIME NOT NULL,\
	`usage` VARCHAR(20) NOT NULL,\
	`rd` VARCHAR(100) NOT NULL,\
	`param` VARCHAR(200) NOT NULL,\
	PRIMARY KEY (`id`)\
)\
COMMENT='pageview'\
COLLATE='utf8_general_ci'\
ENGINE=InnoDB\
;\
");
        sql.push("CREATE TABLE `web_event` ( \
	`id` INT(11) NOT NULL AUTO_INCREMENT,\
	`en` VARCHAR(50) NOT NULL ,\
	`ea` VARCHAR(50) NOT NULL,\
	`el` VARCHAR(50) NOT NULL ,\
	`ev` VARCHAR(50) NOT NULL,\
	`v` VARCHAR(10) NOT NULL,\
	`_v` VARCHAR(10) NOT NULL,\
	`ct` INT(11) NOT NULL,\
	`vid` VARCHAR(50) NOT NULL,\
	`uid` VARCHAR(50) NOT NULL ,\
	`sid` VARCHAR(50) NOT NULL ,\
	`tid` VARCHAR(50) NOT NULL ,\
	`je` VARCHAR(1) NOT NULL ,\
	`vp` VARCHAR(20) NOT NULL ,\
	`cs` VARCHAR(10) NOT NULL ,\
	`fv` VARCHAR(10) NOT NULL ,\
	`la` VARCHAR(10) NOT NULL ,\
	`sc` VARCHAR(20) NOT NULL ,\
	`sr` VARCHAR(10) NOT NULL ,\
	`re` VARCHAR(2000) NOT NULL ,\
	`pt` VARCHAR(500) NOT NULL ,\
	`ot` VARCHAR(5) NOT NULL ,\
	`lp` VARCHAR(2000) NOT NULL ,\
	`aid` VARCHAR(64) NOT NULL,\
	`ip` VARCHAR(128) NOT NULL,\
	`queueid` VARCHAR(100) NOT NULL,\
	`utctime` DATETIME NOT NULL,\
	`country` VARCHAR(2) NOT NULL,\
	`region` VARCHAR(10) NOT NULL,\
	`city` VARCHAR(20) NOT NULL,\
	`browser` VARCHAR(50) NOT NULL,\
	`device` VARCHAR(50) NOT NULL,\
	`os` VARCHAR(50) NOT NULL,\
	`engine` VARCHAR(50) NOT NULL,\
	`browserversion` VARCHAR(20) NOT NULL,\
	`deviceversion` VARCHAR(20) NOT NULL,\
	`osversion` VARCHAR(20) NOT NULL,\
	`engineversion` VARCHAR(20) NOT NULL,\
	`ab` VARCHAR(10) NOT NULL,\
	`p1` VARCHAR(20) NOT NULL,\
	`p2` VARCHAR(20) NOT NULL,\
	`p3` VARCHAR(20) NOT NULL,\
	`p4` VARCHAR(20) NOT NULL,\
	`p5` VARCHAR(20) NOT NULL,\
	`p6` VARCHAR(20) NOT NULL,\
	`p7` VARCHAR(20) NOT NULL,\
	`p8` VARCHAR(20) NOT NULL,\
	`p9` VARCHAR(20) NOT NULL,\
	`p10` VARCHAR(20) NOT NULL,\
	`ts` DATETIME NOT NULL,\
	`usage` VARCHAR(20) NOT NULL,\
	PRIMARY KEY (`id`)\
)\
COMMENT='event'\
COLLATE='utf8_general_ci'\
ENGINE=InnoDB\
;\
");

        db_pool.query(sql.join("\r\n"), function (err, result) {
            if (err) {
                console.log(err);
            } else {
                console.log(result);
            }
            callback();
        });


    }

};



function isFunction(fn) {
    return Object.prototype.toString.call(fn) === '[object Function]';
}