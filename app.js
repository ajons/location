/**
 * Created by ajon on 2017/5/23.
 * 用于ruckus定位数据拉取，并存入到海鼎的数据库中
 */
'use strict';
require('./lib/prototype')
const koa = require('koa');
const app = new koa();
const conf = require('./conf');
const co = require('co');
const schedule = require('node-schedule');
const thunkify = require('thunkify');

const orm_mongodb = require('./lib/orm_mongdb');
const orm_mysql = require('./lib/orm_mysql');

const get_member_biz = require('./biz/get_member_biz');
const ruckus_api_biz = require('./biz/ruckus_api_biz');
const report_biz = require('./biz/report_biz');


//启动监听
app.listen(conf.env.port || 2000, () => {
    console.log(`%d - server is strating,port is ${conf.env.port || 2000}`, new Date());

    orm_mysql.connect(function (err, mysqldb) {
        global.mysql_db_obj = mysqldb;
        orm_mongodb.connect(function (err, db) {
            if (!err) {
                global.mongodb_db_obj = db;

                co(function *() {
                    // yield ruckus_api_biz.getToken();
                    // yield ruckus_api_biz.getVenues();
                    // yield ruckus_api_biz.getVenueDetailByID();
                    // yield ruckus_api_biz.getRadioMapsByVenuesID();
                    // yield ruckus_api_biz.getRadioMapsDetailByName();

                    //yield ruckus_api_biz.LocationTest();

                    //获取会员数据，并缓存
                    yield thunkify(get_member_biz.getMember)();
                    //获取定位数据
                    yield ruckus_api_biz.getLocationByVenuesID();
                    //统计区域人流信息
                    // yield report_biz.BI_I_PASFLW_DOOR('20170709');
                    // 临时时间&区域过滤
                    // yield ruckus_api_biz.Mongo_LocationMap_Removal_TimeZonesNameInvalid_Temp("20170704", "f1_door1");

                })
                // schedule_works();
            }
        })
    })


});

/**
 * 每天的凌晨1点执行任务
 */
var schedule_works = function () {
    var rule = new schedule.RecurrenceRule();
    rule.dayOfWeek = [0, new schedule.Range(1, 6)];
    rule.hour = 1;
    rule.minute = 0;
    var j = schedule.scheduleJob(rule, function () {
        co(function *() {
            yield ruckus_api_biz.getLocationByVenuesID();
            //统计客流天气信息
            // yield report_biz.weather_report();
        })
    });
}

