/**
 * Created by ajon on 2017/6/23.
 * bussion:统计报表
 */
'use strict';
const co = require('co');
const fs = require('fs');
const thunkify = require('thunkify');
const conf = require('../conf');
let xml2json = require('xml2js');
let xml2js = xml2json.parseString;
let _requesthttp = require('../lib/request_http');
let request_api = new _requesthttp();

/**
 * 天气统计客流
 */
module.exports.weather_report = function *() {
    try {

        let today = new Date();
        let yesterday = today.getTime() - 1000 * 60 * 60 * 24;
        let curDate = new Date(yesterday).toLocaleDateString();

        let report_weather_people_flowrate = {
            Storecode: '',
            OccurDate: curDate,
            Weather: '',
            Temperature: '',
            Total: 0,
            Memo: '',
            ATT1: '',
            ATT2: ''
        }

        //总人数
        let count = yield db.Get_Mongo_Weather_report(curDate.replace(/\//g, '').replace(/-/g, ''));

        report_weather_people_flowrate.Total = count;

        //获取天气状况
        let options = {
            url: conf.weather.url,
            method: "get"
        };
        let result = yield thunkify(request_api.requesthttp)(options);

        if (result && result.data) {
            report_weather_people_flowrate.Weather = result.data.yesterday.type + ' ' + result.data.yesterday.fx + ' ' + result.data.yesterday.fl;
            report_weather_people_flowrate.Temperature = result.data.yesterday.high + ' ' + result.data.yesterday.low;
        }
        //国家天气网
        // let json_result = yield thunkify(xml2js)(result);
        // json_result.china.city.forEach(function (item) {
        //     if (item.$.pyName == "shanghai") {
        //         report_weather_people_flowrate.Weather = item.$.stateDetailed + ' ' + item.$.windState;
        //         report_weather_people_flowrate.Temperature = `最高温度:${item.$.tem2 },最低温度:${item.$.tem1 }`;
        //     }
        // })
        yield db.Insert_Mongo_Weather_report(report_weather_people_flowrate);
    } catch (ex) {
        console.log(`${new Date().valueOf()} weather_report err:${JSON.stringify(ex)}`);
    }
}

/**
 * 入口客流信息分析
 */
module.exports.BI_I_PASFLW_DOOR = function *(current_date) {
    try {
        let current_date_format;
        if (!current_date) {
            let today = new Date();
            let yesterday = today.getTime() - 1000 * 60 * 60 * 24;
            current_date_format = new Date(yesterday).format('yyyy-MM-dd');
            current_date = current_date_format.replace(/\//g, '').replace(/-/g, '');
        }

        var tb_loc = mongodb_db_obj.collection('tb_locations_' + current_date);
        var tb_pasflw_door_report = mongodb_db_obj.collection('tb_pasflw_door_report');

        fs.writeFileSync('sqlfile/BI_I_PASFLW_DOOR.sql', "");
        //let overtime_zones_list = conf.overtime_zones_list;
        //统计入口区域停留1小时以上的
        // tb_loc.aggregate([{$match: {"zones_name": {$in: overtime_zones_list}}},
        //     {
        //         $group: {
        //             _id: {client_mac: "$client_mac", zones_name: "$zones_name"},
        //             last_time: {$last: "$timestamp"},
        //             first_time: {$first: "$timestamp"}
        //         }
        //     }
        // ], '', function (err, _result) {
            // let overTime_oneHour_clientmac_arr = {};

            // for (let i = 0; i < _result.length; i++) {
            //     let ret = _result[i];
            //
            //     let firstTime = new Date(ret.first_time);
            //     let lastTime = new Date(ret.last_time);
            //     let count = lastTime.getTime() - firstTime.getTime();
            //
            //     // 过滤入口区域停留1小时以上的
            //     if (count > 60 * 60 * 1000) {
            //         if (overTime_oneHour_clientmac_arr[ret._id.zones_name] && overTime_oneHour_clientmac_arr[ret._id.zones_name].floor_name == ret._id.zones_name) {
            //             overTime_oneHour_clientmac_arr[ret._id.zones_name].client_mac_list.push(ret._id.client_mac);
            //         } else {
            //             overTime_oneHour_clientmac_arr[ret._id.zones_name] = {}
            //             overTime_oneHour_clientmac_arr[ret._id.zones_name].floor_name = ret._id.zones_name;
            //             overTime_oneHour_clientmac_arr[ret._id.zones_name].client_mac_list = new Array();
            //             overTime_oneHour_clientmac_arr[ret._id.zones_name].client_mac_list.push(ret._id.client_mac);
            //         }
            //     }
            // }

            for (let i = 9; i < 23; i++) {   //数据切片并存库
                let starttime = new Date(`${current_date_format} ${i}:00:00`);
                let endtime = new Date(`${current_date_format} ${i + 1}:00:00`);

                tb_loc.group({"floor_number": true, "zones_name": true},
                    {
                        "timestamp": {
                            "$lt": endtime,
                            "$gte": starttime
                        }
                    },
                    {"num": 0},
                    function (cur, prev) {
                        prev.floor_number;
                        prev.zones_name;
                        prev.num++;
                    },
                    '', '', '', ''
                ).then(function (result) {
                    var sqlArr = '\r\n';
                    for (let ret of result) {
                        ret.starttime = starttime;
                        ret.endtime = endtime;
                        ret.updatetime = new Date().toLocaleString();

                        if (ret.zones_name !== null && ret.zones_name !== undefined && ret.zones_name !== '') {
                            // 生成 insert 语句 --- 2017/7/6 Jennie
                            var insert = "INSERT INTO BI_I_PASFLW_DOOR (" +
                                "PASFLW_STORE_CODE, " +
                                "BEGIN_DATE, " +
                                "END_DATE, " +
                                "DOOR_NAME, " +
                                "FLOOR_NAME, " +
                                "HUMAN_NUMBER_IN, " +
                                "MEMBER_NUMBER_IN, " +
                                "LAST_UPDATE_TIME, " +
                                "TAG) VALUES (" +
                                "'0210', to_date('" +
                                ret.starttime.toLocaleString() + "'),to_date('" +
                                ret.endtime.toLocaleString() + "'),'" +
                                ret.zones_name + "','" +
                                ret.floor_number + "'," +
                                ret.num + "," +
                                0 + ",to_date('" +
                                ret.updatetime + "')," + 0 + ");\r\n";

                            sqlArr += insert;
                        }
                    }

                    sqlArr += "commit;";
                    fs.appendFileSync('sqlfile/BI_I_PASFLW_DOOR.sql', sqlArr);

                    if (result && result.length > 0) {
                        //将切片好的数据存入mongodb中
                        tb_pasflw_door_report.insertMany(result, function (err, ret) {
                            console.log(ret.result)
                        })
                    }
                });


                // overtime_zones_list.forEach(function (item) {
                //     if (overTime_oneHour_clientmac_arr[item]) {
                //         let mac_list = overTime_oneHour_clientmac_arr[item].client_mac_list;
                //         if (mac_list) {
                //             tb_loc.count({
                //                 "zones_name": item, "client_mac": {$in: mac_list}, "timestamp": {
                //                     "$lt": endtime,
                //                     "$gte": starttime
                //                 }
                //             }).then(function (_result) {
                //
                //             })
                //         }
                //     }
                // });


            }
        // })
    } catch (ex) {
        console.log(`%d - BI_I_PASFLW_DOOR err:` + ex, new Date())
    }
}

/**
 * 停留时长客流信息分析
 */
module.exports.BI_I_STAY_TIME = function *(current_date) {
    try {
        // 0~20， 20~60， 60~120， 120~240， 240+
        let current_date_format;
        if (!current_date) {
            let today = new Date();
            let yesterday = today.getTime() - 1000 * 60 * 60 * 24;
            current_date_format = new Date(yesterday).format('yyyy-MM-dd');
            current_date = current_date_format.replace(/\//g, '').replace(/-/g, '');
        }

        var tb_loc = mongodb_db_obj.collection('tb_locations_' + current_date);
        var tb_pasflw_door_report = mongodb_db_obj.collection('tb_pasflw_time_report');

        fs.writeFileSync('sqlfile/BI_I_STAY_TIME.sql', "");

        tb_loc.aggregate([
            {
                $group: {
                    _id: {
                        client_mac: "$client_mac",
                        floor_number: "$floor_number"
                    },
                    last_time: {$last: "$timestamp"},
                    first_time: {$first: "$timestamp"}
                }
            }], {allowDiskUse: true}, function (e, values) {

            var sqlArr = '\r\n';
            values.forEach(function (val) {
                let firstTime = new Date(val.first_time);
                let lastTime = new Date(val.last_time);
                let count = (lastTime.getTime() - firstTime.getTime()) / 1000 / 60;

                val.updatetime = new Date().toLocaleString();

                var insert = "INSERT INTO BI_I_STAY_TIME (" +
                    "PASFLW_STORE_CODE, " +
                    "OCCUR_DATE, " +
                    "PHONE_NUM, " +
                    "MAC_ADDR, " +
                    "TYPE, " +
                    "CARD_TYPE, " +
                    "FLOOR_NAME, " +
                    "STAY_TIME, " +
                    "LAST_UPDATE_TIME, " +
                    "TAG) VALUES (" +
                    "'0210', " +
                    "to_date('" + current_date_format + "', 'yyyy-mm-dd')," +
                    "''," +
                    "'" + val._id.client_mac + "'," +
                    0 + "," +
                    "''," +
                    "'" + val._id.floor_number + "'," +
                    count.toFixed(2) + "," +
                    "to_date('" + val.updatetime + "', 'yyyy-mm-dd hh24:mi:ss')," +
                    0 + ");\r\n";

                sqlArr += insert;
            });
            fs.appendFileSync('sqlfile/BI_I_STAY_TIME.sql', sqlArr);
            if (values && values.length > 0) {
                //将数据存入mongodb中
                tb_pasflw_door_report.insertMany(values, function (err, ret) {
                    console.log(ret.result)
                })
            }
        });
    } catch (ex) {
        console.log(`%d - BI_I_PASFLW_DOOR err:` + ex, new Date())
    }
}

/**
 * 楼层客流信息分析（区分会员）
 */
module.exports.BI_I_PASFLW_FLOOR = function *(current_date) {
    try {
        let current_date_format;
        if (!current_date) {
            let today = new Date();
            let yesterday = today.getTime() - 1000 * 60 * 60 * 24;
            current_date_format = new Date(yesterday).format('yyyy-MM-dd');
            current_date = current_date_format.replace(/\//g, '').replace(/-/g, '');
        }

        var tb_loc = mongodb_db_obj.collection('tb_locations_' + current_date);
        var tb_pasflw_door_report = mongodb_db_obj.collection('tb_pasflw_floor_report');

        fs.writeFileSync('sqlfile/BI_I_PASFLW_FLOOR.sql', "");

        for (let i = 9; i < 23; i++) {   //数据切片并存库
            let starttime = new Date(`${current_date_format} ${i}:00:00`);
            let endtime = new Date(`${current_date_format} ${i + 1}:00:00`);

            tb_loc.group({"floor_number": true},
                {
                    "timestamp": {
                        "$lt": endtime,
                        "$gte": starttime
                    }
                },
                {"num": 0},
                function (cur, prev) {
                    prev.num++;
                },
                '', '', '', ''
            ).then(function (result) {
                var sqlArr = '\r\n';
                for (let ret of result) {
                    ret.starttime = starttime;
                    ret.endtime = endtime;
                    ret.updatetime = new Date().toLocaleString();

                    var insert = "INSERT INTO BI_I_PASFLW_FLOOR (" +
                        "PASFLW_STORE_CODE, " +
                        "BEGIN_DATE, " +
                        "END_DATE, " +
                        "FLOOR_NAME, " +
                        "HUMAN_NUMBER_IN, " +
                        "MEMBER_NUMBER_IN, " +
                        "LAST_UPDATE_TIME, " +
                        "TAG) VALUES (" +
                        "'0210', to_date('" +
                        ret.starttime.toLocaleString() + "'),to_date('" +
                        ret.endtime.toLocaleString() + "'),'" +
                        ret.floor_number + "'," +
                        ret.num + "," +
                        0 + ",to_date('" +
                        ret.updatetime + "')," + 0 + ");\r\n";

                    sqlArr += insert;
                }
                sqlArr += "commit;";
                fs.appendFileSync('sqlfile/BI_I_PASFLW_FLOOR.sql', sqlArr);

                if (result && result.length > 0) {
                    //将切片好的数据存入mongodb中
                    tb_pasflw_door_report.insertMany(result, function (err, ret) {
                        console.log(ret.result)
                    })
                }
            })
        }
    } catch (ex) {
        console.log(`%d - BI_I_PASFLW_FLOOR err:` + ex, new Date())
    }
}


/**
 * 店铺客流信息分析
 */
module.exports.BI_I_PASFLW_SHOP = function *(current_date) {
    try {
        let current_date_format;
        if (!current_date) {
            let today = new Date();
            let yesterday = today.getTime() - 1000 * 60 * 60 * 24;
            current_date_format = new Date(yesterday).format('yyyy-MM-dd');
            current_date = current_date_format.replace(/\//g, '').replace(/-/g, '');
        }

        var tb_loc = mongodb_db_obj.collection('tb_locations_' + current_date);
        var tb_pasflw_door_report = mongodb_db_obj.collection('tb_pasflw_shop_report');

        fs.writeFileSync('sqlfile/BI_I_PASFLW_SHOP.sql', "");

        for (let i = 9; i < 23; i++) {   //数据切片并存库
            let starttime = new Date(`${current_date_format} ${i}:00:00`);
            let endtime = new Date(`${current_date_format} ${i + 1}:00:00`);

            tb_loc.group({"zones_map_name": true},
                {
                    "timestamp": {
                        "$lt": endtime,
                        "$gte": starttime
                    }
                },
                {"num": 0},
                function (cur, prev) {
                    prev.floor_number;
                    prev.zones_name;
                    prev.num++;
                },
                '', '', '', ''
            ).then(function (result) {
                var sqlArr = '\r\n';
                for (let ret of result) {
                    ret.starttime = starttime;
                    ret.endtime = endtime;
                    ret.updatetime = new Date().toLocaleString();

                    var insert = "INSERT INTO BI_I_PASFLW_SHOP (" +
                        "PASFLW_STORE_CODE, " +
                        "BEGIN_DATE, " +
                        "END_DATE, " +
                        "SHOP_CODE, " +
                        "HUMAN_NUMBER_IN, " +
                        "MEMBER_NUMBER_IN, " +
                        "LAST_UPDATE_TIME, " +
                        "TAG) VALUES (" +
                        "'0210', to_date('" +
                        ret.starttime.toLocaleString() + "'),to_date('" +
                        ret.endtime.toLocaleString() + "'),'" +
                        ret.zones_map_name + "'," +
                        ret.num + "," +
                        0 + ",to_date('" +
                        ret.updatetime + "')," + 0 + ");\r\n";

                    sqlArr += insert;
                }
                sqlArr += "commit;";
                fs.appendFileSync('sqlfile/BI_I_PASFLW_SHOP.sql', sqlArr);

                if (result && result.length > 0) {
                    //将切片好的数据存入mongodb中
                    tb_pasflw_door_report.insertMany(result, function (err, ret) {
                        console.log(ret.result)
                    })
                }
            })
        }
    } catch (ex) {
        console.log(`%d - BI_I_PASFLW_SHOP err:` + ex, new Date())
    }
}


/**
 * 定义数据操作对象
 */
var db_option = function () {
}

/**
 * 统计总人流数
 * @param result
 * @constructor
 */
db_option.prototype.Get_Mongo_Weather_report = function *(current_date) {
    let tb_loc_model = mongodb_db_obj.collection('tb_locations_' + current_date);
    // let tb_loc_model = mongodb_db_obj.collection('tb_locations');
    return yield tb_loc_model.count();
}

/**
 * 将天气人流量统计汇总存入mongodb中
 * @param result
 * @constructor
 */
db_option.prototype.Insert_Mongo_Weather_report = function *(data) {
    if (!data || data.length == 0) {
        return;
    }

    let report_weather_people_flowrate_model = mongodb_db_obj.collection('tb_report_weather_people_flowrate');

    yield report_weather_people_flowrate_model.remove({OccurDate: data.OccurDate});

    let result = yield report_weather_people_flowrate_model.insert(data);

    console.log(`${new Date().valueOf()} into tb_report_weather_people_flowrate result:${JSON.stringify(result.result)}`);
}

var db = new db_option();