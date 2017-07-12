/**
 * Created by ajon on 2017/6/12.
 */

'use strict';
const co = require('co');
const _ = require("lodash");
var request = require("request");
const conf = require('../conf');
const thunkify = require('thunkify');
let _requesthttp = require('../lib/request_http');
let request_api = new _requesthttp();

/**
 * 获取身份鉴权
 * @returns {string}
 */
module.exports.getToken = function *() {
    const post_data = {"email": conf.ruckus.email, "password": conf.ruckus.password};
    const options = {
        url: `${conf.ruckus.host}/api_keys.json`,
        method: "post",
        json: true,
        headers: {"content-type": "application/json"},
        rejectUnauthorized: false,
        body: JSON.stringify(post_data)
    };
    var result = yield thunkify(request_api.requesthttp)(options);
    console.log("get api_keys.json values:" + JSON.stringify(result));
    let api_key = new Buffer(result.api_key);   //base64编码
    return api_key.toString('base64');
}

/**
 * 获取venues
 * @returns {string}
 */
module.exports.getVenues = function *() {
    let token = yield this.getToken();
    const options = {
        url: `${conf.ruckus.host}/venues.json`,
        method: "get",
        headers: {
            "Authorization": "Basic " + token
        },
        rejectUnauthorized: false
    };

    var result = yield thunkify(request_api.requesthttp)(options);
    console.log("get Venues.json values:" + JSON.stringify(result));

    //存mongodb
    yield db.Mongo_Venues(result);
}

/**
 * 获取venues详情
 * @returns {string}
 */
module.exports.getVenueDetailByID = function *() {
    let token = yield this.getToken();
    const options = {
        url: `${conf.ruckus.host}/venues/hkritkh-001.json`,
        method: "get",
        headers: {
            "Authorization": "Basic " + token
        },
        rejectUnauthorized: false
    };

    var result = yield thunkify(request_api.requesthttp)(options);
    console.log("get Venues/venues_id.json values:" + JSON.stringify(result));

    //存mongodb
    yield db.Mongo_VenuesDetail(result);
}

/**
 * 获取radio_maps列表
 * @returns {string}
 */
module.exports.getRadioMapsByVenuesID = function *() {
    let token = yield this.getToken();
    const options = {
        url: `${conf.ruckus.host}/venues/hkritkh-001/radio_maps.json`,
        method: "get",
        headers: {
            "Authorization": "Basic " + token
        },
        rejectUnauthorized: false
    };

    var result = yield thunkify(request_api.requesthttp)(options);
    console.log("get getRadioMapsByVenuesID values:" + JSON.stringify(result));

    for (let r of result) {
        r.venue_id = 'hkritkh-001';

        //存mongodb
        yield db.Mongo_RadioMap(r);
    }
}

/**
 * 获取radio_maps_detail详情
 * @returns {string}
 */
module.exports.getRadioMapsDetailByName = function *() {
    let token = yield this.getToken();
    const options = {
        url: `${conf.ruckus.host}/venues/hkritkh-001/radio_maps/0_0_1.json`,
        method: "get",
        headers: {
            "Authorization": "Basic " + token
        },
        rejectUnauthorized: false
    };

    var result = yield thunkify(request_api.requesthttp)(options);
    console.log("get getRadioMapsDetailByName values:" + JSON.stringify(result));

    if (result) {
        var r = {
            venue_id: 'hkritkh-001',
            name: '0_0_1',
            width: result.width,
            height: result.height,
            scale: result.scale
        }
        //存mongodb
        yield db.Mongo_RadioMapDetail(r);

        var floor_list = new Array();
        var zone_maps_list = new Array();
        var zones_list = new Array();

        let z_index = 0, zs_index = 0;

        //获取楼层数据
        result.floors.forEach(function (f, f_index) {
            floor_list[f_index] = {
                radio_maps_name: result.name,
                number: f.number,
                display_name: f.display_name,
                map_image_url: f.map_image_url,
                inside_image_url: f.inside_image_url
            };

            //获取zone_maps数据
            f.zone_maps.forEach(function (zoneMap) {
                zone_maps_list[z_index] = {
                    floor_number: f.number,
                    name: zoneMap.name,
                    display_name: zoneMap.display_name,
                    zones_image_url: zoneMap.zones_image_url
                };
                z_index++;

                //获取zones数据
                zoneMap.zones.forEach(function (zone) {
                    zones_list[zs_index] = {
                        floor_number: f.number,
                        zone_maps_name: zoneMap.name,
                        name: zone.name,
                        display_name: zone.display_name,
                        color: zone.color
                    };
                    zs_index++;
                })

            })
        })

        //存mongodb
        yield db.Mongo_floors(floor_list);
        yield db.Mongo_zone_maps(zone_maps_list);
        yield db.Mongo_zones(zones_list);
    }
}

/**
 * 获取Location详情
 * @returns {string}
 */
module.exports.getLocationByVenuesID = function *() {

    // yield db.Mongo_LocationMap("20170628", [{"client_mac": "000AF50EA400"}, {"client_mac": "ww001500651F0B"}, {"client_mac": "000AF50EA400"}]);
    // return;

    var today = new Date();
    var yesterday = today.getTime() - 1000 * 60 * 60 * 48;
    let current_date_format = new Date(yesterday).format('yyyy-MM-dd');
    let current_date = current_date_format.replace(/\//g, '').replace(/-/g, '');

    let token = yield this.getToken();
    const options = {
        url: `${conf.ruckus.host}/venues/hkritkh-001/locations/by_date.json?date=${current_date_format}&limit=30000`,
        method: "get",
        headers: {
            "Authorization": "Basic " + token
        },
        rejectUnauthorized: false
    };

    request(options, function (e, r, body) {
        co(function *() {
            yield insertLocationData(current_date, body);
            if (r.headers) {
                let next_url = r.headers.link.split('>')[0].split('<')[1];
                console.log(next_url);
                if (next_url) {
                    yield getLocationRecursion(current_date, token, next_url);
                }
            }
        })
    })
}

/**
 * 递归获取定位数据
 * @param token
 * @param url
 */
var getLocationRecursion = function*(current_date, token, url) {
    const opt = {
        url: url,
        method: "get",
        headers: {
            "Authorization": "Basic " + token,
        },
        rejectUnauthorized: false
    };
    request(opt, function (e, r, body) {
        co(function *() {
            yield insertLocationData(current_date, body);
            if (r.headers && r.headers.link) {   //如果接口请求无数据，则不会返回link值
                let next_url = r.headers.link.split('>')[0].split('<')[1];
                console.log(`%d - ${next_url}`, new Date());
                if (next_url) {
                    yield getLocationRecursion(current_date, token, next_url);
                }
            }
            else {  //数据存储结束
                console.log(`%d - 结束`, new Date());

                //使用co处理完成数据再作统计
                co(function *() {
                    //去重（去除出现次数小于2次的用户）
                    yield db.Mongo_LocationMap_Removal_NumInvalid(current_date);

                    //去重（删除出现时间小于3分钟大于7小时的用户）
                    yield db.Mongo_LocationMap_Removal_TimeInvalid(current_date);
                }).then(function () {


                })
            }
        })
    })
}

/**
 * 批量增加定位数据
 * @param current_date 数据日期
 * @param data
 */
var insertLocationData = function*(current_date, data) {
    var l_list = new Array();
    var l_list_invalid = new Array();
    var l_list_invalid_arr = new Array();
    let i = 0, j = 0;
    for (let l of JSON.parse(data)) {
        if (l.floor_number) {
            //查询是否是会员
            let member_info = _.where(member_list, {'radmacid': l.mac});
            let timestamp = new Date(l.timestamp);
            let loc = {
                client_mac: l.mac,
                timestamp: new Date(l.timestamp),
                floor_number: l.floor_number,
                x: l.x,
                y: l.y,
                located_inside: l.located_inside.toString(),
                zones_map_name: l.zones.length > 0 ? l.zones[0].zone_map_name : '',
                zones_name: l.zones.length > 0 ? l.zones[0].zone_name : '',
                is_member: member_info.length > 0 ? 1 : 0  //1是会员；0是非会员
            }

            let invalid_starttime_point = timestamp.format('yyyy-MM-dd').toString() + " 09:00:00";
            let invalid_endtime_point = timestamp.format('yyyy-MM-dd').toString() + " 23:00:00";

            if (new Date(timestamp.format('yyyy-MM-dd HH:mm:ss')) > new Date(invalid_starttime_point) &&
                new Date(timestamp.format('yyyy-MM-dd HH:mm:ss')) < new Date(invalid_endtime_point)) {
                l_list[i] = loc;
                i++;
            } else {
                let ishave = false;
                for (let t of l_list_invalid) {   //当前数据集中去重
                    if (t.client_mac == l.mac) {
                        ishave = true;
                        break;
                    }
                }
                if (!ishave) {
                    l_list_invalid_arr[j] = l.mac;
                    l_list_invalid[j] = {client_mac: l.mac};
                    j++;
                }
            }
        }
    }
    yield db.Mongo_LocationMap_invalid(current_date, l_list_invalid, l_list_invalid_arr);
    yield db.Mongo_LocationMap(current_date, l_list);
}

/**
 * 测试用
 * @constructor
 */
module.exports.LocationTest = function *() {
    var today = new Date();
    var yesterday = today.getTime() - 1000 * 60 * 60 * 48;
    let current_date_format = new Date(yesterday).format('yyyy-MM-dd');
    let current_date = current_date_format.replace(/\//g, '').replace(/-/g, '');

    yield db.Mongo_LocationMap_Removal_NumInvalid(current_date);

    yield db.Mongo_LocationMap_Removal_TimeInvalid(current_date);
}


/**
 * 定义数据操作对象
 */
var db_option = function () {
}

/**
 * ========mongodb操作==========
 */

/**
 * 操作venues
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_Venues = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    var venue_model = mongodb_db_obj.collection('tb_venue');
    venue_model.remove({}, function (err, d_ret) {
        if (!err && d_ret.result.ok == 1) {
            venue_model.insert(data, function (err, s_ret) {
                console.log(`${new Date().valueOf()} into tb_venue result:${JSON.stringify(s_ret.result)}`);
            })
        }
    })
}

/**
 * 操作venuesDetail
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_VenuesDetail = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    var venue_model = mongodb_db_obj.collection('tb_venue');

    venue_model.update({'venue_id': data.venue_id}, {
        $set: {
            exterior_image_url: data.exterior_image_url,
            street_address: data.street_address,
            locality: data.locality,
            region: data.region,
            postal_code: data.postal_code,
            country_name: data.country_name,
            coordinates: data.coordinates,
            time_zone_id: data.time_zone_id
        }
    }, function (err, s_ret) {
        console.log(`${new Date().valueOf()} into tb_venue result:${JSON.stringify(s_ret.result)}`);
    })
}

/**
 * 操作radio-maps
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_RadioMap = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    var radio_maps_model = mongodb_db_obj.collection('tb_radio_maps');
    let d_ret = yield radio_maps_model.remove();
    if (d_ret.result.ok == 1) {
        let s_ret = yield radio_maps_model.insert(data);
        console.log(`${new Date().valueOf()} into tb_radio_maps result:${JSON.stringify(s_ret.result)}`);
    }
}

/**
 * 操作RadioMapDetail
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_RadioMapDetail = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    var venueDetail_model = mongodb_db_obj.collection('tb_radio_maps');

    let s_ret = yield  venueDetail_model.update({'venue_id': data.venue_id, 'name': data.name}, {
        $set: {
            width: data.width,
            height: data.height,
            scale: data.scale
        }
    });
    console.log(`${new Date().valueOf()} into tb_venue_detail result:${JSON.stringify(s_ret.result)}`);
}

/**
 * 操作floors
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_floors = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    var floors_model = mongodb_db_obj.collection('tb_floors');
    let d_ret = yield floors_model.remove();
    if (d_ret.result.ok == 1) {
        let s_ret = yield floors_model.insert(data);
        console.log(`${new Date().valueOf()} into tb_floors result:${JSON.stringify(s_ret.result)}`);
    }
}

/**
 * 操作zone_maps
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_zone_maps = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    var zone_maps_model = mongodb_db_obj.collection('tb_zone_maps');
    let d_ret = yield zone_maps_model.remove();
    if (d_ret.result.ok == 1) {
        let s_ret = yield zone_maps_model.insert(data);
        console.log(`${new Date().valueOf()} into tb_zone_maps result:${JSON.stringify(s_ret.result)}`);
    }
}

/**
 * 操作zones
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_zones = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    var zones_model = mongodb_db_obj.collection('tb_zones');
    let d_ret = yield zones_model.remove();
    if (d_ret.result.ok == 1) {
        let s_ret = yield zones_model.insert(data);
        console.log(`${new Date().valueOf()} into tb_zones result:${JSON.stringify(s_ret.result)}`);
    }
}

/**
 * Mongodb操作location
 * @param current_date 数据日期（格式为yyyyMMdd）
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_LocationMap = function *(current_date, data) {
    if (!data || data.length == 0) {
        return true;
    }

    // var today = new Date();
    // var yesterday = today.getTime() - 1000 * 60 * 60 * 24;
    // let current_date = new Date(yesterday).format('yyyy-MM-dd').replace(/\//g, '').replace(/-/g, '');

    //初步过滤
    var tb_loc_invalid = mongodb_db_obj.collection('tb_locations_invalid_' + current_date);

    //数据过滤，过滤完成后再存入数据库
    //过滤非正常时段内的用户
    tb_loc_invalid.distinct("client_mac", "", "", function (err, docs) {
        docs.forEach(function (invalid_data) {
            for (let d of data) {
                if (invalid_data == d.client_mac) {
                    data.remove(d);
                }
            }
        })

        var tb_loc = mongodb_db_obj.collection('tb_locations_' + current_date);

        tb_loc.insertMany(data, function (err, result) {
        })
    })
}

/**
 * Mongodb操作location(拆分无效时间数据)
 * @param current_date 数据日期（格式为yyyyMMdd）
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_LocationMap_invalid = function *(current_date, data, l_list_invalid_arr) {
    if (!data || data.length == 0) {
        return true;
    }

    var tb_loc_invalid = mongodb_db_obj.collection('tb_locations_invalid_' + current_date);
    tb_loc_invalid.remove({"client_mac": {$in: l_list_invalid_arr}}, "", function (err, d_result) {
        console.log(d_result.result);
        tb_loc_invalid.insertMany(data, function (err, result) {
            // console.log(JSON.stringify(result));
        })
    });
}

/**
 * 去重次数（删除出现次数小于2次的用户）
 * @param current_date
 * @constructor
 */
db_option.prototype.Mongo_LocationMap_Removal_NumInvalid = function *(current_date) {
    try {
        var tb_loc = mongodb_db_obj.collection('tb_locations_' + current_date);

        tb_loc.aggregate([{
            $group: {
                _id: "$client_mac",
                count: {$sum: 1}
            }
        }, {$sort: {count: -1}}, {$match: {"count": {"$lt": 2}}}], "", function (err, result) {
            let inavlid_num_arr = new Array()
            var count_max = 0;
            for (let ret of result) {
                if (count_max >= 100000) {
                    tb_loc.remove({"client_mac": {$in: inavlid_num_arr}}, "", function (err, del_ret) {
                        console.log(del_ret.result);
                    });
                    inavlid_num_arr = [];
                    count_max = 0;
                }
                count_max++;
                inavlid_num_arr.push(ret._id);
            }
            tb_loc.remove({"client_mac": {$in: inavlid_num_arr}}, "", function (err, del_ret) {
                console.log(del_ret.result);
            });
        });
    } catch (ex) {
        console.log(`%d - Mongo_LocationMap_Removal_NumInvalid err:` + ex, new Date())
    }
}

/**
 * 去重时间（删除出现时间小于3分钟大于7小时的用户）
 * @param current_date
 * @constructor Jennie
 */
db_option.prototype.Mongo_LocationMap_Removal_TimeInvalid = function *(current_date) {

    try {
        var inavlid_num_arr = new Array();
        var tb_loc = mongodb_db_obj.collection('tb_locations_' + current_date);

        tb_loc.aggregate([{
            $group: {
                _id: "$client_mac",
                last_time: {$last: "$timestamp"},
                first_time: {$first: "$timestamp"}
            }
        }], function (e, values) {
            if (values != null) {
                var count_max = 0;
                values.forEach(function (val) {
                    let firstTime = new Date(val.first_time);
                    let lastTime = new Date(val.last_time);
                    let count = lastTime.getTime() - firstTime.getTime();
                    if (count_max >= 100000) {
                        tb_loc.remove({"client_mac": {$in: inavlid_num_arr}}, "", function (err, del_ret) {
                            console.log(del_ret.result);
                        });
                        inavlid_num_arr = [];
                        count_max = 0;
                    }
                    count_max++;

                    if (count < 3 * 60 * 1000 || count >= 7 * 60 * 60 * 1000) {
                        inavlid_num_arr.push(val._id);
                    }
                });

                tb_loc.remove({"client_mac": {$in: inavlid_num_arr}}, "", function (err, del_ret) {
                    console.log(del_ret.result);
                });
            }
        });
    } catch (ex) {
        console.log(`%d - Mongo_LocationMap_Removal_NumInvalid err:` + ex, new Date())
    }
}


/**
 * 过滤时间 & zone_name（时间区间小于7小时大于1小时）
 * @param current_date
 * @constructor Jennie
 */
module.exports.Mongo_LocationMap_Removal_TimeZonesNameInvalid_Temp = function *(current_date, zone_name) {

    try {
        var num_arr = new Array();
        var tb_loc = mongodb_db_obj.collection('tb_locations_' + current_date);

        var f1_door1_total = 0;
        var total = 0;
        var real_total = 0;
        tb_loc.aggregate([
            {
                $group: {
                    _id: {
                        client_mac: "$client_mac",
                        zones_name: "$zones_name"
                    },
                    last_time: {$last: "$timestamp"},
                    first_time: {$first: "$timestamp"}
                }
            }], {allowDiskUse: true}, function (e, values) {
            if (values != null) {
                values.forEach(function (val) {
                    total++;
                    // 指定 f1_door1
                    if (val._id.zones_name == zone_name) {
                        f1_door1_total++;
                        let firstTime = new Date(val.first_time);
                        let lastTime = new Date(val.last_time);
                        let count = lastTime.getTime() - firstTime.getTime();

                        // 过滤低于7个小时大于1个小时的人数
                        if (count > 60 * 60 * 1000 && count < 7 * 60 * 60 * 1000) {
                            num_arr.push(val);
                            real_total++;
                        }
                    }
                });

                console.log("total 人数 = " + total + "\nf1_door1 人数 = " + f1_door1_total + "\n低于7个小时大于1个小时的人数 : " + real_total);

                var tb_loc_2 = mongodb_db_obj.collection('tb_locations_' + current_date + '_test');
                tb_loc_2.insertMany(num_arr, function (err, result) {
                    if (err) {
                        console.error(err);
                    }
                })
            }
        });
    } catch (ex) {
        console.log(`%d - Mongo_LocationMap_Removal_NumInvalid err:` + ex, new Date())
    }
}


var db = new db_option();