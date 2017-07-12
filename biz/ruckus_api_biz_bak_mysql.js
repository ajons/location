/**
 * Created by ajon on 2017/6/12.
 */

'use strict';
const co = require('co');
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

    //Mysql写法
    // for (let r of result) {
    //     let ret = yield db.Mongo_Venues(r);
    //     console.log(`${new Date().valueOf()} into tb_venue result:${r.venue_id}:${ret}`);
    // }
}

/**
 * 获取venues详情
 * @returns {string}
 */
module.exports.getVenueDetailByID = function *() {
    let token = yield this.getToken();
    const options = {
        url: `${conf.ruckus.host}/venues/hkritaikoohui-01.json`,
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

    //存mysql
    // let ret = yield db.Venues(result);
    // console.log(`${new Date().valueOf()} into tb_venue detail result:${result.venue_id}:${ret}`);
}

/**
 * 获取radio_maps列表
 * @returns {string}
 */
module.exports.getRadioMapsByVenuesID = function *() {
    let token = yield this.getToken();
    const options = {
        url: `${conf.ruckus.host}/venues/hkritaikoohui-01/radio_maps.json`,
        method: "get",
        headers: {
            "Authorization": "Basic " + token
        },
        rejectUnauthorized: false
    };

    var result = yield thunkify(request_api.requesthttp)(options);
    console.log("get getRadioMapsByVenuesID values:" + JSON.stringify(result));

    for (let r of result) {
        r.venue_id = 'hkritaikoohui-01';

        //存mongodb
        yield db.Mongo_RadioMap(r);

        //存mysql
        // let ret = yield db.RadioMap(r);
        // console.log(`${new Date().valueOf()} into tb_radio_maps result:${r.venue_id}:${r.name}:${ret}`);
    }
}

/**
 * 获取radio_maps_detail详情
 * @returns {string}
 */
module.exports.getRadioMapsDetailByName = function *() {
    let token = yield this.getToken();
    const options = {
        url: `${conf.ruckus.host}/venues/hkritaikoohui-01/radio_maps/1_1_1.json`,
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
            venue_id: 'hkritaikoohui-01',
            name: '1_1_1',
            width: result.width,
            height: result.height,
            scale: result.scale
        }
        //存mongodb
        yield db.Mongo_RadioMapDetail(r);

        //存mysql
        // let ret = yield db.RadioMap(r);
        // console.log(`${new Date().valueOf()} into tb_radio_maps result:${r.venue_id}:${r.name}:${ret}`);


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

        //存MySQL
        // for (let [index, f] of result.floors) {
        //     console.log(index);
        //     floor_list[index] = {
        //         radio_maps_name: result.name,
        //         number: f.number,
        //         display_name: f.display_name,
        //         map_image_url: f.map_image_url,
        //         inside_image_url: f.inside_image_url
        //     };
        //
        //     //存mysql
        //     let ret_f = yield db.FloorsMap(f_info);
        //     console.log(`${new Date().valueOf()} into tb_floors result:${result.name}:${f.number}:${ret_f}`);
        //
        //     //保存zoneMap
        //     for (let zoneMap of f.zone_maps) {
        //         let zoneMap_info = {
        //             floor_number: f.number,
        //             name: zoneMap.name,
        //             display_name: zoneMap.display_name,
        //             zones_image_url: zoneMap.zones_image_url
        //         };
        //
        //         //存mysql
        //         let ret_f = yield db.ZoneMapsMap(zoneMap_info);
        //         console.log(`${new Date().valueOf()} into tb_zone_maps result:${result.name}:${f.number}:${zoneMap.name}:${ret_f}`);
        //
        //
        //         //保存zones
        //         for (let zone of zoneMap.zones) {
        //             let zone_info = {
        //                 floor_number: f.number,
        //                 zone_maps_name: zoneMap.name,
        //                 name: zone.name,
        //                 display_name: zone.display_name,
        //                 color: zone.color
        //             };
        //
        //             //存mysql
        //             let ret_f = yield db.ZonesMap(zone_info);
        //             console.log(`${new Date().valueOf()} into tb_zones result:${result.name}:${f.number}:${zoneMap.name}:${zone.name}:${ret_f}`);
        //         }
        //     }
        // }
    }
}

/**
 * 获取Location详情
 * @returns {string}
 */
module.exports.getLocationByVenuesID = function *() {
    let current_date = new Date().format('yyyy-MM-dd');

    let token = yield this.getToken();
    const options = {
        url: `${conf.ruckus.host}/venues/hkritaikoohui-01/locations/by_date.json?date=${current_date}&limit=30000`,
        method: "get",
        headers: {
            "Authorization": "Basic " + token
        },
        rejectUnauthorized: false
    };

    request(options, function (e, r, body) {
        co(function *() {
            yield insertLocationData(body);
            if (r.headers) {
                let next_url = r.headers.link.split('>')[0].split('<')[1];
                console.log(next_url);
                if (next_url) {
                    yield getLocationRecursion(token, next_url);
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
var getLocationRecursion = function*(token, url) {
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
            yield insertLocationData(body);
            if (r.headers) {
                let next_url = r.headers.link.split('>')[0].split('<')[1];
                console.log(next_url);
                if (next_url) {
                    yield getLocationRecursion(token, next_url);
                }
            }
        })
    })
}

/**
 * 批量增加定位数据
 * @param data
 */
var insertLocationData = function*(data) {
    // var tsql = "";
    var l_list = new Array();
    let i = 0;
    for (let l of JSON.parse(data)) {
        if (l.floor_number) {
            let loc = {
                client_mac: l.mac,
                timestamp: l.timestamp,
                floor_number: l.floor_number,
                x: l.x,
                y: l.y,
                located_inside: l.located_inside.toString(),
                zones_map_name: l.zones.length > 0 ? l.zones[0].zone_map_name : '',
                zones_name: l.zones.length > 0 ? l.zones[0].zone_name : '',
            }
            l_list[i] = loc;
            i++;
        }
    }
    yield db.Mongo_LocationMap(l_list);
}

/**
 * 定义数据操作对象
 */
var db_option = function () {
}

/**
 * ========mysql操作==========
 */

/**
 * 创建venue-Model对象
 * @returns {any|Model}
 * @constructor
 */
db_option.prototype.VenuesModel = function *() {
    return db_obj.define('tb_venue', {
        venue_id: {type: 'text', key: true},
        name: String,
        exterior_thumbnail_url: String,
        street_address: String,
        locality: String,
        region: String,
        postal_code: String,
        country_name: String,
        coordinates: String,
        time_zone_id: String,
        exterior_image_url: String
    }, {cache: false});
}

/**
 * 创建RadioMaps-Model对象
 * @returns {any|Model}
 * @constructor
 */
db_option.prototype.RadioMapsModel = function *() {
    return db_obj.define('tb_radio_maps', {
        venue_id: {type: 'text', key: true},
        name: {type: 'text', key: true},
        production: String,
        start_timestamp: String,
        end_timestamp: String,
        width: Number,
        height: Number,
        scale: Number
    }, {cache: false});
}

/**
 * 创建Floors-Model对象
 * @returns {any|Model}
 * @constructor
 */
db_option.prototype.FloorsModel = function *() {
    return db_obj.define('tb_floors', {
        radio_maps_name: {type: 'text', key: true},
        number: {type: 'number', key: true},
        display_name: {type: 'text', key: true},
        map_image_url: String,
        inside_image_url: String
    }, {cache: false});
}

/**
 * 创建zones_maps-Model对象
 * @returns {any|Model}
 * @constructor
 */
db_option.prototype.ZonesMapsModel = function *() {
    return db_obj.define('tb_zone_maps', {
        floor_number: {type: 'number', key: true},
        name: {type: 'text', key: true},
        display_name: String,
        zones_image_url: String
    }, {cache: false});
}

/**
 * 创建zones-Model对象
 * @returns {any|Model}
 * @constructor
 */
db_option.prototype.ZonesModel = function *() {
    return db_obj.define('tb_zones', {
        floor_number: {type: 'number', key: true},
        zone_maps_name: {type: 'text', key: true},
        name: {type: 'text', key: true},
        display_name: String,
        color: String
    }, {cache: false});
}

/**
 * 创建location-Model对象
 * @returns {any|Model}
 * @constructor
 */
db_option.prototype.LocationModel = function *() {
    return db_obj.define('tb_locations', {
        id: {type: 'number', key: true},
        client_mac: String,
        timestamp: String,
        floor_number: Number,
        x: Number,
        y: Number,
        located_inside: String,
        zones_map_name: String,
        zones_name: String
    }, {cache: false});
}

/**
 * 操作venues
 * @param result
 * @constructor
 */
db_option.prototype.Venues = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    //获取venues-model对象
    let venues_Model = yield this.VenuesModel();

    let d_venue = yield thunkify(venues_Model.find)({venue_id: data.venue_id});
    let result = true;
    if (!d_venue || d_venue.length == 0) {
        result = yield thunkify(venues_Model.create)(data);
    } else {
        d_venue[0].venue_id = data.venue_id;
        d_venue[0].name = data.name;
        d_venue[0].exterior_thumbnail_url = data.exterior_thumbnail_url ? data.exterior_thumbnail_url : d_venue[0].exterior_thumbnail_url;

        d_venue[0].exterior_image_url = data.exterior_image_url ? data.exterior_image_url : d_venue[0].exterior_image_url;
        d_venue[0].street_address = data.street_address ? data.street_address : d_venue[0].street_address;
        d_venue[0].locality = data.locality ? data.locality : d_venue[0].locality;
        d_venue[0].region = data.region ? data.region : d_venue[0].region;
        d_venue[0].postal_code = data.postal_code ? data.postal_code : d_venue[0].postal_code;
        d_venue[0].country_name = data.country_name ? data.country_name : d_venue[0].country_name;
        d_venue[0].coordinates = data.coordinates ? data.coordinates : d_venue[0].coordinates;
        d_venue[0].time_zone_id = data.time_zone_id ? data.time_zone_id : d_venue[0].time_zone_id;
        result = yield thunkify(d_venue[0].save)();
    }
    return (result ? true : false);
}

/**
 * 操作radio-maps
 * @param result
 * @constructor
 */
db_option.prototype.RadioMap = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    //获取radioMaps-model对象
    let radioMaps_Model = yield this.RadioMapsModel();

    let d_radioMaps = yield thunkify(radioMaps_Model.find)({venue_id: data.venue_id, name: data.name});
    let result = true;
    if (!d_radioMaps || d_radioMaps.length == 0) {
        result = yield thunkify(radioMaps_Model.create)(data);
    } else {
        d_radioMaps[0].venue_id = data.venue_id;
        d_radioMaps[0].name = data.name;
        d_radioMaps[0].production = data.production ? data.production : d_radioMaps[0].production;
        d_radioMaps[0].start_timestamp = data.start_timestamp ? data.start_timestamp : d_radioMaps[0].start_timestamp;
        d_radioMaps[0].end_timestamp = data.end_timestamp ? data.end_timestamp : d_radioMaps[0].end_timestamp;
        d_radioMaps[0].width = data.width ? data.width : d_radioMaps[0].width;
        d_radioMaps[0].height = data.height ? data.height : d_radioMaps[0].height;
        d_radioMaps[0].scale = data.scale ? data.scale : d_radioMaps[0].scale;
        result = yield thunkify(d_radioMaps[0].save)();
    }
    return (result ? true : false);
}

/**
 * 操作floors-maps
 * @param result
 * @constructor
 */
db_option.prototype.FloorsMap = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    //获取floors-model对象
    let floors_Model = yield this.FloorsModel();

    let d_floorsMaps = yield thunkify(floors_Model.find)({
        radio_maps_name: data.radio_maps_name,
        number: data.number,
        display_name: data.display_name
    });

    let result = true;
    if (!d_floorsMaps || d_floorsMaps.length == 0) {
        result = yield thunkify(floors_Model.create)(data);
    } else {
        d_floorsMaps[0].map_image_url = data.map_image_url;
        d_floorsMaps[0].inside_image_url = data.inside_image_url;
        result = yield thunkify(d_floorsMaps[0].save)();
    }
    return (result ? true : false);
}

/**
 * 操作ZoneMaps-maps
 * @param result
 * @constructor
 */
db_option.prototype.ZoneMapsMap = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    //获取zoneMaps-model对象
    let zoneMaps_Model = yield this.ZonesMapsModel();

    let d_zoneMapsMaps = yield thunkify(zoneMaps_Model.find)({
        floor_number: data.floor_number,
        name: data.name
    });

    let result = true;
    if (!d_zoneMapsMaps || d_zoneMapsMaps.length == 0) {
        result = yield thunkify(zoneMaps_Model.create)(data);
    } else {
        d_zoneMapsMaps[0].display_name = data.display_name;
        d_zoneMapsMaps[0].zones_image_url = data.zones_image_url;
        result = yield thunkify(d_zoneMapsMaps[0].save)();
    }
    return (result ? true : false);
}

/**
 * 操作Zones-maps
 * @param result
 * @constructor
 */
db_option.prototype.ZonesMap = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    //获取zones-model对象
    let zones_Model = yield this.ZonesModel();

    let d_zonesMaps = yield thunkify(zones_Model.find)({
        floor_number: data.floor_number,
        zone_maps_name: data.zone_maps_name,
        name: data.name
    });

    let result = true;
    if (!d_zonesMaps || d_zonesMaps.length == 0) {
        result = yield thunkify(zones_Model.create)(data);
    } else {
        d_zonesMaps[0].display_name = data.display_name;
        d_zonesMaps[0].color = data.color;
        result = yield thunkify(d_zonesMaps[0].save)();
    }
    return (result ? true : false);
}

/**
 * 操作location
 * @param result
 * @constructor
 */
db_option.prototype.LocationMap = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    //获取location-model对象
    let location_Model = yield this.LocationModel();

    return yield thunkify(location_Model.create)(data);
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
 * @param result
 * @constructor
 */
db_option.prototype.Mongo_LocationMap = function *(data) {
    if (!data || data.length == 0) {
        return true;
    }

    //按照日期生成定位数据表
    let current_date = (new Date()).toLocaleDateString().replace(/\//g, '');

    var tb_loc = mongodb_db_obj.collection('tb_locations_' + current_date);
    tb_loc.insertMany(data, function (err, result) {
        // console.log(JSON.stringify(result));
    })
}

var db = new db_option();