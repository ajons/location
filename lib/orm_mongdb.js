/**
 * Created by Administrator on 2017/6/22.
 */

'use strict';

const conf = require('../conf');
const mongoClient = require('mongodb').MongoClient

module.exports = function () {
    return {
        /**
         * 链接数据库
         * @param defineModule  定义的module
         * @param callback      回调函数
         */
        connect: function (callback) {
            mongoClient.connect(conf.mongdb, function (err, db) {
                if (!err) {
                    console.log('%d - mongodb connect success', new Date());
                    callback(false, db);
                } else {
                    console.log('%d - mongodb is connect fail', new Date());
                    callback(true);
                }
            });
        }
    }
}();