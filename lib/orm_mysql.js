/**
 * Created by ajon on 2017/6/13.
 */

'use strict';

const orm_mysql = require('orm');
const conf = require('../conf');

module.exports = function () {
    return {
        /**
         * 链接数据库
         * @param defineModule  定义的module
         * @param callback      回调函数
         */
        connect: function (callback) {
            orm_mysql.connect(conf.mysql, function (err, db) {
                if (!err) {
                    console.log('%d - mysql connect success', new Date());
                    callback(false, db, orm_mysql);
                } else {
                    console.log('%d - mysql is connect fail', new Date());
                    callback(true);
                }
            })
        }
    }
}();