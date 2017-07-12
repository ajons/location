/**
 * Created by ajon on 2017/7/11.
 */

const _ = require("lodash");

module.exports.getMember = function (callback) {
    try {
        var sql = "SELECT radmacid,loginid FROM sa_customermac_member";

        mysql_db_obj.driver.execQuery(sql, function (err, result) {
            global.member_list = result;
            // let a = _.where(member_list,{'radmacid':'10417FA8E1F21'})
            // console.log(a);
            callback(false, result);
        })
    } catch (ex) {
        console.log(`${new Date().valueOf()} getMember err:${JSON.stringify(ex)}`);
    }
}