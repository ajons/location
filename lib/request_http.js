/**
 * Created by Administrator on 2017/6/12.
 */
var request = require("request");

var api = module.exports = function () {

}

/**
 * http request
 * @param options
 *   {
        url:"https://api.weixin.qq.com/cgi-bin/ticket/getticket?access_token="+token+"&type=jsapi" ,
        method:"get",
        headers:{'content-type':'application/x-www-form-urlencoded'},
        body:""
      }
 * @param callback
 */
api.prototype.requesthttp = function (options, callback) {
    request(options, function (e, r, body) {
        if (!e) {
            try {
                body = JSON.parse(body);
                if (body.errcode) {
                    e = null;
                }
            }
            catch (error) {
                //e = error;
            }
        }
        callback && callback(e, body);
    });
}