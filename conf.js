/**
 * Created by ajon on 2017/5/23.
 */

module.exports = {
    env: {
        //HTTP监听端口
        port: 2000,
        //socket服务监听端口
        socket_port: 2001,
        debugLog: true,
        logPath: __dirname + '/logs/'
    },
    ruckus: {
        host: 'https://180.169.127.186/api/v1',  //'https://us-sys.ruckuslbs.com/api/v1',
        email: 'lin.qin@smartac.co',
        password: '12345678'
    },
    mysql: {   //MySQL数据库配置
        database: "scdb",
        protocol: "mysql",
        host: "192.168.99.142",
        user: "sc_u",
        password: "SC30_Passwd",
        query: {pool: true, debug: false}
    },
    mongdb: 'mongodb://sunhj:sunhj1234@172.16.0.32:27017/hkritaikoohui_location',
    //bin/mongod --dbpath /home/mongodb/data/db --rest
    //天气接口
    weather: {
        url: 'http://www.sojson.com/open/api/weather/json.shtml?city=%e4%b8%8a%e6%b5%b7'
        //'http://flash.weather.com.cn/wmaps/xml/china.xml' //国家天气网
    }
}