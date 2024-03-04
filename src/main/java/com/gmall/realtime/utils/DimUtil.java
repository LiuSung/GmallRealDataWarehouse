package com.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection,String tableName, String key) throws Exception {

        //读缓存：先查询Redis
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + key;
        String dimJsonStr = jedis.get(redisKey);
        if(dimJsonStr != null){
            //重置过期时间
            jedis.expire(dimJsonStr,24 * 60 * 60);
            //归还连接
            jedis.close();
            //返回维度数据
            return JSON.parseObject(dimJsonStr);
        }
        //查询phoenix
        String querysql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + key + "'";
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querysql, JSONObject.class, false);

        //写缓存：将维度数据写入redis缓存
        JSONObject diminfo = queryList.get(0);
        jedis.set(redisKey,diminfo.toJSONString());
        jedis.expire(redisKey,24*60*60);
        jedis.close();
        return diminfo;
    }

    public static void delDimInfo(String tableName, String key){
        //获取redis连接
        Jedis jedis = JedisUtil.getJedis();
        //删除数据
        jedis.del( "DIM:" + tableName + ":" + key);
        //归还连接
        jedis.close();
    }
}
