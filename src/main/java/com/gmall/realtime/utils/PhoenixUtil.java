package com.gmall.realtime.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {
    //"data":{"id":1744269580549595177,"user_id":null,"sku_id":null,"spu_id":null,"is_cancel":null,"create_time":null,"cancel_time":null}}

    /**
     * @param connection phoenix 连接
     * @param sinkTable  phoenix 表名
     * @param data       表字段以及表数据
     */
    public static void upSertValues(DruidPooledConnection connection, String sinkTable, JSONObject data) throws SQLException {
        //拼接SQL upsert into db.table_name(id,tm_name) values('0001','admirefx')
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable
                + "(" + StringUtils.join(columns, ",") + ") values ('"
                + StringUtils.join(values, "','") + "')";

        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //执行SQL
        preparedStatement.execute();
        //释放资源
        preparedStatement.close();
    }
}
