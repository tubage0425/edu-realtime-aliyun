package com.atguigu.edu.realtime.common.util;

import com.atguigu.edu.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: JDBCUtil
 * Package: com.atguigu.edu.realtime.common.util
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 15:25
 * @Version: 1.0
 */
public class JDBCUtil {
    // DIM  configMap
    public static Connection getMysqlConnection() throws Exception {
        Class.forName(Constant.MYSQL_DRIVER);
        Connection connection = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        return connection;
    }

    public static void closeMysqlConnection(Connection conn) throws SQLException {
        if(conn != null || !conn.isClosed()) {
            conn.close();
        }
    }

    public static <T> List<T> queryList(Connection conn, String sql, Class<T> tClass,
                                        boolean... isUnderlineToCamel) throws Exception {
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰
        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        List<T> result = new ArrayList<T>();

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();

        //  处理结果集
        while (rs.next()) {
            // 使用反射创建T类型对象
            T tObj = tClass.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);

                if (defaultIsUToC) { // 需要下划线转驼峰:  a_a => aA a_aaaa_aa => aAaaaAa
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }
                BeanUtils.setProperty(tObj, columnName, columnValue);
            }
            result.add(tObj);
        }

        return result;
    }

}
