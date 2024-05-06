package com.atguigu.edu.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import javafx.scene.control.Tab;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * ClassName: HBaseUtil
 * Package: com.atguigu.edu.realtime.common.util
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 14:40
 * @Version: 1.0
 */
public class HBaseUtil {
    // 获取连接
    public static Connection getHbaseConnection() {
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", Constant.ZK_BROKERS);
            Connection connection = ConnectionFactory.createConnection(conf);
            return connection;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    // 关闭连接
    public static void closeHbaseConnection(Connection conn) throws IOException {
        if(conn != null || !conn.isClosed()) {
            conn.close();
        }
    }
    // 建表
    public static void createHBaseTable(Connection conn, String namespace,String tableName, String ...cf) {
        if(cf.length<1) {
            System.out.println("HBASE必须指定一个列族");
            return;
        }

        try(Admin admin = conn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(admin.tableExists(tableNameObj)) {
                System.out.println("要创建" + namespace + "下的" + tableName + "已存在");
                return;
            }

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String colf : cf) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colf)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("创建" + namespace + "下的" + tableName);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 删表
    public static void dropHBaseTable(Connection conn, String namespace,String tableName) {
        try (Admin admin = conn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(!admin.tableExists(tableNameObj)) {
                System.out.println("要删除" + namespace + "下的" + tableName + "不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("删除" + namespace + "下的" + tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    // put数据
    public static void putRow(Connection conn, String namespace, String tableName,
                              String rowKey, String cf, JSONObject jsonObj) {
        TableName tn = TableName.valueOf(namespace, tableName);
        try(Table table = conn.getTable(tn)) {
            Put put = new Put(Bytes.toBytes(rowKey));

            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if(value != null) {
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }

            table.put(put);
            System.out.println("向"+namespace + "下的" + tableName + "rowkey:" + rowKey + "插入成功");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //  删除数据
    public static void deleteRow(Connection conn, String namespace, String tableName,
                                 String rowKey) {
        TableName tn = TableName.valueOf(namespace, tableName);
        try(Table table = conn.getTable(tn)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));

            table.delete(delete);
            System.out.println(namespace + "下的" + tableName + "rowkey:" + rowKey + "删除成功");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 根据rowkey查数据
    public static <T> T getRow(Connection hbaseConn,
                               String nameSpace,
                               String tableName,
                               String rowKey,
                               Class<T> tClass,
                               boolean... isUnderlineToCamel) {

        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try ( Table table = hbaseConn.getTable(tableNameObj)){

            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0) {
                T obj = tClass.newInstance();
                for (Cell cell : cells) {
                    // 拿到列名 列值
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));

                    if(defaultIsUToC) {
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                return obj;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;

    }



    // TODO 测试HBASE
    public static void main(String[] args) {
        Connection hbaseConnection = getHbaseConnection();
        System.out.println("hbaseConnection = " + hbaseConnection);
    }
}
