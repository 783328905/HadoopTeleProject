package com.ctillnow.consumer;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/23 15:28
 * 4
 */

import com.ctillnow.utils.HBaseUtil;
import com.ctillnow.utils.PropertityUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
  * 初始化命名空间
  * 创建表
  * 批量存储数据
  */

public class HBaseDAO {
    //配置信息
    private Properties properties;
    private String tableName;
    private String nameSpace;
    private int regions;
    //列族
    private String cf;
    private SimpleDateFormat sdf;
    private Connection connection;
    private Table table;
    private List<Put> puts;

    //主被叫参数
    private String flag;



    public HBaseDAO() throws IOException {
        properties = PropertityUtil.getPropertity();
        nameSpace = properties.getProperty("hbase.namespace");
        tableName = properties.getProperty("hbase.table.name");
        regions = Integer.valueOf(properties.getProperty("hbase.regions"));
        cf = properties.getProperty("hbase.cf");
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        connection = ConnectionFactory.createConnection();


        table = connection.getTable(TableName.valueOf(tableName));
        puts = new ArrayList();
        flag = "1";


        try {
            HBaseUtil.createNamespace(nameSpace);
        } catch (IOException e) {
            System.out.println(nameSpace+"已存在！");
        }


        HBaseUtil.createTable(tableName,regions,cf,"f2");


    }

    public void put(String value) throws ParseException, IOException {
        if (value == null) {
            return;
        }
        //value:19879419704,18302820904,2017-03-27 21:10:56,1150
        String[] split = value.split(",");

        String call1 = split[0];
        String call2 = split[1];
        String buildTime = split[2];
        String duration = split[3];

        long buildTS = sdf.parse(buildTime).getTime();

        String rowHash = HBaseUtil.getRowHash(regions,call1,buildTime);
        String rowKey = HBaseUtil.getRowKey(rowHash,call1,buildTime,buildTS+"",call2,flag,duration);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("buildTime"),Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("buildTS"),Bytes.toBytes(buildTS+""));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("flag"),Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("duration"),Bytes.toBytes(duration));

        puts.add(put);
        if(puts.size()>20){
            table.put(puts);
            puts.clear();
        }

    }

    public void close() throws IOException{

        table.put(puts);
        table.close();
        connection.close();
    }
}
