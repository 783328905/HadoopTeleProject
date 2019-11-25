package com.ctillnow.utils;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/23 14:44
 * 4
 */


import com.ctillnow.coprocessor.MyCoprocessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表
 * 4.生成rowkey
 * 5.预分区健的生成
 */
public class HBaseUtil {
    public static Configuration conf =null;
    public static Connection connection =null;
    public static Admin admin =null;
    public static Table table = null;
    static {


        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","master:2181");
        try {
            connection = ConnectionFactory.createConnection(conf);

            admin= connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void createNamespace(String namespace) throws IOException {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        admin.createNamespace(builder.build());


    }
    public static boolean isExist(String tableName) throws IOException {
         boolean b = admin.tableExists(TableName.valueOf(tableName));

         return b;

    }

    public static void createTable(String tableName,int regions,String... cfs) throws IOException {

        if(isExist(tableName)){
            System.out.println("表"+tableName+"已存在");
            return;
        }

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        List<ColumnFamilyDescriptor> list =new ArrayList<ColumnFamilyDescriptor>();
        for(String cf : cfs){
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            ColumnFamilyDescriptor build = columnFamilyDescriptorBuilder.build();
            list.add(build);

        }
        tableDescriptorBuilder.setColumnFamilies(list);
        tableDescriptorBuilder.setCoprocessor("com.ctillnow.coprocessor.MyCoprocessor");

        admin.createTable(tableDescriptorBuilder.build(),getSplitsKeys(regions));
        close();
    }
    //获取分区键
    //1,2,3,4,5
    public static byte[][] getSplitsKeys(int regions){
        //创建分区二维数据
        byte[][] splitKeys =new byte[regions][];
        DecimalFormat decimalFormat = new DecimalFormat("00");

        //循环
        for(int i=0;i<regions;i++){
            splitKeys[i] = Bytes.toBytes(decimalFormat.format(i)+"|");

        }
        return splitKeys;



    }
    //手机号+年月进行hash+md5 保证离散
    //0x_13712341234_2017-05-02 12:23:55_时间戳_13598769876_duration
    public static String getRowKey(String rowHash,String caller,String buildTime,String buildTS,String callee,String flag,String duration){
        return rowHash+"_"+caller+"_"+buildTime+"_"+buildTS+"_"+callee+"_"+flag+"_"+duration;

    }

    public static String getRowHash(int regions,String caller,String buildTime){

        DecimalFormat decimalFormat = new DecimalFormat("00");

        //取手机号中间4位
        String phoneMid = caller.substring(3,7);
        String yearMonth = buildTime.replaceAll("-","").substring(0,6);
        int i = (Integer.parseInt(phoneMid)^Integer.parseInt(yearMonth))%regions;

        return decimalFormat.format(i);



    }



    public static void close() throws IOException {
        admin.close();
        connection.close();

    }


    public static void scantable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));


        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for(Result r :scanner){
            Cell[] cells = r.rawCells();
            for(Cell c :cells){
                String rowkey  = Bytes.toString(c.getRowArray(),c.getRowOffset(),c.getRowLength());
                String quify  = Bytes.toString(c.getQualifierArray(),c.getQualifierOffset(),c.getQualifierLength());
                String family  = Bytes.toString(c.getFamilyArray(),c.getFamilyOffset(),c.getFamilyLength());
                String value  = Bytes.toString(c.getValueArray(),c.getQualifierOffset(),c.getFamilyLength());

                System.out.println(rowkey+":  "+family+","+quify+","+value);
            }
            System.out.println();
        }
    }

    public static void main(String args[]) throws IOException {
        scantable("ns_telecom:calllog");
       /* byte [] [] splitKeys = getSplitsKeys(6);
        for(byte[]splitKey : splitKeys){
            System.out.println(Bytes.toString(splitKey)+"--");
        }

        String rowHash = getRowHash(6,"15595005990","2017-07-22 09:43:51");
        System.out.println(getRowKey(rowHash,"15595005990","2017-07-22 09:43:51","4545","18942357253","_","0235"));*/
    }
}
