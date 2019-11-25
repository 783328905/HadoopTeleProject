package com.ctillnow.coprocessor;

import com.ctillnow.constant.Constant;
import com.ctillnow.utils.HBaseUtil;
import com.ctillnow.utils.PropertityUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.wal.Compressor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/25 0:05
 * 4
 */
public class MyCoprocessor implements RegionObserver ,RegionCoprocessor {



    private static Configuration conf = null;
    private static Connection connection = null;
    private static Table order = null;
    private RegionCoprocessorEnvironment env = null;

    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            order = connection.getTable(TableName.valueOf("order"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        this.env = (RegionCoprocessorEnvironment) e;
    }



    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    /**
     * 加入该方法，否则无法生效
     */
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }



    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        String table = c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        String oldTable = PropertityUtil.getPropertity().getProperty("hbase.table.name");
        if(!table.equals(oldTable)){
            return;
        }
        String rowkey =Bytes.toString( put.getRow());
        String[] split = rowkey.split("_");
        if("0".equals(split[5])){
            return;
        }
        String caller = split[1];
        String buildTime = split[2];
        String buildTS = split[3];
        String callee = split[4];
        String duration = split[6];

        int regions = Integer.valueOf(PropertityUtil.getPropertity().getProperty("hbase.regions"));
        String rowHash = HBaseUtil.getRowHash(regions,callee,buildTime);
        String newRowkey= HBaseUtil.getRowKey(rowHash,callee,buildTime,buildTS,caller,"0",duration);


        Put newPut = new Put(Bytes.toBytes(newRowkey));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTS"), Bytes.toBytes(buildTS));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //获取HBase连接以及表对象
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table htable = connection.getTable(TableName.valueOf(oldTable));

        //插入被叫数据

        htable.put(newPut);
        //关闭资源
        htable.close();
        connection.close();

    }
}
