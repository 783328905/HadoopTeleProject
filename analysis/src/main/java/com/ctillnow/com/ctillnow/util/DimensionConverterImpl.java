package com.ctillnow.com.ctillnow.util;


import com.ctillnow.com.ctillnow.kv.BaseDimension;
import com.ctillnow.com.ctillnow.kv.ContantDimension;
import com.ctillnow.com.ctillnow.kv.DateDimension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 1、根据传入的维度数据，得到该数据对应的在表中的主键id
 * ** 做内存缓存，LRUCache
 * 分支
 * -- 缓存中有数据 -> 直接返回id
 * -- 缓存中无数据 ->
 * ** 查询Mysql
 * 分支：
 * -- Mysql中有该条数据 -> 直接返回id -> 将本次读取到的id缓存到内存中
 * -- Mysql中没有该数据  -> 插入该条数据 -> 再次反查该数据，得到id并返回 -> 缓存到内存中
 */
public class DimensionConverterImpl implements DimensionConverter {
    private static final Logger logger = LoggerFactory.getLogger(DimensionConverterImpl.class);
    private ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>();



    LRUCache cache = new LRUCache(1000);


    public int getDimensionID(BaseDimension dimension) {

        //缓存有就return
        String cacheKey = genCacheKey(dimension);
        if(cache.containsKey(cacheKey)){
            return cache.get(cacheKey);

        }
        String []sqls = null;
        if(dimension instanceof DateDimension){
            sqls = getDateDimensionSQL();
        }else if(dimension instanceof ContantDimension){
            sqls = getContactDimensionSQL();

        }else {
            throw new RuntimeException("没有匹配到对应维度信息");
        }
        Connection connection = this.getConnection();
        int id=-1;
        synchronized (this){
            id= execSQL(connection,sqls,dimension);

        }

        cache.put(cacheKey,id);
        return id;

    }

    private int execSQL(Connection connection, String[] sqls, BaseDimension dimension) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(sqls[0]);
            setArguments(preparedStatement,dimension);
            resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                int result = resultSet.getInt(1);
                JDBCUtil.close(null,preparedStatement,resultSet);
                return result;
            }
            JDBCUtil.close(null,preparedStatement,resultSet);

            preparedStatement = connection.prepareStatement(sqls[1]);
            setArguments(preparedStatement,dimension);
            preparedStatement.executeUpdate();

            JDBCUtil.close(null,preparedStatement,null);
            preparedStatement = connection.prepareStatement(sqls[0]);
            setArguments(preparedStatement,dimension);
            resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                return resultSet.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtil.close(null,preparedStatement,resultSet);
            return -1;
        }

    }

    private void setArguments(PreparedStatement preparedStatement, BaseDimension dimension) {
        int i = 0;
        try {
            if (dimension instanceof DateDimension) {
                //可以优化
                DateDimension dateDimension = (DateDimension) dimension;
                preparedStatement.setString(++i, dateDimension.getYear());
                preparedStatement.setString(++i, dateDimension.getMonth());
                preparedStatement.setString(++i, dateDimension.getDay());
            } else if (dimension instanceof ContantDimension) {
                ContantDimension contactDimension = (ContantDimension) dimension;
                preparedStatement.setString(++i, contactDimension.getPhoneNum());
                preparedStatement.setString(++i, contactDimension.getName());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
        private Connection getConnection() {
        Connection connection= null;
        try {
            connection = connectionThreadLocal.get();
            if(connection==null|| connection.isClosed()){
                connection = JDBCUtil.getInstance();
                connectionThreadLocal.set(connection);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private String []getContactDimensionSQL(){

        String query = "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ? AND `name` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_contacts` (`telephone`, `name`) VALUES (?, ?);";
        return new String[]{query,insert};
    }
    private String [] getDateDimensionSQL(){

        String query = "SELECT `id` FROM `tb_dimension_date` WHERE `year` = ? AND `month` = ? AND `day` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_date` (`year`, `month`, `day`) VALUES (?, ?, ?);";
        return new String[]{query,insert};
    }

    private String genCacheKey(BaseDimension demension){

        StringBuffer buffer = new StringBuffer();
        if(demension instanceof DateDimension){
            DateDimension dateDimension = (DateDimension) demension;
            buffer.append("date_dimension").append(dateDimension.getYear()).append(dateDimension.getMonth())
            .append(dateDimension.getDay());


        }else if(demension instanceof ContantDimension ){
            ContantDimension contantDimension = (ContantDimension)demension;
            buffer.append("contact_dimension").append(contantDimension.getPhoneNum());
        }

        return buffer.toString();
    }
}
