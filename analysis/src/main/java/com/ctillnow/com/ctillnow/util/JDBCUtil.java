package com.ctillnow.com.ctillnow.util;

import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.sql.*;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/31 14:57
 * 4
 */
public class JDBCUtil {
    private static Connection connection =null;
    private static final String MYSQL_DRIVER_CLASS="com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/ct?useSSL=false&zeroDateTimeBehavior=convertToNull";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "783328905";


    private static Connection getConnection(){
        try{
            Class.forName(MYSQL_DRIVER_CLASS);
            return DriverManager.getConnection(MYSQL_URL,USERNAME,PASSWORD);
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public static void close(Connection connection, Statement statement, ResultSet resultSet){
        if(connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(statement!=null){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if(resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public  static Connection getInstance(){

        if(connection ==null){
            connection = getConnection();
        }
        return connection;
    }
}
