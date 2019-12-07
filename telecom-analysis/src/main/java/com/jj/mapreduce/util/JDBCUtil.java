package com.jj.mapreduce.util;

import java.sql.*;

public class JDBCUtil {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String JDBC_URL = "jdbc:mysql://hadoop112:3306?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    private static final String JDBC_NAME = "root";
    private static final String JDBC_PASSWORD = "123456";

    public static Connection getConnection(){
        Connection connection = null;
        try {
            Class<?> driverClass = Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(JDBC_URL, JDBC_NAME, JDBC_PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void close(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet){
        try {
            if(connection!=null&&!connection.isClosed())
                connection.close();
            if(preparedStatement!=null&&!preparedStatement.isClosed())
                preparedStatement.close();
            if(resultSet!=null&&!resultSet.isClosed()){
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
