package com.jj.mapreduce.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 升级版jdbc使用druid连接池
 */
public class DruidUtil {
    private static DataSource ds;
    private static ThreadLocal<Connection> threadLocal;

    static {
        initDataSource();
    }

    private static void initDataSource() {
        try {
            InputStream is = DruidUtil.class.getClassLoader().getResourceAsStream("database.properties");
            Properties properties = new Properties();
            properties.load(is);

            ds = DruidDataSourceFactory.createDataSource(properties);
            threadLocal = new ThreadLocal<>();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DataSource getDataSource() {
        if (ds == null) {
            initDataSource();
        }
        return ds;
    }

    public static Connection getConnection() {
        //先获取当前线程的连接
        Connection connection = threadLocal.get();
        try {
            //**如果当前线程的连接还没建立或者已经关闭，从线程池拿一个新的连接，再放回threadpool
            if (connection == null || connection.isClosed()) {
                connection = ds.getConnection();
                threadLocal.set(connection);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
