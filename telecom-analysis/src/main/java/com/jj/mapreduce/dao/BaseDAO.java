package com.jj.mapreduce.dao;

import com.jj.mapreduce.util.DruidUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.Connection;
import java.sql.SQLException;


public class BaseDAO {
    private static QueryRunner qr = null;

    public BaseDAO() {
        if (qr == null) {
            qr = new QueryRunner(DruidUtil.getDataSource());
        }
    }

    public <T> T findColumnByParams(Connection connection, String sql, String idColumnName, Class<T> clazz, Object... params) {
        T id = null;
        try {
            if (connection == null)
                id = qr.query(sql, new ScalarHandler<T>(idColumnName), params);
            else
                id = qr.query(connection, sql, new ScalarHandler<T>(idColumnName), params);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public <T> T findByParams(Connection connection, String sql, Class<T> clazz, Object... params) {
        T t = null;
        try {
            if (connection == null)
                t = qr.query(sql, new BeanHandler<>(clazz), params);
            else
                t = qr.query(connection, sql, new BeanHandler<>(clazz), params);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return t;
    }

    public long insert(Connection connection, String sql, Object... params) {
        long id = 0;
        try {
            if (connection == null)
                id = qr.insert(sql, new ScalarHandler<>(), params);
            else
                id = qr.insert(connection, sql, new ScalarHandler<>(), params);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public int update(Connection connection, String sql, Object... params) {
        int row = 0;
        try {
            if (connection == null)
                row = qr.update(sql, params);
            else
                row = qr.update(connection, sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return row;
    }

}
