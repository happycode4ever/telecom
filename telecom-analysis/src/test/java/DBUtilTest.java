import com.jj.mapreduce.dao.BaseDAO;
import com.jj.mapreduce.dto.mapreduce.key.DateDimension;
import com.jj.mapreduce.util.DruidUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class DBUtilTest {
    @Test
    public void test() {
        BaseDAO baseDAO = new BaseDAO();
        String sql = "select * from telecom.tb_dimension_date where year = ? and month = ? and day = ?";
        DateDimension date = baseDAO.findByParams(null, sql, DateDimension.class, 2019, 2, 3);
        String sql2 = "insert into telecom.tb_dimension_date (year,month.day) values(?,?,?)";
        long id = baseDAO.insert(null, sql2, 2017, 7, 7);
        System.out.println(date);
        System.out.println(id);
    }

    @Test
    public void test2() throws SQLException {
        QueryRunner qr = new QueryRunner(DruidUtil.getDataSource());
        String sql2 = "insert into telecom.tb_dimension_date (year,month,day) values(?,?,?)";
        Long result = qr.insert(sql2, new ScalarHandler<>(), 1234, 12, 12);
        System.out.println(result);
    }

    @Test
    public void test3() {
        BaseDAO baseDAO = new BaseDAO();
        Connection connection = DruidUtil.getConnection();
        try {
            connection.setAutoCommit(false);
            String sql = "select * from telecom.tb_dimension_date where year = ? and month = ? and day = ?";
            Object[] params = {4444,11,11};
            DateDimension result = baseDAO.findByParams(connection, sql, DateDimension.class, params);
            System.out.println(result);
            if (result == null) {
                String sql2 = "insert into telecom.tb_dimension_date (year,month,day) values(?,?,?)";
                long id = baseDAO.insert(connection, sql2, params);
//                int i  = 1/0;
                System.out.println(id);
            }
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        connection = DruidUtil.getConnection();
        String sql = "select id from telecom.tb_dimension_date where year = ? and month = ? and day = ?";
        Object[] params = {4444,11,11};
        int id = baseDAO.findColumnByParams(connection, sql,"id", Integer.class, params);
        System.out.println(id);
    }

    @Test
    public void test4() throws SQLException {
        QueryRunner qr = new QueryRunner(DruidUtil.getDataSource());
//        String sql = "select id,year from telecom.tb_dimension_date where year = ? and month = ? and day = ?";
//        Object[] params = {4444,12,11};
//        Integer id = qr.query(sql,new ScalarHandler<Integer>("id"),params);
//        System.out.println(id);
        String sql = "select * from telecom.tb_call where id_date_dimension = ? and id_contact = ?";
        Object[] params = {5,5};
        String id = qr.query(sql,new ScalarHandler<>("id_date_contact"),params);
        System.out.println(id);
    }

}
