import com.jj.mapreduce.util.JDBCUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCTest {
    @Test
    public void insert() throws SQLException {
        Connection connection = JDBCUtil.getConnection();
        String sql = "INSERT INTO `telecom`.`tb_call`(`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`) VALUES (?,?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1,"2_2");
        ps.setInt(2,3);
        ps.setInt(3,3);
        ps.setInt(4,300);
        ps.setInt(5,30);
        ps.executeUpdate();
        JDBCUtil.close(connection,ps,null);

    }
    @Test
    public void merge() throws SQLException {
        Connection connection = JDBCUtil.getConnection();
        String sql = "INSERT INTO `telecom`.`tb_call`(`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`) " +
                "VALUES (?,?,?,?,?)" +
                "on duplicate key update " +
                "`id_date_dimension`=values(`id_date_dimension`),id_contact=values(`id_contact`),`call_sum` = VALUES(`call_sum`),`call_duration_sum` =VALUES(`call_duration_sum`)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1,"1_1");
        ps.setInt(2,5);
        ps.setInt(3,5);
        ps.setInt(4,500);
        ps.setInt(5,50);
        ps.executeUpdate();
        JDBCUtil.close(connection,ps,null);
    }
}
