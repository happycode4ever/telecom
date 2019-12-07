package com.jj.mapreduce.outputformat;

import com.jj.mapreduce.dao.BaseDAO;
import com.jj.mapreduce.dto.mapreduce.key.CombineDimension;
import com.jj.mapreduce.dto.mapreduce.key.ContactDimension;
import com.jj.mapreduce.dto.mapreduce.key.DateDimension;
import com.jj.mapreduce.dto.mapreduce.value.AnalysisResult;
import com.jj.mapreduce.util.DruidUtil;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class WritebackMysqlService {

    private Logger logger = LoggerFactory.getLogger(WritebackMysqlService.class);
    private LRUCache<String, Integer> cache = new LRUCache<>(500);
    private BaseDAO dao = new BaseDAO();
    private static final String CACHE_CONTACT_KEY_PREFIX = "id_contact_";
    private static final String CACHE_DATE_KEY_PREFIX = "id_date_";
    private static final String DIMENSION_RESULT_SPLITTER = "_";


    /**
     * 第二版 由于RecordWriter.write是多线程操作 对于维度表存在多次同样数据的读写需要同步
     * 返回需要的关键维度主键信息
     * @param combineDimension
     */
    private synchronized String writeDimension(CombineDimension combineDimension){
        DateDimension dateDimension = combineDimension.getDateDimension();
        ContactDimension contactDimension = combineDimension.getContactDimension();
        String telephone = contactDimension.getTelephone();
        String name = contactDimension.getName();
        int year = dateDimension.getYear();
        int month = dateDimension.getMonth();
        int day = dateDimension.getDay();
        String cacheDateKey = CACHE_DATE_KEY_PREFIX + year + month + day;
        String cacheContactKey = CACHE_CONTACT_KEY_PREFIX + telephone;
        Integer dateId = 0;
        Integer contactId = 0;
        String result=null;

        Connection connection = DruidUtil.getConnection();
        try {
            //关闭自动提交手动开启事务
            connection.setAutoCommit(false);
            //时间维度，查缓存，缓存没有就查表再塞缓存
            if (!cache.containsKey(cacheDateKey)) {
                String queryDateIdSql = "select id from telecom.tb_dimension_date where year = ? and month = ? and day = ?";
                dateId = dao.findColumnByParams(connection, queryDateIdSql, "id", Integer.class, year, month, day);
                if (dateId == null) {
                    //如果表里也没有插入回传id
                    String insertDateSql = "INSERT INTO `telecom`.`tb_dimension_date`(`year`, `month`, `day`) VALUES (?,?,?)";
                    dateId = Math.toIntExact(dao.insert(connection, insertDateSql, year, month, day));
                }
//            最后塞缓存
                cache.put(cacheDateKey, dateId);
            } else {
                dateId = cache.get(cacheDateKey);
            }
            //联系人维度表同样处理
            if (!cache.containsKey(cacheContactKey)) {
                String queryContactIdSql = "select id from telecom.tb_contacts where telephone = ?";
                contactId = dao.findColumnByParams(connection, queryContactIdSql, "id", Integer.class, telephone);
                if (contactId == null) {
                    String insertContactSql = "INSERT INTO `telecom`.`tb_contacts`(`telephone`, `name`) VALUES (?,?)";
                    contactId = Math.toIntExact(dao.insert(connection, insertContactSql, telephone, name));
                }
                cache.put(cacheContactKey, contactId);
            } else {
                contactId = cache.get(cacheContactKey);
            }
            connection.commit();
            result = dateId+"_"+contactId;
        }catch (Exception e){
            logger.error("wribackException",e);
            //有异常就回滚
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
        return result;
    }

    /**
     * TODO
     * 第二版优化版 通话数据量大 改造成批量提交
     * 每个reduce方法聚合的数据都不同 调用write的多线程数据不共享 可以不设置同步锁
     * @param dimensionResult
     * @param analysisResult
     */
    private void writeCall(String dimensionResult, AnalysisResult analysisResult){
        String[] results = dimensionResult.split(DIMENSION_RESULT_SPLITTER);
        int dateId = Integer.valueOf(results[0]);
        int contactId = Integer.valueOf(results[1]);
        int callSum = analysisResult.getCallSum();
        int durationSum = analysisResult.getDurationSum();
        //最终通话记录表操作，有则更新，无则插入
        String insertCallSql = "INSERT INTO `telecom`.`tb_call`(`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`)" +
                " VALUES (?,?,?,?,?) on DUPLICATE key UPDATE " +
                "id_date_dimension = values(id_date_dimension), id_contact=values(id_contact),call_sum=values(call_sum),call_duration_sum=values(call_duration_sum)";

    }

    /**
     //从cache拿维度数据对应的主键，有直接拿，没有走数据库连接
     //从线程池获取连接，有直接拿无才创建
     //接下来写三张表 两张维度表先查数据，没有才写 记录表没有插入有则更新
     * 第一版瀑布式
     */
    public void writeMysql(CombineDimension combineDimension, AnalysisResult analysisResult) {

        DateDimension dateDimension = combineDimension.getDateDimension();
        ContactDimension contactDimension = combineDimension.getContactDimension();
        String telephone = contactDimension.getTelephone();
        String name = contactDimension.getName();
        int year = dateDimension.getYear();
        int month = dateDimension.getMonth();
        int day = dateDimension.getDay();
        String cacheDateKey = CACHE_DATE_KEY_PREFIX + year + month + day;
        String cacheContactKey = CACHE_CONTACT_KEY_PREFIX + telephone;
        Integer dateId = 0;
        Integer contactId = 0;
        int callSum = analysisResult.getCallSum();
        int durationSum = analysisResult.getDurationSum();

        Connection connection = DruidUtil.getConnection();
        try {
            //关闭自动提交手动开启事务
            connection.setAutoCommit(false);
            //时间维度，查缓存，缓存没有就查表再塞缓存
            if (!cache.containsKey(cacheDateKey)) {
                String queryDateIdSql = "select id from telecom.tb_dimension_date where year = ? and month = ? and day = ?";
                dateId = dao.findColumnByParams(connection, queryDateIdSql, "id", Integer.class, year, month, day);
                if (dateId == null) {
                    //如果表里也没有插入回传id
                    String insertDateSql = "INSERT INTO `telecom`.`tb_dimension_date`(`year`, `month`, `day`) VALUES (?,?,?)";
                    dateId = Math.toIntExact(dao.insert(connection, insertDateSql, year, month, day));
                }
//            最后塞缓存
                cache.put(cacheDateKey, dateId);
            } else {
                dateId = cache.get(cacheDateKey);
            }
            //联系人维度表同样处理
            if (!cache.containsKey(cacheContactKey)) {
                String queryContactIdSql = "select id from telecom.tb_contacts where telephone = ?";
                contactId = dao.findColumnByParams(connection, queryContactIdSql, "id", Integer.class, telephone);
                if (contactId == null) {
                    String insertContactSql = "INSERT INTO `telecom`.`tb_contacts`(`telephone`, `name`) VALUES (?,?)";
                    contactId = Math.toIntExact(dao.insert(connection, insertContactSql, telephone, name));
                }
                cache.put(cacheContactKey, contactId);
            } else {
                contactId = cache.get(cacheContactKey);
            }
            //最终通话记录表操作，有则更新，无则插入
            String insertCallSql = "INSERT INTO `telecom`.`tb_call`(`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`)" +
                    " VALUES (?,?,?,?,?) on DUPLICATE key UPDATE " +
                    "id_date_dimension = values(id_date_dimension), id_contact=values(id_contact),call_sum=values(call_sum),call_duration_sum=values(call_duration_sum)";
            dao.update(connection, insertCallSql, genTableCallIdKey(dateId, contactId), dateId, contactId, callSum, durationSum);
            //无异常就提交
            connection.commit();
        }catch (Exception e){
            logger.error("wribackException",e);
            //有异常就回滚
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

    }

    private String genDimesionResult(int dateId, int contractId){
        return dateId+DIMENSION_RESULT_SPLITTER+contractId;
    }

    private String genTableCallIdKey(int dateId, int contactId){
        if(dateId<=0||contactId<=0){
            logger.error("genTableCallIdKey error dateId:{},contactId:{}",dateId,contactId);
            throw new RuntimeException("genTableCallIdKey error!!!");
        }
        return dateId+"_"+contactId;
    }
}
