package com.jj.consumer.hbase;

import com.google.common.collect.Lists;
import com.jj.consumer.util.HBaseUtil;
import com.jj.consumer.util.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class HBaseDAO {
    private int regions;
    private String tableName;
    private List<Put> puts = Lists.newArrayList();
    //缓冲区大小
    private long writeBufferSize = 2L*1024*1024;
    //缓存event的大小 最好是缓冲区大小*0.7/每条event的大小
    private int cacheSize = 300;
    private Connection connection;
    private HTable table;

    {
        String nameSpace = PropertiesUtil.getProperty("telecom.namespace");
        tableName = PropertiesUtil.getProperty("telecom.table");
        regions = Integer.parseInt(PropertiesUtil.getProperty("telecom.table.regions"));
        try {
            HBaseUtil.createTable(regions, nameSpace, tableName,"hdfs://hadoop112:9000/user/aaa/hbase/telecom/telecom-consumer-1.0-SNAPSHOT.jar", CalleeWriteBackCoprocessor.class,"f1", "f0");
//            HBaseUtil.createTable(regions, nameSpace, tableName,"H:\\bigdata-dev\\ideaworkspace\\telecom\\telecom-consumer\\target\\telecom-consumer-1.0-SNAPSHOT.jar", CalleeWriteBackCoprocessor.class,"f1", "f0");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 组装原始数据信息到Hbase
     * 15978226424,17005930322,2018-08-03 08:12:22,0086
     * 13980337439,19902496992,2018-04-21 14:48:40,1730
     *
     * day2优化点：
     * 1.put设置缓冲区减少hbase连接次数
     * 2.linux优化每个用户ulimit操作文件和进程数 注意ulimit的坑，必须同时修改两个文件并且重新登录生效
     * 3.hbase-site设置zk超时时间
     * @param oriValues
     */
    public void putCallMsg(List<String> oriValues) {
        //**这个坑 有可能没拉到数据，那就什么都不做
        if (oriValues==null||oriValues.size()==0)return;
//        //批量插入
//        List<Put> puts = Lists.newArrayList();
        try {
//            Connection connection = HBaseUtil.getConnection();
//            Table tableName = connection.getTable(TableName.valueOf(this.tableName));
            //优化批量提交，减少与hbase的连接
            //当连接关闭或表关闭才构建
            if(connection==null||connection.isClosed())
            connection = HBaseUtil.getConnection();
            if(table==null)
            table = (HTable) connection.getTable(TableName.valueOf(this.tableName));
            //关闭表自动flush并设定缓冲区大小2M 该缓冲区在jvm heap中还没到hbase的memstore
            table.setAutoFlushTo(false);
            table.setWriteBufferSize(writeBufferSize);
            for (String oriValue : oriValues) {
                String[] split = oriValue.split(",");
                //清洗不符合的数据
                if(split.length!=4)continue;
                String call1 = split[0];
                String call2 = split[1];
                String buildTime = split[2];
                buildTime = buildTime.
                        replaceAll("-", "")
                        .replaceAll(":", "")
                        .replaceAll(" ","");
                String duration = split[3];
                CallMsg callMsg = CallMsg.builder()
                        .call1(call1)
                        .call2(call2)
                        .buildTime(buildTime)
                        .duration(duration).build();
                String rowKey = HBaseUtil.genRowKey(callMsg,"1", regions);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call1"), Bytes.toBytes(call1));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call2"), Bytes.toBytes(call2));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
                long timestamp = LocalDateTime.now().toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("timestamp"), Bytes.toBytes("" + timestamp));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
                puts.add(put);
            }
            table.put(puts);
            if(puts.size()>=cacheSize){
                table.flushCommits();
                puts.clear();
            }
//            table.close();
//            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(puts.size()==0){
                try {
                    if(table!=null)
                    table.close();
                    if(connection!=null)
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
